package com.genesys.tartarus.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.genesys.tartarus.config.JsonObjectMapper;
import com.genesys.tartarus.constants.TartarusConstants;
import com.genesys.tartarus.models.*;
import com.genesys.tartarus.models.Wrapup;
import com.genesys.tartarus.storage.FeatureToggleCache;
import com.genesys.tartarus.storage.ProcessedConversationsCache;
import com.genesys.tartarus.util.Purpose;
import com.genesys.tartarus.util.SanityCheckHelper;
import com.inin.avro.events.recording.RecordingChange;
import com.inin.events.Constants;
import com.genesys.tartarus.config.TartarusProperties;
import com.genesys.tartarus.storage.ConversationStorage;
import com.genesys.tartarus.util.MediaType;
import com.inin.events.conversation.*;
import com.inin.integration.core.repository.IschemaProvider;
import com.inin.qm.util.config.LoggerFactory;
import com.inin.qm.util.logging.Marker;
import com.inin.qm.util.model.HoldTime;
import com.inin.qm.util.model.MinimizedAnalyticsConversation;
import com.inin.qm.util.model.MinimizedAnalyticsParticipant;
import com.inin.qm.util.model.MinimizedAnalyticsSession;
import com.inin.qm.util.model.analytics.AnalyticsDisconnectType;
import com.inin.qm.util.model.analytics.AnalyticsMediaType;
import com.inin.qm.util.model.analytics.Direction;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Service
public class ConversationProcessingService {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConversationProcessingService.class);

    public static final String DISCONNECTED_SESSION_STATE = "disconnected";
    public static final String TERMINATED_SESSION_STATE = "terminated";
    public static final String conversationInitial = "c";
    public static final String sessionInitial = "s";
    public static final String peerIdExemptSessionInitial = "x";
    public static final String holdInitial = "h";
    public static final String recordingInitial = "r";
    public static final String recordingAvailableInitial = "a";
    public static final String recordingDeletedInitial = "d";
    public static final String wrapupSessionPrefix = "w";
    public static final String pureConnect = "PureConnect";
    public static final String pureEngage = "PureEngage";
    private static final Pattern SIP_PHONE_NUMBER_PATTERN = Pattern.compile("sip:\\+(\\d+)@.*");
    public static final String SMS = "sms";
    public static final String WEB_MESSAGING = "webmessaging";
    private static final String DIALER_CAMPAIGN_ID = "dialerCampaignId";
    private static final String CAMPAIGN_ID = "campaignId";
    private static final String DIRECTION_OUTBOUND = "outbound";
    private static final String DIRECTION_INBOUND = "inbound";
    private static final long ALLOWED_GAP_BETWEEN_SESSIONS_MS = 500;
    private static final long PEER_ID_EXEMPT_SESSION_TTL_SEC = TartarusConstants.ONE_DAY_SECONDS;

    private static final MediaType allowedMediaTypesToCreateHold[] = { MediaType.voice, MediaType.callback, MediaType.chat, MediaType.email, MediaType.message };

    /**
     * SMS: Prefer normalized over raw address.
     */
    private static final List<Function<Address, String>> SMS_ADDRESS_EXTRACTORS =
            Arrays.asList(Address::getAddressNormalized, Address::getAddressRaw);
    /**
     * Web messaging: Prefer displayable over raw and normalized address.
     */
    private static final List<Function<Address, String>> WEB_MESSAGING_ADDRESS_EXTRACTORS =
            Arrays.asList(Address::getAddressDisplayable, Address::getAddressRaw, Address::getAddressNormalized);
    /**
     * Default: Prefer raw over normalized address.
     */
    private static final List<Function<Address, String>> DEFAULT_ADDRESS_EXTRACTORS =
            Arrays.asList(Address::getAddressRaw, Address::getAddressNormalized);

    private static final Comparator<Session> SESSION_DESCENDING =
            Comparator.comparing(Session::getProviderEventTime).reversed();

    @Autowired
    private TartarusProperties tartarusProperties;

    @Autowired
    private IschemaProvider schemaProvider;

    @Autowired
    private ConversationStorage conversationStorage;

    @Autowired
    private ProcessedConversationsCache processedConversationsCache;

    @Autowired
    private FeatureToggleCache featureToggleCache;

    @Autowired
    private SanityCheckHelper sanityCheckHelper;

    @PostConstruct
    public void initialize() {

    }

    public String generateConversationRangeKey(String conversationTime) {
        return conversationInitial + TartarusConstants.COMPOSITE_KEY_DELIMITER + conversationTime;
    }

    public String generateSessionRangeKey(String conversationStart, String sessionTime, String sessionId) {
        return sessionInitial + TartarusConstants.COMPOSITE_KEY_DELIMITER + conversationStart + TartarusConstants.COMPOSITE_KEY_DELIMITER + sessionTime + TartarusConstants.COMPOSITE_KEY_DELIMITER + sessionId;
    }

    public String generatePeerIdExemptSessionRangeKey(String sessionId) {
        return peerIdExemptSessionInitial + TartarusConstants.COMPOSITE_KEY_DELIMITER + sessionId;
    }

    public String generateHoldRangeKey(String conversationStart, String holdStart, String sessionId) {
        return holdInitial + TartarusConstants.COMPOSITE_KEY_DELIMITER + conversationStart + TartarusConstants.COMPOSITE_KEY_DELIMITER + holdStart +  TartarusConstants.COMPOSITE_KEY_DELIMITER + sessionId;
    }

    public String generateSessionWrapupRangeKey(String conversationStart, String connectedTime, String sessionId) {
        return wrapupSessionPrefix + TartarusConstants.COMPOSITE_KEY_DELIMITER + conversationStart + TartarusConstants.COMPOSITE_KEY_DELIMITER + connectedTime +  TartarusConstants.COMPOSITE_KEY_DELIMITER + sessionId;
    }

    public String generateRecordingAvailableRangeKey(String recordingId) {
        return recordingInitial + TartarusConstants.COMPOSITE_KEY_DELIMITER + recordingId + TartarusConstants.COMPOSITE_KEY_DELIMITER + recordingAvailableInitial;
    }

    public String generateRecordingDeletedRangeKey(String recordingId) {
        return recordingInitial + TartarusConstants.COMPOSITE_KEY_DELIMITER + recordingId + TartarusConstants.COMPOSITE_KEY_DELIMITER + recordingDeletedInitial;
    }

    public void processConversationEvent(ConversationEvent conversationEvent) {
        if(shouldSkipConversationEventProcessing(conversationEvent)){
            return;
        }
        if(!isHybridConversation(conversationEvent)) {
            createConversationEntities(conversationEvent);
            if(canMoveConversationToS3(conversationEvent)) {
                MinimizedAnalyticsConversation s3Conversation = moveConversationToS3(conversationEvent.getOrganizationId(), conversationEvent.getConversationId(), conversationEvent.getParticipants());
                // Adding the timeout entry even if s3Conversation is null meaning the conversation failed to move to S3
                // That means session cleanup on timeout will happen regardless and should be safe.
                addTimeoutEntry(conversationEvent.getConversationId(), conversationEvent.getEndTime());
                if( s3Conversation != null )
                {
                    List<String> sanityCheckWarnings = sanityCheckHelper.performSanityCheck(s3Conversation, conversationEvent);
                    if (sanityCheckWarnings.size() > 0) {
                        sanityCheckHelper.warnSanityCheckFailure(s3Conversation.getConversationId(), sanityCheckWarnings);
                    }
                }
            }
        }
        processedConversationsCache.markConversationProcessed(conversationEvent.getConversationId());
    }

    private boolean shouldSkipConversationEventProcessing(ConversationEvent conversationEvent) {
        int totalSessions = Optional.ofNullable(conversationEvent.getParticipants())
                .stream()
                .flatMap(Collection::stream)
                .filter(Objects::nonNull)
                .map(participant -> participant.getCommunications())
                .filter(Objects::nonNull)
                .mapToInt(List::size)
                .sum();
        int maxCommunicationCount = tartarusProperties.getMaxCommunicationCountForConversation();
        boolean isConversationToxic = totalSessions > maxCommunicationCount;
        boolean isBlacklisted = tartarusProperties.getBlackListedConversationList().contains(conversationEvent.getConversationId());
        boolean isStartTimeMissing = conversationEvent.getStartTime() == null;
        if (isConversationToxic || isBlacklisted || isStartTimeMissing) {
            LOGGER.info("Skipping processing conversation event. isToxic : {}, totalConversationSessions: {}, maxCommunicationCount: {}, isBlacklisted: {}, isStartTimeMissing : {}",  isConversationToxic, totalSessions, maxCommunicationCount, isBlacklisted, isStartTimeMissing);
            return true;
        } else {
            return false;
        }
    }


    private void addTimeoutEntry(String conversationId, Long time) {
        if(time != null) {
            Timeout timeout = createTimeoutEntry(conversationId, time, tartarusProperties.getConversationTtl());
            conversationStorage.addTimeout(timeout);
            LOGGER.debug("Created timeout for {}", timeout.getTtl());
        }
    }

    public boolean canMoveConversationToS3(ConversationEvent conversationEvent) {
        if (conversationEvent != null &&
                conversationEvent.getEndTime() != null &&
                conversationEvent.getParticipants() != null &&
                !conversationEvent.getParticipants().isEmpty()) {
            
            //Wait until all participants are disconnected
            for (com.inin.events.conversation.Participant p : conversationEvent.getParticipants()) {

                //If any participant isn't disconnected or scheduled for a callback, not ready for processing
                if (!((StringUtils.equalsIgnoreCase(p.getState(), TartarusConstants.PARTICIPANT_STATE_DISCONNECTED)) ||
                        (StringUtils.equalsIgnoreCase(p.getState(), TartarusConstants.PARTICIPANT_STATE_SCHEDULED)))) {
                    return false;
                }

                //If any participant is pending wrapup, not ready for processing
                if (Boolean.TRUE.equals(p.getWrapupRequired()) && p.getWrapup() == null
                        && !StringUtils.equalsIgnoreCase(p.getPurpose(), Constants.PARTICIPANT_PURPOSE_DIALER_SYSTEM)) {
                    return false;
                }
            }
            return true;

        } else {
            LOGGER.debug("Ignoring event {}", conversationEvent);
            return false;
        }
    }

    private boolean isHybridConversation(ConversationEvent conversationEvent) {
        String provider = "";

        if(conversationEvent.getParticipants().size() > 0 && conversationEvent.getParticipants().get(0).getCommunications().size() > 0) {
            if (conversationEvent.getParticipants().get(0).getCommunications().get(0) instanceof Call) {
                provider = ((Call) conversationEvent.getParticipants().get(0).getCommunications().get(0)).getProvider();
            }
            else if (conversationEvent.getParticipants().get(0).getCommunications().get(0) instanceof Callback) {
                provider = ((Callback) conversationEvent.getParticipants().get(0).getCommunications().get(0)).getProvider();
            }
            else if (conversationEvent.getParticipants().get(0).getCommunications().get(0) instanceof Chat) {
                provider = ((Chat) conversationEvent.getParticipants().get(0).getCommunications().get(0)).getProvider();
            }
            else if (conversationEvent.getParticipants().get(0).getCommunications().get(0) instanceof Email) {
                provider = ((Email) conversationEvent.getParticipants().get(0).getCommunications().get(0)).getProvider();
            }
            else if (conversationEvent.getParticipants().get(0).getCommunications().get(0) instanceof Message) {
                provider = ((Message) conversationEvent.getParticipants().get(0).getCommunications().get(0)).getProvider();
            }
            else if (conversationEvent.getParticipants().get(0).getCommunications().get(0) instanceof Cobrowse) {
                provider = ((Cobrowse) conversationEvent.getParticipants().get(0).getCommunications().get(0)).getProvider();
            }
            else if (conversationEvent.getParticipants().get(0).getCommunications().get(0) instanceof ScreenShare) {
                provider = ((ScreenShare) conversationEvent.getParticipants().get(0).getCommunications().get(0)).getProvider();
            }
            else if (conversationEvent.getParticipants().get(0).getCommunications().get(0) instanceof VideoComm) {
                provider = ((VideoComm) conversationEvent.getParticipants().get(0).getCommunications().get(0)).getProvider();
            }
        }
        return (provider.equals(pureConnect) || provider.equals(pureEngage));
    }

    private Conversation generateTopLevelConversationInformation(ConversationEvent conversationEvent) {
        LOGGER.debug("Creating top level conversation information");
        Conversation conversation = new Conversation();
        if (conversationEvent.getStartTime() == null) {
            LOGGER.error("generateTopLevelConversationInformation : conversationEvent with id {} has a null start time",
                    conversationEvent.getConversationId());
        }
        String conversationRangeKey = generateConversationRangeKey(conversationEvent.getStartTime().toString());
        conversation.setConversationId(conversationEvent.getConversationId());
        conversation.setSortKey(conversationRangeKey);
        conversation.setConversationStart(conversationEvent.getStartTime());
        conversation.setExternalSource(conversationEvent.getExternalSourceId());
        conversation.setExternalInteractionId(conversationEvent.getExternalInteractionId());
        conversation.setExternalTag(conversationEvent.getExternalId());
        conversation.setOrganizationId(
                conversationEvent.getOrganizationId(),
                featureToggleCache.isFeatureEnabled(
                        TartarusConstants.WRITE_ORGANIZATION_ID_SHARDED_FEATURE_TOGGLE_NAME,
                        conversationEvent.getOrganizationId()),
                tartarusProperties.maxOrgIdShardIndexToWrite()
        );

        return conversation;
    }

    private Hold createHold(ConversationsTableEntities conversationsTableEntities, MediaType mediaType, Hold hold, ConversationEvent conversationEvent, String communicationId, Long communicationProviderEventTime, Long communicationStartHoldTimeMilliseconds, 
                            Long communicationEndHoldTimeMilliseconds, Long communicationHoldDurationMilliseconds, String communicationState, Long communicationDisconnectedTimeMilliseconds) {
        // If there is already a hold segment for this session with same startTime, then skip creating a new hold
        if (conversationsTableEntities != null && conversationsTableEntities.getHolds() != null && conversationsTableEntities.getHolds().size() > 0) {
            Optional<Hold> optionalFoundHold = conversationsTableEntities.getHolds().stream().filter(h -> h.getSessionId().equals(communicationId) && h.getHoldStart() != null && h.getHoldStart().equals(communicationStartHoldTimeMilliseconds) && h.getHoldEnd() != null).findFirst();
            if (optionalFoundHold != null && optionalFoundHold.isPresent()) {
                return hold;
            }
        }

        // Check for any pending hold info in dynamo i.e. with startHoldTime but no endHoldTime
        // If one is found, then assume hold is complete and use the providerEventTime as the endHoldTime
        List<Hold> sessionHolds = Optional.ofNullable(conversationsTableEntities.getHolds()).orElse(Collections.emptyList())
                                    .stream().filter(h -> communicationId.equals(h.getSessionId()) && h.getHoldEnd() == null).collect(Collectors.toList());
        Hold foundPendingHold = sessionHolds != null && sessionHolds.size() > 0 ? sessionHolds.get(sessionHolds.size() - 1) : null;
        if (foundPendingHold != null && foundPendingHold.getHoldEnd() == null && foundPendingHold.getHoldStart() != null) {
            // Remove the pendingHold from dynamo
            List<String> sortKeysDeleteEntities = new ArrayList<>();
            sortKeysDeleteEntities.add(foundPendingHold.getSortKey());
            conversationStorage.deleteConversationEntitiesFromDynamo(conversationEvent.getConversationId(), sortKeysDeleteEntities);

            // Also remove pendingHold from our copy of conversationTableEntities
            if (conversationsTableEntities.getHolds() != null && conversationsTableEntities.getHolds().size() > 0) {
                conversationsTableEntities.getHolds().remove(foundPendingHold);
            }

            // Generate a complete hold info
            hold = generateHoldInformation(hold,
                    foundPendingHold.getConversationId(),
                    conversationEvent.getOrganizationId(),
                    foundPendingHold.getSessionId(),
                    foundPendingHold.getHoldStart(),
                    mediaType,
                    communicationProviderEventTime,
                    communicationProviderEventTime - foundPendingHold.getHoldStart());
        }

        // If hold info is complete i.e. endHoldTime is available, then just generate normal hold info
        // If hold info only contains startHoldTime, then continue to save in dynamo. Next event will assume
        //  hold is complete.
        if (foundPendingHold == null && communicationEndHoldTimeMilliseconds != null) {
            // If associated communication is terminated, then use disconnectedTime as the endHoldTime and adjust
            //  hold duration accordingly
            Long endHoldTimeMS = communicationState != null && isCommunicationDisconnected(communicationState) 
                        && communicationDisconnectedTimeMilliseconds != null ? 
                        communicationDisconnectedTimeMilliseconds : communicationEndHoldTimeMilliseconds;
            Long holdDurationMS = communicationState != null && isCommunicationDisconnected(communicationState)
                        && communicationDisconnectedTimeMilliseconds != null ? 
                        communicationHoldDurationMilliseconds + (communicationDisconnectedTimeMilliseconds - communicationEndHoldTimeMilliseconds) : communicationHoldDurationMilliseconds;
            hold = generateHoldInformation(hold,
                    conversationEvent.getConversationId(),
                    conversationEvent.getOrganizationId(),
                    communicationId,
                    conversationEvent.getStartTime(),
                    mediaType,
                    endHoldTimeMS,
                    holdDurationMS);
        } else if (communicationStartHoldTimeMilliseconds != null ){
            hold = generatePendingHoldInformation(hold,
                    conversationEvent.getConversationId(),
                    conversationEvent.getOrganizationId(),
                    communicationId,
                    conversationEvent.getStartTime(),
                    communicationStartHoldTimeMilliseconds);                    
        }
        return hold;
    }

    private boolean processCommunication(GenericCommunication genericCommunication, ConversationsTableEntities conversationsTableEntities, ConversationEvent conversationEvent,
                                      Participant participant, Session sessionInDB, Session session, Wrapup wrapupSession, Hold hold) {
        genericCommunication.setFirstEvent(conversationEvent.getStartTime().equals(genericCommunication.getProviderEventTime()));
        if (isCommunicationDisconnected(genericCommunication.getState()) || (sessionInDB == null && genericCommunication.getConnectedTimeMilliseconds() != null)) {
            // Analytics uses the very first providerEventTime as the connectedTime. However it only does this if event's communication.connectedTime is not null
            Long connectedTime = genericCommunication.getConnectedTimeMilliseconds() != null ? genericCommunication.getProviderEventTime() : null;
            Long disconnectedTime = genericCommunication.getDisconnectedTimeMilliseconds();
            String disconnectType  = genericCommunication.getDisconnectType() != null ? genericCommunication.getDisconnectType() : null;

            // Restore originally saved connectedTime and disconnectType using sessionInDB values
            if (sessionInDB != null) {
                if (sessionInDB.getConnectedTime() != null) {
                    connectedTime = sessionInDB.getConnectedTime();
                }
                if (sessionInDB.getDisconnectType() != null) {
                    disconnectType = sessionInDB.getDisconnectType();
                }
            }

            // If there is no connectedTime in the communications, then don't supply a connectedTime value for the session as well
            //  unless this is an outbound email where the disconnectedTimeMilliseconds is used as the the connectedTime
            if (sessionInDB == null && genericCommunication.getConnectedTimeMilliseconds() == null) {
                if (genericCommunication.getDisconnectedTimeMilliseconds() != null && genericCommunication.getMediaType().equals(MediaType.email) && 
                    genericCommunication.getDirection().equals(DIRECTION_OUTBOUND)) {
                        connectedTime = genericCommunication.getDisconnectedTimeMilliseconds();
                } else {
                    connectedTime = null;
                }
            }

            // Also for email outbound, the most recent providerEventTime is used as the disconnectedTime and the conversation endTime
            if (genericCommunication.getMediaType().equals(MediaType.email) && genericCommunication.getDirection().equals(DIRECTION_OUTBOUND)) {
                disconnectedTime = genericCommunication.getProviderEventTime();
                conversationEvent.setEndTime(disconnectedTime);
            }

            // Retain the session's original sortKey so session chronological order is maintained
            if (sessionInDB != null && sessionInDB.getSortKey() != null && sessionInDB.getConnectedTime() == null) {
                session.setSortKey(sessionInDB.getSortKey());
            }

            session = generateSessionInformation(session,
                    conversationEvent.getStartTime().toString(),
                    genericCommunication.getId(),
                    genericCommunication.getMediaType(),
                    connectedTime,
                    disconnectedTime,
                    disconnectType,
                    genericCommunication.getFlowExecution() != null ? genericCommunication.getFlowExecution().getFlowId() : null,
                    genericCommunication.getDirection(),
                    genericCommunication.getPeerId(),
                    participant.getStartTime()
            );

            if (genericCommunication.getMediaType() == MediaType.voice) {
                String aniAddress = null;
                String dnisAddress = null;
                if (genericCommunication.getAni() != null && genericCommunication.getDnis() != null) {
                    LOGGER.debug("Using new ANI/DNIS addresses");
                    // Extract ANI/DNIS fields
                    aniAddress = getAddressString(genericCommunication.getAni());
                    dnisAddress = getAddressString(genericCommunication.getDnis());
                }
                final Pair<String, String> selfOtherAniDnis = extractAniDnisFromSelfAndOther(genericCommunication, participant, conversationEvent);
                if (aniAddress == null) {
                    LOGGER.debug("Using old ANI address logic");
                    aniAddress = selfOtherAniDnis.getLeft();
                }
                if (dnisAddress == null) {
                    LOGGER.debug("Using old DNIS address logic");
                    dnisAddress = selfOtherAniDnis.getRight();
                }
                session.setAni(aniAddress);
                session.setDnis(dnisAddress);
                session.setSessionDnis(selfOtherAniDnis.getRight());
            }

            if (genericCommunication.getMediaType() == MediaType.email) {
                String self = getAddressString(genericCommunication.getSelf());
                String other = getAddressString(genericCommunication.getOther());
                Purpose purpose = participant.getPurpose() != null ? Purpose.fromString(participant.getPurpose()) : Purpose.UNKNOWN;
                if (purpose == Purpose.EXTERNAL) {
                    if (Constants.DIRECTION_OUTBOUND.equals(genericCommunication.getDirection())) {
                        session.setAddressFrom(other);
                        session.setAddressTo(self);
                    } else {
                        session.setAddressFrom(self);
                        session.setAddressTo(other);
                    }
                } else if (Constants.DIRECTION_OUTBOUND.equals(genericCommunication.getDirection())) {
                    session.setAddressFrom(self);
                    session.setAddressTo(other);
                } else {
                    session.setAddressFrom(other);
                    session.setAddressTo(self);
                }                    
            }

            if (genericCommunication.getMediaType() == MediaType.message) {
                final List<Function<Address, String>> extractors = getAddressExtractors(genericCommunication.getMessageType());
                session.setAddressFrom(getAddress(genericCommunication.getFromAddress(), extractors));
                session.setAddressTo(getAddress(genericCommunication.getToAddress(), extractors));

            }

            if (genericCommunication.getWrapup() != null) {
                generateWrapupInformation(wrapupSession,
                        conversationEvent,
                        genericCommunication.getId(),
                        genericCommunication.getMediaType(),
                        genericCommunication.getConnectedTimeMilliseconds(),
                        participant.getParticipantId(),
                        genericCommunication.getWrapup().getCode());
            }

            if(genericCommunication.getMediaType() == MediaType.callback && genericCommunication.getDialerPreview() != null)
                session.setOutboundCampaignId(genericCommunication.getDialerPreview().getCampaignId());                
        }

        if (Arrays.stream(allowedMediaTypesToCreateHold).anyMatch(m -> m.equals(genericCommunication.getMediaType()))) {
            createHold(conversationsTableEntities, 
                genericCommunication.getMediaType(), 
                hold, 
                conversationEvent, 
                genericCommunication.getId(),
                genericCommunication.getProviderEventTime(), 
                genericCommunication.getStartHoldTimeMilliseconds(), 
                genericCommunication.getEndHoldTimeMilliseconds(), 
                genericCommunication.getHoldDurationMilliseconds(),
                genericCommunication.getState(),
                genericCommunication.getDisconnectedTimeMilliseconds());
        }
        return genericCommunication.getFirstEvent();
    }

    private boolean isParticipantExternal(Participant participant) {
        return participant.getPurpose() != null && (participant.getPurpose().equals("customer") || participant.getPurpose().equals("external"));
    }

    private GenericCommunication isOverlapping(GenericCommunication internalCommunication, List<Participant> externalConversationEventParticipants) {
        GenericCommunication foundGenericCommunication = null;
        if (internalCommunication != null && internalCommunication.getProviderEventTime() != null && externalConversationEventParticipants != null) {
            for (Participant externalParticipant: externalConversationEventParticipants) {
                if (externalParticipant.getCommunications() != null) {
                    for (Object externalCommunication : externalParticipant.getCommunications()) {
                        try {
                            GenericCommunication externalGenericCommunication = new GenericCommunication(externalCommunication);
                            if (externalGenericCommunication.getConnectedTimeMilliseconds() != null && externalGenericCommunication.getDisconnectedTimeMilliseconds() != null &&
                                externalGenericCommunication.getConnectedTimeMilliseconds() <= internalCommunication.getProviderEventTime() &&  
                                internalCommunication.getProviderEventTime() <= externalGenericCommunication.getDisconnectedTimeMilliseconds()) {
                                    foundGenericCommunication = externalGenericCommunication;
                                    break;
                            }
                        } catch (Exception ex) {
                            LOGGER.warn(ex.getMessage());
                        }
                    }
                }
                // As soon one is found, then break out of the loop
                if (foundGenericCommunication != null) {
                    break;
                }    
            }
        }
        return foundGenericCommunication;
    }

    private boolean isPeerIdExempt(GenericCommunication genericCommunication, List<Participant> participants, List<Participant> externalConversationEventParticipants) {
        // Exempt if internal participant alerting that does not have a connectedTime and all external participants have already been disconnected (i.e. there must be at least one external participant)
        if (genericCommunication.getConnectedTimeMilliseconds() == null
                && genericCommunication.getState() != null && genericCommunication.getState().equals("alerting")
                && externalConversationEventParticipants.size() > 0) {
            // Look for at least one non-disconnected external participant that could possibly be a peer for this internal participant
            Optional<Participant> optionalFoundNotDisconnectedExternalParticipant = 
                externalConversationEventParticipants.stream().filter(p -> !p.getState().equals("disconnected") 
                                                                            && !p.getState().equals("terminated") 
                                                                            && !p.getState().equals("scheduled")).findFirst();
            if (!optionalFoundNotDisconnectedExternalParticipant.isPresent()) {
                // Only return true if this session's alerting time is not within connected/disconnected time of any of the external sessions
                GenericCommunication overlappingGenericCommunication = isOverlapping(genericCommunication, externalConversationEventParticipants);
                if (overlappingGenericCommunication == null) {
                    return true;
                }
            }
        }

        // Exempt if there are only 2 participants and both are internal (i.e. no external participants at all in the conversation)
        if (participants.size() == 2 && externalConversationEventParticipants.size() == 0) {
            return true;
        }
        
        return false;
    }

    private void createConversationEntities(ConversationEvent conversationEvent) {
        List<Session> sessions = new ArrayList<>();
        List<Wrapup> wrapups = new ArrayList<>();
        List<ParticipantChange> participantChanges = conversationEvent.getParticipantChanges();
        List<com.inin.events.conversation.Participant> participants = conversationEvent.getParticipants();
        boolean firstEvent = false;

        if (participantChanges.size() > 0) {

            // Initialize the externalConversationEventParticipants
            List<Participant> externalConversationEventParticipants = new ArrayList<>();
            for (Participant participant : conversationEvent.getParticipants()) {
                if (isParticipantExternal(participant)) {
                    externalConversationEventParticipants.add(participant);
                }
            }                                                

            for (ParticipantChange participantChange : participantChanges) {
                // Find the affected participant 
                Participant participantFromEvent = null;
                Optional<Participant> optionalEventParticipant = participants.stream().filter(p -> p.getParticipantId().equals(participantChange.getParticipantId())).findFirst();
                if (optionalEventParticipant.isPresent()) {
                    participantFromEvent = optionalEventParticipant.get();
                }

                List<String> changedCommunications = participantChange.getChangedCommunications();

                boolean wrapupAlreadyProcessed = false;
                if (changedCommunications.size() == 0 && participantFromEvent != null) {
                    // When participantChanges.changedCommunications is empty, changed participants are checked if they have wrapUp
                    wrapupAlreadyProcessed = true;
                    if (participantFromEvent.getWrapup() != null && participantFromEvent.getWrapupSource().equals("participant")) {
                        List<Session> sessionList = new ArrayList<>();
                        for(int i =0; i < participantFromEvent.getCommunications().size(); i++)
                        {
                            Session session = new Session();
                            try {
                                GenericCommunication genericCommunication = new GenericCommunication(participantFromEvent.getCommunications().get(i));
                                session = generateSessionInformationForWrapup(session, genericCommunication.getId(), genericCommunication.getConnectedTimeMilliseconds(), 
                                                                                genericCommunication.getDisconnectedTimeMilliseconds(), 
                                                                                genericCommunication.getProviderEventTime(), genericCommunication.getMediaType());
                            } catch (Exception ex) {
                                LOGGER.info(ex.getMessage());
                            }

                            if (session.getSessionId() != null)
                                sessionList.add(session);
                        }

                        final Map<MediaType, LinkedList<Session>> sessionsByMediaType = new HashMap<>();
                        sessionList.forEach(s -> sessionsByMediaType.computeIfAbsent(s.getMediaType(), x -> new LinkedList<>()).add(s));
                        for (LinkedList<Session> mediaSessions : sessionsByMediaType.values()) {
                            mediaSessions.sort(SESSION_DESCENDING);
                            // Propagate wrapup to last session for each media type
                            Session last = mediaSessions.poll();

                            if(last.getDisconnectedTime() != null) {
                                Wrapup wrapupSession = new Wrapup();
                                wrapupSession = generateWrapupInformation(wrapupSession,
                                        conversationEvent,
                                        last.getSessionId(),
                                        last.getMediaType(),
                                        last.getConnectedTime(),
                                        participantFromEvent.getParticipantId(),
                                        participantFromEvent.getWrapup().getCode());

                                if (wrapupSession.getConversationId() != null) {
                                    wrapups.add(wrapupSession);
                                }
                            }
                        }
                    }

                    //  Also if there are no changedCommunications, then there could be a change in participant attribute
                    //  e.g. externalContactId.  So force an update by adding the existing participant session ids
                    //  to the changedCommunications which should result to update for changed participant session in S3
                    if (participantFromEvent != null && participantFromEvent.getCommunications() != null) {
                        for (Object communication : participantFromEvent.getCommunications()) {
                            try {
                                GenericCommunication genericCommunication = new GenericCommunication(communication);
                                // Only add if it does not yet exists
                                if (changedCommunications.stream().filter(c -> c.equals(genericCommunication.getId())).count() == 0) {
                                    changedCommunications.add(genericCommunication.getId());
                                }
                            } catch (Exception ex) {
                                LOGGER.info(ex.getMessage());        
                            }
                        }
                    }
                }
                
                // Process changeCommunications
                if (changedCommunications.size() > 0) {
                    // Get conversation/sessions from dynamo
                    ConversationsTableEntities conversationsTableEntities = conversationStorage.getConversationEntities(conversationEvent.getConversationId());
                    for (String changedCommunication : changedCommunications) {                        
                        // Find matching session in dynamoDB
                        Session sessionInDB = null;
                        if (conversationsTableEntities != null && conversationsTableEntities.getSessions() != null && conversationsTableEntities.getSessions().size() > 0) {
                            Optional<Session> foundSessionInDB = conversationsTableEntities.getSessions().stream().filter(s -> s.getSessionId().equals(changedCommunication)).findFirst();
                            if (foundSessionInDB.isPresent()) {
                                sessionInDB = foundSessionInDB.get();
                            }
                        }

                        // Find existing communication in eventParticipant
                        Object communicationInEventParticipant = null;
                        if (participantFromEvent != null) {
                            Optional<Object> foundCommunicationInEventParticipant = participantFromEvent.getCommunications().stream().filter(c -> {
                                try {
                                    return new GenericCommunication(c).getId().equals(changedCommunication);
                                } catch(Exception ex) {
                                    return false;
                                }
                            }).findFirst();

                            if (foundCommunicationInEventParticipant.isPresent()) {
                                communicationInEventParticipant = foundCommunicationInEventParticipant.get();

                                Session session = new Session();
                                Hold hold = new Hold();
                                Wrapup wrapupSession = new Wrapup();
                                    
                                try {
                                    GenericCommunication genericCommunication = new GenericCommunication(communicationInEventParticipant);

                                    // For new internal voice participants without peerId from event, check if this session should never be
                                    // assigned a peerId (in moveConversationToS3) based on the result of isPeerIdExempt, and if so flag
                                    // it by creating PeerIdExemptSession entry in dynamoDB 
                                    if (sessionInDB == null && !isParticipantExternal(participantFromEvent) && genericCommunication.getPeerId() == null
                                                && genericCommunication.getMediaType() != null && genericCommunication.getMediaType() == MediaType.voice) {
                                        if (isPeerIdExempt(genericCommunication, participants, externalConversationEventParticipants)) {
                                            PeerIdExemptSession peerIdExemptSession = new PeerIdExemptSession();
                                            String sortKey = generatePeerIdExemptSessionRangeKey(genericCommunication.getId());
                                            peerIdExemptSession.setSortKey(sortKey);
                                            peerIdExemptSession.setConversationId(conversationEvent.getConversationId());
                                            peerIdExemptSession.setSessionId(genericCommunication.getId());
                                            peerIdExemptSession.setTtl((System.currentTimeMillis() / 1000) + PEER_ID_EXEMPT_SESSION_TTL_SEC);
                                            conversationStorage.addPeerIdExemptSession(peerIdExemptSession);
                                        }
                                    }
                                   
                                    firstEvent = processCommunication(genericCommunication,
                                                                        conversationsTableEntities, conversationEvent, participantFromEvent,
                                                                        sessionInDB, session, wrapupSession, hold);
                                } catch (Exception ex) {
                                    LOGGER.info(ex.getMessage());        
                                }

                                if (session.getSessionId() != null) {
                                    session.setConversationId(conversationEvent.getConversationId());
                                    session.setQueueId(participantFromEvent.getQueueId());
                                    session.setExternalContactId(participantFromEvent.getExternalContactId());
                                    session.setParticipantId(participantFromEvent.getParticipantId());
                                    // Only allow outbounds to change purpose on subsequent events
                                    if (sessionInDB != null && sessionInDB.getPurpose() != null && session.getDirection() != null && session.getDirection().equals(DIRECTION_OUTBOUND) && session.getMediaType() != null 
                                            && session.getMediaType().equals(MediaType.voice)) {
                                        session.setPurpose(sessionInDB.getPurpose());
                                    } else {
                                        session.setPurpose(participantFromEvent.getPurpose());
                                    }
                                    session.setTeamId(participantFromEvent.getTeamId());
                                    session.setUserId(participantFromEvent.getUserId());
                                                                        
                                    // Retain any previously saved value for the monitored/coached/bargedParticipant Id
                                    // i.e don't allow it to be overwritten by null values from subsequent events
                                    if (sessionInDB != null && sessionInDB.getMonitoredParticipantId() != null) {
                                        session.setMonitoredParticipantId(sessionInDB.getMonitoredParticipantId());
                                    } else {
                                        session.setMonitoredParticipantId(participantFromEvent.getMonitoredParticipantId());
                                    }
                                    if (sessionInDB != null && sessionInDB.getCoachedParticipantId() != null) {
                                        session.setCoachedParticipantId(sessionInDB.getCoachedParticipantId());
                                    } else {
                                        session.setCoachedParticipantId(participantFromEvent.getCoachedParticipantId());
                                    }
                                    if (sessionInDB != null && sessionInDB.getBargedParticipantId() != null) {
                                        session.setBargedParticipantId(sessionInDB.getBargedParticipantId());
                                    } else {
                                        session.setBargedParticipantId(participantFromEvent.getBargedParticipantId());
                                    }
                                    if(session.getOutboundCampaignId() == null) {
                                        final Map<String, String> attributes = participantFromEvent.getAttributes();
                                        if (attributes != null) {
                                            String campaignId = getAttribute(attributes, CAMPAIGN_ID, DIALER_CAMPAIGN_ID);
                                            if (campaignId != null) {
                                                session.setOutboundCampaignId(campaignId);
                                            }
                                        }
                                    }

                                    // if a participant has routingData and languageId, then use those values to populate the session's
                                    //  languageId. Otherwise, check if the participant has a queueId and if so, then
                                    //  find a participant that has matching queueId and also has routingData and languageId and use 
                                    //  that as the source value for the session's languageId
                                    Participant targetParticipant = null;
                                    if (participantFromEvent.getRoutingData() != null && participantFromEvent.getRoutingData().getLanguageId() != null) {
                                        targetParticipant = participantFromEvent;
                                    } else if (participantFromEvent.getQueueId() != null) {
                                        Participant participant = participantFromEvent;
                                        Optional<Participant> foundParticipant = participants.stream().filter(p -> p.getRoutingData() != null && p.getRoutingData().getLanguageId() != null &&
                                                                                                               p.getQueueId() != null && p.getQueueId().equals(participant.getQueueId())).findFirst();
                                        if (foundParticipant != null && foundParticipant.isPresent()) {
                                            targetParticipant = foundParticipant.get();
                                        }
                                    }

                                    if (targetParticipant != null && targetParticipant.getRoutingData() != null) {
                                        session.setRequestedLanguageId(targetParticipant.getRoutingData().getLanguageId());
                                    }

                                    // if a participant has routingData and skillIds, then use those values to populate the session's
                                    //  skillIds. Otherwise, check if the participant has a queueId and if so, then
                                    //  find a participant that has matching queueId and also has routingData and skillIds and use 
                                    //  that as the source value for the session's skillIds
                                    targetParticipant = null;
                                    if (participantFromEvent.getRoutingData() != null && participantFromEvent.getRoutingData().getSkillIds() != null && participantFromEvent.getRoutingData().getSkillIds().size() > 0) {
                                        targetParticipant = participantFromEvent;
                                    } else if (participantFromEvent.getQueueId() != null) {
                                        Participant participant = participantFromEvent;
                                        Optional<Participant> foundParticipant = participants.stream().filter(p -> p.getRoutingData() != null && p.getRoutingData().getSkillIds() != null &&
                                                                                                               p.getQueueId() != null && p.getQueueId().equals(participant.getQueueId())).findFirst();
                                        if (foundParticipant != null && foundParticipant.isPresent()) {
                                            targetParticipant = foundParticipant.get();
                                        }
                                    }

                                    if (targetParticipant != null && targetParticipant.getRoutingData() != null) {
                                        if (targetParticipant.getRoutingData().getSkillIds() != null && targetParticipant.getRoutingData().getSkillIds().size() > 0) {
                                            Set<String> requestedRoutingSkillIds = new HashSet<>(targetParticipant.getRoutingData().getSkillIds());
                                            session.setRequestedRoutingSkillIds(requestedRoutingSkillIds);
                                        }
                                    }
                                    
                                    // Remove the existing sessionInDB from dynamo to ensure
                                    // there's only 1 entry for this session in DB
                                    // i.e. another entry for the same session can be created because
                                    // conversationStart changes which is used as part of the sortKey
                                   if (sessionInDB != null) {
                                       List<String> sortKeysDeleteEntities = new ArrayList<>();
                                       sortKeysDeleteEntities.add(sessionInDB.getSortKey());
                                       conversationStorage.deleteConversationEntitiesFromDynamo(conversationEvent.getConversationId(), sortKeysDeleteEntities);
                                   }
                                  
                                    sessions.add(session);

                                    if (participantFromEvent.getWrapup() != null && participantFromEvent.getWrapupSource().equals("participant")) {
                                        wrapupSession =  generateWrapupInformation(wrapupSession,
                                                conversationEvent,
                                                session.getSessionId(),
                                                session.getMediaType(),
                                                session.getConnectedTime(),
                                                participantFromEvent.getParticipantId(),
                                                participantFromEvent.getWrapup().getCode());
                                    }
                                } else if (sessionInDB != null) {
                                    // If there are incoming values for coached/monitored/barged participantIds, ensure that
                                    //  the session saved in DB is updated
                                    boolean updatedSessionInDB = false;
                                    if (sessionInDB != null && sessionInDB.getCoachedParticipantId() == null && participantFromEvent.getCoachedParticipantId() != null) {
                                        sessionInDB.setCoachedParticipantId(participantFromEvent.getCoachedParticipantId());
                                        updatedSessionInDB = true;
                                    }
                                    if (sessionInDB != null && sessionInDB.getMonitoredParticipantId() == null && participantFromEvent.getMonitoredParticipantId() != null) {
                                        sessionInDB.setMonitoredParticipantId(participantFromEvent.getMonitoredParticipantId());
                                        updatedSessionInDB = true;
                                    }
                                    if (sessionInDB != null && sessionInDB.getBargedParticipantId() == null && participantFromEvent.getBargedParticipantId() != null) {
                                        sessionInDB.setBargedParticipantId(participantFromEvent.getBargedParticipantId());
                                        updatedSessionInDB = true;
                                    }
                                    if (updatedSessionInDB == true) {
                                        sessions.add(sessionInDB);
                                    }
                                }
                                if (hold.getConversationId() != null) {
                                    conversationStorage.addSessionHold(hold);
                                }
                                if (!wrapupAlreadyProcessed && wrapupSession.getConversationId() != null) {
                                    wrapups.add(wrapupSession);
                                }
                            }
                        }
                    }
                }
            }
            firstEvent = firstEvent ||
                    !processedConversationsCache.wasProcessedRecently(conversationEvent.getConversationId()) &&
                            !conversationStorage.doesConversationExist(conversationEvent.getConversationId());
            createTopLevelConversation(conversationEvent, firstEvent);

            if (sessions.size() > 0) {
                conversationStorage.addConversationSessions(sessions);
                LOGGER.debug("Created session rows");
            }
            if(wrapups.size() > 0) {
                conversationStorage.addConversationWrapups(wrapups);
                LOGGER.debug("Created wrapup rows");
            }
        }
        else {
            createTopLevelConversation(conversationEvent, false);
        }
    }

    private void createTopLevelConversation(ConversationEvent conversationEvent, boolean firstEvent) {
        Conversation conversation = null;
        boolean firstWrite = false;

        // Communication providerEventTime == Conversation startTime condition satisfied, hence first conversation event
        if(firstEvent) {
            LOGGER.info("First event");
            conversation = generateTopLevelConversationInformation(conversationEvent);
            firstWrite = true;
        }

        // CE has endTime, need to add endTime and divisionIds to top level conversation
        if(conversationEvent.getEndTime() != null) {
            Set<String> divisionIds = new HashSet<>();
            conversationEvent.getDivisions().forEach(division -> divisionIds.add(division.getDivisionId()));
    
            //If not the first CE, check if top level conversation already exists. If it does just need to update endTime and divisionIds else need to add a new top level entry
            if (!firstEvent) {
                LOGGER.debug("Conversation has endTime. This is not first conversation event.");
                conversation = conversationStorage.getTopLevelConversation(conversationEvent.getConversationId());
                // This condition checks if the end time or divisionIds have changed with the new conversation event.
                if (conversation != null && conversation.getConversationEnd() != null
                        && conversation.getConversationEnd().equals(conversationEvent.getEndTime())
                        && Objects.equals(conversation.getDivisionIds(), divisionIds)){
                    LOGGER.debug("Skipping updating top level conversation. End time or divisionIds didn't change");
                    return;
                }
                if (conversation == null) {
                    LOGGER.debug("First conversation had endTime");
                    conversation = generateTopLevelConversationInformation(conversationEvent);
                    firstWrite = true;
                }
            }

            LOGGER.info("First write to dynamo");
            conversation.setConversationEnd(conversationEvent.getEndTime());
            conversation.setDivisionIds(divisionIds);
        }

        if(conversation != null && conversation.getConversationId() != null) {
            if(firstWrite) {
                conversationStorage.addConversation(conversation);
                LOGGER.debug("Conversation added to dynamo for the first time");

                addTimeoutEntry(conversation.getConversationId(), conversation.getConversationStart());
            }
            else {
                conversationStorage.updateEndTimeAndDivisionIds(conversation);
                LOGGER.debug("Conversation updated in dynamo");
            }
            LOGGER.debug("Created top level conversation information");
        }

    }


    private Session generateSessionInformationForWrapup(Session session, String callId, Long connectedTimeMilliseconds, Long disconnectedTimeMilliseconds, Long providerEventTime, MediaType media) {
        session.setSessionId(callId);
        session.setConnectedTime(connectedTimeMilliseconds);
        session.setDisconnectedTime(disconnectedTimeMilliseconds);
        session.setMediaType(media);
        session.setProviderEventTime(providerEventTime);
        return session;
    }

    private Wrapup generateWrapupInformation(Wrapup wrapup, ConversationEvent conversationEvent, String sessionId, MediaType media, Long connectedTime, String participantId, String code) {
        wrapup.setConversationId(conversationEvent.getConversationId());

        String sortKey = connectedTime == null ? generateSessionWrapupRangeKey(conversationEvent.getStartTime().toString(), "null", sessionId) : generateSessionWrapupRangeKey(conversationEvent.getStartTime().toString(), connectedTime.toString(), sessionId);
        wrapup.setSortKey(sortKey);
        wrapup.setSessionId(sessionId);
        wrapup.setParticipantId(participantId);
        wrapup.setWrapUpCode(code);
        return wrapup;
    }

    private boolean isCommunicationDisconnected(String state) {
        return state.equals(DISCONNECTED_SESSION_STATE) || state.equals(TERMINATED_SESSION_STATE);
    }

    private Hold generatePendingHoldInformation(Hold hold, String conversationId, String organizationId, String sessionId, Long conversationStart, Long startHoldTimeMilliseconds) {
        hold.setConversationId(conversationId);
        hold.setSessionId(sessionId);
        hold.setHoldStart(startHoldTimeMilliseconds);
        hold.setSortKey(generateHoldRangeKey(conversationStart.toString(), hold.getHoldStart().toString(), sessionId));
        return hold;
    }

    private Hold generateHoldInformation(Hold hold, String conversationId, String organizationId, String sessionId, Long conversationStart, MediaType media, Long endHoldTimeMilliseconds, Long holdDurationMilliseconds) {

        hold.setConversationId(conversationId);
        hold.setSessionId(sessionId);
        hold.setHoldStart(endHoldTimeMilliseconds - holdDurationMilliseconds);
        hold.setHoldEnd(endHoldTimeMilliseconds);
        hold.setSortKey(generateHoldRangeKey(conversationStart.toString(), hold.getHoldStart().toString(), sessionId));
        return hold;
    }

    private String getAttribute(Map<String, String> attributes, String name, String alternativeName) {
        return attributes.getOrDefault(name, attributes.get(alternativeName));
    }

    public static String getAddressString(Address address) {
        if (address != null) {
            // Extract address, order of preference:
            // - addressNormalized
            // - addressDisplayable
            // - addressRaw
            final String addressString;
            if (address.getAddressNormalized() != null) {
                addressString = address.getAddressNormalized();
            } else if (address.getAddressDisplayable() != null) {
                addressString = address.getAddressDisplayable();
            } else {
                addressString = address.getAddressRaw();
            }
            if (addressString != null) {
                return normalizeAddress(addressString);
            }
        }
        return null;
    }

    public static String normalizeAddress(String address) {
        final Matcher matcher = SIP_PHONE_NUMBER_PATTERN.matcher(address);
        if (matcher.find()) {
            return "tel:+" + matcher.group(1);
        } else {
            return address;
        }
    }

    private List<Function<Address, String>> getAddressExtractors(String messageType) {
        switch (messageType.toLowerCase(Locale.ENGLISH)) {
            case SMS:
                return SMS_ADDRESS_EXTRACTORS;
            case WEB_MESSAGING:
                return WEB_MESSAGING_ADDRESS_EXTRACTORS;
            default:
                return DEFAULT_ADDRESS_EXTRACTORS;
        }
    }

    private Pair<String, String> extractAniDnisFromSelfAndOther(GenericCommunication genericCommunication, Participant participant, ConversationEvent conversationEvent) {
        final Purpose purpose = participant.getPurpose() != null? Purpose.fromString(participant.getPurpose()) : Purpose.UNKNOWN;
        String ani = null;
        String dnis = null;
        /*-
         * Treatment of ANI/DNIS depends on the direction and purpose
         *
         * +---------------+-----------+--------+------------------------------+
         * | Purpose       | Direction | ANI    | DNIS                         |
         * +---------------+-----------+--------+------------------------------+
         * | ACD           | inbound   | other  | original target phone number |
         * | IVR           | inbound   | other  | original target phone number |
         * | INTERNAL      | inbound   | other  | self                         |
         * | INTERNAL      | outbound  | self   | other                        |
         * | EXTERNAL      | inbound   | self   | other                        |
         * | EXTERNAL      | outbound  | other  | self                         |
         * | VOICEMAIL     | -         | other  | self                         |
         * | DIALER_SYSTEM | -         | self   | other                        |
         * | Other         | -         | self   | other                        |
         * +---------------+-----------+--------+------------------------------+
         *
         */
        switch (purpose) {
            case IVR:
            case ACD:
                if (Constants.DIRECTION_INBOUND.equals(genericCommunication.getDirection())) {
                    ani = getAddressString(genericCommunication.getOther());
                    // Extract DNIS from original target number
                    dnis = getAddressString(getOriginalTargetNumber(conversationEvent));
                }
                break;
            case INTERNAL:
                if (Constants.DIRECTION_INBOUND.equals(genericCommunication.getDirection())) {
                    ani = getAddressString(genericCommunication.getOther());
                    dnis = getAddressString(genericCommunication.getSelf());
                } else {
                    ani = getAddressString(genericCommunication.getSelf());
                    dnis = getAddressString(genericCommunication.getOther());
                }
                break;
            case EXTERNAL:
                if (Constants.DIRECTION_INBOUND.equals(genericCommunication.getDirection())) {
                    ani = getAddressString(genericCommunication.getSelf());
                    dnis = getAddressString(genericCommunication.getOther());
                } else {
                    ani = getAddressString(genericCommunication.getOther());
                    dnis = getAddressString(genericCommunication.getSelf());
                }
                break;
            case VOICEMAIL:
                ani = getAddressString(genericCommunication.getOther());
                dnis = getAddressString(genericCommunication.getSelf());
                break;
            case DIALER_SYSTEM:
            default:
                ani = getAddressString(genericCommunication.getSelf());
                dnis = getAddressString(genericCommunication.getOther());
                break;
        }
        return Pair.of(ani, dnis);
    }
    /**
     * In an inbound call scenario with an IVR the IVR participant may sometimes occur before the EXTERNAL participant. This
     * method tries to find the EXTERNAL participant. Only if the first participant is an IVR, the second participant will be
     * checked.
     */
    private Participant findInboundExternal(List<Participant> participants) {
        int num = 1;
        for (Participant participant : participants) {
            final Purpose purpose = Purpose.fromString(participant.getPurpose());
            if (Purpose.EXTERNAL == purpose) {
                return participant;
            } else if (Purpose.IVR == purpose && num++ < 2) {
                // Allow IVR to occur before EXTERNAL
                continue;
            }
            break;
        }
        return null;
    }

    /**
     * Extract the original target number from the original EXTERNAL voice session.
     */
    private Address getOriginalTargetNumber(ConversationEvent conversation) {
        final Participant external = findInboundExternal(conversation.getParticipants());
        if (external != null) {
            final List<Object> communications = external.getCommunications();
            if (communications != null) {
                for (Object communication : communications) {
                    if (communication instanceof Call) {
                        Call call = (Call) communication;
                        if (Constants.DIRECTION_INBOUND.equals(call.getDirection())) {
                            return call.getOther();
                        } else {
                            return call.getSelf();
                        }
                    }
                }
            }
        }
        return null;
    }

    /**
     * Try different functions to extract an address string from an {@link Address} until one is successful.
     */
    private String getAddress(Address address, List<Function<Address, String>> extractors) {
        for (Function<Address, String> extractor : extractors) {
            final String addressValue = extractor.apply(address);
            if (StringUtils.isNotBlank(addressValue)) {
                return addressValue;
            }
        }
        return null;
    }

    private Session generateSessionInformation(Session session, String conversationStart, String id, MediaType media, Long connectedTimeMilliseconds, Long disconnectedTimeMilliseconds, String disconnectType, String flowId, String direction, String peerId, Long participantStartTime) {

        // // If sortKey was previously saved, then retain same sortKey
        if (session.getSortKey() == null) {
            String sessionConnectedTimeAndId;
            if(connectedTimeMilliseconds != null) {
                sessionConnectedTimeAndId = generateSessionRangeKey(conversationStart, connectedTimeMilliseconds.toString(), id);
            } else {
                // If connectedTime is not available, use the participant's start time if present
                //  otherwise just use current time to maintain session chronological ordering
                if (participantStartTime != null) {
                    sessionConnectedTimeAndId = generateSessionRangeKey(conversationStart, participantStartTime.toString(), id);
                } else {
                    sessionConnectedTimeAndId = generateSessionRangeKey(conversationStart, "null", id);
                 }
            }
            session.setSortKey(sessionConnectedTimeAndId);    
        }
        
        session.setSessionId(id);
        session.setMediaType(media);
        session.setConnectedTime(connectedTimeMilliseconds);
        session.setDisconnectedTime(disconnectedTimeMilliseconds);
        session.setDisconnectType(disconnectType);
        session.setFlowId(flowId);
        if(direction == null){
            if(media == MediaType.callback) {
                direction = DIRECTION_OUTBOUND;
            } else {
                direction = DIRECTION_INBOUND;
            }
        }
        session.setDirection(direction);
        session.setPeerId(peerId);
        LOGGER.debug("Generated session information for session {}", session.getSessionId());
        return session;
    }

    public MinimizedAnalyticsConversation moveConversationToS3(String organizationId, String conversationId, List<Participant> conversationEventParticipants) {
        MinimizedAnalyticsConversation tartarusConversation = new MinimizedAnalyticsConversation();
        MinimizedAnalyticsConversation existingConversationS3 = null;
        MinimizedAnalyticsConversation backfilledConversation = null;
        List<String> sortKeysDeleteEntities = new ArrayList<>();

        //Get conversation, backfill, sessions, holds and wrapup from dynamo
        ConversationsTableEntities conversationsTableEntities = conversationStorage.getConversationEntities(conversationId);

        //Get Conversation if it exists in s3
        String s3Conversation = conversationStorage.getConversationFromS3(organizationId, conversationId);
        if(!StringUtils.isEmpty(s3Conversation)) {
            try {
                existingConversationS3 = JsonObjectMapper.getMapper().readValue(s3Conversation, MinimizedAnalyticsConversation.class);
            } catch (JsonProcessingException ex) {
                LOGGER.error(Marker.SUMO_CONTINUOUS, "Unable to deserialize conversation from S3 with content: " + s3Conversation, ex);
                return null;
            }
        }

        Backfill backfill = conversationsTableEntities.getBackfill();
        if(backfill != null){
            backfilledConversation = backfill.getConversationDetails();
        }

        List<Session> sessions = conversationsTableEntities.getSessions();
        boolean emailOutboundOnly = false;
        MediaType mediaType = null;
        if (sessions.size() == 0){
            LOGGER.debug("Conversation had no session in dynamo");
            return null;
        } else {
            // Need to lookout for email outbound only conversation to ensure correct conversationEnd value
            Optional<Session> optionalSession = sessions.stream().filter(s -> s.getMediaType() != null).findFirst();
            if (optionalSession != null && optionalSession.isPresent()) {
                mediaType = optionalSession.get().getMediaType();
            }
        }
        if (mediaType != null && mediaType.equals(MediaType.email)) {
            emailOutboundOnly = true;
            Optional<Session> optionalSession = sessions.stream().filter(s -> s.getDirection() != null && s.getDirection().equals(Direction.inbound)).findFirst();
            if (optionalSession != null && optionalSession.isPresent()) {
                emailOutboundOnly = false;
            }
        }

        Conversation conversation = conversationsTableEntities.getConversation();
        if(conversation != null) {
            tartarusConversation.setConversationId(conversation.getConversationId());
            tartarusConversation.setConversationStart(generateDate(conversation.getConversationStart()));
            // Do not retain previously saved conversationEnd for mediaType=email and outbound
            if (existingConversationS3 != null && existingConversationS3.getConversationEnd() != null && emailOutboundOnly == false) {
                tartarusConversation.setConversationEnd(existingConversationS3.getConversationEnd());
            } else {
                tartarusConversation.setConversationEnd(generateDate(conversation.getConversationEnd()));
            }
            tartarusConversation.setDivisionIds(conversation.getDivisionIds() != null ? conversation.getDivisionIds().stream().collect(Collectors.toList()) : new ArrayList<>());
            tartarusConversation.setExternalSource(conversation.getExternalSource());
            tartarusConversation.setExternalInteractionId(conversation.getExternalInteractionId());
            tartarusConversation.setExternalTag(conversation.getExternalTag());
        } else {
            LOGGER.info("Top level conversation information not found in dynamo");
            return null;
        }

        //Find outboundCampaignId present in any of the sessions from dynamo
        Optional<String> outboundCampaignId = sessions.stream().filter(session -> session.getOutboundCampaignId() != null)
                .findFirst()
                .map(Session::getOutboundCampaignId);

        //Check existing conversation in S3 has outboundCampaignId
        if(outboundCampaignId.isEmpty() && existingConversationS3 != null && existingConversationS3.getParticipants() != null){
            //If it is a campaign, all the sessions would have same outboundCampaignId, so deriving it from first participant's session
            outboundCampaignId = Optional.ofNullable(existingConversationS3.getParticipants().get(0).getSessions().get(0).getOutboundCampaignId());
        }

        for (Session session : sessions) {
            //Filter hold entries related to a session
            List<Hold> sessionHolds = Optional.ofNullable(conversationsTableEntities.getHolds()).orElse(Collections.emptyList()).stream().filter(hold -> session.getSessionId().equals(hold.getSessionId())).collect(Collectors.toList());
            List<String> holdSortKeys = Optional.ofNullable(sessionHolds).orElse(Collections.emptyList()).stream().map(Hold::getSortKey).collect(Collectors.toList());

            MinimizedAnalyticsParticipant existingParticipantS3 = null;
            MinimizedAnalyticsSession existingSessionS3 = null;

            // Find existing participant / session from S3 conversation
            if (existingConversationS3 != null && existingConversationS3.getParticipants() != null && existingConversationS3.getParticipants().size() > 0) {
                Optional<MinimizedAnalyticsParticipant> optionalExistingParticipantS3 = existingConversationS3.getParticipants().stream().filter(p -> p.getParticipantId().equals(session.getParticipantId())).findFirst();
                if (optionalExistingParticipantS3 != null && optionalExistingParticipantS3.isPresent()) {
                    existingParticipantS3 = optionalExistingParticipantS3.get();
                    if (existingParticipantS3.getSessions() != null && existingParticipantS3.getSessions().size() > 0) {
                        Optional<MinimizedAnalyticsSession> optionalExistingSessionS3 = existingParticipantS3.getSessions().stream().filter(s -> s.getSessionId().equals(session.getSessionId())).findFirst();
                        if (optionalExistingSessionS3.isPresent()) {
                            existingSessionS3 = optionalExistingSessionS3.get();
                        }
                    }
                }
            }

            if (existingSessionS3 != null) {
                // Check existingS3Conversation if there are already a hold segments for this session and make sure
                //  we retain them if subsequent events does not contain hold info for this session
                if (sessionHolds != null && sessionHolds.size() == 0 && existingSessionS3.getHolds() != null && existingSessionS3.getHolds().size() > 0) {
                    for (HoldTime h : existingSessionS3.getHolds()) {
                        Hold sessionHold = new Hold();
                        sessionHold.setConversationId(existingConversationS3.getConversationId());
                        if (h.getStartTime() != null) {
                            sessionHold.setHoldStart(h.getStartTime().getTime());
                        }
                        if (h.getEndTime() != null) {
                            sessionHold.setHoldEnd(h.getEndTime().getTime());
                        }
                        sessionHolds.add(sessionHold);
                    }
                }

                // Ensure any previously saved values for the coached/monitored/barged participant Ids are
                //  retained on subsequent writes to S3
                if (existingSessionS3.getCoachedParticipantId() != null) {
                    session.setCoachedParticipantId(existingSessionS3.getCoachedParticipantId());
                }
                if (existingSessionS3.getMonitoredParticipantId() != null) {
                    session.setMonitoredParticipantId(existingSessionS3.getMonitoredParticipantId());
                }
                if (existingSessionS3.getBargedParticipantId() != null) {
                    session.setBargedParticipantId(existingSessionS3.getBargedParticipantId());
                }
                
                if (existingSessionS3.getDisconnectType() != null) {
                    session.setDisconnectType(existingSessionS3.getDisconnectType().toString());
                }

                if (existingSessionS3.getConnectedTime() != null) {
                    // Keep the original connected time previously saved in S3
                    // i.e. Analytics uses the first communication.providerEventTime as the session.connectedTime (and not communication.connectedTime)
                    //  The communication.providerEventTime changes for any new communication change, so we need to keep the original as the 
                    //  session.connectedTime and not overwrite it
                    session.setConnectedTime(existingSessionS3.getConnectedTime().getTime());                        
                }
            }

            //Get wrapup code for session if present
            Optional<Wrapup> sessionWrapup = Optional.ofNullable(conversationsTableEntities.getWrapups()).orElse(Collections.emptyList()).stream().filter(wrapup -> session.getSessionId().equals(wrapup.getSessionId())).findAny();

            //Add session and hold range keys to delete it from dynamo after moved to s3
            sortKeysDeleteEntities.add(session.getSortKey());
            sortKeysDeleteEntities.addAll(holdSortKeys);

            //If Tartarus Conversation contains participant add session to it else create a new participant
            Optional<MinimizedAnalyticsParticipant> tartarusParticipant = tartarusConversation.getParticipants().stream().filter(participant -> session.getParticipantId().equals(participant.getParticipantId())).findAny();
            if (tartarusParticipant.isPresent()) {
                //Create new session
                MinimizedAnalyticsSession tartarusSession = createParticipantSession(session, outboundCampaignId, sessionHolds, sessionWrapup);

                // Only allow outbounds to change purpose on subsequent events
                if (existingParticipantS3 != null && existingParticipantS3.getPurpose() != null && existingSessionS3 != null && 
                        existingSessionS3.getDirection() != null && existingSessionS3.getDirection().toString().equals(DIRECTION_OUTBOUND) && existingSessionS3.getMediaType() != null && 
                            existingSessionS3.getMediaType().equals(AnalyticsMediaType.voice)) {
                    tartarusParticipant.get().setPurpose(existingParticipantS3.getPurpose());
                }

                // Retain wrapup code from S3
                if (existingSessionS3 != null && existingSessionS3.getWrapUpCode() != null) {
                    tartarusSession.setWrapUpCode(existingSessionS3.getWrapUpCode());
                }

                // Persist existing value of callPeerId across multiple saves in S3
                if (existingSessionS3 != null && existingSessionS3.getCallPeerId() != null) {
                    tartarusSession.setCallPeerId(existingSessionS3.getCallPeerId());
                }

                //Get existing sessions for the participant
                List<MinimizedAnalyticsSession> sessionList = tartarusParticipant.get().getSessions();
                sessionList.add(tartarusSession);
            } else {
                //Create new participant
                MinimizedAnalyticsParticipant participant = new MinimizedAnalyticsParticipant();
                participant.setExternalContactId(session.getExternalContactId());
                participant.setParticipantId(session.getParticipantId());
                // Note: As per analytics code - https://bitbucket.org/inindca/analytics-core/src/main/analytics-core-persistence/src/main/java/com/genesys/analytics/persistence/segments/PurposeAdapter.java
                //  Replace dialer.system with outbound
                if (session.getPurpose().equals("dialer.system")) {
                    session.setPurpose(DIRECTION_OUTBOUND);
                }
                participant.setPurpose(com.inin.qm.util.model.analytics.Purpose.forValue(session.getPurpose()));
                participant.setTeamId(session.getTeamId());
                participant.setUserId(session.getUserId());

                List<MinimizedAnalyticsSession> sessionList = new ArrayList<>();
                MinimizedAnalyticsSession tartarusSession = createParticipantSession(session, outboundCampaignId, sessionHolds, sessionWrapup);

                // Retain wrapup code from S3
                if (existingSessionS3 != null && existingSessionS3.getWrapUpCode() != null) {
                    tartarusSession.setWrapUpCode(existingSessionS3.getWrapUpCode());
                }

                // Persist existing value of callPeerId across multiple saves in S3
                if (existingSessionS3 != null && existingSessionS3.getCallPeerId() != null) {
                    tartarusSession.setCallPeerId(existingSessionS3.getCallPeerId());
                }
                //Add new session
                sessionList.add(tartarusSession);
                participant.setSessions(sessionList);
                tartarusConversation.getParticipants().add(participant);
            }
        }

        if(existingConversationS3 != null){
            tartarusConversation = mergeConversations(tartarusConversation, existingConversationS3);
        }

        if(backfilledConversation != null){
            tartarusConversation = mergeConversations(tartarusConversation, backfilledConversation);
        }

        // Ensure participant order is the same order as that of the participants in the conversationEvent
        //  before attempting to determine peerIds
        if (conversationEventParticipants != null && tartarusConversation.getParticipants() != null && conversationEventParticipants.size() == tartarusConversation.getParticipants().size()) {
            List<MinimizedAnalyticsParticipant> newTartarusConversationParticipants = new ArrayList<>();
            for (Participant conversationEventParticipant : conversationEventParticipants) {
                Optional<MinimizedAnalyticsParticipant> foundTartarusConversationParticipant = tartarusConversation.getParticipants().stream().filter(p -> p.getParticipantId().equals(conversationEventParticipant.getParticipantId())).findFirst();
                if (foundTartarusConversationParticipant.isPresent()) {
                    newTartarusConversationParticipants.add(foundTartarusConversationParticipant.get());
                }
            }
            tartarusConversation.setParticipants(newTartarusConversationParticipants);
        }

        List<PeerIdExemptSession> peerIdExemptSessions = conversationsTableEntities.getPeerIdExemptSessions();

        for (MinimizedAnalyticsParticipant participant : tartarusConversation.getParticipants()) {
            if (participant.getSessions() == null || participant.getSessions().size() == 0) {
                LOGGER.debug("Participant " + participant.getParticipantId() + " has no sessions - skip finding peer");
                continue;
            }
            for (MinimizedAnalyticsSession session: participant.getSessions()) {
                // If this session has been flagged as peerId exempt, then setPeerId(null) and move on
                Optional<PeerIdExemptSession> optionalFoundSessionExtra = peerIdExemptSessions.stream().filter(s -> s.getSessionId().equals(session.getSessionId())).findFirst();
                if (optionalFoundSessionExtra.isPresent()) {
                    session.setPeerId(null);
                    continue;
                }
                
                // Skip sessions that already has a peerId (coming from the event)
                if (session.getPeerId() != null) {
                    LOGGER.debug("ParticipantId={}, sessionId={}, peerId={} already has a peerId - skip finding peer", 
                                    participant.getParticipantId(), session.getSessionId(), session.getPeerId());                    
                    continue;
                }

                // Ensure external sessions have null peerId regardless of mediaType
                if (session.getMediaType() != null && (participant.getPurpose() == null || Purpose.fromString(participant.getPurpose().toString()) == Purpose.EXTERNAL)) {
                    LOGGER.debug("ParticipantId={}, sessionId={}, mediaType={}, purpose={} considered not internal - skip finding peer", 
                                    participant.getParticipantId(), session.getSessionId(), session.getMediaType().toString(), 
                                    participant.getPurpose() != null ? participant.getPurpose().toString() : "");
                    session.setPeerId(null);
                    continue;
                }

                // Check if the session already has a peerId in S3 and if does, then use it
                if (existingConversationS3 != null && existingConversationS3.getParticipants() != null && existingConversationS3.getParticipants().size() > 0) {
                    Optional<MinimizedAnalyticsParticipant> optionalParticipantInS3 = existingConversationS3.getParticipants().stream().filter(p -> p.getParticipantId().equals(participant.getParticipantId())).findFirst();
                    if (optionalParticipantInS3 != null && optionalParticipantInS3.isPresent()) {
                        MinimizedAnalyticsParticipant foundParticipantInS3 = optionalParticipantInS3.get();
                        if (foundParticipantInS3.getSessions() != null && foundParticipantInS3.getSessions().size() > 0) {
                            Optional<MinimizedAnalyticsSession> optionalSessionInS3 = foundParticipantInS3.getSessions().stream().filter(s -> s.getSessionId().equals(session.getSessionId())).findFirst();
                            if (optionalSessionInS3 != null && optionalSessionInS3.isPresent()) {
                                MinimizedAnalyticsSession foundSessionInS3 = optionalSessionInS3.get();
                                if (foundSessionInS3.getPeerId() != null) {
                                    session.setPeerId(foundSessionInS3.getPeerId());
                                    continue;
                                }
                            }
                        }
                    }
                }
                MinimizedAnalyticsSession externalPeerSession = findExternalPeerSession(session, tartarusConversation.getParticipants(), participant);

                if (externalPeerSession != null) {
                    LOGGER.debug("Setting peerId={} for ParticipantId={}, sessionId={}", 
                                externalPeerSession.getSessionId(), participant.getParticipantId(), session.getSessionId());
                    session.setPeerId(externalPeerSession.getSessionId());
                }
            }
        }

        //Add wrapup range keys to delete it from dynamo once it is moved to S3
        for(Wrapup wrapup : conversationsTableEntities.getWrapups()) {
            sortKeysDeleteEntities.add(wrapup.getSortKey());
        }
        
        // Write conversation to S3
        String conversationString;
        try {
            conversationString = JsonObjectMapper.getMapper().writeValueAsString(tartarusConversation);
        } catch (JsonProcessingException ex) {
            LOGGER.error(Marker.SUMO_CONTINUOUS, "Failed to serialize conversation to write to S3", ex);
            return null;
        }

        conversationStorage.updateConversationToS3(organizationId, tartarusConversation.getConversationId(), conversationString);

        LOGGER.info("Saved conversation to S3. Sources: dynamo. " +
                (backfilledConversation != null ? "backfill. " : "") +
                (existingConversationS3 != null ? "S3." : "")
        );

        conversationStorage.deleteConversationEntitiesFromDynamo(tartarusConversation.getConversationId(), sortKeysDeleteEntities);

        return tartarusConversation;
    }

    private void mergeParticipantsToConversation(List<MinimizedAnalyticsParticipant> sourceParticipants, List<MinimizedAnalyticsParticipant> targetParticipants, MinimizedAnalyticsConversation tartarusConversation, boolean addIfPresent) {
        for (MinimizedAnalyticsParticipant sourceParticipant : sourceParticipants) {
            Optional<MinimizedAnalyticsParticipant> optionalFoundParticipant = targetParticipants.stream().filter(targetParticipant -> targetParticipant.getParticipantId().equals(sourceParticipant.getParticipantId())).findFirst();
            if (optionalFoundParticipant.isPresent() == addIfPresent) {
                if (addIfPresent) {
                    tartarusConversation.getParticipants().add(optionalFoundParticipant.get());
                } else {
                    tartarusConversation.getParticipants().add(sourceParticipant);
                }
            }
        }
    }
    
    private MinimizedAnalyticsConversation mergeConversations(MinimizedAnalyticsConversation tartarusConversation, MinimizedAnalyticsConversation partialConversation) {

        Set<String> mergedDivisionIds = new HashSet<>();
        mergedDivisionIds.addAll(tartarusConversation.getDivisionIds());
        mergedDivisionIds.addAll(partialConversation.getDivisionIds());

        tartarusConversation.setDivisionIds(mergedDivisionIds.stream().collect(Collectors.toList()));

        List<MinimizedAnalyticsParticipant> tartarusConversationParticipants = tartarusConversation.getParticipants();
        tartarusConversation.setParticipants(new ArrayList<>());

        //Get all participantIds currently in tartarus conversation
        List<String> participantIds = Optional.ofNullable(tartarusConversationParticipants).orElse(Collections.emptyList()).stream().map(MinimizedAnalyticsParticipant::getParticipantId).distinct().collect(Collectors.toList());

        List<MinimizedAnalyticsParticipant> commonParticipants = partialConversation.getParticipants().stream().filter(existingParticipant -> participantIds.contains(existingParticipant.getParticipantId())).collect(Collectors.toList());
        List<MinimizedAnalyticsParticipant> uncommonParticipants = partialConversation.getParticipants().stream().filter(existingParticipant -> !participantIds.contains(existingParticipant.getParticipantId())).collect(Collectors.toList());

        if(uncommonParticipants.size() > 0) {
            for (MinimizedAnalyticsParticipant participant : uncommonParticipants) {
                tartarusConversationParticipants.add(participant);
            }
        }

        if(commonParticipants.size() > 0) {
            for(MinimizedAnalyticsParticipant commonParticipant : commonParticipants) {
                Optional<MinimizedAnalyticsParticipant> participant = tartarusConversationParticipants.stream().filter(p -> p.getParticipantId().equals(commonParticipant.getParticipantId())).findAny();
                List<String> sessionIds = Optional.ofNullable(participant.get().getSessions()).orElse(Collections.emptyList()).stream().map(MinimizedAnalyticsSession::getSessionId).distinct().collect(Collectors.toList());

                List<MinimizedAnalyticsSession> uncommonSessions = commonParticipant.getSessions().stream().filter(existingSessions -> !sessionIds.contains(existingSessions.getSessionId())).collect(Collectors.toList());
                for(MinimizedAnalyticsSession session : uncommonSessions){
                    participant.get().getSessions().add(session);
                }
            }
        }

        // Ensure original order of participants from the partialConversation is maintained 
        if (partialConversation.getParticipants() != null && partialConversation.getParticipants().size() > 0 && tartarusConversationParticipants != null && tartarusConversationParticipants.size() > 0) {
            mergeParticipantsToConversation(partialConversation.getParticipants(), tartarusConversationParticipants, tartarusConversation, true);
        }

        if (tartarusConversationParticipants != null && tartarusConversationParticipants.size() > 0) {
            mergeParticipantsToConversation(tartarusConversationParticipants, tartarusConversation.getParticipants(), tartarusConversation, false);
        }

        return tartarusConversation;
    }

    private MinimizedAnalyticsSession createParticipantSession(Session session, Optional<String> outboundCampaignId, List<Hold> sessionHolds, Optional<Wrapup> sessionWrapup) {
            MinimizedAnalyticsSession tartarusSession = new MinimizedAnalyticsSession();
            tartarusSession.setSessionId(session.getSessionId());
            tartarusSession.setAni(session.getAni());
            tartarusSession.setDnis(session.getDnis());
            tartarusSession.setSessionDnis(session.getSessionDnis());
            tartarusSession.setAddressFrom(session.getAddressFrom());
            tartarusSession.setAddressTo(session.getAddressTo());
            tartarusSession.setDisconnectType(AnalyticsDisconnectType.forValue(session.getDisconnectType()));
            tartarusSession.setFlowId(session.getFlowId());
            tartarusSession.setMediaType(AnalyticsMediaType.forValue(session.getMediaType().toString()));
            tartarusSession.setQueueId(session.getQueueId());
            tartarusSession.setRequestedLanguageId(session.getRequestedLanguageId());
            tartarusSession.setRequestedRoutingSkillIds(session.getRequestedRoutingSkillIds() != null ? new HashSet<>(session.getRequestedRoutingSkillIds()) : null);
            tartarusSession.setConnectedTime(generateDate(session.getConnectedTime()));
            tartarusSession.setDisconnectedTime(generateDate(session.getDisconnectedTime()));
            tartarusSession.setMonitoredParticipantId(session.getMonitoredParticipantId());
            tartarusSession.setCoachedParticipantId(session.getCoachedParticipantId());
            tartarusSession.setBargedParticipantId(session.getBargedParticipantId());
            if(outboundCampaignId.isPresent()){
                tartarusSession.setOutboundCampaignId(outboundCampaignId.get());
            }
            if(sessionWrapup.isPresent()) {
                tartarusSession.setWrapUpCode(sessionWrapup.get().getWrapUpCode());
            }
            tartarusSession.setDirection(Direction.forValue(session.getDirection()));
            if(sessionHolds.size() > 0){
                List<HoldTime> holdTimes = new ArrayList<>();
                for(Hold sessionHold: sessionHolds){
                    HoldTime holdTime = new HoldTime();
                    holdTime.setStartTime(generateDate(sessionHold.getHoldStart()));
                    holdTime.setEndTime(generateDate(sessionHold.getHoldEnd()));
                    holdTimes.add(holdTime);
                }
                tartarusSession.setHolds(holdTimes);
            }
            tartarusSession.setPeerId(session.getPeerId());
            return tartarusSession;
    }

    private Date generateDate(Long value) {
        if(value == null)
            return null;
        else
            return new Date(value);
    }

    public void handleDeletedRecordings(List<RecordingChange> recordingChanges) {
        List<Recording> recordings = new ArrayList<>();
        List<Timeout> timeouts = new ArrayList<>();
        for(RecordingChange recording : recordingChanges) {
            LOGGER.debug("Processing Recording Delete for {}/{}/{}",
            recording.getOrganizationId(), recording.getConversationId(), recording.getCommunicationId());

            Recording deletedRecording = new Recording();
            deletedRecording.setConversationId(recording.getConversationId());
            deletedRecording.setSortKey(generateRecordingDeletedRangeKey(recording.getRecordingId()));

            recordings.add(deletedRecording);

            Timeout timeout = conversationStorage.getTimeout(recording.getConversationId());
            if(timeout == null){
               timeout = createTimeoutEntry(recording.getConversationId(), null, TartarusConstants.ONE_DAY_SECONDS);
               timeouts.add(timeout);
            }
        }

        conversationStorage.addRecordings(recordings);
        if(timeouts.size() > 0){
            conversationStorage.addTimeouts(timeouts);
        }
    }

    private Timeout createTimeoutEntry(String conversationId, Long time, long seconds) {
        Timeout timeout = new Timeout();
        timeout.setConversationId(conversationId);
        timeout.setSortKey("t");
        if (time != null) {
            timeout.setTtl((time / 1000) + seconds);
        } else {
            timeout.setTtl((System.currentTimeMillis() / 1000) + seconds);
        }
        return timeout;
    }

    public void handleAvailableRecordings(List<RecordingChange> recordingChanges) {
        List<Recording> recordings = new ArrayList<>();
        for(RecordingChange recording : recordingChanges) {
            LOGGER.debug("Processing Recording Available event received from Kafka for {}/{}/{}",
                    recording.getOrganizationId(), recording.getConversationId(), recording.getCommunicationId());

            Recording availableRecording = new Recording();
            availableRecording.setConversationId(recording.getConversationId());
            availableRecording.setSortKey(generateRecordingAvailableRangeKey(recording.getRecordingId()));
            recordings.add(availableRecording);
        }

        conversationStorage.addRecordings(recordings);        
    }
    
    private static long findProperDifference(long value1, long value2) {
        return value1 >= value2 ? value1 - value2 : value2 - value1;
    }

    private static long findTimeProximity(MinimizedAnalyticsSession externalSession, MinimizedAnalyticsSession internalSession) {
        long timeProximity = 0;
        if (externalSession.getConnectedTime() != null && internalSession.getConnectedTime() != null) {
            timeProximity = findProperDifference(externalSession.getConnectedTime().getTime(), internalSession.getConnectedTime().getTime());
        } else if (externalSession.getDisconnectedTime() != null && internalSession.getDisconnectedTime() != null) {
            timeProximity = findProperDifference(externalSession.getDisconnectedTime().getTime(), internalSession.getDisconnectedTime().getTime());
        }
        return timeProximity;
    }

    private static boolean isOverlapping(MinimizedAnalyticsSession externalSession, MinimizedAnalyticsSession internalSession, long allowedToleranceMs)  {
        if (externalSession.getConnectedTime() != null && externalSession.getDisconnectedTime() != null && internalSession.getConnectedTime() != null && internalSession.getDisconnectedTime() != null ) {
            // Return if the 2 sessions overlaps or overlaps within the ALLOWED_GAP_BETWEEN_SESSIONS between connected/disconnected time of the 2 sessions
            return (!(externalSession.getConnectedTime().getTime() > internalSession.getDisconnectedTime().getTime() || externalSession.getDisconnectedTime().getTime() < internalSession.getConnectedTime().getTime())) || 
                    (((externalSession.getConnectedTime().getTime() > internalSession.getDisconnectedTime().getTime() &&  externalSession.getConnectedTime().getTime() - internalSession.getDisconnectedTime().getTime() < allowedToleranceMs) ||
                    (internalSession.getConnectedTime().getTime() > externalSession.getDisconnectedTime().getTime() &&  internalSession.getConnectedTime().getTime() - externalSession.getDisconnectedTime().getTime() < allowedToleranceMs)));
        } else if (externalSession.getConnectedTime() == null && externalSession.getDisconnectedTime() != null && internalSession.getConnectedTime() != null && internalSession.getDisconnectedTime() != null) {
            // externalSession connectedTime is missing - check if externalSession.disconnectedTime overlaps with the internalSession's connected and disconnectedTime
            return externalSession.getDisconnectedTime().getTime() >= internalSession.getConnectedTime().getTime() && externalSession.getDisconnectedTime().getTime() <= internalSession.getDisconnectedTime().getTime() ||
                    findProperDifference(externalSession.getDisconnectedTime().getTime(), internalSession.getConnectedTime().getTime()) < allowedToleranceMs || 
                    findProperDifference(externalSession.getDisconnectedTime().getTime(), internalSession.getDisconnectedTime().getTime()) < allowedToleranceMs;
        } else if (externalSession.getConnectedTime() != null && externalSession.getDisconnectedTime() != null && internalSession.getConnectedTime() == null && internalSession.getDisconnectedTime() != null) {
            // internalSession connectedTime is missing - check if internal.disconnectedTime overlaps with the externalSession's connected and disconnectedTime
            return internalSession.getDisconnectedTime().getTime() >= externalSession.getConnectedTime().getTime() && internalSession.getDisconnectedTime().getTime() <= externalSession.getDisconnectedTime().getTime() ||
                    findProperDifference(internalSession.getDisconnectedTime().getTime(), externalSession.getConnectedTime().getTime()) < allowedToleranceMs || 
                    findProperDifference(internalSession.getDisconnectedTime().getTime(), externalSession.getDisconnectedTime().getTime()) < allowedToleranceMs;
        } else if (externalSession.getConnectedTime() != null && externalSession.getDisconnectedTime() == null && internalSession.getConnectedTime() != null && internalSession.getDisconnectedTime() != null) {
            // externalSession disconnectedTime is missing - check if externalSession.connectedTime overlaps with the internalSession's connected and disconnectedTime
            return externalSession.getConnectedTime().getTime() >= internalSession.getConnectedTime().getTime() && externalSession.getConnectedTime().getTime() <= internalSession.getDisconnectedTime().getTime() ||
                    findProperDifference(externalSession.getConnectedTime().getTime(), internalSession.getConnectedTime().getTime()) < allowedToleranceMs || 
                    findProperDifference(externalSession.getConnectedTime().getTime(), internalSession.getDisconnectedTime().getTime()) < allowedToleranceMs;
        } else if (externalSession.getConnectedTime() != null && externalSession.getDisconnectedTime() != null && internalSession.getConnectedTime() != null && internalSession.getDisconnectedTime() == null) {
            // internalSession disconnectedTime is missing - check if internalSession.connectedTime overlaps with the externalSession's connected and disconnectedTime
            return internalSession.getConnectedTime().getTime() >= externalSession.getConnectedTime().getTime() && internalSession.getConnectedTime().getTime() <= externalSession.getDisconnectedTime().getTime() ||
                    findProperDifference(internalSession.getConnectedTime().getTime(), externalSession.getConnectedTime().getTime()) < allowedToleranceMs || 
                    findProperDifference(internalSession.getConnectedTime().getTime(), externalSession.getDisconnectedTime().getTime()) < allowedToleranceMs;
        }
        return false;
    }

    public static MinimizedAnalyticsSession findExternalPeerSession(MinimizedAnalyticsSession internalSession, List<MinimizedAnalyticsParticipant> participants, MinimizedAnalyticsParticipant internalParticipant) {
        MinimizedAnalyticsSession externalPeerSession = null;
        long closestProximity = Long.MAX_VALUE;
        MinimizedAnalyticsSession firstExternalSessionMissingConnectedTime = null;
        List<MinimizedAnalyticsParticipant> externalParticipants = new ArrayList<>();

        for (MinimizedAnalyticsParticipant participant : participants) {
            if (participant.getSessions() != null && participant.getPurpose() != null && Purpose.fromString(participant.getPurpose().toString()) == Purpose.EXTERNAL) {
                externalParticipants.add(participant);
                for (MinimizedAnalyticsSession externalSession : participant.getSessions()) {
                    // Only process external sessions with matching mediaType as that of the internalSession
                    if (externalSession.getMediaType() != null && internalSession.getMediaType() != null && internalSession.getMediaType().toString().equals(externalSession.getMediaType().toString())) {
                        // Check that this external session is not yet a peerId of one of internalSession's sibling session
                        Optional<MinimizedAnalyticsSession> optionalFoundSibling = internalParticipant.getSessions().stream().filter(s -> !internalSession.getSessionId().equals(s.getSessionId()) && s.getPeerId() != null && s.getPeerId().equals(externalSession.getSessionId())).findFirst();
                        if (!optionalFoundSibling.isPresent() || (internalSession.getPeerId() != null && externalSession.getSessionId().equals(internalSession.getPeerId()))) {
                            // If there is an overlap between externalSession and internalSession, then keep track of the externalSession closest to the internalSession either using connectedTime or disconnectedTime
                            // Edge case: if internalParticipant is an ivr and is not disconnected due to a transfer and is the last participant in the list, then set 0 tolerance                            
                            long allowedToleranceMs = internalParticipant.getPurpose() != null && internalParticipant.getPurpose().toString().equals("ivr") && internalSession.getDisconnectType() != null 
                                                        && !internalSession.getDisconnectType().toString().equals("transfer")
                                                        && participants.get(participants.size()-1).getParticipantId().equals(internalParticipant.getParticipantId()) ? 0 : ALLOWED_GAP_BETWEEN_SESSIONS_MS;
                            if (isOverlapping(externalSession, internalSession, allowedToleranceMs)) {
                                long timeProximity = findTimeProximity(externalSession, internalSession);
                                if (timeProximity < closestProximity) {
                                    closestProximity = timeProximity;
                                    if((externalPeerSession != null && externalPeerSession.getConnectedTime() != null && externalSession.getConnectedTime() != null) || externalPeerSession == null) {
                                        externalPeerSession = externalSession;
                                    }
                                }
                            } else if (firstExternalSessionMissingConnectedTime == null && externalSession.getConnectedTime() == null) {
                                // Keep track of the first external session encountered that does not have a connectedTime and did not
                                //  result to an overlap with the internal session which will be used later if unable to find an
                                //  external session as a peer
                                firstExternalSessionMissingConnectedTime = externalSession;
                            }
                        }
                    }
                }
            }
        }

        // If unable to find an overlapping external session and the internal session does not have a
        //  connectedTime, then pair it with the first encountered external session that also did not have
        //  an connected time
        if (externalPeerSession == null && internalSession.getConnectedTime() == null) {
            externalPeerSession = firstExternalSessionMissingConnectedTime;
        }

        // Otherwise, if there are only 2 participants - one external and one internal - and external session only has 1 session, then pick that
        //  as the implied peer of the internalSession
        if (externalPeerSession == null && participants != null && participants.size() == 2 && externalParticipants.size() == 1 
                && externalParticipants.get(0).getSessions() != null && externalParticipants.get(0).getSessions().size() == 1) {
            externalPeerSession = externalParticipants.get(0).getSessions().get(0);
        }
        
        return externalPeerSession;
    }
    
}