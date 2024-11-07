import ko from "knockout";
import _ from "lodash";
import Promise from "bluebird";
import ininAudioUtils from "util/ininAudioUtils";
import errorCallbackFactory from "util/errorCallbackFactory";
import authService from "services/authService";
import LOG from "services/loggingService";
import notificationService from "services/notificationService";
import qualityAuthorization from "services/qualityAuth";
import qualityManagementService from "services/qualityManagementService";
import FileStateEnum from "viewModels/util/FileStateEnum";
import RecordingView from "viewModels/RecordingView";
import AcdVoicemailView from "viewModels/AcdVoicemailView";
import MessagesRecordingView from "viewModels/MessagesRecordingView";
import ScreenRecordingDetailsView from "viewModels/ScreenRecordingDetailsView";
import ViewModel from "viewModels/ViewModel";
import embeddingService from "services/embeddingService";
import EmailRecordingView from "viewModels/EmailRecordingView";
import featureToggleService, {
    FeatureToggle,
} from "services/featureToggleService";
import speechAndTextAnalyticsService from "services/speechAndTextAnalyticsService";

const RecordingManagerView = ViewModel.extend(function (
    conversationId,
    acdVoicemailInfo,
    options,
    // callback to provide currently active recording to the caller
    // used to show transcript tab for the active recording
    selectedRecordingCallback,
    conversationOriginatingDirection
) {
    const self = this;
    let failsafeTimer = null;
    const failsafeTimeoutMs = 5 * 60 * 60 * 1000; //5 minutes
    let recordingRequestTimeout;
    let recordingPromises = null;
    let maxReloads = 3;
    let lastPositionWhenStalled = 0;
    let wasPlayingWhenStalled = false;
    let playerSpeedWhenStalled = 1;

    self.hasEmail = ko.observable(false);
    self.hasMessages = ko.observable(false);
    self.hasChats = ko.observable(false);
    self.hasCalls = ko.observable(false);
    self.externalContacts = ko.observableArray();
    self.internalUsers = ko.observableArray();
    self.conversation = ko.observable();
    self.conversationId = ko.isObservable(conversationId)
        ? conversationId
        : ko.observable(conversationId);
    self.isRecordingLoading = ko.observable(true);
    self.showLongRunningMessage = ko.observable(false);
    self.recordingRequestError = ko.observable();
    self.httpErrorStatus = ko.observable(-1);
    self.conversationRecordingInformation = ko.observableArray();
    self.vmRecordingInformation = ko.observableArray();
    self.conversationRecordingViews = ko.observableArray();
    self.vmRecordingViews = ko.observableArray();
    self.recordingViews = ko.observableArray();
    self.playbackContext = ko.observable();
    self.recordingInformation = ko.observableArray();
    self.screenRecordingDetailsView = ko.observable();
    self.conversationRecordings = ko.observableArray();
    self.isAcdvoicemail = ko.observable(false);
    self.isFirstCallToGetRecording = ko.observable(true);
    self.isFromStalledEvent = ko.observable(false);

    self.subscribe(self.conversationId, function (newConvId) {
        self.recordingViews(null);
        recordingItemContainer(newConvId, true);
    });

    if (options && options.playbackContext) {
        self.playbackContext(options.playbackContext);
    }

    const handleRecordingUpdates = function (incomingPayload) {
        //If the incoming payload is from playback BUT we did not have a 202 recording, we don't want to call playback again.
        if (
            !recordingPromises &&
            incomingPayload &&
            !(
                incomingPayload.actualTranscodeTimeMs &&
                !self.showLongRunningMessage()
            )
        ) {
            LOG.info("Requesting recordings for conversation after CP event.", {
                conversation: self.conversationId(),
            });
            recordingItemContainer(self.conversationId(), false);
        }
    };

    // polling failsafe. Begin polling if we get a long transcode.
    // This is needed because the event doesn't have all the information, and sometimes it is possible for the event
    // to come, and then the API returns with a 202. At that point, we have to poll until we have consistency.
    const pollForRecordings = function (initialTimeoutMs) {
        recordingRequestTimeout = setTimeout(function () {
            if (self.showLongRunningMessage()) {
                // If we're waiting on a request already, don't do it again.
                if (!recordingPromises && self.conversationId()) {
                    LOG.info(
                        "Requesting recordings for conversation after waiting " +
                            initialTimeoutMs.toString() +
                            "ms",
                        { conversation: self.conversationId() }
                    );
                    recordingItemContainer(self.conversationId(), false);
                }
                // Back off the rate at which we poll
                pollForRecordings(initialTimeoutMs + initialTimeoutMs);
            }
        }, initialTimeoutMs);
    };

    const recordingItemContainer = function (conversationId, shouldSubscribe) {
        const formatId = ininAudioUtils.getSuggestedFormatId();
        const mediaFormats = ininAudioUtils.getAllAcceptedFormatIds();

        //This is passed down to the AudioPlayerView so it can force a reload if we have an unexpected playback
        //error (such as happens when the cached items get deleted out from under it)
        const reloadCallback = function (reloadData) {
            self.isRecordingLoading(true);
            lastPositionWhenStalled = reloadData.lastPosition;
            wasPlayingWhenStalled = reloadData.wasPlaying;
            playerSpeedWhenStalled = reloadData.lastPlayerSpeed;

            if (reloadData.isFromStalledEvent) {
                self.isFromStalledEvent(true);
                getRecordings(conversationId, self.isFirstCallToGetRecording());
            } else if (maxReloads-- > 0) {
                getRecordings(conversationId);
            }
        };

        const buildAcdVoicemailViews = function (ids) {
            self.vmRecordingViews(
                _.map(ids, function (voicemailId) {
                    return self.subView(AcdVoicemailView, voicemailId);
                })
            );

            self.vmRecordingInformation(
                _.map(ids, function (voicemailId, index) {
                    self.isAcdvoicemail(true);
                    // AcdVoicemailView services as viewmodel for top and bottom parts
                    return self.vmRecordingViews()[index];
                })
            );
        };

        const createRecordingInformation = function (
            recordingModel,
            isDisplayed,
            isDeleted
        ) {
            return {
                recordingContext: self.playbackContext(),
                media: recordingModel.media,
                mediaUris: recordingModel.mediaUris,
                recordingFileState: recordingModel.fileState,
                recordingId: recordingModel.id,
                conversationId: recordingModel.conversationId,
                mediaSubject: recordingModel.mediaSubject,
                mediaSubtype: recordingModel.mediaSubtype,
                retentionInformation: {
                    creationTime: recordingModel.creationTime,
                    archiveDate: recordingModel.archiveDate,
                    deleteDate: recordingModel.deleteDate,
                    exportedDate: recordingModel.exportedDate,
                },
                isDisplayed,
                isDeleted,
            };
        };

        const getRecordings = function (
            conversationId,
            isFirstCallFromStalledEvent = false
        ) {
            recordingPromises = [];
            if (acdVoicemailInfo && acdVoicemailInfo.acdVoicemailIds) {
                const canViewAcdVoicemails =
                    authService.hasAnyAuthorization([
                        "perm:voicemail:acdVoicemail:view",
                    ]) ||
                    (acdVoicemailInfo.isCurrentUserOnConversation &&
                        acdVoicemailInfo.isCurrentUserOnConversation());

                if (canViewAcdVoicemails) {
                    if (acdVoicemailInfo.acdVoicemailIds()) {
                        buildAcdVoicemailViews(
                            acdVoicemailInfo.acdVoicemailIds()
                        );
                    } else {
                        let notifyHasVoicemailIds;

                        recordingPromises.push(
                            new Promise(function (resolve) {
                                notifyHasVoicemailIds = resolve;
                            })
                        );

                        self.subscribe(
                            acdVoicemailInfo.acdVoicemailIds,
                            function () {
                                buildAcdVoicemailViews(
                                    acdVoicemailInfo.acdVoicemailIds()
                                );

                                if (notifyHasVoicemailIds) {
                                    notifyHasVoicemailIds(true);
                                    notifyHasVoicemailIds = null;
                                }
                            }
                        );
                    }
                } else {
                    LOG.info("Not requesting acdVoicemails", {
                        isCurrentUserOnConversation: acdVoicemailInfo.isCurrentUserOnConversation(),
                        hasViewAuthorization: authService.hasAnyAuthorization([
                            "perm:voicemail:acdVoicemail:view",
                        ]),
                    });
                }
            }

            if (
                qualityAuthorization.canViewRecording() ||
                qualityAuthorization.canViewRecordingSegment()
            ) {
                recordingPromises.push(
                    qualityManagementService
                        .getRecordings(conversationId, formatId, mediaFormats)
                        .then(function (recordingModels) {
                            self.httpErrorStatus(-1);
                            self.recordingRequestError(false);
                            //If any are long running (beyond the API call), just wait for the CP event
                            //(which was set below, before the API call)
                            if (isLongRunning(recordingModels)) {
                                // If we already got recording data back, don't overwrite it with long running
                                if (
                                    self.conversationRecordingViews().length ===
                                        0 ||
                                    self
                                        .conversationRecordingViews()
                                        .find((v) =>
                                            v.recordingRestoreDetails.isUploading()
                                        ) ||
                                    isFirstCallFromStalledEvent
                                ) {
                                    self.showLongRunningMessage(true);
                                    self.isRecordingLoading(true);
                                    if (!recordingRequestTimeout) {
                                        // Start with big timeout. This it not really meant to be responsive.
                                        // It is a failsafe.
                                        pollForRecordings(5000);
                                    }
                                    if (isFirstCallFromStalledEvent) {
                                        self.isFirstCallToGetRecording(false);
                                    }
                                }
                            } else {
                                //Clear the failsafe timer
                                if (failsafeTimer) {
                                    clearTimeout(failsafeTimer);
                                    failsafeTimer = null;
                                }
                                if (self.showLongRunningMessage()) {
                                    self.showLongRunningMessage(false);
                                }

                                if (
                                    recordingModels &&
                                    _.find(recordingModels, function (rec) {
                                        return rec.media === "email";
                                    })
                                ) {
                                    self.conversationRecordings(
                                        recordingModels
                                    );
                                    self.hasEmail(true);
                                }

                                if (
                                    recordingModels &&
                                    _.find(recordingModels, function (rec) {
                                        return (
                                            !!rec.messageTranscript ||
                                            rec.media === "message"
                                        );
                                    })
                                ) {
                                    self.hasMessages(true);
                                }

                                if (
                                    recordingModels &&
                                    _.find(recordingModels, function (rec) {
                                        return rec.media === "audio";
                                    })
                                ) {
                                    self.hasCalls(true);
                                }

                                if (
                                    recordingModels &&
                                    _.find(recordingModels, function (rec) {
                                        return (
                                            !!rec.transcript ||
                                            rec.media === "chat"
                                        );
                                    })
                                ) {
                                    self.hasChats(true);
                                }
                                // Handle message recordings separately
                                const messageModels = [];
                                const emailModels = [];
                                const screenModels = [];
                                let sortedModels = [];
                                _.each(recordingModels, function (
                                    recordingModel
                                ) {
                                    if (recordingModel.media === "message") {
                                        messageModels.push(recordingModel);
                                    } else if (
                                        recordingModel.media === "screen"
                                    ) {
                                        screenModels.push(recordingModel);
                                    } else if (
                                        recordingModel.media === "email"
                                    ) {
                                        emailModels.push(recordingModel);
                                    } else {
                                        sortedModels.push(recordingModel);
                                    }
                                });
                                self.internalUsers([]);
                                self.externalContacts([]);
                                _.each(recordingModels, function (
                                    recordingModel
                                ) {
                                    //assign internal users details from recording api
                                    if (!_.isEmpty(recordingModel.users)) {
                                        _.each(recordingModel.users, function (
                                            user
                                        ) {
                                            if (
                                                !self
                                                    .internalUsers()
                                                    .some(
                                                        (item) =>
                                                            item.id === user.id
                                                    )
                                            ) {
                                                user.userId = user.id;
                                                user.coachedParticipantId = null;
                                                user.monitoredParticipantId = null;
                                                user.bargedInParticipantId = null;
                                                self.internalUsers.push(user);
                                            }
                                        });
                                    }
                                    //assign external contact details from recording api
                                    if (
                                        !_.isEmpty(
                                            recordingModel.externalContacts
                                        )
                                    ) {
                                        _.each(
                                            recordingModel.externalContacts,
                                            function (externalContact) {
                                                externalContact.externalContactId =
                                                    externalContact.id;
                                                externalContact.participantType =
                                                    "external";
                                                if (
                                                    !self
                                                        .externalContacts()
                                                        .some(
                                                            (item) =>
                                                                item.id ===
                                                                externalContact.id
                                                        )
                                                ) {
                                                    self.externalContacts.push(
                                                        externalContact
                                                    );
                                                }
                                            }
                                        );
                                    }
                                });

                                // Sort screen recordings to the end
                                sortedModels = _.sortBy(sortedModels, function (
                                    recordingModel
                                ) {
                                    return recordingModel.media === "screen";
                                });

                                const recordingViews = _.map(
                                    sortedModels,
                                    function (recordingModel, index) {
                                        const temp = {
                                            reloadCallback: reloadCallback,
                                            maxReloads: maxReloads,
                                            isLast:
                                                index ===
                                                sortedModels.length - 1,
                                        };

                                        if (index === counter()) {
                                            const stalledData = {
                                                lastPositionWhenStalled: lastPositionWhenStalled,
                                                wasPlayingWhenStalled: wasPlayingWhenStalled,
                                                playerSpeedWhenStalled: playerSpeedWhenStalled,
                                            };
                                            temp.stalledData = stalledData;
                                        }

                                        if (
                                            _.isNil(recordingModel.media) &&
                                            self.hasEmail() &&
                                            recordingModel.fileState ===
                                                FileStateEnum.DELETED
                                        ) {
                                            recordingModel.media = "email";
                                        }
                                        return self.subView(
                                            RecordingView,
                                            conversationId,
                                            recordingModel,
                                            temp,
                                            self.conversation,
                                            index
                                        );
                                    }
                                );

                                let messagesRecordingView;

                                if (messageModels.length) {
                                    messagesRecordingView = self.subView(
                                        MessagesRecordingView,
                                        conversationId,
                                        messageModels,
                                        self.conversation
                                    );
                                    recordingViews.splice(
                                        0,
                                        0,
                                        messagesRecordingView
                                    );
                                }
                                // insert the email recording to recordingViews when use updated UI
                                let emailRecordingView;
                                if (emailModels.length) {
                                    emailRecordingView = self.subView(
                                        EmailRecordingView,
                                        emailModels,
                                        self.conversation,
                                        conversationOriginatingDirection
                                    );
                                    recordingViews.splice(
                                        0,
                                        0,
                                        emailRecordingView
                                    );
                                }

                                if (screenModels.length) {
                                    const parentLocationHref = embeddingService.getParentHref();
                                    const screenRecordingDetailsView = self.subView(
                                        ScreenRecordingDetailsView,
                                        parentLocationHref
                                    );
                                    const screenRecordingView = self.subView(
                                        RecordingView,
                                        conversationId,
                                        screenModels[0],
                                        { url: parentLocationHref },
                                        self.conversation
                                    );
                                    self.screenRecordingDetailsView(
                                        screenRecordingDetailsView
                                    );
                                    recordingViews.push(screenRecordingView);
                                }

                                self.conversationRecordingViews(recordingViews);

                                if (!recordingViews.length) {
                                    //initialize empty recording
                                    self.conversationRecordingViews([
                                        self.subView(
                                            RecordingView,
                                            conversationId,
                                            ko.observable({}),
                                            {
                                                reloadCallback: reloadCallback,
                                                displayed: true,
                                            },
                                            self.conversation
                                        ),
                                    ]);
                                }

                                //since this really doesn't affect recordings, this is placed after the isLoading part
                                let recordingInformation = _.map(
                                    sortedModels,
                                    function (recordingModel, index) {
                                        return createRecordingInformation(
                                            recordingModel,
                                            recordingViews[index].displayed,
                                            recordingViews[index].isDeleted
                                        );
                                    }
                                );

                                // Add the email recording information
                                recordingInformation = recordingInformation.concat(
                                    _.map(emailModels, function (
                                        recordingModel
                                    ) {
                                        const isDeleted =
                                            recordingModel.fileState &&
                                            recordingModel.fileState.toLowerCase() ===
                                                "deleted";
                                        return createRecordingInformation(
                                            recordingModel,
                                            ko.observable(true),
                                            isDeleted
                                        );
                                    })
                                );

                                //There are many message recording models, but only one recordingView for them so don't index against recordingViews
                                recordingInformation = recordingInformation.concat(
                                    _.map(messageModels, function (
                                        recordingModel
                                    ) {
                                        const isDisplayed =
                                            messagesRecordingView.displayed;
                                        const isDeleted =
                                            recordingModel.fileState &&
                                            recordingModel.fileState.toLowerCase() ===
                                                "deleted";
                                        return createRecordingInformation(
                                            recordingModel,
                                            isDisplayed,
                                            isDeleted
                                        );
                                    })
                                );

                                self.conversationRecordingInformation(
                                    recordingInformation
                                );
                            }
                            return null;
                        })
                        .catch(function (error) {
                            self.recordingRequestError(true);
                            const httpStatus =
                                error.type === "http"
                                    ? error.details.status
                                    : undefined;
                            self.httpErrorStatus(httpStatus);

                            if (
                                !options ||
                                (options.isNotActive && options.isNotActive())
                            ) {
                                if (
                                    self.vmRecordingInformation &&
                                    self.vmRecordingInformation().length == 0
                                ) {
                                    //initialize empty recording
                                    self.conversationRecordingViews([
                                        self.subView(
                                            RecordingView,
                                            conversationId,
                                            ko.observable({}),
                                            {
                                                reloadCallback: reloadCallback,
                                                displayed: true,
                                            },
                                            self.conversation,
                                            0,
                                            self.recordingRequestError(),
                                            self.httpErrorStatus()
                                        ),
                                    ]);
                                }
                            }
                            if (httpStatus !== 404 && httpStatus !== 403) {
                                errorCallbackFactory.createCallback({
                                    namespace:
                                        "playback.recordingRequest.error.default",
                                })(error);
                                throw error;
                            }
                        })
                );
            }

            Promise.all(recordingPromises)
                .then(function () {
                    // resetting maxReload once recording load success
                    maxReloads = 3;
                })
                .catch(function () {
                    self.recordingRequestError(true);
                })
                .finally(function () {
                    recordingPromises = null;
                    const conversationRecordingViews = self.conversationRecordingViews();
                    const vmRecordingViews = self.vmRecordingViews();
                    self.recordingViews(
                        vmRecordingViews.concat(conversationRecordingViews)
                    );
                    if (self.isFromStalledEvent()) {
                        switchRecording(counter());
                    } else if (self.recordingViews().length > 0) {
                        switchRecording(0);
                    }

                    const conversationRecordingInformation = self.conversationRecordingInformation();
                    const vmRecordingInformation = self.vmRecordingInformation();
                    self.recordingInformation(
                        vmRecordingInformation.concat(
                            conversationRecordingInformation
                        )
                    );
                    if (!self.showLongRunningMessage()) {
                        self.isRecordingLoading(false);
                    }
                });
        };

        const subscribeToNotifications = function (topicCallbacks) {
            notificationService
                .batchSubscribe(topicCallbacks)
                .catch(
                    errorCallbackFactory.createCallback({
                        namespace: "playback.recordingNotifications",
                    })
                )
                .then(function () {
                    getRecordings(conversationId);

                    //Just in case we run long and then also never get a CP transcode complete event,
                    // set up a timer to clean up after a while
                    failsafeTimer = _.delay(function () {
                        self.isRecordingLoading(false);
                        self.recordingRequestError(true);
                        LOG.error(
                            "Transcode failsafe timer fired for conversation ",
                            {
                                conversation: self.conversationId(),
                            }
                        );
                    }, failsafeTimeoutMs);
                });
        };

        //The .then() following the notificationService.subscribe is not called on
        //a second pass (after receiving a transcode completed event)... skip the
        //subscribe in this case and just call getRecordings directly.
        if (
            (shouldSubscribe && qualityAuthorization.canViewRecording()) ||
            qualityAuthorization.canViewRecordingSegment()
        ) {
            let topicCallbacks;
            if (featureToggleService.isEnabled(FeatureToggle.QMHAWKTOPICSV2)) {
                topicCallbacks = {
                    [`v2.conversations.${conversationId}.recordings.upload`]: handleRecordingUpdates,
                    [`v2.users.${authService.principalId()}.conversations.${conversationId}.recordings`]: handleRecordingUpdates,
                    [`v2.users.${authService.principalId()}.recordings`]: handleRecordingUpdates,
                };
            } else {
                topicCallbacks = {
                    [`conversations.${conversationId}.recordings`]: handleRecordingUpdates,
                };
            }

            subscribeToNotifications(topicCallbacks);
        } else {
            getRecordings(conversationId);
        }
    };

    LOG.info(
        "Requesting recordings for conversation " +
            self.conversationId() +
            " on page load."
    );
    recordingItemContainer(self.conversationId(), true);

    const isLongRunning = function (recordingModels) {
        if (!recordingModels) {
            return true;
        }
        //If any are still not done, wait for the rest to finish
        let anyLongRunning = false;
        _.each(recordingModels, function (recordingModel) {
            if (
                recordingModel.estimatedTranscodeTimeMs &&
                !recordingModel.actualTranscodeTimeMs
            ) {
                anyLongRunning = true;
            }
        });
        return anyLongRunning;
    };

    const counter = ko.observable(0);
    const updateCounterDisplay = function (newIndex) {
        _.each(self.recordingViews(), function (view, index) {
            view.displayed(index === newIndex);
        });
        counter(newIndex);
    };

    const pauseCurrentRecording = function () {
        if (self.recordingViews()[counter()].pause) {
            self.recordingViews()[counter()].pause();
        }
    };

    const switchRecording = function (newIndex) {
        updateCounterDisplay(newIndex);
        self.recordingViews()[counter()].displayed(true);
        speechAndTextAnalyticsService.resetSentimentsList();
    };

    self.nextRecording = function () {
        pauseCurrentRecording();
        if (counter() < self.recordingViews().length - 1) {
            switchRecording(counter() + 1);
        }
    };

    self.previous = function () {
        pauseCurrentRecording();
        if (counter() > 0) {
            switchRecording(counter() - 1);
        }
    };

    self.callRecordingItemContainer = function (isActive, conversationId) {
        if (!isActive) {
            recordingItemContainer(conversationId, false);
        }
    };
    self.getRecording = function (index) {
        pauseCurrentRecording();
        if (self.recordingViews()[index]) {
            switchRecording(index);
        }
    };

    self.setConversation = function (conversation) {
        self.conversation(ko.unwrap(conversation));
    };

    self.getRecordingCount = function () {
        return counter() + 1;
    };

    self.getRecordingTotalCount = function () {
        return self.recordingViews().length;
    };

    self.hasPrevious = function () {
        return counter() > 0;
    };

    self.hasNext = function () {
        return counter() < self.recordingViews().length - 1;
    };

    self.selectedRecording = ko.computed(function () {
        if (self.recordingViews()) {
            if (self.recordingViews()[counter()]) {
                return self.recordingViews()[counter()];
            }
        }
        return null;
    });
    self.isAgedConversation = self.computed(function () {
        return self.conversation() && !self.conversation().id;
    });

    self.saveAnnotation = function () {
        if (self.recordingViews()[counter()]) {
            self.recordingViews()[counter()].saveAnnotation();
        }
    };

    self.hasRecordings = function () {
        return !_.isEmpty(self.recordingViews());
    };

    self.hasScreenRecordings = self.computed(function () {
        return self.screenRecordingDetailsView();
    });

    self.isDigitalMediaType = function () {
        const currentRecordingView = self.recordingViews()?.[counter()];
        return (
            self.recordingViews() &&
            self.recordingViews()[counter()] &&
            ((self.recordingViews()[counter()].hasEmail &&
                self.recordingViews()[coutner()].hasEmail()) ||
                self.recordingViews()[counter()].hasMessages ||
               (self.recordingViews()[counter()].hasChat &&
                  self.recordingViews()[counter()].hasChat().length > 0))
        );
    };

    self.shouldDisplayEmailRecordingInfo = function () {
        if (self.recordingViews() && self.recordingViews()[counter()]) {
            return self.recordingViews()[counter()].hasEmail();
        }
        return false;
    };

    // provide the currently active recording to caller if the callback was specified
    if (_.isFunction(selectedRecordingCallback)) {
        self.subscribe(self.selectedRecording, selectedRecordingCallback);
    }

    /**
     * CLEAN UP
     */
    self.onDispose = function () {
        clearTimeout(recordingRequestTimeout);
    };
});

export default RecordingManagerView;
