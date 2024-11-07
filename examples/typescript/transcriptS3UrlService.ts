import { apiClient, ApiResponse } from "@common/apiClient";
import { ErrorName } from "@common/error";
import {
    TranscriptProperty,
    TranscriptPropertyNew,
    TranscriptPropertyUrl
} from "@models/publicApi/transcriptProperty";
import _ from "lodash";

class TranscriptS3UrlService {
    /**
     * Get transcript pre-signed s3 URL for chat and message interactions.
     *
     * @param conversationId: string representing the conversation's Id
     * @param communicationId: string representing the communication's Id
     * @return s3 URL: string
     */
    public async getS3Url(conversationId: string, communicationId: string): Promise<string> {
        if (
            !(conversationId && communicationId) ||
            !conversationId.trim().length ||
            !communicationId.trim().length
        ) {
            throw new Error(
                `Issue getting transcript property: conversationId, and communicationId are required.`
            );
        }

        let transcriptProperty: TranscriptProperty | undefined = undefined;
        const api = `/api/v2/speechandtextanalytics/conversations/${conversationId}/communications/${communicationId}/transcriptUrl`;
        try {
            const apiResponse = await apiClient.get<TranscriptProperty>(
                api,
                `get_chat_message_transcript_s3_url/${conversationId}/communications/${communicationId}`
            );
            transcriptProperty = apiResponse.data;
            return transcriptProperty ? transcriptProperty.url || "" : "";
        } catch (err) {
            const e = err as ApiResponse<any>;
            const error = new Error(
                `Issue getting transcript property (Get ${api}): ${e.errorMessage}. Correlation ID: ${e.correlationId}`
            );
            if (e.status === 404) {
                error.name = ErrorName.notFound;
            }
            throw error;
        }
    }

    /**
     * Get transcript pre-signed s3 URL for audio interactions.
     *
     * @param conversationId: string representing the conversation's Id
     * @param communicationId: string representing the communication's Id
     * @param recordingId: string representing the recording's Id
     * @return s3 URL: string
     */
    public async getTranscriptS3Url(
        conversationId: string,
        communicationId: string,
        recordingId: string
    ): Promise<string> {
        if (
            !(conversationId && communicationId && recordingId) ||
            !conversationId.trim().length ||
            !communicationId.trim().length ||
            !recordingId.trim().length
        ) {
            throw new Error(
                `Issue getting transcript property: conversationId, communicationId and recordingId are required.`
            );
        }

        let urls: TranscriptPropertyUrl[] = [];
        let apiResponse: ApiResponse<TranscriptPropertyNew>;
        const api = `/api/v2/speechandtextanalytics/conversations/${conversationId}/communications/${communicationId}/transcriptUrls`;
        try {
            apiResponse = await apiClient.get<TranscriptPropertyNew>(
                api,
                `get_audio_transcript_s3_url/${conversationId}/communications/${communicationId}`
            );
            if (apiResponse.data && apiResponse.data.urls) {
                urls = apiResponse.data.urls;
            }
        } catch (err) {
            const e = err as ApiResponse<any>;
            const error = new Error(
                `Issue getting transcript property (Get ${api}): ${e.errorMessage}. Correlation ID: ${e.correlationId}.`
            );

            if (e.status === 403) {
                error.name = ErrorName.noTranscriptViewPerms;
            } else if (e.status === 404) {
                error.name = ErrorName.notFound;
            }
            throw error;
        }

        for (const url of urls) {
            if (url.recording && url.recording.id && url.recording.id === recordingId) {
                return url.url || "";
            }
        }
        let errMsg = `Could not find the transcript url with a matching recording Id ${recordingId}.`;
        if (apiResponse.correlationId) {
            errMsg += ` Correlation ID: ${apiResponse.correlationId}`;
        }
        const error = new Error(errMsg);
        error.name = ErrorName.notFound;
        throw error;
    }

    /**
     * Get pre-signed s3 urls of multiple transcripts for email interactions.
     *
     * @param listOfRequestUrls: list of urls to get pre-signed s3 urls of multiple transcripts
     * @return list of pre-signed s3 urls: string[]
     */
    public async getS3Urls(listOfRequestUrls: string[]): Promise<string[]> {
        const promises: Promise<string>[] = await _.map(listOfRequestUrls, async function(
            url
        ): Promise<string> {
            const apiResponse = await apiClient.get<TranscriptProperty>(
                url,
                `get_email_transcript_s3_urls`
            );
            const transcriptProperty = apiResponse.data;
            return transcriptProperty ? transcriptProperty.url || "" : "";
        });
        let fulfilledPromises: string[] = [];
        let rejectedPromises: any[] = []; //eslint-disable-line
        await this.allSettled(promises).then((results): void => {
            fulfilledPromises = results
                .filter((result): boolean => result.status === "fulfilled")
                .map((result): string => result.value);
            rejectedPromises = results
                .filter((result): boolean => result.status === "rejected")
                .map((result): string => result.value);
            if (rejectedPromises.length > 0) {
                const rejectedPromisesWithNotFoundError = rejectedPromises.filter(
                    (r): boolean => r.status === 404
                );
                const rejectedPromisesWithOtherErrors = rejectedPromises.filter(
                    (r): boolean => r.status !== 404
                );
                const error = new Error(`Issue in getting pre-signed s3 url`);
                if (rejectedPromisesWithOtherErrors.length > 0) {
                    throw error;
                }
                if (
                    rejectedPromisesWithNotFoundError.length > 0 &&
                    rejectedPromisesWithNotFoundError.length === listOfRequestUrls.length
                ) {
                    error.name = rejectedPromisesWithNotFoundError[0].status;
                    throw error;
                }
            }
        });
        return fulfilledPromises;
    }

    private allSettled(promises: Promise<string>[]): Promise<{ status: string; value: string }[]> {
        const wrappedPromises = promises.map(
            (promise): Promise<{ status: string; value: string }> =>
                Promise.resolve(promise).then(
                    (val): { status: string; value: string } => ({
                        status: "fulfilled",
                        value: val
                    }),
                    (err): { status: string; value: string } => ({
                        status: "rejected",
                        value: err
                    })
                )
        );
        return Promise.all(wrappedPromises);
    }
}

const transcriptS3UrlService = new TranscriptS3UrlService();
export { transcriptS3UrlService };
