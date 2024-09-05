package com.cs643;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.rekognition.RekognitionClient;
import software.amazon.awssdk.services.rekognition.model.*;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.util.List;
import java.util.Map;

public class AWSDetection {

    public static void main(String[] args) {
        // Set up AWS clients and parameters
        String bucketName = "njit-cs-643";
        String queueName = "CS643862.fifo"; // -1 is the last one to get processed in the FIFO queue
        String queueGroup = "G1";

        S3Client s3Client = S3Client.builder()
                .region(Region.US_EAST_1)
                .build();
        RekognitionClient rekClient = RekognitionClient.builder()
                .region(Region.US_EAST_1)
                .build();
        SqsClient sqsClient = SqsClient.builder()
                .region(Region.US_EAST_1)
                .build();

        // Process images in the S3 bucket
        processBucketImages(s3Client, rekClient, sqsClient, bucketName, queueName, queueGroup);
    }

    /**
     * Process images in the specified S3 bucket.
     * Detects labels in the images and sends a message to an SQS queue if a "Car" label is detected.
     * @param s3 S3Client instance
     * @param rek RekognitionClient instance
     * @param sqs SqsClient instance
     * @param bucketName Name of the S3 bucket
     * @param queueName Name of the SQS queue
     * @param queueGroup Message group ID for FIFO queue
     */
    public static void processBucketImages(S3Client s3, RekognitionClient rek, SqsClient sqs, String bucketName,
                                           String queueName, String queueGroup) {
        // Create or retrieve the SQS queue URL
        String queueUrl = "";
        try {
            ListQueuesRequest listQueuesRequest = ListQueuesRequest.builder()
                    .queueNamePrefix(queueName)
                    .build();
            ListQueuesResponse listQueuesResponse = sqs.listQueues(listQueuesRequest);

            if (listQueuesResponse.queueUrls().isEmpty()) {
                CreateQueueRequest createQueueRequest = CreateQueueRequest.builder()
                        .attributesWithStrings(Map.of("FifoQueue", "true", "ContentBasedDeduplication", "true"))
                        .queueName(queueName)
                        .build();
                sqs.createQueue(createQueueRequest);

                GetQueueUrlRequest getQueueUrlRequest = GetQueueUrlRequest.builder()
                        .queueName(queueName)
                        .build();
                queueUrl = sqs.getQueueUrl(getQueueUrlRequest).queueUrl();
            } else {
                queueUrl = listQueuesResponse.queueUrls().get(0);
            }
        } catch (QueueNameExistsException e) {
            throw e;
        }

        // Process images in the S3 bucket
        try {
            ListObjectsV2Request listObjectsRequest = ListObjectsV2Request.builder()
                    .bucket(bucketName)
                    .maxKeys(10)
                    .build();
            ListObjectsV2Response listObjectsResponse = s3.listObjectsV2(listObjectsRequest);

            for (software.amazon.awssdk.services.s3.model.S3Object s3Object : listObjectsResponse.contents()) {
                System.out.println("Processing image in S3 bucket: " + s3Object.key());

                Image image = Image.builder()
                        .s3Object(software.amazon.awssdk.services.rekognition.model.S3Object.builder()
                                .bucket(bucketName)
                                .name(s3Object.key())
                                .build())
                        .build();
                DetectLabelsRequest detectLabelsRequest = DetectLabelsRequest.builder()
                        .image(image)
                        .minConfidence((float) 90)
                        .build();
                DetectLabelsResponse detectLabelsResponse = rek.detectLabels(detectLabelsRequest);
                List<Label> labels = detectLabelsResponse.labels();

                for (Label label : labels) {
                    if (label.name().equals("Car")) {
                        SendMessageRequest sendMessageRequest = SendMessageRequest.builder()
                                .queueUrl(queueUrl)
                                .messageGroupId(queueGroup)
                                .messageBody(s3Object.key())
                                .build();
                        sqs.sendMessage(sendMessageRequest);
                        break;
                    }
                }
            }

            // Signal the end of image processing by sending "-1" to the queue
            SendMessageRequest endMessageRequest = SendMessageRequest.builder()
                    .queueUrl(queueUrl)
                    .messageGroupId(queueGroup)
                    .messageBody("-1")
                    .build();
            sqs.sendMessage(endMessageRequest);
        } catch (Exception e) {
            System.err.println(e.getLocalizedMessage());
            System.exit(1);
        }
    }
}