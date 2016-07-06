/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Amazon Software License (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/asl/
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.concurrent.Executor;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Class to execute a parallel scan on a DynamoDB table.
 */
public class DynamoDBTableScan {

    private final RateLimiter rateLimiter;
    private final AmazonDynamoDBClient client;

    /**
     * Initializes the RateLimiter and sets the AmazonDynamoDBClient.
     */
    public DynamoDBTableScan(double rateLimit, AmazonDynamoDBClient client) {
        rateLimiter = RateLimiter.create(rateLimit);
        this.client = client;
    }

    /**
     * This function copies a scan request for the number of segments and then
     * adds those workers to the executor service to begin scanning.
     *
     * @param totalSections
     * @param section
     *
     * @return <ParallelScanExecutor> the parallel scan executor to grab results
     *         when a segment is finished.
     */
    public ParallelScanExecutor getParallelScanCompletionService(
            ScanRequest initialRequest, int numSegments, Executor executor,
            int section, int totalSections) {
        final int segments = Math.max(1, numSegments);

        int sectionSize = segments / totalSections;
        int start = sectionSize * section;
        int end = start + sectionSize;
        if (section + 1 == totalSections) {
            end = segments;
        }
        final int numberToComplete = end - start;
        final ParallelScanExecutor completion = new ParallelScanExecutor(
                executor, segments, numberToComplete);

        for (int segment = start; segment < end; segment++) {
            ScanRequest scanSegment = copyScanRequest(initialRequest)
                    .withTotalSegments(segments).withSegment(segment);
            completion.addWorker(new ScanSegmentWorker(this.client,
                    this.rateLimiter, scanSegment), segment);
        }

        return completion;
    }

    public ScanRequest copyScanRequest(ScanRequest request) {
        return new ScanRequest()
                .withTableName(request.getTableName())
                .withTotalSegments(request.getTotalSegments())
                .withSegment(request.getSegment())
                .withReturnConsumedCapacity(request.getReturnConsumedCapacity())
                .withLimit(request.getLimit())
                .withConsistentRead(request.getConsistentRead());
    }
}
