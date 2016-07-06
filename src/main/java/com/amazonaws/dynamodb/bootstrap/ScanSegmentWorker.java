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

import java.util.concurrent.Callable;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.google.common.util.concurrent.RateLimiter;

/**
 * This class executes multiple scan requests on one segment of a table in
 * series, as a runnable. Instances meant to be used as tasks of the worker
 * thread pool for parallel scans.
 *
 */
public class ScanSegmentWorker implements Callable<SegmentedScanResult> {
    private final ScanRequest request;
    private boolean hasNext;
    private int lastConsumedCapacity;
    private long exponentialBackoffTime;
    private final AmazonDynamoDBClient client;
    private final RateLimiter rateLimiter;

    ScanSegmentWorker(final AmazonDynamoDBClient client,
            final RateLimiter rateLimiter, ScanRequest request) {
        this.request = request;
        this.client = client;
        this.rateLimiter = rateLimiter;
        this.hasNext = true;
        lastConsumedCapacity = 256;
    }

    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public SegmentedScanResult call() {
        ScanResult result = null;
        result = runWithBackoff();

        final ConsumedCapacity cc = result.getConsumedCapacity();

        if (cc != null && cc.getCapacityUnits() != null) {
            lastConsumedCapacity = result.getConsumedCapacity()
                    .getCapacityUnits().intValue();
        } else if (result.getScannedCount() != null && result.getCount() != null) {
            final boolean isConsistent = request.getConsistentRead();
            int itemSize = isConsistent ? BootstrapConstants.STRONGLY_CONSISTENT_READ_ITEM_SIZE
                    : BootstrapConstants.EVENTUALLY_CONSISTENT_READ_ITEM_SIZE;

            lastConsumedCapacity = (result.getScannedCount() / (int) Math.max(1.0, result.getCount()))
                    * (ItemSizeCalculator.calculateScanResultSizeInBytes(result) / itemSize);
        }

        if (result.getLastEvaluatedKey() != null
                && !result.getLastEvaluatedKey().isEmpty()) {
            hasNext = true;
            request.setExclusiveStartKey(result.getLastEvaluatedKey());
        } else {
            hasNext = false;
        }

        if (lastConsumedCapacity > 0) {
            rateLimiter.acquire(lastConsumedCapacity);
        }
        return new SegmentedScanResult(result, request.getSegment());
    }

    /**
     * begins a scan with an exponential back off if throttled.
     */
    public ScanResult runWithBackoff() {
        ScanResult result = null;
        boolean interrupted = false;
        exponentialBackoffTime = BootstrapConstants.INITIAL_RETRY_TIME_MILLISECONDS;
        try {
            do {
                try {
                    result = client.scan(request);
                } catch (Exception e) {
                    try {
                        Thread.sleep(exponentialBackoffTime);
                    } catch (InterruptedException ie) {
                        interrupted = true;
                    } finally {
                        exponentialBackoffTime = Math.min(
                            BootstrapConstants.MAX_EXPONENTIAL_BACKOFF_TIME,
                            exponentialBackoffTime * 2);
                    }
                    continue;
                }
            } while (result == null);
            return result;
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
