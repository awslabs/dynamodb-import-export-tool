/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.expectNew;
import static org.powermock.api.easymock.PowerMock.mockStatic;
import static org.powermock.api.easymock.PowerMock.replay;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.util.concurrent.ExecutorService;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Unit Tests for DynamoDBTableScan
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest({ RateLimiter.class, DynamoDBTableScan.class })
@PowerMockIgnore("javax.management.*")
public class DynamoDBTableScanTest {

    private static String tableName = "testTableName";
    private static Integer totalSegments = 1;
    private static Integer segment = 0;
    private static ScanRequest req = new ScanRequest().withTableName(tableName)
            .withTotalSegments(totalSegments).withSegment(segment);
    private double rateLimit = 12.3;

    /**
     * Test the parallel executor completion service with multiple segments and
     * make sure it creates the correct number of segments
     */
    @Test
    public void testGetParallelExecutorCompletionServiceWithVariousNumberOfSegments()
            throws Exception {
        int segments = 0;
        ExecutorService mockExec = createMock(ExecutorService.class);
        mockStatic(RateLimiter.class);
        AmazonDynamoDBClient mockClient = createMock(AmazonDynamoDBClient.class);
        RateLimiter mockRateLimiter = createMock(RateLimiter.class);
        expect(RateLimiter.create(rateLimit)).andReturn(mockRateLimiter);

        replay(RateLimiter.class);
        DynamoDBTableScan scanner = new DynamoDBTableScan(rateLimit, mockClient);
        ParallelScanExecutor mockScanExecutor = createMock(ParallelScanExecutor.class);
        ScanSegmentWorker mockSegmentWorker = createMock(ScanSegmentWorker.class);

        expectNew(ScanSegmentWorker.class, mockClient, mockRateLimiter, req)
                .andReturn(mockSegmentWorker);
        expectNew(ParallelScanExecutor.class, mockExec, 1).andReturn(
                mockScanExecutor);

        mockScanExecutor.addWorker(mockSegmentWorker, 0);

        int segments2 = 3;
        ScanRequest testReq = scanner.copyScanRequest(req).withTotalSegments(
                segments2);
        expectNew(ParallelScanExecutor.class, mockExec, segments2).andReturn(
                mockScanExecutor);
        for (int i = 0; i < segments2; i++) {
            expectNew(ScanSegmentWorker.class, mockClient, mockRateLimiter,
                    scanner.copyScanRequest(testReq).withSegment(i)).andReturn(
                    mockSegmentWorker);
            mockScanExecutor.addWorker(mockSegmentWorker, i);
        }

        replayAll();
        scanner.getParallelScanCompletionService(req, segments, mockExec, 0, 1);
        scanner.getParallelScanCompletionService(req, segments2, mockExec, 0, 1);
        verifyAll();
    }

    /**
     * Test CopyScanRequest to copy the parts that we want.
     */
    @Test
    public void testCopyScanRequest() {
        DynamoDBTableScan scanner = new DynamoDBTableScan(rateLimit, null);

        ScanRequest copy = scanner.copyScanRequest(req);

        assertEquals(req.getTableName(), copy.getTableName());
        assertEquals(req.getTotalSegments(), copy.getTotalSegments());
        assertEquals(req.getSegment(), copy.getSegment());
    }

}
