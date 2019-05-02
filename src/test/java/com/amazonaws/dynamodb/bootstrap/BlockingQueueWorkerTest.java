/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.powermock.api.easymock.PowerMock.*;
import static org.easymock.EasyMock.expect;

import com.amazonaws.dynamodb.bootstrap.BlockingQueueWorker;
import com.amazonaws.dynamodb.bootstrap.SegmentedScanResult;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Unit Tests for LogStashQueueWorker
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockingQueueWorker.class)
@PowerMockIgnore("javax.management.*")
public class BlockingQueueWorkerTest {
    
    /**
     * Test the initialization of a BlockingQueueWorker and make sure it places the items in the queue when called.
     */
    @Test
    public void testInitializationAndCall() {
        ScanResult mockResult = createMock(ScanResult.class);
        SegmentedScanResult segmentedScanResult = new SegmentedScanResult(
                mockResult, 0);
        BlockingQueue<DynamoDBEntryWithSize> queue = new ArrayBlockingQueue<DynamoDBEntryWithSize>(
                20);
        BlockingQueueWorker callable = new BlockingQueueWorker(queue,
                segmentedScanResult);
        List<Map<String, AttributeValue>> items = new LinkedList<Map<String, AttributeValue>>();

        Map<String, AttributeValue> sampleScanResult = new HashMap<String, AttributeValue>();
        sampleScanResult.put("sample key", new AttributeValue(
                "sample attribute value"));
        items.add(sampleScanResult);

        expect(mockResult.getItems()).andReturn(items);

        replayAll();

        callable.call();

        verifyAll();

        assertEquals(1, queue.size());
        assertSame(sampleScanResult, queue.poll().getEntry());
    }

}
