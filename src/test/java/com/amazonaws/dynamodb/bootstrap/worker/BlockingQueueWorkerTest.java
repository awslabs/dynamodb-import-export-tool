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
package com.amazonaws.dynamodb.bootstrap.worker;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

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

import com.amazonaws.dynamodb.bootstrap.SegmentedScanResult;
import com.amazonaws.dynamodb.bootstrap.items.DynamoDBEntryWithSize;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Unit Tests for LogStashQueueWorker
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
        SegmentedScanResult segmentedScanResult = new SegmentedScanResult(mockResult, 0);
        BlockingQueue<DynamoDBEntryWithSize> queue = new ArrayBlockingQueue<DynamoDBEntryWithSize>(20);
        BlockingQueueWorker callable = new BlockingQueueWorker(queue, segmentedScanResult);
        List<Map<String, AttributeValue>> items = new LinkedList<Map<String, AttributeValue>>();

        Map<String, AttributeValue> sampleScanResult = new HashMap<String, AttributeValue>();
        sampleScanResult.put("sample key", new AttributeValue("sample attribute value"));
        items.add(sampleScanResult);

        expect(mockResult.getItems()).andReturn(items);

        replayAll();

        callable.call();

        verifyAll();

        assertEquals(1, queue.size());
        assertSame(sampleScanResult, queue.poll().getEntry());
    }

}
