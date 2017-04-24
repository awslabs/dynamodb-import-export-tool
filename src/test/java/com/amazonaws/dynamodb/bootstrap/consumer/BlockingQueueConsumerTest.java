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
package com.amazonaws.dynamodb.bootstrap.consumer;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.mockStatic;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.dynamodb.bootstrap.items.DynamoDBEntryWithSize;

/**
 * Unit Tests for LogStashExecutor
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(BlockingQueueConsumer.class)
@PowerMockIgnore("javax.management.*")
public class BlockingQueueConsumerTest {

    int totalThreads = 8;

    /**
     * Test the initialization of a BlockingQueueConsumer and make sure it adds
     * the unique item to the end of the queue when shutting down.
     */
    @Test
    public void testInitializeAndShutdown() {
        BlockingQueueConsumer logExec = new BlockingQueueConsumer(totalThreads);
        mockStatic(Executors.class);
        ExecutorService mockThreadPool = createMock(ExecutorService.class);

        expect(Executors.newFixedThreadPool(totalThreads)).andReturn(mockThreadPool);

        BlockingQueue<DynamoDBEntryWithSize> queue = logExec.getQueue();

        assertNotNull(queue);
        assertEquals(queue.size(), 0);

        logExec.shutdown(true);
        assertEquals(queue.size(), 1);
        DynamoDBEntryWithSize poisonPill = queue.poll();
        assertNull(poisonPill.getEntry());
        assertEquals(-1, poisonPill.getSize());
    }

}
