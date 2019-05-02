/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import static org.junit.Assert.*;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.powermock.api.easymock.PowerMock.*;
import static org.easymock.EasyMock.expect;

import com.amazonaws.dynamodb.bootstrap.BlockingQueueConsumer;

/**
 * Unit Tests for LogStashExecutor
 * 
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

        expect(Executors.newFixedThreadPool(totalThreads)).andReturn(
                mockThreadPool);

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
