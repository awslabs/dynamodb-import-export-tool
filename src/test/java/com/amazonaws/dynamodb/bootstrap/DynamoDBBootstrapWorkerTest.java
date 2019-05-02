/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import static org.easymock.EasyMock.expect;
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.createMock;

import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.powermock.api.easymock.PowerMock.*;

import com.amazonaws.dynamodb.bootstrap.exception.SectionOutOfRangeException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

/**
 * Unit Tests for DynamoDBBootstrapWorker
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DynamoDBBootstrapWorker.class)
@PowerMockIgnore("javax.management.*")
public class DynamoDBBootstrapWorkerTest {

    private static String tableName = "testTableName";
    private double rateLimit = 12.3;
    private static AmazonDynamoDBClient mockClient;
    private static ThreadPoolExecutor mockThreadPool;

    /**
     * Sets up the initialization of common mocks for the below tests.
     */
    private static void setupMockInitialization() throws Exception {
        mockClient = createMock(AmazonDynamoDBClient.class);
        mockThreadPool = createMock(ThreadPoolExecutor.class);
    }

    /**
     * Test the initialization of a DynamoDBBootstrapWorker and that it creates the necessary threadPool.
     */
    @Test
    public void testInitialization() throws Exception {
        setupMockInitialization();

        replayAll();

        new DynamoDBBootstrapWorker(mockClient, rateLimit, tableName,
                mockThreadPool, 0, 1, 10, false);

        verifyAll();
    }
    
    /**
     * Test the initialization of a DynamoDBBootstrapWorker with an invalid section.
     */
    @Test
    public void testInitializationInvalidSection() throws Exception {
        setupMockInitialization();

        replayAll();
        boolean exceptionThrown = false;

        try{
            new DynamoDBBootstrapWorker(mockClient, rateLimit, tableName,
                    mockThreadPool, 1, 1, 10, false);
        }catch (SectionOutOfRangeException e){
            exceptionThrown = true;
        }

        assertTrue(exceptionThrown);
        verifyAll();
    }

    /**
     * Test the shutdown of a DynamoDBBootstrapWorker without waiting for threads to complete.
     */
    @Test
    public void testShutdownWithoutWaiting() throws Exception {
        setupMockInitialization();

        expect(mockThreadPool.shutdownNow()).andReturn(null);

        replayAll();
        DynamoDBBootstrapWorker worker = new DynamoDBBootstrapWorker(
                mockClient, rateLimit, tableName, mockThreadPool, 0, 1, 10, false);
        worker.shutdown(false);

        verifyAll();
    }

}
