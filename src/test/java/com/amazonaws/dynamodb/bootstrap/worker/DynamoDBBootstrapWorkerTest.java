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
import static org.junit.Assert.assertTrue;
import static org.powermock.api.easymock.PowerMock.createMock;
import static org.powermock.api.easymock.PowerMock.replayAll;
import static org.powermock.api.easymock.PowerMock.verifyAll;

import java.util.concurrent.ThreadPoolExecutor;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.dynamodb.bootstrap.exception.SectionOutOfRangeException;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;

/**
 * Unit Tests for DynamoDBBootstrapWorker
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

        new DynamoDBBootstrapWorker(mockClient, rateLimit, tableName, mockThreadPool, 0, 1, 10, false);

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

        try {
            new DynamoDBBootstrapWorker(mockClient, rateLimit, tableName, mockThreadPool, 1, 1, 10, false);
        } catch (SectionOutOfRangeException e) {
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
        DynamoDBBootstrapWorker worker = new DynamoDBBootstrapWorker(mockClient, rateLimit, tableName, mockThreadPool, 0, 1, 10, false);
        worker.shutdown(false);

        verifyAll();
    }

}
