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

import static org.powermock.api.easymock.PowerMock.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Unit tests for DynamoDBConsumerWorker
 * 
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(DynamoDBConsumer.class)
@PowerMockIgnore("javax.management.*")
public class DynamoDBConsumerTest {

    /**
     * Test that a ScanResult splits into the correct number of batches.
     */
    @Test
    public void splitResultIntoBatchesTest() {
        final double numItems = 111.0;

        String tableName = "test tableName";

        ScanResult scanResult = new ScanResult();
        List<Map<String, AttributeValue>> items = new LinkedList<Map<String, AttributeValue>>();
        for (int i = 0; i < numItems; i++) {
            Map<String, AttributeValue> sampleScanResult = new HashMap<String, AttributeValue>();
            sampleScanResult.put("key", new AttributeValue("attribute value "
                    + i));
            items.add(sampleScanResult);
        }
        scanResult.setItems(items);

        SegmentedScanResult result = new SegmentedScanResult(scanResult, 0);

        replayAll();
        List<BatchWriteItemRequest> batches = DynamoDBConsumer
                .splitResultIntoBatches(result.getScanResult(), tableName);
        assertEquals(Math.ceil(numItems / BootstrapConstants.MAX_BATCH_SIZE_WRITE_ITEM),
                batches.size(), 0.0);

        verifyAll();
    }

}
