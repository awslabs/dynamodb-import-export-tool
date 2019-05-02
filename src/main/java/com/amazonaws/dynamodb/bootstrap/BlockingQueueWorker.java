/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.BlockingQueue;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * This class implements Callable, and when called iterates through it's
 * SegmentedScanResult, then pushes each item with it's size onto a blocking
 * queue.
 */
public class BlockingQueueWorker implements Callable<Void> {

    /**
     * Logger for the LogStashQueueWorker.
     */
    private static final Logger LOGGER = LogManager
            .getLogger(BlockingQueueWorker.class);

    private final BlockingQueue<DynamoDBEntryWithSize> queue;
    private final SegmentedScanResult result;

    public BlockingQueueWorker(BlockingQueue<DynamoDBEntryWithSize> queue,
            SegmentedScanResult result) {
        this.queue = queue;
        this.result = result;
    }

    @Override
    public Void call() {
        final ScanResult scanResult = result.getScanResult();
        final List<Map<String, AttributeValue>> items = scanResult.getItems();
        final Iterator<Map<String, AttributeValue>> it = items.iterator();
        boolean interrupted = false;
        try {
            do {
                try {
                    Map<String, AttributeValue> item = it.next();
                    DynamoDBEntryWithSize entryWithSize = new DynamoDBEntryWithSize(
                            item,
                            ItemSizeCalculator.calculateItemSizeInBytes(item));
                    queue.put(entryWithSize);
                } catch (InterruptedException e) {
                    interrupted = true;
                    LOGGER.warn("interrupted when writing item to queue: "
                            + e.getMessage());
                }
            } while (it.hasNext());
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        return null;
    }
}
