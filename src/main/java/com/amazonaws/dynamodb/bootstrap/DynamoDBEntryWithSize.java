/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;

/**
 * Encapsulates DynamoDBEntry with the size of the entry.
 */
public class DynamoDBEntryWithSize {

    private Map<String, AttributeValue> entry;
    private int size;

    public DynamoDBEntryWithSize(Map<String, AttributeValue> entry, int size) {
        this.entry = entry;
        this.size = size;
    }

    public int getSize() {
        return size;
    }

    public Map<String, AttributeValue> getEntry() {
        return entry;
    }
}
