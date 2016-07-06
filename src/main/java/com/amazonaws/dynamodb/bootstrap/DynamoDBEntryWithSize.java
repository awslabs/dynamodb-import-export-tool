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
