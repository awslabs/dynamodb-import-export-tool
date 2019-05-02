/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Class used to calculate the size of a DynamoDB item in bytes.
 * 
 */
public class ItemSizeCalculator {

    /**
     * Calculate DynamoDB item size.
     */
    public static int calculateItemSizeInBytes(Map<String, AttributeValue> item) {
        int size = 0;

        if (item == null) {
            return size;
        }

        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            String name = entry.getKey();
            AttributeValue value = entry.getValue();
            size += name.getBytes(BootstrapConstants.UTF8).length;
            size += calculateAttributeSizeInBytes(value);
        }
        return size;
    }
    
    public static int calculateScanResultSizeInBytes(ScanResult result) {
        final Iterator<Map<String, AttributeValue>> it = result.getItems().iterator();
        int totalBytes = 0;
        while(it.hasNext()){
            totalBytes += calculateItemSizeInBytes(it.next());
        }
        return totalBytes;
    }

    /** Calculate attribute value size */
    private static int calculateAttributeSizeInBytes(AttributeValue value) {
        int attrValSize = 0;
        if (value == null) {
            return attrValSize;
        }

        if (value.getB() != null) {
            ByteBuffer b = value.getB();
            attrValSize += b.remaining();
        } else if (value.getS() != null) {
            String s = value.getS();
            attrValSize += s.getBytes(BootstrapConstants.UTF8).length;
        } else if (value.getN() != null) {
            attrValSize += BootstrapConstants.MAX_NUMBER_OF_BYTES_FOR_NUMBER;
        } else if (value.getBS() != null) {
            List<ByteBuffer> bs = value.getBS();
            for (ByteBuffer b : bs) {
                if (b != null) {
                    attrValSize += b.remaining();
                }
            }
        } else if (value.getSS() != null) {
            List<String> ss = value.getSS();
            for (String s : ss) {
                if (s != null) {
                    attrValSize += s.getBytes(BootstrapConstants.UTF8).length;
                }
            }
        } else if (value.getNS() != null) {
            List<String> ns = value.getNS();
            for (String n : ns) {
                if (n != null) {
                    attrValSize += BootstrapConstants.MAX_NUMBER_OF_BYTES_FOR_NUMBER;
                }
            }
        } else if (value.getBOOL() != null) {
            attrValSize += 1;
        } else if (value.getNULL() != null) {
            attrValSize += 1;
        } else if (value.getM() != null) {
            for (Map.Entry<String, AttributeValue> entry : value.getM()
                    .entrySet()) {
                attrValSize += entry.getKey().getBytes(BootstrapConstants.UTF8).length;
                attrValSize += calculateAttributeSizeInBytes(entry.getValue());
                attrValSize += BootstrapConstants.BASE_LOGICAL_SIZE_OF_NESTED_TYPES;
            }
            attrValSize += BootstrapConstants.LOGICAL_SIZE_OF_EMPTY_DOCUMENT;
        } else if (value.getL() != null) {
            List<AttributeValue> list = value.getL();
            for (Integer i = 0; i < list.size(); i++) {
                attrValSize += calculateAttributeSizeInBytes(list.get(i));
                attrValSize += BootstrapConstants.BASE_LOGICAL_SIZE_OF_NESTED_TYPES;
            }
            attrValSize += BootstrapConstants.LOGICAL_SIZE_OF_EMPTY_DOCUMENT;
        }
        return attrValSize;
    }
}
