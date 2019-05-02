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

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Unit test for AttributeValueMixIn
 * 
 */
public class AttributeValueMixInTest {

    /**
     * A sample scan result with one item to use for tests.
     */
    public static List<Map<String, AttributeValue>> sampleScanResult() {
        List<Map<String, AttributeValue>> items = new LinkedList<Map<String, AttributeValue>>();
        Map<String, AttributeValue> sampleScanResult = new HashMap<String, AttributeValue>();
        sampleScanResult.put("key", new AttributeValue("attribute value"));
        items.add(sampleScanResult);

        return items;
    }

    /**
     * Test the Mixin to make sure that it capitalizes the values, and is
     * different from an ObjectMapper without the Mixin.
     */
    @Test
    public void testReturnsCapitalSWithMixin() throws JsonProcessingException {
        String capitalS = "S";
        String lowercaseS = "s";
        ObjectMapper mapperWith = new ObjectMapper();
        mapperWith.setSerializationInclusion(Include.NON_NULL);

        mapperWith.addMixInAnnotations(AttributeValue.class,
                AttributeValueMixIn.class);

        String withMixIn = mapperWith.writeValueAsString(sampleScanResult()
                .get(0));

        ObjectMapper mapperWithout = new ObjectMapper();
        mapperWithout.setSerializationInclusion(Include.NON_NULL);

        String withoutMixIn = mapperWithout
                .writeValueAsString(sampleScanResult().get(0));

        assertTrue(withMixIn.contains(capitalS));
        assertTrue(withoutMixIn.contains(lowercaseS));
    }

}
