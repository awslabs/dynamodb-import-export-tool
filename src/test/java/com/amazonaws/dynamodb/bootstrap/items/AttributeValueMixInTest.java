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
package com.amazonaws.dynamodb.bootstrap.items;

import static org.junit.Assert.assertTrue;

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

        mapperWith.addMixIn(AttributeValue.class, AttributeValueMixIn.class);

        String withMixIn = mapperWith.writeValueAsString(sampleScanResult().get(0));

        ObjectMapper mapperWithout = new ObjectMapper();
        mapperWithout.setSerializationInclusion(Include.NON_NULL);

        String withoutMixIn = mapperWithout.writeValueAsString(sampleScanResult().get(0));

        assertTrue(withMixIn.contains(capitalS));
        assertTrue(withoutMixIn.contains(lowercaseS));
    }

}
