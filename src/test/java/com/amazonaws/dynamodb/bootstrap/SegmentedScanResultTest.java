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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Unit Tests for SegmentedScanResult
 */
public class SegmentedScanResultTest {

    /**
     * Test the getters and constructor of segmented scan result.
     */
    @Test
    public void test() {
        ScanResult result = new ScanResult();
        int numSegments = 3;
        SegmentedScanResult segmentedScanResult = new SegmentedScanResult(result, numSegments);

        assertSame(result, segmentedScanResult.getScanResult());
        assertEquals(numSegments, segmentedScanResult.getSegment());
    }

}
