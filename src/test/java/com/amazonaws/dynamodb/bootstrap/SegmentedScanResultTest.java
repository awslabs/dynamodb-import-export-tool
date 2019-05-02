/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import static org.junit.Assert.*;

import org.junit.Test;

import com.amazonaws.dynamodb.bootstrap.SegmentedScanResult;
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
        SegmentedScanResult segmentedScanResult = new SegmentedScanResult(
                result, numSegments);

        assertSame(result, segmentedScanResult.getScanResult());
        assertEquals(numSegments, segmentedScanResult.getSegment());
    }

}
