/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import com.amazonaws.services.dynamodbv2.model.ScanResult;

/**
 * Encapsulates segment number in scan result
 * 
 */
public class SegmentedScanResult {
    private final ScanResult result;
    private final int segment;

    public SegmentedScanResult(ScanResult result, int segment) {
        this.result = result;
        this.segment = segment;
    }

    public ScanResult getScanResult() {
        return result;
    }

    public int getSegment() {
        return segment;
    }
}
