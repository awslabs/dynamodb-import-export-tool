/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap.exception;

/**
 * Section is out of valid range Exception.
 */
public class SectionOutOfRangeException extends Exception {

    private static final long serialVersionUID = -4924652673622223172L;

    /**
     * Constructor calls superclass and nothing more.
     */
    public SectionOutOfRangeException(String s) {
        super(s);
    }
}
