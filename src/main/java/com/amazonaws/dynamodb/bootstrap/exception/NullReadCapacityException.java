/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap.exception;

/**
 * Read Capacity is null exception.
 */
public class NullReadCapacityException extends Exception {

    private static final long serialVersionUID = -4924652673622223172L;

    /**
     * Constructor calls superclass and nothing more.
     */
    public NullReadCapacityException(String s) {
        super(s);
    }
}
