/*
 * Copyright 2015 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package com.amazonaws.dynamodb.bootstrap;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.fasterxml.jackson.annotation.JsonProperty;


/**
 * Mixin for attribute values to stay all capital when mapping them as strings.
 * 
 */
public abstract class AttributeValueMixIn {
        @JsonProperty("S") public abstract String getS();
        @JsonProperty("S") public abstract void setS(String s);
        @JsonProperty("N") public abstract String getN();
        @JsonProperty("N") public abstract void setN(String n);
        @JsonProperty("B") public abstract ByteBuffer getB();
        @JsonProperty("B") public abstract void setB(ByteBuffer b);
        @JsonProperty("NULL") public abstract Boolean isNULL();
        @JsonProperty("NULL") public abstract void setNULL(Boolean nU);
        @JsonProperty("BOOL") public abstract Boolean getBOOL();
        @JsonProperty("BOOL") public abstract void setBOOL(Boolean bO);
        @JsonProperty("SS") public abstract List<String> getSS();
        @JsonProperty("SS") public abstract void setSS(List<String> sS);
        @JsonProperty("NS") public abstract List<String> getNS();
        @JsonProperty("NS") public abstract void setNS(List<String> nS);
        @JsonProperty("BS") public abstract List<String> getBS();
        @JsonProperty("BS") public abstract void setBS(List<String> bS);
        @JsonProperty("M") public abstract Map<String, AttributeValue> getM();
        @JsonProperty("M") public abstract void setM(Map<String, AttributeValue> val);
        @JsonProperty("L") public abstract List<AttributeValue> getL();
        @JsonProperty("L") public abstract void setL(List<AttributeValue> val);
}
