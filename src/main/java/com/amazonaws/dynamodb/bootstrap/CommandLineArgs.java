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

import java.util.List;

import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.beust.jcommander.Parameter;

import lombok.Getter;

/**
 * This class contains the parameters to input when executing the program from
 * command line.
 */
@Getter
public class CommandLineArgs {
    public static final String HELP = "--help";
    @Parameter(names = HELP, description = "Display usage information", help = true)
    private boolean help;

    public boolean getHelp() {
        return help;
    }

    public static final String SOURCE_ENDPOINT = "--sourceEndpoint";
    @Parameter(names = SOURCE_ENDPOINT, description = "Endpoint of the source table")
    private String sourceEndpoint;

    public static final String SOURCE_SIGNING_REGION = "--sourceSigningRegion";
    @Parameter(names = SOURCE_SIGNING_REGION, description = "Signing region for the source endpoint", required = true)
    private String sourceSigningRegion;

    public static final String SOURCE_TABLE = "--sourceTable";
    @Parameter(names = SOURCE_TABLE, description = "Name of the source table", required = true)
    private String sourceTable;

    public static final String DESTINATION_ENDPOINT = "--destinationEndpoint";
    @Parameter(names = DESTINATION_ENDPOINT, description = "Endpoint of the destination table")
    private String destinationEndpoint;

    public static final String DESTINATION_SIGNING_REGION = "--destinationSigningRegion";
    @Parameter(names = DESTINATION_SIGNING_REGION, description = "Signing region for the destination endpoint", required = true)
    private String destinationSigningRegion;

    public static final String DESTINATION_TABLE = "--destinationTable";
    @Parameter(names = DESTINATION_TABLE, description = "Name of the destination table", required = true)
    private String destinationTable;

    public static final String CREATE_DESTINATION_TABLE_IF_MISSING = "--createDestination";
    @Parameter(names = CREATE_DESTINATION_TABLE_IF_MISSING, description = "Create destination table if it does not exist")
    private boolean createDestinationTableIfMissing;

    public static final String CREATE_ALL_LSI = "--createAllLsi";
    @Parameter(names = CREATE_ALL_LSI, description = "Create all LSI in destination table")
    private boolean createAllLsi;

    public static final String CREATE_ALL_GSI = "--createAllGsi";
    @Parameter(names = CREATE_ALL_GSI, description = "Create all GSI in destination table")
    private boolean createAllGsi;

    public static final String INCLUDE_LSI = "--includeLsi";
    @Parameter(names = INCLUDE_LSI, description = "Include the following LSI in the destination table")
    private List<String> includeLsi;

    public static final String INCLUDE_GSI = "--includeGsi";
    @Parameter(names = INCLUDE_GSI, description = "Include the following GSI in the destination table")
    private List<String> includeGsi;

    public static final String COPY_STREAM_SPECIFICATION_WHEN_CREATING = "--copyStreamSpecificationWhenCreating";
    @Parameter(names = COPY_STREAM_SPECIFICATION_WHEN_CREATING, description = "Use the source table stream specification for the destination table during its creation.")
    private boolean copyStreamSpecification;

    public static final String READ_THROUGHPUT_RATIO = "--readThroughputRatio";
    @Parameter(names = READ_THROUGHPUT_RATIO, description = "Percentage of total read throughput to scan the source table", required = true)
    private double readThroughputRatio;

    public static final String WRITE_THROUGHPUT_RATIO = "--writeThroughputRatio";
    @Parameter(names = WRITE_THROUGHPUT_RATIO, description = "Percentage of total write throughput to write the destination table", required = true)
    private double writeThroughputRatio;

    public static final String MAX_WRITE_THREADS = "--maxWriteThreads";
    @Parameter(names = MAX_WRITE_THREADS, description = "Number of max threads to write to destination table", required = false)
    private int maxWriteThreads = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_MAX_POOL_SIZE;

    public static final String TOTAL_SECTIONS = "--totalSections";
    @Parameter(names = TOTAL_SECTIONS, description = "Total number of sections to divide the scan into", required = false)
    private int totalSections = 1;

    public static final String SECTION = "--section";
    @Parameter(names = SECTION, description = "Section number to scan when running multiple programs concurrently [0, 1... totalSections-1]", required = false)
    private int section = 0;

    public static final String CONSISTENT_SCAN = "--consistentScan";
    @Parameter(names = CONSISTENT_SCAN, description = "Use this flag to use strongly consistent scan. If the flag is not used it will default to eventually consistent scan")
    private boolean consistentScan = false;
}
