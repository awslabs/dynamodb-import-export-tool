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

import java.util.Optional;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.dynamodb.bootstrap.constants.BootstrapConstants;
import com.amazonaws.dynamodb.bootstrap.consumer.DynamoDBConsumer;
import com.amazonaws.dynamodb.bootstrap.exception.NullReadCapacityException;
import com.amazonaws.dynamodb.bootstrap.exception.SectionOutOfRangeException;
import com.amazonaws.dynamodb.bootstrap.worker.DynamoDBBootstrapWorker;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.DescribeTableRequest;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.waiters.WaiterParameters;
import com.amazonaws.waiters.WaiterTimedOutException;
import com.amazonaws.waiters.WaiterUnrecoverableException;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Preconditions;

import lombok.NonNull;
import lombok.extern.log4j.Log4j;

/**
 * The interface that parses the arguments, and begins to transfer data from one
 * DynamoDB table to another
 */
@Log4j
public class CommandLineInterface {

    public static final String ENCOUNTERED_EXCEPTION_WHEN_EXECUTING_TRANSFER = "Encountered exception when executing transfer.";

    static AwsClientBuilder.EndpointConfiguration createEndpointConfiguration(Region region, Optional<String> endpoint, String endpointPrefix) {
        return new AwsClientBuilder.EndpointConfiguration(endpoint.orElse("https://" + region.getServiceEndpoint(endpointPrefix)), region.getName());
    }

    @NonNull
    private final AwsClientBuilder.EndpointConfiguration sourceEndpointConfiguration;
    @NonNull
    private final String sourceTable;
    @NonNull
    private final AwsClientBuilder.EndpointConfiguration destinationEndpointConfiguration;
    @NonNull
    private final String destinationTable;
    private final double readThroughputRatio;
    private final double writeThroughputRatio;
    private final int maxWriteThreads;
    private final boolean isConsistentScan;
    private final boolean createDestinationTableIfMissing;
    private final boolean createAllGsi;
    private final boolean createAllLsi;
    @NonNull
    private final SortedSet<String> includeGsi;
    @NonNull
    private final SortedSet<String> includeLsi;
    private final boolean copyStreamSpecification;
    private final int sectionNumber;
    private final int totalSections;

    private CommandLineInterface(final CommandLineArgs params) {
        sourceEndpointConfiguration =
            createEndpointConfiguration(Region.getRegion(Regions.fromName(params.getSourceSigningRegion())), Optional.ofNullable(params.getSourceEndpoint()),
                AmazonDynamoDB.ENDPOINT_PREFIX);
        sourceTable = params.getSourceTable();
        destinationEndpointConfiguration =
            createEndpointConfiguration(Region.getRegion(Regions.fromName(params.getDestinationSigningRegion())), Optional.ofNullable(params.getDestinationEndpoint()),
                AmazonDynamoDB.ENDPOINT_PREFIX);
        destinationTable = params.getDestinationTable();
        readThroughputRatio = params.getReadThroughputRatio();
        writeThroughputRatio = params.getWriteThroughputRatio();
        maxWriteThreads = params.getMaxWriteThreads();
        isConsistentScan = params.isConsistentScan();
        createDestinationTableIfMissing = params.isCreateDestinationTableIfMissing();
        createAllGsi = params.isCreateAllGsi();
        createAllLsi = params.isCreateAllLsi();
        includeLsi = new TreeSet<>(params.getIncludeLsi());
        Preconditions.checkArgument(includeLsi.size() == params.getIncludeLsi().size(), "list of LSI names must be unique");
        includeGsi = new TreeSet<>(params.getIncludeGsi());
        Preconditions.checkArgument(includeGsi.size() == params.getIncludeGsi().size(), "list of GSI names must be unique");
        copyStreamSpecification = params.isCopyStreamSpecification();
        sectionNumber = params.getSection();
        totalSections = params.getTotalSections();
    }

    private void bootstrapTable() throws InterruptedException, ExecutionException, SectionOutOfRangeException {
        final ClientConfiguration config = new ClientConfiguration().withMaxConnections(BootstrapConstants.MAX_CONN_SIZE);

        final DefaultAWSCredentialsProviderChain credentials = new DefaultAWSCredentialsProviderChain();
        final AmazonDynamoDB sourceClient =
            AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(sourceEndpointConfiguration).withCredentials(credentials).withClientConfiguration(config).build();
        final AmazonDynamoDB destinationClient =
            AmazonDynamoDBClientBuilder.standard().withEndpointConfiguration(destinationEndpointConfiguration).withCredentials(credentials).withClientConfiguration(config).build();

        final TableDescription readTableDescription = sourceClient.describeTable(sourceTable).getTable();
        try {
            destinationClient.describeTable(destinationTable).getTable();
        } catch (ResourceNotFoundException e) {
            if (!createDestinationTableIfMissing) {
                throw new IllegalArgumentException("Destination table " + destinationTable + " did not exist", e);
            }
            try {
                final TableDescriptionToCreateTableRequestConverter converter =
                    TableDescriptionToCreateTableRequestConverter.builder().copyStreamSpecification(copyStreamSpecification).newTableName(destinationTable)
                        .createAllGsi(createAllGsi).gsiToInclude(includeGsi).createAllLsi(createAllLsi).lsiToInclude(includeLsi).build();
                destinationClient.createTable(converter.apply(readTableDescription));
                destinationClient.waiters().tableExists().run(new WaiterParameters<>(new DescribeTableRequest(destinationTable)));
            } catch (WaiterUnrecoverableException | WaiterTimedOutException | AmazonServiceException ase) {
                throw new IllegalArgumentException("Unable to create destination table", ase);
            }
        }

        final TableDescription writeTableDescription = destinationClient.describeTable(destinationTable).getTable();

        final int numSegments;
        try {
            numSegments = DynamoDBBootstrapWorker.estimateNumberOfSegments(readTableDescription);
        } catch (NullReadCapacityException e) {
            throw new IllegalStateException("All tables should have a read capacity set", e);
        }

        final double readThroughput = calculateThroughput(readTableDescription, readThroughputRatio, true);
        final double writeThroughput = calculateThroughput(writeTableDescription, writeThroughputRatio, false);

        final ExecutorService sourceExec = getThreadPool(numSegments);
        final DynamoDBBootstrapWorker worker =
            new DynamoDBBootstrapWorker(sourceClient, readThroughput, sourceTable, sourceExec, sectionNumber, totalSections, numSegments, isConsistentScan);

        final ExecutorService destinationExec = getThreadPool(maxWriteThreads);
        final DynamoDBConsumer consumer = new DynamoDBConsumer(destinationClient, destinationTable, writeThroughput, destinationExec);

        log.info("Starting transfer.");
        worker.pipe(consumer);
        log.info("Finished transfer.");
    }

    /**
     * Main class to begin transferring data from one DynamoDB table to another
     * DynamoDB table.
     *
     * @param args
     */
    public static void main(String[] args) {
        CommandLineArgs params = new CommandLineArgs();
        JCommander cmd = new JCommander(params);

        try {
            // parse given arguments
            cmd.parse(args);
        } catch (ParameterException e) {
            log.error(e);
            cmd.usage();
            System.exit(1);
        }

        // show usage information if help flag exists
        if (params.getHelp()) {
            cmd.usage();
            System.exit(0);
        }

        final CommandLineInterface cli = new CommandLineInterface(params);
        try {
            cli.bootstrapTable();
        } catch (InterruptedException e) {
            log.error("Interrupted when executing transfer.", e);
            System.exit(1);
        } catch (SectionOutOfRangeException e) {
            log.error("Invalid section parameter", e);
            System.exit(1);
        } catch (Exception e) { //coalesces ExecutionException
            log.error(ENCOUNTERED_EXCEPTION_WHEN_EXECUTING_TRANSFER, e);
            System.exit(1);
        }
    }

    /**
     * returns the provisioned throughput based on the input ratio and the
     * specified DynamoDB table provisioned throughput.
     */
    private static double calculateThroughput(TableDescription tableDescription, double throughputRatio, boolean read) {
        if (read) {
            return tableDescription.getProvisionedThroughput().getReadCapacityUnits() * throughputRatio;
        }
        return tableDescription.getProvisionedThroughput().getWriteCapacityUnits() * throughputRatio;
    }

    /**
     * Returns the thread pool for the destination DynamoDB table.
     */
    private static ExecutorService getThreadPool(int maxWriteThreads) {
        int corePoolSize = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_CORE_POOL_SIZE;
        if (corePoolSize > maxWriteThreads) {
            corePoolSize = maxWriteThreads - 1;
        }
        final long keepAlive = BootstrapConstants.DYNAMODB_CLIENT_EXECUTOR_KEEP_ALIVE;
        ExecutorService exec = new ThreadPoolExecutor(corePoolSize, maxWriteThreads, keepAlive, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>(maxWriteThreads),
            new ThreadPoolExecutor.CallerRunsPolicy());
        return exec;
    }

}
