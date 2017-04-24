/*
 * Copyright 2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndex;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.TableDescription;

import lombok.Builder;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;

/**
 * Created by amcp on 2017/04/23.
 */
@RequiredArgsConstructor
@Builder
public class TableDescriptionToCreateTableRequestConverter implements Function<TableDescription, CreateTableRequest> {

    @NonNull
    private final String newTableName;
    private final boolean createAllGsi;
    private final boolean createAllLsi;
    private final boolean copyStreamSpecification;
    @NonNull
    private final SortedSet<String> gsiToInclude;
    @NonNull
    private final SortedSet<String> lsiToInclude;

    static ProvisionedThroughput getProvisionedThroughputFromDescription(ProvisionedThroughputDescription description) {
        return new ProvisionedThroughput(description.getReadCapacityUnits(), description.getWriteCapacityUnits());
    }

    private static GlobalSecondaryIndex convertGlobalSecondaryIndexDescription(GlobalSecondaryIndexDescription d) {
        return new GlobalSecondaryIndex().withIndexName(d.getIndexName()).withKeySchema(d.getKeySchema()).withProjection(d.getProjection())
            .withProvisionedThroughput(getProvisionedThroughputFromDescription(d.getProvisionedThroughput()));
    }

    private static LocalSecondaryIndex convertLocalSecondaryIndexDescription(LocalSecondaryIndexDescription d) {
        return new LocalSecondaryIndex().withIndexName(d.getIndexName()).withKeySchema(d.getKeySchema()).withProjection(d.getProjection());
    }

    @Override
    public CreateTableRequest apply(TableDescription description) {
        final List<LocalSecondaryIndexDescription> lsiDesc = description.getLocalSecondaryIndexes();
        final List<LocalSecondaryIndex> lsi;
        if (lsiDesc == null || (!createAllLsi && lsiToInclude.isEmpty())) {
            lsi = null;
        } else {
            lsi = lsiDesc.stream().filter(l -> createAllLsi || lsiToInclude.contains(l.getIndexName()))
                .map(TableDescriptionToCreateTableRequestConverter::convertLocalSecondaryIndexDescription).collect(Collectors.toList());
        }

        final List<GlobalSecondaryIndexDescription> gsiDesc = description.getGlobalSecondaryIndexes();
        final List<GlobalSecondaryIndex> gsi;
        if (gsiDesc == null || (!createAllGsi && gsiToInclude.isEmpty())) {
            gsi = null;
        } else {
            gsi = gsiDesc.stream().filter(g -> createAllGsi || gsiToInclude.contains(g.getIndexName()))
                .map(TableDescriptionToCreateTableRequestConverter::convertGlobalSecondaryIndexDescription).collect(Collectors.toList());
        }

        ProvisionedThroughput pt = getProvisionedThroughputFromDescription(description.getProvisionedThroughput());
        CreateTableRequest ctr = new CreateTableRequest().withTableName(newTableName).withProvisionedThroughput(pt).withAttributeDefinitions(description.getAttributeDefinitions())
            .withKeySchema(description.getKeySchema()).withGlobalSecondaryIndexes(gsi).withLocalSecondaryIndexes(lsi);
        if (copyStreamSpecification) {
            ctr.withStreamSpecification(description.getStreamSpecification());
        }
        return ctr;
    }
}
