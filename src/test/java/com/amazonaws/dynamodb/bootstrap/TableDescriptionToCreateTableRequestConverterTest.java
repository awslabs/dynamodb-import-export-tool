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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

import org.junit.Test;

import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.GlobalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.LocalSecondaryIndexDescription;
import com.amazonaws.services.dynamodbv2.model.Projection;
import com.amazonaws.services.dynamodbv2.model.ProjectionType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughputDescription;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.StreamSpecification;
import com.amazonaws.services.dynamodbv2.model.StreamViewType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.google.common.collect.Lists;

/**
 * Created by amcp on 2017/04/23.
 */
public class TableDescriptionToCreateTableRequestConverterTest {

    public static final String ORDER_ID = "order_id";
    public static final String CUSTOMER_ID = "customer_id";
    public static final String INPUT_TABLE = "InputTable";
    public static final String OUTPUT_TABLE = "OutputTable";
    public static final ProvisionedThroughput DEFAULT_PT = new ProvisionedThroughput(1L, 4L);


    static class NoLsiNoGsi {
        static final List<AttributeDefinition> attributeDefinitionList =
            Lists.newArrayList(new AttributeDefinition(ORDER_ID, ScalarAttributeType.S), new AttributeDefinition(CUSTOMER_ID, ScalarAttributeType.S));
        static final List<KeySchemaElement> keySchemata = Lists.newArrayList(new KeySchemaElement(CUSTOMER_ID, KeyType.HASH), new KeySchemaElement(ORDER_ID, KeyType.RANGE));
        static final ProvisionedThroughputDescription pt = new ProvisionedThroughputDescription().withReadCapacityUnits(1L).withWriteCapacityUnits(4L);
        static final TableDescription description =
            new TableDescription().withAttributeDefinitions(NoLsiNoGsi.attributeDefinitionList).withKeySchema(NoLsiNoGsi.keySchemata).withProvisionedThroughput(NoLsiNoGsi.pt)
                .withTableName(INPUT_TABLE);
    }

    @Test
    public void apply_whenNoGsiButIncludeAllGsi_andNoLsiButIncludeAllLsi_andNoStreamButIncludeStream() {
        TableDescriptionToCreateTableRequestConverter converter =
            TableDescriptionToCreateTableRequestConverter.builder().gsiToInclude(new TreeSet<>()).lsiToInclude(new TreeSet<>()).newTableName(OUTPUT_TABLE).createAllGsi(true)
                .createAllLsi(true).copyStreamSpecification(true).build();
        CreateTableRequest ctr = converter.apply(NoLsiNoGsi.description);
        assertEquals(OUTPUT_TABLE, ctr.getTableName());
        assertEquals(DEFAULT_PT, ctr.getProvisionedThroughput());
        assertEquals(NoLsiNoGsi.attributeDefinitionList, NoLsiNoGsi.description.getAttributeDefinitions());
        assertEquals(NoLsiNoGsi.keySchemata, NoLsiNoGsi.description.getKeySchema());
        assertNull(ctr.getLocalSecondaryIndexes());
        assertNull(ctr.getGlobalSecondaryIndexes());
        assertNull(ctr.getStreamSpecification());
    }

    @Test
    public void apply_whenNoGsiButDontIncludeAllGsi_andNoLsiButDontIncludeAllLsi_andNoStreamButDontIncludeStream() {
        TableDescriptionToCreateTableRequestConverter converter =
            TableDescriptionToCreateTableRequestConverter.builder().gsiToInclude(new TreeSet<>()).lsiToInclude(new TreeSet<>()).newTableName(OUTPUT_TABLE).createAllGsi(false)
                .createAllLsi(false).copyStreamSpecification(false).build();
        CreateTableRequest ctr = converter.apply(NoLsiNoGsi.description);
        assertEquals(OUTPUT_TABLE, ctr.getTableName());
        assertEquals(DEFAULT_PT, ctr.getProvisionedThroughput());
        assertEquals(NoLsiNoGsi.attributeDefinitionList, NoLsiNoGsi.description.getAttributeDefinitions());
        assertEquals(NoLsiNoGsi.keySchemata, NoLsiNoGsi.description.getKeySchema());
        assertNull(ctr.getLocalSecondaryIndexes());
        assertNull(ctr.getGlobalSecondaryIndexes());
        assertNull(ctr.getStreamSpecification());
    }

    private static final String GSI_NAME_ONE = "gsi1";
    private static final String GSI_NAME_TWO = "gsi2";
    private static final String LSI_NAME_ONE = "lsi1";
    private static final String LSI_NAME_TWO = "lsi2";
    private static final String ORDER_TS_MILLIS = "order_ts_millis";
    private static final String ORDER_DATE = "order_date";


    static class TwoLsiTwoGsiStream {
        static final List<AttributeDefinition> attributeDefinitionList = Lists
            .newArrayList(new AttributeDefinition(ORDER_ID, ScalarAttributeType.S), new AttributeDefinition(CUSTOMER_ID, ScalarAttributeType.S),
                new AttributeDefinition(ORDER_DATE, ScalarAttributeType.N), new AttributeDefinition(ORDER_TS_MILLIS, ScalarAttributeType.N));
        static final List<KeySchemaElement> baseKeySchema = Lists.newArrayList(new KeySchemaElement(CUSTOMER_ID, KeyType.HASH), new KeySchemaElement(ORDER_ID, KeyType.RANGE));
        static final List<KeySchemaElement> gsiKeySchema = Lists.newArrayList(new KeySchemaElement(ORDER_DATE, KeyType.HASH), new KeySchemaElement(ORDER_TS_MILLIS, KeyType.RANGE));
        static final List<KeySchemaElement> lsiKeySchema =
            Lists.newArrayList(new KeySchemaElement(CUSTOMER_ID, KeyType.HASH), new KeySchemaElement(ORDER_TS_MILLIS, KeyType.RANGE));
        static final ProvisionedThroughputDescription pt = new ProvisionedThroughputDescription().withReadCapacityUnits(1L).withWriteCapacityUnits(4L);
        static final StreamSpecification streamSpecification = new StreamSpecification().withStreamEnabled(true).withStreamViewType(StreamViewType.NEW_AND_OLD_IMAGES);
        static final TableDescription description = new TableDescription().withAttributeDefinitions(attributeDefinitionList).withKeySchema(baseKeySchema)
            .withGlobalSecondaryIndexes(new GlobalSecondaryIndexDescription().withIndexName(GSI_NAME_ONE).withKeySchema(gsiKeySchema).withProvisionedThroughput(pt)
                    .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY)),
                new GlobalSecondaryIndexDescription().withIndexName(GSI_NAME_TWO).withKeySchema(gsiKeySchema).withProvisionedThroughput(pt)
                    .withProjection(new Projection().withProjectionType(ProjectionType.ALL))).withLocalSecondaryIndexes(
                new LocalSecondaryIndexDescription().withIndexName(LSI_NAME_ONE).withKeySchema(lsiKeySchema)
                    .withProjection(new Projection().withProjectionType(ProjectionType.KEYS_ONLY)),
                new LocalSecondaryIndexDescription().withIndexName(LSI_NAME_TWO).withKeySchema(lsiKeySchema)
                    .withProjection(new Projection().withProjectionType(ProjectionType.ALL))).withStreamSpecification(streamSpecification).withProvisionedThroughput(pt)
            .withTableName(INPUT_TABLE);
    }

    @Test
    public void apply_whenTwoGsiAndIncludeAllGsi_andTwoLsiAndIncludeAllLsi_andStreamAndIncludeStream() {
        TableDescriptionToCreateTableRequestConverter converter =
            TableDescriptionToCreateTableRequestConverter.builder().gsiToInclude(new TreeSet<>()).lsiToInclude(new TreeSet<>()).newTableName(OUTPUT_TABLE).createAllGsi(true)
                .createAllLsi(true).copyStreamSpecification(true).build();
        CreateTableRequest ctr = converter.apply(TwoLsiTwoGsiStream.description);
        assertEquals(OUTPUT_TABLE, ctr.getTableName());
        assertEquals(DEFAULT_PT, ctr.getProvisionedThroughput());
        assertEquals(TwoLsiTwoGsiStream.attributeDefinitionList, TwoLsiTwoGsiStream.description.getAttributeDefinitions());
        assertEquals(TwoLsiTwoGsiStream.baseKeySchema, TwoLsiTwoGsiStream.description.getKeySchema());

        //LSI
        assertNotNull(ctr.getLocalSecondaryIndexes());
        assertFalse(ctr.getLocalSecondaryIndexes().isEmpty());
        assertEquals(2, ctr.getLocalSecondaryIndexes().size());
        assertEquals(TwoLsiTwoGsiStream.lsiKeySchema, ctr.getLocalSecondaryIndexes().get(0).getKeySchema());
        assertEquals(ProjectionType.KEYS_ONLY.toString(), ctr.getLocalSecondaryIndexes().get(0).getProjection().getProjectionType());
        assertEquals(LSI_NAME_ONE, ctr.getLocalSecondaryIndexes().get(0).getIndexName());
        assertEquals(TwoLsiTwoGsiStream.lsiKeySchema, ctr.getLocalSecondaryIndexes().get(1).getKeySchema());
        assertEquals(ProjectionType.ALL.toString(), ctr.getLocalSecondaryIndexes().get(1).getProjection().getProjectionType());
        assertEquals(LSI_NAME_TWO, ctr.getLocalSecondaryIndexes().get(1).getIndexName());

        //GSI
        assertNotNull(ctr.getGlobalSecondaryIndexes());
        assertFalse(ctr.getGlobalSecondaryIndexes().isEmpty());
        assertEquals(2, ctr.getGlobalSecondaryIndexes().size());
        assertEquals(TwoLsiTwoGsiStream.gsiKeySchema, ctr.getGlobalSecondaryIndexes().get(0).getKeySchema());
        assertEquals(ProjectionType.KEYS_ONLY.toString(), ctr.getGlobalSecondaryIndexes().get(0).getProjection().getProjectionType());
        assertEquals(GSI_NAME_ONE, ctr.getGlobalSecondaryIndexes().get(0).getIndexName());
        assertEquals(DEFAULT_PT, ctr.getGlobalSecondaryIndexes().get(0).getProvisionedThroughput());
        assertEquals(TwoLsiTwoGsiStream.gsiKeySchema, ctr.getGlobalSecondaryIndexes().get(1).getKeySchema());
        assertEquals(ProjectionType.ALL.toString(), ctr.getGlobalSecondaryIndexes().get(1).getProjection().getProjectionType());
        assertEquals(GSI_NAME_TWO, ctr.getGlobalSecondaryIndexes().get(1).getIndexName());
        assertEquals(DEFAULT_PT, ctr.getGlobalSecondaryIndexes().get(1).getProvisionedThroughput());

        //STREAM SPECIFICATION
        assertNotNull(ctr.getStreamSpecification());
        assertTrue(ctr.getStreamSpecification().getStreamEnabled());
        assertEquals(StreamViewType.NEW_AND_OLD_IMAGES.toString(), ctr.getStreamSpecification().getStreamViewType());
    }

    @Test
    public void apply_whenTwoGsiAndDontIncludeAllGsi_andTwoLsiAndDontIncludeAllLsi_andStreamAndDontIncludeStream() {
        TableDescriptionToCreateTableRequestConverter converter =
            TableDescriptionToCreateTableRequestConverter.builder().gsiToInclude(new TreeSet<>()).lsiToInclude(new TreeSet<>()).newTableName(OUTPUT_TABLE).createAllGsi(false)
                .createAllLsi(false).copyStreamSpecification(false).build();
        CreateTableRequest ctr = converter.apply(TwoLsiTwoGsiStream.description);
        assertEquals(OUTPUT_TABLE, ctr.getTableName());
        assertEquals(DEFAULT_PT, ctr.getProvisionedThroughput());
        assertEquals(TwoLsiTwoGsiStream.attributeDefinitionList, TwoLsiTwoGsiStream.description.getAttributeDefinitions());
        assertEquals(TwoLsiTwoGsiStream.baseKeySchema, TwoLsiTwoGsiStream.description.getKeySchema());

        //LSI
        assertNull(ctr.getLocalSecondaryIndexes());

        //GSI
        assertNull(ctr.getGlobalSecondaryIndexes());

        //STREAM SPECIFICATION
        assertNull(ctr.getStreamSpecification());
    }

    @Test
    public void apply_whenTwoGsiAndDontIncludeAllGsiButIncludeOne_andTwoLsiAndDontIncludeAllLsiButIncludeOne_andStreamAndDontIncludeStream() {
        SortedSet<String> gsiToInclude = new TreeSet<>();
        gsiToInclude.add(GSI_NAME_TWO);
        SortedSet<String> lsiToInclude = new TreeSet<>();
        lsiToInclude.add(LSI_NAME_TWO);
        TableDescriptionToCreateTableRequestConverter converter =
            TableDescriptionToCreateTableRequestConverter.builder().gsiToInclude(gsiToInclude).lsiToInclude(lsiToInclude).newTableName(OUTPUT_TABLE).createAllGsi(false)
                .createAllLsi(false).copyStreamSpecification(false).build();
        CreateTableRequest ctr = converter.apply(TwoLsiTwoGsiStream.description);
        assertEquals(OUTPUT_TABLE, ctr.getTableName());
        assertEquals(DEFAULT_PT, ctr.getProvisionedThroughput());
        assertEquals(TwoLsiTwoGsiStream.attributeDefinitionList, TwoLsiTwoGsiStream.description.getAttributeDefinitions());
        assertEquals(TwoLsiTwoGsiStream.baseKeySchema, TwoLsiTwoGsiStream.description.getKeySchema());

        //LSI
        assertNotNull(ctr.getLocalSecondaryIndexes());
        assertFalse(ctr.getLocalSecondaryIndexes().isEmpty());
        assertEquals(TwoLsiTwoGsiStream.lsiKeySchema, ctr.getLocalSecondaryIndexes().get(0).getKeySchema());
        assertEquals(ProjectionType.ALL.toString(), ctr.getLocalSecondaryIndexes().get(0).getProjection().getProjectionType());
        assertEquals(LSI_NAME_TWO, ctr.getLocalSecondaryIndexes().get(0).getIndexName());

        //GSI
        assertNotNull(ctr.getGlobalSecondaryIndexes());
        assertFalse(ctr.getGlobalSecondaryIndexes().isEmpty());
        assertEquals(TwoLsiTwoGsiStream.gsiKeySchema, ctr.getGlobalSecondaryIndexes().get(0).getKeySchema());
        assertEquals(ProjectionType.ALL.toString(), ctr.getGlobalSecondaryIndexes().get(0).getProjection().getProjectionType());
        assertEquals(GSI_NAME_TWO, ctr.getGlobalSecondaryIndexes().get(0).getIndexName());
        assertEquals(DEFAULT_PT, ctr.getGlobalSecondaryIndexes().get(0).getProvisionedThroughput());

        //STREAM SPECIFICATION
        assertNull(ctr.getStreamSpecification());
    }
}
