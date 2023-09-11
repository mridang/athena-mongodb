/*-
 * #%L
 * athena-mongodb
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.docdb;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.docdb.DocDBMetadataHandler.DOCDB_CONN_STR;
import static com.amazonaws.athena.connectors.docdb.TestBase.IDENTITY;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.S3;
import static org.testcontainers.containers.localstack.LocalStackContainer.Service.SECRETSMANAGER;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.jeasy.random.EasyRandom;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.localstack.LocalStackContainer;
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy;
import org.testcontainers.utility.DockerImageName;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.data.S3BlockSpillReader;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.domain.spill.S3SpillLocation;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.records.RecordResponse;
import com.amazonaws.athena.connector.lambda.records.RemoteReadRecordsResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connector.lambda.security.LocalKeyFactory;
import com.amazonaws.athena.connectors.docdb.DoGetRecordsTest.PersonEntity;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DocDBRecordHandlerTest extends RealMongoTest {

    @ClassRule
    public static final LocalStackContainer localStackContainer = new LocalStackContainer(DockerImageName.parse("localstack/localstack:2.2.0-arm64"))
            .withServices(S3, SECRETSMANAGER)
            .waitingFor(new DockerHealthcheckWaitStrategy());
    private static final Logger logger = LoggerFactory.getLogger(DocDBRecordHandlerTest.class);
    private final EncryptionKeyFactory keyFactory = new LocalKeyFactory();
    private final BlockAllocator blockAllocator = new BlockAllocatorImpl();
    private final EasyRandom easyRandom = new EasyRandom();
    private DocDBRecordHandler handler;
    private S3BlockSpillReader spillReader;
    private Schema schemaForRead;

    @Before
    public void setUp() {
        schemaForRead = SchemaBuilder.newBuilder()
                .addField("col1", new ArrowType.Int(32, true))
                .addField("col2", new ArrowType.Utf8())
                .build();

        AmazonS3 s3 = AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                localStackContainer.getEndpoint().toString(),
                                localStackContainer.getRegion()
                        )
                )
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(localStackContainer.getAccessKey(), localStackContainer.getSecretKey())
                        )
                )
                .build();

        s3.createBucket("myspill");

        AWSSecretsManager secretsManager = AWSSecretsManagerClientBuilder
                .standard()
                .withEndpointConfiguration(
                        new AwsClientBuilder.EndpointConfiguration(
                                localStackContainer.getEndpoint().toString(),
                                localStackContainer.getRegion()
                        )
                )
                .withCredentials(
                        new AWSStaticCredentialsProvider(
                                new BasicAWSCredentials(localStackContainer.getAccessKey(), localStackContainer.getSecretKey())
                        )
                )
                .build();

        handler = new DocDBRecordHandler(s3, secretsManager, AmazonAthenaClientBuilder.defaultClient(), new DocDBConnectionFactory(), Collections.emptyMap());
        spillReader = new S3BlockSpillReader(s3, blockAllocator);
    }

    @Test
    public void doReadRecordsNoSpill() throws Exception {
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:" + mongoDBContainer.getMappedPort(27017))) {
            String mongoUri = "mongodb://localhost:" + mongoDBContainer.getMappedPort(27017);

            new FixturesBuilder(mongoClient)
                    .withDatabase("foo", database -> database
                            .withCollection("Person_1", PersonEntity.class, () ->
                                    easyRandom.objects(PersonEntity.class, 10000)
                                            .collect(Collectors.toList())));

            Map<String, ValueSet> constraintsMap = new HashMap<>();

            S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                    .withBucket("myspill")
                    .withSplitId(UUID.randomUUID().toString())
                    .withQueryId(UUID.randomUUID().toString())
                    .withIsDirectory(true)
                    .build();

            ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                    "default",
                    UUID.randomUUID().toString(),
                    new TableName("foo", "Person_1"),
                    schemaForRead,
                    Split.newBuilder(splitLoc, keyFactory.create()).add(DOCDB_CONN_STR, mongoUri).build(),
                    new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                    100_000_000_000L, //100GB don't expect this to spill
                    100_000_000_000L
            );

            RecordResponse rawResponse = handler.doReadRecords(blockAllocator, request);

            assertTrue(rawResponse instanceof ReadRecordsResponse);

            ReadRecordsResponse response = (ReadRecordsResponse) rawResponse;
            logger.info("doReadRecordsNoSpill: rows[{}]", response.getRecordCount());

            assertEquals(10000, response.getRecords().getRowCount());
            logger.info("doReadRecordsNoSpill: {}", BlockUtils.rowToString(response.getRecords(), 0));
        }
    }

    @Test
    public void doReadRecordsSpill() throws Exception {
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:" + mongoDBContainer.getMappedPort(27017))) {
            String mongoUri = "mongodb://localhost:" + mongoDBContainer.getMappedPort(27017);

            new FixturesBuilder(mongoClient)
                    .withDatabase("foo", database -> database
                            .withCollection("Person_1", PersonEntity.class, () ->
                                    easyRandom.objects(PersonEntity.class, 100000)
                                            .collect(Collectors.toList())));

            Map<String, ValueSet> constraintsMap = new HashMap<>();

            S3SpillLocation splitLoc = S3SpillLocation.newBuilder()
                    .withBucket("myspill")
                    .withSplitId(UUID.randomUUID().toString())
                    .withQueryId(UUID.randomUUID().toString())
                    .withIsDirectory(true)
                    .build();

            ReadRecordsRequest request = new ReadRecordsRequest(IDENTITY,
                    "default",
                    UUID.randomUUID().toString(),
                    new TableName("foo", "Person_1"),
                    schemaForRead,
                    Split.newBuilder(splitLoc, keyFactory.create()).add(DOCDB_CONN_STR, mongoUri).build(),
                    new Constraints(constraintsMap, Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT),
                    1000L, //~1.5MB so we should see some spill
                    0L
            );
            RecordResponse rawResponse = handler.doReadRecords(blockAllocator, request);

            assertTrue(rawResponse instanceof RemoteReadRecordsResponse);

            try (RemoteReadRecordsResponse response = (RemoteReadRecordsResponse) rawResponse) {

                assertTrue(response.getNumberBlocks() > 1);

                int blockNum = 0;
                for (SpillLocation next : response.getRemoteBlocks()) {
                    S3SpillLocation spillLocation = (S3SpillLocation) next;
                    try (Block block = spillReader.read(spillLocation, response.getEncryptionKey(), response.getSchema())) {

                        logger.info("doReadRecordsSpill: blockNum[{}] and recordCount[{}]", blockNum++, block.getRowCount());
                        // assertTrue(++blockNum < response.getRemoteBlocks().size() && block.getRowCount() > 10_000);

                        logger.info("doReadRecordsSpill: {}", BlockUtils.rowToString(block, 0));
                        assertNotNull(BlockUtils.rowToString(block, 0));
                    }
                }
            }
        }
    }
}
