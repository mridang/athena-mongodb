package com.amazonaws.athena.connectors.docdb;

import static com.amazonaws.athena.connector.lambda.domain.predicate.Constraints.DEFAULT_NO_LIMIT;
import static com.amazonaws.athena.connectors.docdb.DocDBMetadataHandler.DOCDB_CONN_STR;
import static com.amazonaws.athena.connectors.docdb.TestBase.PARTITION_ID;
import static org.junit.Assert.assertEquals;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

import org.apache.arrow.vector.types.Types;
import org.bson.Document;
import org.junit.Test;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.data.BlockUtils;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DoGetSplitsTest extends RealMongoTest {

    @Test
    public void doGetSplits() {
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:" + mongoDBContainer.getMappedPort(27017))) {
            String mongoUri =  "mongodb://localhost:" + mongoDBContainer.getMappedPort(27017);
            EncryptionKey fixedKey = new EncryptionKey("".getBytes(), "".getBytes());

            mongoClient.getDatabase("bravo").getCollection("moo").insertOne(new Document());

            DoGetSplits getSplits = new DoGetSplits() {

                @Override
                public String getConnStr(MetadataRequest request) {
                    return mongoUri;
                }

                @Override
                public EncryptionKey makeEncryptionKey() {
                    return fixedKey;
                }

                @Override
                public SpillLocation makeSpillLocation(GetSplitsRequest request) {
                    return null;
                }
            };

            Constraints constraints = new Constraints(Collections.emptyMap(), Collections.emptyList(), Collections.emptyList(), DEFAULT_NO_LIMIT);
            Block partition = BlockUtils.newBlock(new BlockAllocatorImpl(), PARTITION_ID, Types.MinorType.INT.getType(), 0);
            GetSplitsResponse response = new GetSplitsResponse("missing", Set.of(Split.newBuilder(null, fixedKey).add(DOCDB_CONN_STR, mongoUri).build()), null);
            GetSplitsRequest request = new GetSplitsRequest(TestBase.IDENTITY, UUID.randomUUID().toString(), "missing", new TableName("bravo", "moo"), partition, Collections.emptyList(), constraints, null);
            assertEquals(response, getSplits.doGetSplits(new BlockAllocatorImpl(), request));
        }
    }
}
