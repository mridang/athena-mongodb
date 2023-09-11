package com.amazonaws.athena.connectors.docdb;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.UUID;

import org.bson.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DoListSchemaNamesTest extends RealMongoTest {

    private static final Logger logger = LoggerFactory.getLogger(DoListSchemaNamesTest.class);

    @Test
    public void doTest() throws Exception {
        try (MongoClient mongoClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            mongoClient.getDatabase("bravo").getCollection("moo").insertOne(new Document());

            DoListSchemaNames listSchemaNames = new DoListSchemaNames() {
                @Override
                public MongoClient getOrCreateConn(MetadataRequest request) {
                    return mongoClient;
                }

                @Override
                public Logger getLogger() {
                    return logger;
                }
            };

            ListSchemasResponse response = new ListSchemasResponse("missing", List.of("admin", "bravo", "config", "local"));
            assertEquals(response, listSchemaNames.doListSchemaNames(new BlockAllocatorImpl(), new ListSchemasRequest(TestBase.IDENTITY, UUID.randomUUID().toString(), "missing")));
        }
    }
}
