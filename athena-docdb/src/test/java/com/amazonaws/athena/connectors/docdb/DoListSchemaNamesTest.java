package com.amazonaws.athena.connectors.docdb;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.UUID;

import org.bson.Document;
import org.junit.Test;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DoListSchemaNamesTest extends RealMongoTest {

    @Test
    public void doTest() throws Exception {
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:" + mongoDBContainer.getMappedPort(27017))) {
            mongoClient.getDatabase("bravo").getCollection("moo").insertOne(new Document());

            DoListSchemaNames listSchemaNames = request -> mongoClient;

            ListSchemasResponse response = new ListSchemasResponse("missing", List.of("admin", "bravo", "config", "local"));
            assertEquals(response, listSchemaNames.doListSchemaNames(new BlockAllocatorImpl(), new ListSchemasRequest(TestBase.IDENTITY, UUID.randomUUID().toString(), "missing")));
        }
    }
}
