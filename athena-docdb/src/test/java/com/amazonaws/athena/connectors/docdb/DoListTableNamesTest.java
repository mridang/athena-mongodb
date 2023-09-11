package com.amazonaws.athena.connectors.docdb;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.UUID;

import org.bson.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DoListTableNamesTest extends RealMongoTest {

    private static final Logger logger = LoggerFactory.getLogger(DoListTableNamesTest.class);

    @Test
    public void doTest() throws Exception {
        try (MongoClient mongoClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            mongoClient.getDatabase("bravo").getCollection("moo").insertOne(new Document());
            mongoClient.getDatabase("alpha").getCollection("foo").insertOne(new Document());

            DoListTableNames listTableNames = new DoListTableNames() {

                @Override
                public MongoClient getOrCreateConn(MetadataRequest request) {
                    return mongoClient;
                }

                @Override
                public Logger getLogger() {
                    return logger;
                }

                @Override
                public GlobHandler getGlobHandler() {
                    return new GlobHandler();
                }
            };

            ListTablesResponse response = new ListTablesResponse("missing", List.of(new TableName("bravo", "moo")), null);
            ListTablesRequest request = new ListTablesRequest(TestBase.IDENTITY, UUID.randomUUID().toString(), "missing", "bravo", null, 0);
            assertEquals(response, listTableNames.doListTables(new BlockAllocatorImpl(), request));
        }
    }
}
