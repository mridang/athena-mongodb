package com.amazonaws.athena.connectors.docdb;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.UUID;

import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.data.BlockAllocatorImpl;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DoGetTableTest extends RealMongoTest {

    @Test
    public void doTest() throws Exception {
        try (MongoClient mongoClient = MongoClients.create("mongodb://localhost:" + mongoDBContainer.getMappedPort(27017))) {
            mongoClient.getDatabase("bravo").getCollection("moo").insertOne(new Document());

            DoGetTable getTable = new DoGetTable() {

                @Override
                public MongoClient getOrCreateConn(MetadataRequest request) {
                    return mongoClient;
                }

                @Override
                public Logger getLogger() {
                    return LoggerFactory.getLogger(DoListTableNamesTest.class);
                }

                @Override
                public GlobHandler getGlobHandler() {
                    return new GlobHandler();
                }
            };

            GetTableResponse response = new GetTableResponse("missing", new TableName("bravo", "moo"), new Schema(List.of(new Field("_id", FieldType.nullable(Utf8.INSTANCE), null))));
            GetTableRequest request = new GetTableRequest(TestBase.IDENTITY, UUID.randomUUID().toString(), "missing", new TableName("bravo", "moo"));
            assertEquals(response, getTable.doGetTable(new BlockAllocatorImpl(), request));
        }
    }
}
