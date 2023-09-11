package com.amazonaws.athena.connectors.docdb;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.arrow.vector.types.pojo.ArrowType.Utf8;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connectors.docdb.schema.DefaultSchemaProvider;
import com.amazonaws.athena.connectors.docdb.schema.SchemaProvider;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

public class DoGetTableTest extends RealMongoTest implements AthenaTest {

    private static final Logger logger = LoggerFactory.getLogger(DoGetTableTest.class);

    @Test
    public void doTest() throws Exception {
        try (MongoClient mongoClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            mongoClient.getDatabase("bravo").getCollection("moo").insertOne(new Document());

            DoGetTable getTable = new DoGetTable() {

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

                @Override
                public SchemaProvider getSchemaProvider() {
                    return new DefaultSchemaProvider();
                }
            };

            GetTableResponse response = new GetTableResponse("missing", new TableName("bravo", "moo"), new Schema(List.of(new Field("_id", FieldType.nullable(Utf8.INSTANCE), null))));
            GetTableRequest request = new GetTableRequest(getIdentity(), generateId(), "missing", new TableName("bravo", "moo"));
            assertEquals(response, getTable.doGetTable(request));
        }
    }

    @Test
    public void testThatMultiTenantCollectionsAreListed() throws Exception {
        try (MongoClient mongoClient = MongoClients.create(mongoDBContainer.getConnectionString())) {
            mongoClient.getDatabase("alpha").getCollection("foo_1").insertOne(new Document());
            mongoClient.getDatabase("alpha").getCollection("foo_2").insertOne(new Document());

            DoGetTable getTable = new DoGetTable() {

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
                    return new GlobHandler("foo_{{myid}}");
                }

                @Override
                public SchemaProvider getSchemaProvider() {
                    return new DefaultSchemaProvider();
                }
            };

            GetTableResponse response = new GetTableResponse("missing", new TableName("alpha", "foo_myid"), new Schema(List.of(new Field("_id", FieldType.nullable(Utf8.INSTANCE), null), new Field("myid", FieldType.nullable(Utf8.INSTANCE), null))));
            GetTableRequest request = new GetTableRequest(getIdentity(), generateId(), "missing", new TableName("alpha", "foo_myid"));
            assertEquals(response, getTable.doGetTable(request));
        }
    }
}
