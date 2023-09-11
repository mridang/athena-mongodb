package com.amazonaws.athena.connectors.docdb;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.types.ObjectId;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ConstraintEvaluator;
import com.amazonaws.athena.connector.lambda.domain.predicate.Constraints;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.athena.connectors.InMemorySingleBlockSpiller;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import dev.morphia.annotations.Entity;
import dev.morphia.annotations.Id;

public class DoGetRecordsTest extends RealMongoTest {

    @SuppressWarnings("deprecation")
    @Test
    public void doTest() {
        MongoClient mongoClient = MongoClients.create("mongodb://localhost:" + mongoDBContainer.getMappedPort(27017));

        new FixturesBuilder(mongoClient)
                .withDatabase("foo", database -> database
                        .withCollection("Person_1", PersonEntity.class, () ->
                                List.of(new PersonEntity("john", 2), new PersonEntity("jack", 40)))
                        .withCollection("Person_2", PersonEntity.class, () ->
                                List.of(new PersonEntity("sanny", 5), new PersonEntity("broo", 33))))
                .withDatabase("bar", database -> database
                        .withCollection("Places", PersonEntity.class, () ->
                                List.of(new PersonEntity("nancy", 5), new PersonEntity("kate", 33))));

        Schema schema = SchemaBuilder.newBuilder()
                .addField("age", Types.MinorType.INT.getType())
                .addField("name", Types.MinorType.VARCHAR.getType())
                .build();

        Split split = Split.newBuilder(null, null).build();

        DoGetRecords getRecords = new DoGetRecords() {

            @Override
            public Map<String, String> getConfig() {
                return Collections.emptyMap();
            }

            @Override
            public int getBatchSize() {
                return Integer.MAX_VALUE;
            }

            @Override
            public MongoClient getOrCreateConn(Split split) {
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

        BlockSpiller blockSpiller = new InMemorySingleBlockSpiller(schema, ConstraintEvaluator.emptyEvaluator());
        ReadRecordsRequest request = new ReadRecordsRequest(TestBase.IDENTITY, "missing", UUID.randomUUID().toString(), new TableName("foo", "Person_1"), schema, split, new Constraints(Collections.emptyMap()), Integer.MAX_VALUE, Integer.MAX_VALUE);
        getRecords.readWithConstraint(blockSpiller, request, () -> true);
        System.out.println(blockSpiller.getBlock().getRowCount());
    }

    @Entity("persons")
    public static class PersonEntity {

        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final String name;
        @SuppressWarnings({"FieldCanBeLocal", "unused"})
        private final Integer age;
        @SuppressWarnings("unused")
        @Id
        private ObjectId id;

        public PersonEntity(String name, Integer age) {
            this.name = name;
            this.age = age;
        }
    }
}
