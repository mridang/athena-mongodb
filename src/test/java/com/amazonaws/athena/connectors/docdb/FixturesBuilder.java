package com.amazonaws.athena.connectors.docdb;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.mongodb.client.MongoClient;

import dev.morphia.Datastore;
import dev.morphia.Morphia;

public class FixturesBuilder {

    private final MongoClient mongoClient;

    public FixturesBuilder(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public FixturesBuilder withDatabase(String databaseName, Consumer<WithDatabase> databaseConsumer) {
        databaseConsumer.accept(new WithDatabase(Morphia.createDatastore(mongoClient, databaseName)));
        return this;
    }

    public static class WithDatabase {

        private final Datastore database;

        public WithDatabase(Datastore datastore) {
            this.database = datastore;
        }

        public <T> WithDatabase withCollection(@SuppressWarnings("unused") String collectionName, Class<T> klass, Supplier<List<T>> collectionConsumer) {
            this.database.getCollection(klass).insertMany(collectionConsumer.get());
            return this;
        }
    }
}
