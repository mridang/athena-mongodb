package com.amazonaws.athena.connectors.docdb;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connectors.util.CloseableLazyIteratorChain;
import com.mongodb.Function;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;

public class ChainedMongoCursor extends CloseableLazyIteratorChain<Document> {

    private static final Logger logger = LoggerFactory.getLogger(ChainedMongoCursor.class);
    private final MongoDatabase mongoDatabase;
    private final List<String> collectionList;
    private final Function<MongoCollection<Document>, MongoIterable<Document>> collectionFunction;

    public ChainedMongoCursor(String databaseName, List<String> collectionList, MongoClient client, Function<MongoCollection<Document>, MongoIterable<Document>> collectionFunction) {
        this.collectionFunction = collectionFunction;
        this.mongoDatabase = client.getDatabase(databaseName);
        this.collectionList = collectionList;
        logger.info("Initialized chained cursor for {}", databaseName);
    }

    @Override
    protected Iterator<Document> nextIterator(int count) {
        if (count <= this.collectionList.size()) {
            String collectionName = this.collectionList.get(count - 1);
            logger.info("Iterating over collection {}", collectionName);
            return Optional.of(collectionName)
                    .map(this.mongoDatabase::getCollection)
                    .map(this.collectionFunction::apply)
                    .map(MongoIterable::iterator)
                    .orElseThrow();
        } else {
            logger.info("No more collections to chain");
            return null;
        }
    }
}
