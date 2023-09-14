package com.amazonaws.athena.connectors.docdb;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;

public interface DoListSchemaNames {

    /**
     * List databases in your DocumentDB instance treating each as a 'schema' (aka database)
     *
     * @see GlueMetadataHandler
     */
    @SuppressWarnings({"RedundantThrows", "unused"})
    default ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request) throws Exception {
        getLogger().info("Listing all schemas");
        List<String> collectionNames = new ArrayList<>();
        MongoClient client = getOrCreateConn(request);
        try (MongoCursor<String> itr = client.listDatabaseNames().iterator()) {
            while (itr.hasNext()) {
                String collectionName = itr.next();
                collectionNames.add(collectionName);
                getLogger().info("Discovered schema {}", collectionName);
            }

            return new ListSchemasResponse(request.getCatalogName(), collectionNames);
        }
    }

    MongoClient getOrCreateConn(MetadataRequest request);

    Logger getLogger();
}
