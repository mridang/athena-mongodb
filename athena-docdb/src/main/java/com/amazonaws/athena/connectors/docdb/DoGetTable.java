package com.amazonaws.athena.connectors.docdb;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Schema;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connectors.docdb.schema.SchemaProvider;
import com.google.common.collect.Streams;
import com.mongodb.Function;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

public interface DoGetTable {

    /**
     * The number of documents to scan when attempting to infer schema from a collection.
     */
    int SCHEMA_INFERENCE_NUM_DOCS = Optional.ofNullable(System.getProperty("SCHEMA_INFERENCE_NUM_DOCS"))
            .map(Integer::parseInt)
            .orElse(10);

    /**
     * If Glue is enabled as a source of supplemental metadata, we look up the requested Schema/Table in Glue and
     * filter out any results that don't have the DOCDB_METADATA_FLAG set. If no matching results were found in Glue,
     * then we resort to inferring the schema of the DocumentDB collection using SchemaUtils.inferSchema(...). If there
     * is no such collection, the operation will fail.
     *
     * @see GlueMetadataHandler
     */
    @SuppressWarnings("RedundantThrows")
    default GetTableResponse doGetTable(GetTableRequest request) throws Exception {
        getLogger().info("Inferring schema for table[{}].", request.getTableName());
        MongoClient client = getOrCreateConn(request);
        List<String> collectionNames = Streams.stream(client.getDatabase(request.getTableName().getSchemaName()).listCollectionNames())
                .filter(collectionName -> getGlobHandler().test(request.getTableName().getTableName(), collectionName))
                .collect(Collectors.toList());

        Schema schema = getSchemaProvider().getSchema(new ChainedMongoCursor(request.getTableName().getSchemaName(), collectionNames, client, new Function<>() {
            @Override
            public @NotNull FindIterable<Document> apply(@NotNull MongoCollection<Document> mongoCollection) {
                return mongoCollection.find()
                        .batchSize(SCHEMA_INFERENCE_NUM_DOCS)
                        .limit(SCHEMA_INFERENCE_NUM_DOCS);
            }
        }));
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    MongoClient getOrCreateConn(MetadataRequest request);

    Logger getLogger();

    GlobHandler getGlobHandler();

    SchemaProvider getSchemaProvider();
}
