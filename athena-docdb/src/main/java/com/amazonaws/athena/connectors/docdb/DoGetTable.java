package com.amazonaws.athena.connectors.docdb;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import com.amazonaws.athena.connector.lambda.data.SchemaBuilder;
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
        getLogger().info("Inferring schema for {}.", request.getTableName().getQualifiedTableName());
        MongoClient client = getOrCreateConn(request);

        String tableName = request.getTableName().getTableName();
        String schemaName = request.getTableName().getSchemaName();

        List<String> collectionNames;
        if (getGlobHandler().isMultiTenant(tableName)) {
            collectionNames = Streams.stream(client.getDatabase(schemaName).listCollectionNames())
                    .filter(collectionName -> {
                        if (getGlobHandler().test(tableName, collectionName)) {
                            getLogger().info("Collection {} matches requested table {}", collectionName, tableName);
                            return true;
                        } else {
                            getLogger().info("Collection {} doesn't match requested table {}", collectionName, tableName);
                            return false;
                        }
                    })
                    .collect(Collectors.toList());
        } else {
            collectionNames = Streams.stream(client.getDatabase(schemaName).listCollectionNames())
                    .filter(s -> s.equalsIgnoreCase(tableName))
                    .collect(Collectors.toList());
        }

        getLogger().info("Querying collection {}", collectionNames);

        SchemaBuilder schema = getSchemaProvider().getSchema(new ChainedMongoCursor(request.getTableName().getSchemaName(), collectionNames, client, new Function<>() {
            @Override
            public @NotNull FindIterable<Document> apply(@NotNull MongoCollection<Document> mongoCollection) {
                return mongoCollection.find()
                        .batchSize(SCHEMA_INFERENCE_NUM_DOCS)
                        .limit(SCHEMA_INFERENCE_NUM_DOCS);
            }
        }));

        getGlobHandler().getFields(request.getTableName().getTableName()).forEach(schema::addStringField);
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema.build());
    }

    MongoClient getOrCreateConn(MetadataRequest request);

    Logger getLogger();

    GlobHandler getGlobHandler();

    SchemaProvider getSchemaProvider();
}
