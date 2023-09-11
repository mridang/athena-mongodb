package com.amazonaws.athena.connectors.docdb;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.google.common.collect.Streams;
import com.mongodb.client.MongoClient;

public interface DoGetTable {

    //The number of documents to scan when attempting to infer schema from an DocDB collection.
    int SCHEMA_INFERENCE_NUM_DOCS = Optional.ofNullable(System.getProperty("SCHEMA_INFERENCE_NUM_DOCS")).map(Integer::parseInt).orElse(10);

    /**
     * If Glue is enabled as a source of supplemental metadata we look up the requested Schema/Table in Glue and
     * filters out any results that don't have the DOCDB_METADATA_FLAG set. If no matching results were found in Glue,
     * then we resort to inferring the schema of the DocumentDB collection using SchemaUtils.inferSchema(...). If there
     * is no such table in DocumentDB the operation will fail.
     *
     * @see GlueMetadataHandler
     */
    default GetTableResponse doGetTable(BlockAllocator blockAllocator, GetTableRequest request) throws Exception {
        getLogger().info("enter {}", request.getTableName());
        Schema schema = null;
//        try {
//            if (glue != null) {
//                schema = super.doGetTable(blockAllocator, request, TABLE_FILTER).getSchema();
//                getLogger().info("Retrieved schema for table[{}] from AWS Glue.", request.getTableName());
//            }
//        } catch (RuntimeException ex) {
//            getLogger().warn("Unable to retrieve table[{}:{}] from AWS Glue.",
//                    request.getTableName().getSchemaName(),
//                    request.getTableName().getTableName(),
//                    ex);
//        }

        if (schema == null) {
            getLogger().info("Inferring schema for table[{}].", request.getTableName());
            MongoClient client = getOrCreateConn(request);
            List<String> collectionNames = Streams.stream(client.getDatabase(request.getTableName().getSchemaName()).listCollectionNames())
                    .filter(collectionName -> getGlobHandler().test(request.getTableName().getTableName(), collectionName))
                    .collect(Collectors.toList());
            schema = SchemaUtils.inferSchema(client, request.getTableName().getSchemaName(), collectionNames, SCHEMA_INFERENCE_NUM_DOCS);
        }
        return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
    }

    MongoClient getOrCreateConn(MetadataRequest request);

    Logger getLogger();

    GlobHandler getGlobHandler();
}
