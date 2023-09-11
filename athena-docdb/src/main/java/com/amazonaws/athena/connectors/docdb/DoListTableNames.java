package com.amazonaws.athena.connectors.docdb;

import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCursor;

public interface DoListTableNames {

    /**
     * List collections in the requested schema in your DocumentDB instance treating the requested schema as an DocumentDB
     * database.
     *
     * @see GlueMetadataHandler
     */
    @SuppressWarnings({"unused", "RedundantThrows"})
    default ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request) throws Exception {
        MongoClient client = getOrCreateConn(request);
        Set<TableName> tables = new HashSet<>();
        try (MongoCursor<String> itr = client.getDatabase(request.getSchemaName()).listCollectionNames().iterator()) {
            while (itr.hasNext()) {
                String tableName = itr.next();
                getLogger().info("Checking for table {} against glob patterns", tableName);
                tables.add(new TableName(request.getSchemaName(), getGlobHandler().apply(tableName)));
            }

            return new ListTablesResponse(request.getCatalogName(), tables, null);
        }
    }

    MongoClient getOrCreateConn(MetadataRequest request);

    Logger getLogger();

    GlobHandler getGlobHandler();
}
