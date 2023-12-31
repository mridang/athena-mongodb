package com.amazonaws.athena.connectors.docdb;

import static com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler.SOURCE_TABLE_PROPERTY;
import static com.amazonaws.athena.connectors.docdb.DocDBFieldResolver.DEFAULT_FIELD_RESOLVER;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.apache.arrow.vector.types.Types;
import org.apache.arrow.vector.types.pojo.Field;
import org.bson.Document;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import com.amazonaws.athena.connector.lambda.data.Block;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.domain.TableName;
import com.amazonaws.athena.connector.lambda.domain.predicate.ValueSet;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.google.common.collect.Streams;
import com.mongodb.Function;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

public interface DoGetRecords {

    // This needs to be turned on if the user is using a Glue table, and their docdb tables contain cased column names
    String DISABLE_PROJECTION_AND_CASING_ENV = "disable_projection_and_casing";

    /**
     * Scans DocumentDB using the scan settings set on the requested Split by DocDBeMetadataHandler.
     *
     * @see RecordHandler
     */
    @SuppressWarnings({"ConstantValue", "ReassignedVariable"})
    default void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, Supplier<Boolean> isQueryRunning) {
        TableName tableNameObj = recordsRequest.getTableName();
        String schemaName = tableNameObj.getSchemaName();
        String tableName = recordsRequest.getSchema().getCustomMetadata().getOrDefault(SOURCE_TABLE_PROPERTY, tableNameObj.getTableName());

        getLogger().info("Resolved tableName to: {}", tableName);

        Map<String, ValueSet> constraintSummary = recordsRequest.getConstraints().getSummary();

        MongoClient client = getOrCreateConn(recordsRequest.getSplit());
        getLogger().info("Running query with constraints {}", constraintSummary);
        Document query = QueryUtils.makeQuery(recordsRequest.getSchema(), constraintSummary);

        String disableProjectionAndCasingEnvValue = getConfig().getOrDefault(DISABLE_PROJECTION_AND_CASING_ENV, "false").toLowerCase();
        boolean disableProjectionAndCasing = disableProjectionAndCasingEnvValue.equals("true");
        getLogger().info("{} environment variable set to: {}. Resolved to: {}", DISABLE_PROJECTION_AND_CASING_ENV, disableProjectionAndCasingEnvValue, disableProjectionAndCasing);

        // TODO: Currently AWS DocumentDB does not support collation, which is required for case insensitive indexes:
        // https://www.mongodb.com/docs/manual/core/index-case-insensitive/
        // Once AWS DocumentDB supports collation, then projections do not have to be disabled anymore because case
        // insensitive indexes allows for case insensitive projections.
        Document projection = disableProjectionAndCasing ? null : QueryUtils.makeProjection(recordsRequest.getSchema());

        getLogger().info("query[{}] projection[{}]", query, projection);

        long numRows;
        AtomicLong numResultRows;

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

        try (ChainedMongoCursor iterable = new ChainedMongoCursor(schemaName, collectionNames, client, new Function<>() {
            @Override
            public @NotNull FindIterable<Document> apply(@NotNull MongoCollection<Document> mongoCollection) {
                return mongoCollection.find(query).projection(projection).batchSize(getBatchSize());
            }
        })) {
            numRows = 0;
            numResultRows = new AtomicLong(0);
            while (iterable.hasNext() && isQueryRunning.get()) {
                numRows++;
                spiller.writeRows((Block block, int rowNum) -> {
                    Map<String, Object> doc = documentAsMap(iterable.next(), disableProjectionAndCasing);
                    boolean matched = true;
                    for (Field nextField : recordsRequest.getSchema().getFields()) {
                        Object value = TypeUtils.coerce(nextField, doc.get(nextField.getName()));
                        Types.MinorType fieldType = Types.getMinorTypeForArrowType(nextField.getType());
                        try {
                            switch (fieldType) {
                                case LIST:
                                case STRUCT:
                                    matched &= block.offerComplexValue(nextField.getName(), rowNum, DEFAULT_FIELD_RESOLVER, value);
                                    break;
                                default:
                                    matched &= block.offerValue(nextField.getName(), rowNum, value);
                                    break;
                            }
                            if (!matched) {
                                return 0;
                            }
                        } catch (Exception ex) {
                            throw new RuntimeException("Error while processing field " + nextField.getName(), ex);
                        }
                    }

                    numResultRows.getAndIncrement();
                    return 1;
                });
            }
        }

        getLogger().info("numRows[{}] numResultRows[{}]", numRows, numResultRows.get());
    }

    private Map<String, Object> documentAsMap(Document document, boolean caseInsensitive) {
        if (!caseInsensitive) {
            return document;
        }

        TreeMap<String, Object> caseInsensitiveMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        caseInsensitiveMap.putAll(document);
        return caseInsensitiveMap;
    }

    Map<String, String> getConfig();

    int getBatchSize();

    MongoClient getOrCreateConn(Split split);

    Logger getLogger();

    GlobHandler getGlobHandler();
}
