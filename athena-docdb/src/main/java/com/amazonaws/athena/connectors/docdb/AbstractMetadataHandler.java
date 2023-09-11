package com.amazonaws.athena.connectors.docdb;

import java.util.Map;

import org.apache.arrow.vector.types.pojo.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.glue.model.Table;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;

public abstract class AbstractMetadataHandler extends GlueMetadataHandler {

    private static final Logger logger = LoggerFactory.getLogger(AbstractMetadataHandler.class);
    //The Glue table property that indicates that a table matching the name of an DocDB table
    //is indeed enabled for use by this connector.
    private static final String DOCDB_METADATA_FLAG = "docdb-metadata-flag";
    //Used to filter out Glue tables which lack a docdb metadata flag.
    private static final TableFilter TABLE_FILTER = (Table table) -> table.getParameters().containsKey(DOCDB_METADATA_FLAG);

    public AbstractMetadataHandler(String sourceType, Map<String, String> configOptions) {
        super(sourceType, configOptions);
    }

    protected AbstractMetadataHandler(AWSGlue awsGlue, EncryptionKeyFactory encryptionKeyFactory, AWSSecretsManager secretsManager, AmazonAthena athena, String sourceType, String spillBucket, String spillPrefix, Map<String, String> configOptions) {
        super(awsGlue, encryptionKeyFactory, secretsManager, athena, sourceType, spillBucket, spillPrefix, configOptions);
    }

    /**
     * If Glue is enabled as a source of supplemental metadata, we look up the requested Schema/Table in Glue and
     * filter out any results that don't have the DOCDB_METADATA_FLAG set. If no matching results were found in Glue,
     * then we resort to inferring the schema of the DocumentDB collection using SchemaUtils.inferSchema(...). If there
     * is no such collection, the operation will fail.
     *
     * @see GlueMetadataHandler
     */
    public GetTableResponse doGetTable(@SuppressWarnings("unused") BlockAllocator blockAllocator, GetTableRequest request) throws Exception {
        logger.info("Getting table {}.{}", request.getTableName().getSchemaName(), request.getTableName().getTableName());
        try {
            Schema schema = super.doGetTable(blockAllocator, request, TABLE_FILTER).getSchema();
            logger.info("Retrieved schema for {}.{} from AWS Glue", request.getTableName().getSchemaName(), request.getTableName().getTableName());
            return new GetTableResponse(request.getCatalogName(), request.getTableName(), schema);
        } catch (RuntimeException ex) {
            logger.warn("Unable to retrieve table {}.{} from AWS Glue", request.getTableName().getSchemaName(), request.getTableName().getTableName(), ex);
            return doInferSchema(request);
        }
    }

    protected abstract GetTableResponse doInferSchema(GetTableRequest tableRequest) throws Exception;
}
