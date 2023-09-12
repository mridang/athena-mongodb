/*-
 * #%L
 * athena-mongodb
 * %%
 * Copyright (C) 2019 Amazon Web Services
 * %%
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * #L%
 */
package com.amazonaws.athena.connectors.docdb;

import java.util.Map;

import org.apache.arrow.util.VisibleForTesting;
import org.apache.arrow.vector.types.pojo.Field;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockAllocator;
import com.amazonaws.athena.connector.lambda.data.BlockWriter;
import com.amazonaws.athena.connector.lambda.domain.spill.SpillLocation;
import com.amazonaws.athena.connector.lambda.handlers.GlueMetadataHandler;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetSplitsResponse;
import com.amazonaws.athena.connector.lambda.metadata.GetTableLayoutRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableRequest;
import com.amazonaws.athena.connector.lambda.metadata.GetTableResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListSchemasResponse;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesRequest;
import com.amazonaws.athena.connector.lambda.metadata.ListTablesResponse;
import com.amazonaws.athena.connector.lambda.metadata.MetadataRequest;
import com.amazonaws.athena.connector.lambda.metadata.glue.GlueFieldLexer;
import com.amazonaws.athena.connector.lambda.security.EncryptionKey;
import com.amazonaws.athena.connector.lambda.security.EncryptionKeyFactory;
import com.amazonaws.athena.connectors.docdb.schema.DefaultSchemaProvider;
import com.amazonaws.athena.connectors.docdb.schema.SchemaProvider;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.glue.AWSGlue;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.mongodb.client.MongoClient;

/**
 * Handles metadata requests for the Athena DocumentDB Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Uses a Glue table property (docfb-metadata-flag) to indicate that the table (whose name matched the DocDB collection
 * name) can indeed be used to supplement metadata from DocDB itself.
 * 2. Attempts to resolve sensitive fields such as DocDB connection strings via SecretsManager so that you can substitute
 * variables with values from by doing something like:
 * mongodb://${docdb_instance_1_creds}@myhostname.com:123/?ssl=true&ssl_ca_certs=rds-combined-ca-bundle.pem&replicaSet=rs0
 */
public class DocDBMetadataHandler extends AbstractMetadataHandler implements DoListSchemaNames, DoListTableNames, DoGetTable, DoGetSplits {
    //Field name used to store the connection string as a property on Split objects.
    protected static final String DOCDB_CONN_STR = "connStr";
    private static final Logger logger = LoggerFactory.getLogger(DocDBMetadataHandler.class);
    //Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "documentdb";
    //The Env variable name used to store the default DocDB connection string if no catalogue-specific
    //env variable is set.
    private static final String DEFAULT_DOCDB = "default_docdb";
    //The Glue table property that indicates that a table matching the name of an DocDB table
    //is indeed enabled for use by this connector.
    private final DocDBConnectionFactory connectionFactory;
    private final GlobHandler globHandler = new GlobHandler();
    private final SchemaProvider schemaProvider = new DefaultSchemaProvider();


    public DocDBMetadataHandler(Map<String, String> configOptions) {
        super(SOURCE_TYPE, configOptions);
        connectionFactory = new DocDBConnectionFactory();
    }

    @VisibleForTesting
    protected DocDBMetadataHandler(
            AWSGlue glue,
            DocDBConnectionFactory connectionFactory,
            EncryptionKeyFactory keyFactory,
            AWSSecretsManager secretsManager,
            AmazonAthena athena,
            String spillBucket,
            String spillPrefix,
            Map<String, String> configOptions) {
        super(glue, keyFactory, secretsManager, athena, SOURCE_TYPE, spillBucket, spillPrefix, configOptions);
        this.connectionFactory = connectionFactory;
    }

    @Override
    public MongoClient getOrCreateConn(MetadataRequest request) {
        String endpoint = resolveSecrets(getConnStr(request));
        return connectionFactory.getOrCreateConn(endpoint);
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public GlobHandler getGlobHandler() {
        return this.globHandler;
    }

    @Override
    public SchemaProvider getSchemaProvider() {
        return schemaProvider;
    }

    /**
     * Retrieves the DocDB connection details from an env variable matching the catalog name, if no such
     * env variable exists we fall back to the default env variable defined by DEFAULT_DOCDB.
     */
    public String getConnStr(MetadataRequest request) {
        String conStr = configOptions.get(request.getCatalogName());
        if (conStr == null) {
            logger.info("No environment variable found for catalog {} , using default {}", request.getCatalogName(), DEFAULT_DOCDB);
            conStr = configOptions.get(DEFAULT_DOCDB);
        }
        return conStr;
    }

    /**
     * Our table doesn't support complex layouts or partitioning so we simply make this method a NoOp.
     *
     * @see GlueMetadataHandler
     */
    @Override
    public void getPartitions(BlockWriter blockWriter, GetTableLayoutRequest request, QueryStatusChecker queryStatusChecker) {
        //NoOp as we do not support partitioning.
    }

    @Override
    public EncryptionKey makeEncryptionKey() {
        return super.makeEncryptionKey();
    }

    @Override
    public SpillLocation makeSpillLocation(GetSplitsRequest request) {
        return super.makeSpillLocation(request);
    }

    /**
     * @see GlueMetadataHandler
     */
    @Override
    protected Field convertField(String name, String glueType) {
        return GlueFieldLexer.lex(name, glueType);
    }

    @Override
    protected GetTableResponse doInferSchema(GetTableRequest tableRequest) throws Exception {
        return DoGetTable.super.doGetTable(tableRequest);
    }

    @Override
    public ListSchemasResponse doListSchemaNames(BlockAllocator blockAllocator, ListSchemasRequest request) throws Exception {
        return DoListSchemaNames.super.doListSchemaNames(blockAllocator, request);
    }

    @Override
    public ListTablesResponse doListTables(BlockAllocator blockAllocator, ListTablesRequest request) throws Exception {
        return DoListTableNames.super.doListTables(blockAllocator, request);
    }

    @Override
    public GetSplitsResponse doGetSplits(BlockAllocator allocator, GetSplitsRequest request) {
        return DoGetSplits.super.doGetSplits(allocator, request);
    }

}
