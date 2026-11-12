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

import static com.amazonaws.athena.connectors.docdb.DocDBMetadataHandler.DOCDB_CONN_STR;

import java.util.Map;
import java.util.Optional;

import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connector.lambda.QueryStatusChecker;
import com.amazonaws.athena.connector.lambda.data.BlockSpiller;
import com.amazonaws.athena.connector.lambda.domain.Split;
import com.amazonaws.athena.connector.lambda.handlers.RecordHandler;
import com.amazonaws.athena.connector.lambda.records.ReadRecordsRequest;
import com.amazonaws.services.athena.AmazonAthena;
import com.amazonaws.services.athena.AmazonAthenaClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.secretsmanager.AWSSecretsManager;
import com.amazonaws.services.secretsmanager.AWSSecretsManagerClientBuilder;
import com.mongodb.client.MongoClient;

/**
 * Handles data read record requests for the Athena DocumentDB Connector.
 * <p>
 * For more detail, please see the module's README.md, some notable characteristics of this class include:
 * <p>
 * 1. Attempts to resolve sensitive configuration fields such as HBase connection string via SecretsManager so that you can
 * substitute variables with values from by doing something like hostname:port:password=${my_secret}
 */
public class DocDBRecordHandler extends RecordHandler implements DoGetRecords {

    private static final Logger logger = LoggerFactory.getLogger(DocDBRecordHandler.class);
    //Used to denote the 'type' of this connector for diagnostic purposes.
    private static final String SOURCE_TYPE = "documentdb";
    private final DocDBConnectionFactory connectionFactory;
    private final GlobHandler globHandler = new GlobHandler();

    public DocDBRecordHandler(Map<String, String> configOptions) {
        this(
                AmazonS3ClientBuilder.defaultClient(),
                AWSSecretsManagerClientBuilder.defaultClient(),
                AmazonAthenaClientBuilder.defaultClient(),
                new DocDBConnectionFactory(),
                configOptions);
    }

    @VisibleForTesting
    protected DocDBRecordHandler(AmazonS3 amazonS3, AWSSecretsManager secretsManager, AmazonAthena athena, DocDBConnectionFactory connectionFactory, Map<String, String> configOptions) {
        super(amazonS3, secretsManager, athena, SOURCE_TYPE, configOptions);
        this.connectionFactory = connectionFactory;
    }

    @Override
    public void readWithConstraint(BlockSpiller spiller, ReadRecordsRequest recordsRequest, QueryStatusChecker queryStatusChecker) {
        DoGetRecords.super.readWithConstraint(spiller, recordsRequest, queryStatusChecker::isQueryRunning);
    }

    @Override
    public Map<String, String> getConfig() {
        return configOptions;
    }

    @Override
    public int getBatchSize() {
        return Optional.ofNullable(System.getenv("MONGO_QUERY_BATCH_SIZE"))
                .map(Integer::parseInt)
                .orElse(100);
    }

    /**
     * Gets the special DOCDB_CONN_STR property from the provided split and uses
     * its contents to getOrCreate a MongoDB client connection.
     * <p>
     * This method attempts to resolve any SecretsManager secrets that are
     * using in the connection string and denoted by ${secret_name}.
     *
     * @param split The split to that we need to read and this DocDB instance to connector.
     * @return A MongoClient connected to the request DB instance.
     */
    @Override
    public MongoClient getOrCreateConn(Split split) {
        String conStr = split.getProperty(DOCDB_CONN_STR);
        if (conStr == null) {
            throw new RuntimeException(DOCDB_CONN_STR + " Split property is null! Unable to create connection.");
        }
        String endpoint = resolveSecrets(conStr);
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

}
