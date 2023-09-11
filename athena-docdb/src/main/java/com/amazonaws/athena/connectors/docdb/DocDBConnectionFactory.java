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

import java.util.HashMap;
import java.util.Map;

import org.apache.arrow.util.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

/**
 * Creates and Caches HBase Connection Instances, using the connection string as the cache key.
 *
 * @Note Connection String format is expected to be like:
 * mongodb://<username>:<password>@<hostname>:<port>/?ssl=true&ssl_ca_certs=<certs.pem>&replicaSet=<replica_set>
 */
public class DocDBConnectionFactory {

    private static final Logger logger = LoggerFactory.getLogger(DocDBConnectionFactory.class);
    private final Map<String, MongoClient> clientCache = new HashMap<>();

    /**
     * Used to get an existing, pooled, connection or to create a new connection
     * for the given connection string.
     *
     * @param connectionString MongoClient connection details, format is expected to be:
     *                         mongodb://<username>:<password>@<hostname>:<port>/?ssl=true&ssl_ca_certs=<certs.pem>&replicaSet=<replica_set>
     * @return A MongoClient connection if the connection succeeded, else the function will throw.
     */
    public synchronized MongoClient getOrCreateConn(String connectionString) {
        logger.info("Setting up connection to {}", connectionString);
        MongoClient result = clientCache.get(connectionString);

        if (result == null || !connectionTest(result)) {
            result = MongoClients.create(connectionString);
            clientCache.put(connectionString, result);
        }

        return result;
    }

    /**
     * Runs a 'quick' test on the connection and then returns it if it passes.
     */
    private boolean connectionTest(MongoClient mongoClient) {
        try {
            logger.info("Testing connection to MongoDB");
            mongoClient.listDatabaseNames();
            logger.info("Connection successful");
            return true;
        } catch (Exception ex) {
            throw new RuntimeException("Connection test failed", ex);
        }
    }

    /**
     * Injects a connection into the client cache.
     *
     * @param connectionString The connection string (aka the cache key)
     * @param mongoClient      The connection to inject into the client cache, most often a Mock used in testing.
     */
    @SuppressWarnings("SameParameterValue")
    @VisibleForTesting
    protected synchronized void addConnection(String connectionString, MongoClient mongoClient) {
        clientCache.put(connectionString, mongoClient);
    }
}
