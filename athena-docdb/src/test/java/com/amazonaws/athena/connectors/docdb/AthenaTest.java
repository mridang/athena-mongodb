package com.amazonaws.athena.connectors.docdb;

import java.util.Collections;
import java.util.UUID;

import com.amazonaws.athena.connector.lambda.security.FederatedIdentity;

public interface AthenaTest {

    default FederatedIdentity getIdentity() {
        return new FederatedIdentity("arn", "account", Collections.emptyMap(), Collections.emptyList());
    }

    default String generateId() {
        return UUID.randomUUID().toString();
    }
}
