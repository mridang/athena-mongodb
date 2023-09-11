package com.amazonaws.athena.connectors.docdb;

import org.junit.Rule;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public abstract class RealMongoTest {

    @Rule
    public MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:4.0.10"))
            .waitingFor(new DockerHealthcheckWaitStrategy());
}
