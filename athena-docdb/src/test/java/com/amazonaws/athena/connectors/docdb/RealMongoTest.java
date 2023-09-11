package com.amazonaws.athena.connectors.docdb;

import org.junit.ClassRule;
import org.testcontainers.containers.MongoDBContainer;
import org.testcontainers.containers.wait.strategy.DockerHealthcheckWaitStrategy;
import org.testcontainers.utility.DockerImageName;

public abstract class RealMongoTest {

    @ClassRule
    public static MongoDBContainer mongoDBContainer = new MongoDBContainer(DockerImageName.parse("mongo:4.0.10"))
            .waitingFor(new DockerHealthcheckWaitStrategy());
}
