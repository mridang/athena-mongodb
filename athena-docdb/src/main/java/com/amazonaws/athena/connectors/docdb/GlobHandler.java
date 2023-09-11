package com.amazonaws.athena.connectors.docdb;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GlobHandler implements Function<String, String>, BiPredicate<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(GlobHandler.class);
    private final List<CollectionConfig> collectionConfigs;

    public GlobHandler() {
        this(Optional.ofNullable(System.getenv("GLOB_PATTERN")).orElse(""));
    }

    public GlobHandler(String globConfig) {
        logger.info("Received glob pattern {}", globConfig);
        this.collectionConfigs = Arrays.stream(globConfig.split(";"))
                .map(CollectionConfig::new)
                .collect(Collectors.toList());
    }

    @Override
    public String apply(String s) {
        return this.collectionConfigs.stream()
                .filter(collectionConfig -> collectionConfig.test(s))
                .findFirst()
                .map(collectionConfig -> collectionConfig.collectionName)
                .orElse(s);
    }

    @Override
    public boolean test(String tableName, String collectionName) {
        return this.collectionConfigs.stream()
                .filter(collectionConfig -> collectionConfig.collectionName.equals(tableName))
                .findFirst()
                .map(collectionConfig -> collectionConfig.test(collectionName))
                .orElse(true);
    }

    private static class CollectionConfig implements Predicate<String> {

        private static final Logger logger = LoggerFactory.getLogger(CollectionConfig.class);
        protected final String collectionName;
        private final Pattern globPattern;

        private CollectionConfig(String pattern) {
            this.collectionName = pattern.replaceAll("\\{\\{(.*?)}}", "$1");
            this.globPattern = Pattern.compile("^" + pattern.replaceAll("\\{\\{(.*?)}}", "(?<$1>[a-zA-Z0-9]*?)") + "$");
            logger.info("Matching {} to regex {}", collectionName, globPattern);
        }

        @Override
        public boolean test(String s) {
            return this.globPattern.matcher(s).matches();
        }
    }
}
