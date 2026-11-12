package com.amazonaws.athena.connectors.docdb;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.athena.connectors.util.RegexUtils;

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
                .filter(collectionConfig -> collectionConfig.collectionName.equalsIgnoreCase(tableName))
                .findFirst()
                .map(collectionConfig -> collectionConfig.test(collectionName))
                .orElse(false);
    }

    public Collection<String> getFields(String tableName) {
        return this.collectionConfigs.stream()
                .filter(collectionConfig -> collectionConfig.test(tableName))
                .findFirst()
                .map(collectionConfig -> collectionConfig.inferredFields)
                .orElse(Collections.emptySet());
    }

    public boolean isMultiTenant(String tableName) {
        if (this.collectionConfigs.stream().anyMatch(collectionConfig -> collectionConfig.test(tableName))) {
            logger.info("Table {} is a multi-tenant collection", tableName);
            return true;
        } else {
            logger.info("Table {} is not a multi-tenant collection", tableName);
            return false;
        }
    }

    public Map<String, String> parseFields(String tableName) {
        return this.collectionConfigs.stream()
                .filter(collectionConfig -> collectionConfig.test(tableName))
                .findFirst()
                .map(collectionConfig -> collectionConfig.parseFields(tableName)).orElse(Collections.emptyMap());
    }

    private static class CollectionConfig implements Predicate<String> {

        private static final Logger logger = LoggerFactory.getLogger(CollectionConfig.class);
        public final Set<String> inferredFields;
        protected final String collectionName;
        private final Pattern globPattern;

        private CollectionConfig(String pattern) {
            this.collectionName = pattern.replaceAll("\\{\\{(.*?)}}", "$1");
            this.globPattern = Pattern.compile("^" + pattern.replaceAll("\\{\\{(.*?)}}", "(?<$1>[a-zA-Z0-9]*?)") + "$", Pattern.CASE_INSENSITIVE);
            this.inferredFields = Pattern.compile("\\{\\{(.*?)}}").matcher(pattern)
                    .results()
                    .map(matchResult -> matchResult.group(1))
                    .collect(Collectors.toSet());
            logger.debug("Matching {} to regex {}", collectionName, globPattern);
        }

        @Override
        public boolean test(String s) {
            if (this.globPattern.matcher(s).matches()) {
                logger.debug("Collection {} matches {}", s, globPattern);
                return true;
            } else {
                logger.debug("Collection {} does not match {}", s, globPattern);
                return false;
            }
        }

        public Map<String, String> parseFields(String tableName) {
            return RegexUtils.getGroups(this.globPattern, tableName);
        }
    }
}
