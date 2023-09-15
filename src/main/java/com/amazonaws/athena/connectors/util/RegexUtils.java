package com.amazonaws.athena.connectors.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Utility class to work with regular-expressions and help overcome the limitations
 * in the java.util.regex package.
 *
 * @author mridang
 */
public class RegexUtils {

    private static final Method namedGroups;

    static {
        try {
            namedGroups = Pattern.class.getDeclaredMethod("namedGroups");
            namedGroups.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private RegexUtils() {
        //
    }

    public static Map<String, String> getGroups(String pattern, String text) {
        return getGroups(Pattern.compile(pattern), text);
    }

    /**
     * Helper method that allows you to get extract all matches from a string keyed by
     * the name of the capturing group.
     * <p>
     * This method relies on reflection to extract the list of the named groups. This
     * has been added in Java 20
     *
     * <a href="https://download.java.net/java/early_access/valhalla/docs/api/java.base/java/util/regex/Pattern.html#namedGroups()">...</a>
     *
     * @param pattern the regular expression pattern to use
     * @param text    the text that should be matched
     * @return a map matches from a string keyed by the name of the capturing group.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> getGroups(Pattern pattern, String text) {
        try {
            Map<String, Integer> groups = ((Map<String, Integer>) namedGroups.invoke(pattern));

            Matcher patternMatcher = pattern.matcher(text);
            if (patternMatcher.find()) {
                return groups.keySet().stream()
                        .collect(Collectors.toMap(s -> s, patternMatcher::group));
            } else {
                return Collections.emptyMap();
            }
        } catch (IllegalAccessException | InvocationTargetException e) {
            throw new RuntimeException(e);
        }
    }
}
