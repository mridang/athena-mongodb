package com.amazonaws.athena.connectors.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

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
