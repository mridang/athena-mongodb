package com.amazonaws.athena.connectors.util;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import org.junit.Test;

public class RegexUtilsTest {

    @Test
    public void testThatRegexesWithoutCapturingGroupsWork() {
        assertEquals(Map.of(), RegexUtils.getGroups("foo", "foo"));
    }

    @Test
    public void testThatRegexesWithoutNamedCapturingGroupsWork() {
        assertEquals(Map.of(), RegexUtils.getGroups("(foo)", "foo"));
    }

    @Test
    public void testThatNamedPatternsAreExtracted() {
        assertEquals(Map.of("foo", "1"), RegexUtils.getGroups("(?<foo>\\d+)", "1"));
    }
}
