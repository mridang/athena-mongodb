package com.amazonaws.athena.connectors.docdb;


import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.Test;

public class GlobHandlerTest {

    @Test
    public void testThatEmptyGlobHandlerAllowsAll() {
        GlobHandler globHandler = new GlobHandler();
        assertEquals("Foo_Bar", globHandler.apply("Foo_Bar"));
    }

    @Test
    public void testThatSameGlobHandlerAllowsAll() {
        GlobHandler globHandler = new GlobHandler("Foo_Bar");
        assertEquals("Foo_Bar", globHandler.apply("Foo_Bar"));
    }

    @Test
    public void testThatIncorrectGlobHandlerAllowsAll() {
        GlobHandler globHandler = new GlobHandler("Foo_Bar");
        assertEquals("Foo_Bar_1", globHandler.apply("Foo_Bar_1"));
    }

    @Test
    public void testThatCorrectGlobHandlerReturnsTransformedName() {
        GlobHandler globHandler = new GlobHandler("Foo_Bar_{{stackId}}");
        assertEquals("Foo_Bar_stackId", globHandler.apply("Foo_Bar_1"));
    }

    @Test
    public void testThatCorrectGlobHandlerReturnsAllTransformedName() {
        GlobHandler globHandler = new GlobHandler("Foo_{{Id}};Foo_Bar_{{Id}}");
        List<String> getCollections = List.of("Foo_1", "Foo_2", "Foobar", "Foo_Bar_2425");
        Set<String> parsed = getCollections.stream()
                .map(globHandler)
                .collect(Collectors.toSet());

        assertEquals(Set.of("Foo_Id", "Foobar", "Foo_Bar_Id"), parsed);
    }

    @Test
    public void testMosod() {
        GlobHandler globHandler = new GlobHandler("Foo_{{Id}};Foo_Bar_{{Id}}");
        List<String> getCollections = List.of("Foo_1", "Foo_2", "Foobar", "Foo_Bar_2425");

        List<String> parsed = getCollections.stream()
                .filter(collectionName -> globHandler.test("Foo_Id", collectionName))
                .collect(Collectors.toList());

        assertEquals(List.of("Foo_1", "Foo_2"), parsed);
    }
}
