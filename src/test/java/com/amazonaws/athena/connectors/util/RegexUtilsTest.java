package com.amazonaws.athena.connectors.util;

import org.junit.Test;

public class RegexUtilsTest {

    @Test
    public void dotest() {
        System.out.println(RegexUtils.getGroups("Page (?<moo>\\d+) of (?<modo>\\d+)", "Page 1 of 20"));
    }
}
