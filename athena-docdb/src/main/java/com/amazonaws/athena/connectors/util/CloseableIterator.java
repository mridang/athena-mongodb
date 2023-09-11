package com.amazonaws.athena.connectors.util;

import java.util.Iterator;

public interface CloseableIterator<T> extends Iterator<T>, AutoCloseable {

    @Override
    void close();
}
