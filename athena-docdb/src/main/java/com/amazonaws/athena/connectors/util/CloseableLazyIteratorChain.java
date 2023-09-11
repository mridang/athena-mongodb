/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.amazonaws.athena.connectors.util;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.collections4.iterators.EmptyIterator;

/**
 * An LazyIteratorChain is an Iterator that wraps a number of Iterators in a lazy manner.
 * <p>
 * This class makes multiple iterators look like one to the caller. When any
 * method from the Iterator interface is called, the LazyIteratorChain will delegate
 * to a single underlying Iterator. The LazyIteratorChain will invoke the Iterators
 * in sequence until all Iterators are exhausted.
 * <p>
 * The Iterators are provided by {@link #nextIterator(int)} which has to be overridden by
 * sub-classes and allows to lazily create the Iterators as they are accessed:
 * <pre>
 * return new LazyIteratorChain&lt;String&gt;() {
 *     protected Iterator&lt;String&gt; nextIterator(int count) {
 *         return count == 1 ? Arrays.asList("foo", "bar").iterator() : null;
 *     }
 * };
 * </pre>
 * <p>
 * Once the inner Iterator's {@link Iterator#hasNext()} method returns false,
 * {@link #nextIterator(int)} will be called to obtain another iterator, and so on
 * until {@link #nextIterator(int)} returns null, indicating that the chain is exhausted.
 * <p>
 * NOTE: The LazyIteratorChain may contain no iterators. In this case the class will
 * function as an empty iterator.
 *
 * @since 4.0
 */
public abstract class CloseableLazyIteratorChain<E> implements Iterator<E>, AutoCloseable {

    /**
     * The number of times {@link #next()} was already called.
     */
    private int callCounter = 0;

    /**
     * Indicates that the Iterator chain has been exhausted.
     */
    private boolean chainExhausted = false;

    /**
     * The current iterator.
     */
    private Iterator<? extends E> currentIterator = null;

    /**
     * Gets the next iterator after the previous one has been exhausted.
     * <p>
     * This method <b>MUST</b> return null when there are no more iterators.
     *
     * @param count the number of time this method has been called (starts with 1)
     * @return the next iterator, or null if there are no more.
     */
    protected abstract Iterator<? extends E> nextIterator(int count);

    @Override
    public void close() {
        if (this.currentIterator != null && this.currentIterator instanceof Closeable) {
            try {
                ((Closeable) this.currentIterator).close();
            } catch (IOException e) {
                throw new RuntimeException("Unable to close the current iterator", e);
            }
        }
    }

    /**
     * Updates the current iterator field to ensure that the current Iterator
     * is not exhausted.
     */
    private void updateCurrentIterator() {
        if (callCounter == 0) {
            currentIterator = nextIterator(++callCounter);
            if (currentIterator == null) {
                currentIterator = EmptyIterator.emptyIterator();
                chainExhausted = true;
            }
        }

        if (!currentIterator.hasNext() && !chainExhausted) {
            final Iterator<? extends E> nextIterator = nextIterator(++callCounter);
            if (nextIterator != null) {
                this.close();
                currentIterator = nextIterator;
            } else {
                chainExhausted = true;
            }
        }
    }

    //-----------------------------------------------------------------------

    /**
     * Return true if any Iterator in the chain has a remaining element.
     *
     * @return true if elements remain
     */
    @Override
    public boolean hasNext() {
        updateCurrentIterator();

        return currentIterator.hasNext();
    }

    /**
     * Returns the next element of the current Iterator
     *
     * @return element from the current Iterator
     * @throws java.util.NoSuchElementException if all the Iterators are exhausted
     */
    @Override
    public E next() {
        updateCurrentIterator();

        return currentIterator.next();
    }
}
