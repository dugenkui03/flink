/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * A special form of blocking queue with two additions:
 * <ol>
 *     <li>The queue can be closed atomically when empty. Adding elements after the queue
 *         is closed fails. This allows queue consumers to atomically discover that no elements
 *         are available and mark themselves as shut down.</li>
 *     <li>The queue allows to poll batches of elements in one polling call.</li>
 * </ol>
 *
 * <p>The queue has no capacity restriction and is safe for multiple producers and consumers.
 *
 * <p>Note: Null elements are prohibited.
 *
 * @param <E> The type of elements in the queue.
 */
@Internal
public class ClosableBlockingQueue<E> {

	/** Flag marking the status of the queue. */
	private volatile boolean open = true;

	/** The deque of elements. */
	private final LinkedBlockingQueue<E> elements = new LinkedBlockingQueue<>();


	public ClosableBlockingQueue() { }

	/**
	 * Creates a new queue that contains the given elements.
	 *
	 * @param initialElements The elements to initially add to the queue.
	 */
	public ClosableBlockingQueue(Collection<? extends E> initialElements) {
		this.elements.addAll(initialElements);
	}

	// ------------------------------------------------------------------------
	//  Size and status
	// ------------------------------------------------------------------------

	/**
	 * Gets the number of elements currently in the queue.
	 * @return The number of elements currently in the queue.
	 */
	public int size() {
		return elements.size();
	}

	/**
	 * Checks whether the queue is empty (has no elements).
	 * @return True, if the queue is empty; false, if it is non-empty.
	 */
	public boolean isEmpty() {
		return elements.isEmpty();
	}

	/**
	 * Checks whether the queue is currently open, meaning elements can be added and polled.
	 * @return True, if the queue is open; false, if it is closed.
	 */
	public boolean isOpen() {
		return open;
	}

	/**
	 * Tries to close the queue. Closing the queue only succeeds when no elements are
	 * in the queue when this method is called. Checking whether the queue is empty, and
	 * marking the queue as closed is one atomic operation.
	 *
	 * @return True, if the queue is closed, false if the queue remains open.
	 */
	public boolean close() {
		if (open) {
			if (elements.isEmpty()) {
				// 不允许在读写
				open = false;
				return true;
			} else {
				return false;
			}
		} else {
			// already closed
			return true;
		}
	}

	// ------------------------------------------------------------------------
	//  Adding / Removing elements
	// ------------------------------------------------------------------------

	/**
	 * Tries to add an element to the queue, if the queue is still open. Checking whether the queue
	 * is open and adding the element is one atomic operation.
	 *
	 * <p>Unlike the {@link #add(Object)} method, this method never throws an exception,
	 * but only indicates via the return code if the element was added or the
	 * queue was closed.
	 *
	 * @param element The element to add.
	 * @return True, if the element was added, false if the queue was closes.
	 */
	public boolean addIfOpen(E element) {
		requireNonNull(element);

		final boolean open = this.open;
		if (open) {
			elements.add(element);
		}
		return open;
	}

	/**
	 * Adds the element to the queue, or fails with an exception, if the queue is closed.
	 * Checking whether the queue is open and adding the element is one atomic operation.
	 *
	 * @param element The element to add.
	 * @throws IllegalStateException Thrown, if the queue is closed.
	 */
	public void add(E element) throws IllegalStateException {
		requireNonNull(element);

		if (open) {
			elements.add(element);
		} else {
			throw new IllegalStateException("queue is closed");
		}
	}

	/**
	 * Returns the queue's next element without removing it, if the queue is non-empty.
	 * Otherwise, returns null.
	 *
	 * <p>The method throws an {@code IllegalStateException} if the queue is closed.
	 * Checking whether the queue is open and getting the next element is one atomic operation.
	 *
	 * <p>This method never blocks.
	 *
	 * @return The queue's next element, or null, if the queue is empty.
	 * @throws IllegalStateException Thrown, if the queue is closed.
	 */
	public E peek() {
		if (open) {
			return elements.peek();
		} else {
			throw new IllegalStateException("queue is closed");
		}
	}

	/**
	 * Returns the queue's next element and removes it, the queue is non-empty.
	 * Otherwise, this method returns null.
	 *
	 * <p>The method throws an {@code IllegalStateException} if the queue is closed.
	 * Checking whether the queue is open and removing the next element is one atomic operation.
	 *
	 * <p>This method never blocks.
	 *
	 * @return The queue's next element, or null, if the queue is empty.
	 * @throws IllegalStateException Thrown, if the queue is closed.
	 */
	public E poll() {
		if (open) {
			return elements.poll();
		} else {
			throw new IllegalStateException("queue is closed");
		}
	}

	/**
	 * Returns all of the queue's current elements in a list, if the queue is non-empty.
	 * Otherwise, this method returns null.
	 *
	 * <p>The method throws an {@code IllegalStateException} if the queue is closed.
	 * Checking whether the queue is open and removing the elements is one atomic operation.
	 *
	 * <p>This method never blocks.
	 *
	 * @return All of the queue's elements, or null, if the queue is empty.
	 * @throws IllegalStateException Thrown, if the queue is closed.
	 */
	public List<E> pollBatch() {
		if (open) {
			List<E> result = new LinkedList();
			E element;
			while ((element = elements.poll()) != null) {
				result.add(element);
			}

			if (result.size() == 0) {
				return null;
			}
			return result;
		} else {
			throw new IllegalStateException("queue is closed");
		}
	}

	/**
	 * Returns the next element in the queue. If the queue is empty, this method
	 * waits until at least one element is added.
	 *
	 * <p>The method throws an {@code IllegalStateException} if the queue is closed.
	 * Checking whether the queue is open and removing the next element is one atomic operation.
	 *
	 * @return The next element in the queue, never null.
	 *
	 * @throws IllegalStateException Thrown, if the queue is closed.
	 * @throws InterruptedException Throw, if the thread is interrupted while waiting for an
	 *                              element to be added.
	 */
	public E getElementBlocking() throws InterruptedException {
		if (open) {
			return elements.take();
		} else {
			throw new IllegalStateException("queue is closed");
		}
	}

	/**
	 * Returns the next element in the queue. If the queue is empty, this method
	 * waits at most a certain time until an element becomes available. If no element
	 * is available after that time, the method returns null.
	 *
	 * <p>The method throws an {@code IllegalStateException} if the queue is closed.
	 * Checking whether the queue is open and removing the next element is one atomic operation.
	 *
	 * @param timeoutMillis The number of milliseconds to block, at most.
	 * @return The next element in the queue, or null, if the timeout expires  before an element is available.
	 *
	 * @throws IllegalStateException Thrown, if the queue is closed.
	 * @throws InterruptedException Throw, if the thread is interrupted while waiting for an
	 *                              element to be added.
	 */
	public E getElementBlocking(long timeoutMillis) throws InterruptedException {
		if (timeoutMillis == 0L) {
			// wait forever case
			return getElementBlocking();
		} else if (timeoutMillis < 0L) {
			throw new IllegalArgumentException("invalid timeout");
		}

		if (open) {
			return elements.poll(timeoutMillis, TimeUnit.MILLISECONDS);
		} else {
			throw new IllegalStateException("queue is closed");
		}
	}

	/**
	 * Gets all the elements found in the list, or blocks until at least one element
	 * was added. If the queue is empty when this method is called, it blocks until
	 * at least one element is added.
	 *
	 * <p>This method always returns a list with at least one element.
	 *
	 * <p>The method throws an {@code IllegalStateException} if the queue is closed.
	 * Checking whether the queue is open and removing the next element is one atomic operation.
	 *
	 * @return A list with all elements in the queue, always at least one element.
	 *
	 * @throws IllegalStateException Thrown, if the queue is closed.
	 * @throws InterruptedException Throw, if the thread is interrupted while waiting for an
	 *                              element to be added.
	 */
	public List<E> getBatchBlocking() throws InterruptedException {
		if (open) {
			List<E> result = new LinkedList();
			E element;
			while ((element = elements.poll()) != null || result.size() == 0) {
				if (element != null) {
					result.add(element);
				}
			}
			return result;
		} else {
			throw new IllegalStateException("queue is closed");
		}
	}

	/**
	 * Gets all the elements found in the list, or blocks until at least one element
	 * was added. This method is similar as {@link #getBatchBlocking()}, but takes
	 * a number of milliseconds that the method will maximally wait before returning.
	 *
	 * <p>This method never returns null, but an empty list, if the queue is empty when
	 * the method is called and the request times out before an element was added.
	 *
	 * <p>The method throws an {@code IllegalStateException} if the queue is closed.
	 * Checking whether the queue is open and removing the next element is one atomic operation.
	 *
	 * @param timeoutMillis The number of milliseconds to wait, at most.
	 * @return A list with all elements in the queue, possible an empty list.
	 *
	 * @throws IllegalStateException Thrown, if the queue is closed.
	 * @throws InterruptedException Throw, if the thread is interrupted while waiting for an
	 *                              element to be added.
	 */
	public List<E> getBatchBlocking(long timeoutMillis) throws InterruptedException {
		if (timeoutMillis == 0L) {
			// wait forever case
			return getBatchBlocking();
		} else if (timeoutMillis < 0L) {
			throw new IllegalArgumentException("invalid timeout");
		}

		final long deadline = System.nanoTime() + timeoutMillis * 1_000_000L;

		if (open) {
			List<E> result = new LinkedList();
			E element;
			while ((element = elements.poll()) != null || result.isEmpty()) {
				if (timeoutMillis <= 0 && result.isEmpty()) {
					return Collections.emptyList();
				}
				timeoutMillis = (deadline - System.nanoTime()) / 1_000_000L;

				if (element != null) {
					result.add(element);
				}
			}
			return result;
		} else {
			throw new IllegalStateException("queue is closed");
		}
	}

	// ------------------------------------------------------------------------
	//  Standard Utilities
	// ------------------------------------------------------------------------

	@Override
	public int hashCode() {
		int hashCode = 17;
		for (E element : elements) {
			hashCode = 31 * hashCode + element.hashCode();
		}
		return hashCode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (obj != null && obj.getClass() == ClosableBlockingQueue.class) {
			@SuppressWarnings("unchecked")
			ClosableBlockingQueue<E> that = (ClosableBlockingQueue<E>) obj;

			if (this.elements.size() == that.elements.size()) {
				Iterator<E> thisElements = this.elements.iterator();
				for (E thatNext : that.elements) {
					E thisNext = thisElements.next();
					if (!(thisNext == null ? thatNext == null : thisNext.equals(thatNext))) {
						return false;
					}
				}
				return true;
			} else {
				return false;
			}
		} else {
			return false;
		}
	}

	@Override
	public String toString() {
		return elements.toString();
	}
}
