/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.core.testutils;

/**
 * A utility to help synchronize two threads in cases where one of them is supposed to reach
 * a blocking state before the other may continue.
 *
 * <p>Use as follows:
 * <pre>
 * {@code
 *
 * final BlockerSync sync = new BlockerSync();
 *
 * // thread to be blocked
 * Runnable toBeBlocked = () -> {
 *     // do something, like acquire a shared resource
 *     sync.blockNonInterruptible();
 *     // release resource
 * }
 *
 * new Thread(toBeBlocked).start();
 * sync.awaitBlocker();
 *
 * // do stuff that requires the other thread to still hold the resource
 * sync.releaseBlocker();
 * }
 * </pre>
 */
public class BlockerSync {

	private final Object lock = new Object();

	// 线程阻塞标识
	// 没有volatile、因为synchronized保证可见性: http://gee.cs.oswego.edu/dl/cpj/jmm.html
	private boolean blockerReady;

	// 线程中断标识
	private boolean blockerReleased;

	/** wait()、知道 block() 被调用 或者线程中断
	 *
	 * Waits until the blocking thread has entered the method {@link #block()}
	 * or {@link #blockNonInterruptible()}.
	 */
	public void awaitBlocker() throws InterruptedException {
		synchronized (lock) {
			// blockXX()方法中将 阻塞标识 设置为true的时候、才跳出循环
			while (!blockerReady) {
				lock.wait();
			}
		}
	}

	// Lets the blocked thread continue.
	// 使得中断的线程继续执行
	public void releaseBlocker() {
		synchronized (lock) {
			blockerReleased = true;
			lock.notifyAll();
		}
	}

	/** 阻塞、直到release标识被设置为true 或者线程被中断
	 *
	 * Blocks until {@link #releaseBlocker()} is called or this thread is interrupted.
	 * Notifies the awaiting thread that waits in the method {@link #awaitBlocker()}.
	 */
	public void block() throws InterruptedException {
		synchronized (lock) {
			// 线程阻塞标识：blockerReady
			blockerReady = true;
			lock.notifyAll();

			// 释放线程标识：blockerReady
			while (!blockerReleased) {
				lock.wait();
			}
		}
	}

	/** 阻塞、直到blockerReleased设置为true。Note：即时线程被中断、也不会跳出该方法
	 *
	 * Blocks until {@link #releaseBlocker()} is called.
	 * Notifies the awaiting thread that waits in the method {@link #awaitBlocker()}.
	 */
	public void blockNonInterruptible() {
		synchronized (lock) {
			blockerReady = true;
			// 唤醒其他所有等待该对象监视器的线程
			lock.notifyAll();

			//"释放阻塞"
			while (!blockerReleased) {
				try {
					// wait是可中断的
					// 因为该方法是 non-interruptible，所以catch
					lock.wait();
				} catch (InterruptedException ignored) {}
			}
		}
	}

}
