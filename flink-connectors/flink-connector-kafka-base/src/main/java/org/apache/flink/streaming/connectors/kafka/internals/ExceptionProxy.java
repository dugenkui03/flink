/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.annotation.Internal;

import javax.annotation.Nullable;

import java.util.concurrent.atomic.AtomicReference;

/**
 * A proxy that communicates exceptions between threads. Typically used if an exception
 * from a spawned thread needs to be recognized by the "parent" (spawner) thread.
 *
 * <p>The spawned thread would set the exception via {@link #reportError(Throwable)}.
 * The parent would check (at certain points) for exceptions via {@link #checkAndThrowException()}.
 * Optionally, the parent can pass itself in the constructor to be interrupted as soon as
 * an exception occurs.
 *
 * <pre>
 * {@code
 *
 * final ExceptionProxy errorProxy = new ExceptionProxy(Thread.currentThread());
 *
 * Thread subThread = new Thread() {
 *
 *     public void run() {
 *         try {
 *             doSomething();
 *         } catch (Throwable t) {
 *             errorProxy.reportError(
 *         } finally {
 *             doSomeCleanup();
 *         }
 *     }
 * };
 * subThread.start();
 *
 * doSomethingElse();
 * errorProxy.checkAndThrowException();
 *
 * doSomethingMore();
 * errorProxy.checkAndThrowException();
 *
 * try {
 *     subThread.join();
 * } catch (InterruptedException e) {
 *     errorProxy.checkAndThrowException();
 *     // restore interrupted status, if not caused by an exception
 *     Thread.currentThread().interrupt();
 * }
 * }
 * </pre>
 */
@Internal
public class ExceptionProxy {

	// The thread that should be interrupted when an exception occurs.
	// 当异常发生时、应该中断的线程
	private final Thread toInterrupt;

	// The exception to throw.
	// 抛出异常的原子引用
	private final AtomicReference<Throwable> exception;

	/**
	 * Creates an exception proxy that interrupts the given thread upon
	 * report of an exception. The thread to interrupt may be null.
	 *
	 * @param toInterrupt The thread to interrupt upon an exception. May be null.
	 */
	//
	public ExceptionProxy(@Nullable Thread toInterrupt) {
		this.toInterrupt = toInterrupt;
		this.exception = new AtomicReference<>();
	}

	// ------------------------------------------------------------------------

	/**
	 * 如果是首次发生异常：设置异常信息、并中断目标线程。fixme：不是加锁、而是使用CAS处理"首次调用"，学习一下
	 *
	 * Sets the exception and interrupts the target thread, if no other exception has occurred so far.
	 *
	 * <p>The exception is only set (and the interruption is only triggered), if no other exception was set before.
	 * fixme: 只在没有发生异常的情况下、才会成功的设置异常信息和中断没有中断的线程
	 *
	 * @param t The exception that occurred 发生的异常
	 */
	public void reportError(Throwable t) {
		// set the exception, if it is the first (and the exception is non null)
		// Note:如果compareAndSet失败则表示已经有其他线程调用该方法、并中断线程
		if (t != null && exception.compareAndSet(null, t) && toInterrupt != null) {
			toInterrupt.interrupt();
		}
	}

	/**
	 * Checks whether an exception has been set via {@link #reportError(Throwable)}.
	 * If yes, that exception if re-thrown by this method.
	 *
	 * @throws Exception This method re-throws the exception, if set.
	 */
	public void checkAndThrowException() throws Exception {
		Throwable t = exception.get();
		if (t != null) {
			if (t instanceof Exception) {
				throw (Exception) t;
			}
			else if (t instanceof Error) {
				throw (Error) t;
			}
			else {
				throw new Exception(t);
			}
		}
	}
}
