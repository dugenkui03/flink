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

package org.apache.flink.runtime.iterative.concurrent;

import org.apache.flink.runtime.event.TaskEvent;
import org.apache.flink.runtime.iterative.event.AllWorkersDoneEvent;
import org.apache.flink.runtime.iterative.event.TerminationEvent;
import org.apache.flink.runtime.util.event.EventListener;
import org.apache.flink.types.Value;

import java.util.concurrent.CountDownLatch;

/**
 * A re-settable one-shot latch(闭锁).
 *
 */
public class SuperstepBarrier implements EventListener<TaskEvent> {

	// 类加载器
	private final ClassLoader userCodeClassLoader;

	// "结束信号"
	private boolean terminationSignaled = false;

	private CountDownLatch latch;

	private String[] aggregatorNames;
	private Value[] aggregates;


	public SuperstepBarrier(ClassLoader userCodeClassLoader) {
		this.userCodeClassLoader = userCodeClassLoader;
	}

	// Setup the barrier, has to be called at the beginning of each superstep.
	// 设置 "闭锁"，必须在初始化之后、使用之前调用。 todo 为啥不再构造函数中调用？
	public void setup() {
		latch = new CountDownLatch(1);
	}

	// Wait on the barrier.
	// 在 "闭锁" 处等待
	public void waitForOtherWorkers() throws InterruptedException {
		latch.await();
	}

	public String[] getAggregatorNames() {
		return aggregatorNames;
	}

	public Value[] getAggregates() {
		return aggregates;
	}

	public boolean terminationSignaled() {
		return terminationSignaled;
	}

	// Barrier will release the waiting thread if an event occurs.
	// 如果指定的事件发生、barrier将会释放所有等待的线程。
	@Override
	public void onEvent(TaskEvent event) {
		// 如果是结束任务、设置结束标识
		if (event instanceof TerminationEvent) {
			terminationSignaled = true;
		}

		// 如果是 "所有worker结束"事件
		else if (event instanceof AllWorkersDoneEvent) {
			AllWorkersDoneEvent wde = (AllWorkersDoneEvent) event;
			// 获取聚合器名称
			aggregatorNames = wde.getAggregatorNames();
			// 获取聚合器
			aggregates = wde.getAggregates(userCodeClassLoader);
		}

		// 如果不是 TaskEvent 或者 AllWorkersDoneEvent，抛异常
		else {
			throw new IllegalArgumentException("Unknown event type.");
		}

		// 释放所有线程
		latch.countDown();
	}
}
