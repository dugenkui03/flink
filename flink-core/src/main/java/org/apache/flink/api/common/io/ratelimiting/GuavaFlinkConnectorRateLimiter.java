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

package org.apache.flink.api.common.io.ratelimiting;

import org.apache.flink.api.common.functions.RuntimeContext;

import org.apache.flink.shaded.guava18.com.google.common.util.concurrent.RateLimiter;

/**
 * An implementation of {@link FlinkConnectorRateLimiter} that uses  for rate limiting.
 *
 * 使用 Guava's RateLimiter 对 FlinkConnectorRateLimiter 的实现
 */
public class GuavaFlinkConnectorRateLimiter implements FlinkConnectorRateLimiter {

	private static final long serialVersionUID = -3680641524643737192L;

	// Rate in bytes per second for the consumer on a whole.
	// 消费者每秒钟的限速值
	private long globalRateBytesPerSecond;

	// Rate in bytes per second per subtask of the consumer
	// 消费者的每个子任务美妙的限速值
	private long localRateBytesPerSecond;

	// Runtime context，运行时上下文： 任务名称、指标组
	private RuntimeContext runtimeContext;

	// RateLimiter 限速器
	private RateLimiter rateLimiter;

	// Creates a rate limiter with the runtime context provided.
	// 根据运行时的上下文、设置速率值
	@Override
	public void open(RuntimeContext runtimeContext) {
		// 设置运行时上下文
		this.runtimeContext = runtimeContext;

		// 美秒全局速率/并行的子任务
		localRateBytesPerSecond = globalRateBytesPerSecond / runtimeContext.getNumberOfParallelSubtasks();

		//初始化限速器
		this.rateLimiter = RateLimiter.create(localRateBytesPerSecond);
	}

	/**
	 * Set the global per consumer(消费者) and per sub-task rates.
	 * 为设置全局的每个消费者、每个子任务设置限速值
	 *
	 * @param globalRate Value of rate in bytes per second.
	 *                   每秒速率值(以字节为单位？)
	 */
	@Override
	public void setRate(long globalRate) {
		this.globalRateBytesPerSecond = globalRate;
	}

	// 保证许可数量至少为1
	// fixme 单位是秒
	@Override
	public void acquire(int permits) {
		rateLimiter.acquire(Math.max(1, permits));
	}

	@Override
	public long getRate() {
		return globalRateBytesPerSecond;
	}

	@Override
	public void close() { }
}
