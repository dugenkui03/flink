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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.RuntimeContext;

import java.io.Serializable;
/**
 * An interface to create a rate-limiter.
 *
 * <p>The rate-limiter is configured via {@link #setRate(long)} and created via {@link #open(RuntimeContext)}.
 * An example implementation can be found {@link GuavaFlinkConnectorRateLimiter}.
 *
 * fixme:
 * 		创建限速器的接口；
 * 		限速器可以通过setRate(long)进行配置，通过open(RuntimeContext)进行创建，实现可参考：GuavaFlinkConnectorRateLimiter。
 *
 *
 * */

@PublicEvolving
public interface FlinkConnectorRateLimiter extends Serializable {

	// A method that can be used to create and configure a rate-limiter based on the runtimeContext.
	// 基于运行时上下文配置和创建限速器
	void open(RuntimeContext runtimeContext);

	// Sets the desired rate for the rate limiter.
	// 为限速器设置期望的速率
	void setRate(long rate);

	// Acquires permits for the rate limiter.
	// 获取限速器的许可
	//
	void acquire(int permits);

	long getRate();

	void close();
}
