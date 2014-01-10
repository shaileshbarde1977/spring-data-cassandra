/*
 * Copyright 2013 the original author or authors.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springdata.cassandra.base.core.query;

/**
 * Contains Statement Options for Cassandra command execution. This controls the Consistency Tuning and Retry Policy for
 * a Statement.
 * 
 * @author David Webb
 * @author Alex Shvid
 * 
 */
public class StatementOptions implements StatementOptionsAccessor {

	private ConsistencyLevel consistencyLevel;
	private RetryPolicy retryPolicy;
	private Boolean queryTracing;
	private Integer ttl;
	private Long timestamp;

	/**
	 * Static Default Constructor
	 * 
	 * @return QueryOptions new instance
	 */

	public static StatementOptions builder() {
		return new StatementOptions();
	}

	/**
	 * @return Returns the consistencyLevel.
	 */
	public ConsistencyLevel getConsistencyLevel() {
		return consistencyLevel;
	}

	/**
	 * @param consistencyLevel The consistencyLevel to set.
	 */
	public StatementOptions withConsistencyLevel(ConsistencyLevel consistencyLevel) {
		this.consistencyLevel = consistencyLevel;
		return this;
	}

	/**
	 * @return Returns the retryPolicy.
	 */
	public RetryPolicy getRetryPolicy() {
		return retryPolicy;
	}

	/**
	 * @param retryPolicy The retryPolicy to set.
	 */
	public StatementOptions withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
		return this;
	}

	/**
	 * @return Returns the queryTracing.
	 */
	public Boolean getQueryTracing() {
		return queryTracing;
	}

	/**
	 * @param queryTracing The queryTracing to set.
	 */
	public StatementOptions withQueryTracing(Boolean queryTracing) {
		this.queryTracing = queryTracing;
		return this;
	}

	/**
	 * @return Returns the ttl of the entry.
	 */
	public Integer getTtl() {
		return ttl;
	}

	/**
	 * @param ttl The ttl of the entry to set.
	 */
	public StatementOptions withTtl(Integer ttl) {
		this.ttl = ttl;
		return this;
	}

	/**
	 * @return Returns the timestamp of the entry.
	 */
	public Long getTimestamp() {
		return timestamp;
	}

	/**
	 * @param timestamp The timestamp of the entry to set.
	 */
	public StatementOptions withTimestamp(Long timestamp) {
		this.timestamp = timestamp;
		return this;
	}
}
