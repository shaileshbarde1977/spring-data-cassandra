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
 * Contains Query Options for Cassandra queries. This controls the Consistency Tuning and Retry Policy for a Query.
 * 
 * @author David Webb
 * @author Alex Shvid
 * 
 */
public class QueryOptions implements QueryOptionsAccessor {

	private ConsistencyLevel consistencyLevel;
	private RetryPolicy retryPolicy;
	private Integer ttl;
	private Long timestamp;

	/**
	 * Static Default Constructor
	 * 
	 * @return QueryOptions new instance
	 */

	public static QueryOptions builder() {
		return new QueryOptions();
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
	public QueryOptions withConsistencyLevel(ConsistencyLevel consistencyLevel) {
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
	public QueryOptions withRetryPolicy(RetryPolicy retryPolicy) {
		this.retryPolicy = retryPolicy;
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
	public QueryOptions withTtl(Integer ttl) {
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
	public QueryOptions withTimestamp(Long timestamp) {
		this.timestamp = timestamp;
		return this;
	}
}
