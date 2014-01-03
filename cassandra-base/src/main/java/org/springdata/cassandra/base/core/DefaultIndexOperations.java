/*
 * Copyright 2014 the original author or authors.
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
package org.springdata.cassandra.base.core;

import org.springframework.util.Assert;

/**
 * Default Index Operations implementation
 * 
 * @author Alex Shvid
 * 
 */
public class DefaultIndexOperations implements IndexOperations {

	private final CassandraTemplate cassandraTemplate;
	private final String keyspace;
	private final String tableName;

	protected DefaultIndexOperations(CassandraTemplate cassandraTemplate, String keyspace, String tableName) {

		Assert.notNull(cassandraTemplate);
		Assert.notNull(keyspace);
		Assert.notNull(tableName);

		this.cassandraTemplate = cassandraTemplate;
		this.keyspace = keyspace;
		this.tableName = tableName;
	}

}
