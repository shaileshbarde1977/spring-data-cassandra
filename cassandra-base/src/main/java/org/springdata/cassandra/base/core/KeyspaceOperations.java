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

import org.springdata.cassandra.base.core.cql.options.KeyspaceOptions;
import org.springdata.cassandra.base.core.query.QueryOptions;

import com.datastax.driver.core.KeyspaceMetadata;

/**
 * Operations for managing a Cassandra keyspace.
 * 
 * @author David Webb
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public interface KeyspaceOperations {

	/**
	 * Creates Keyspace with given options
	 * 
	 * @param keyspaceOptions Keyspace options.
	 * @param optionsOrNull The Query Options Object is exists
	 */
	void createKeyspace(KeyspaceOptions keyspaceOptions, QueryOptions optionsOrNull);

	/**
	 * Alters Keyspace with given name and options
	 * 
	 * @param keyspaceOptions Keyspace options.
	 * @param optionsOrNull The Query Options Object is exists
	 */
	void alterKeyspace(KeyspaceOptions keyspaceOptions, QueryOptions optionsOrNull);

	/**
	 * Drop keyspace
	 * 
	 * @param optionsOrNull The Query Options Object is exists
	 * 
	 */
	void dropKeyspace(QueryOptions optionsOrNull);

	/**
	 * Use keyspace
	 * 
	 * @param optionsOrNull The Query Options Object is exists
	 * 
	 */
	void useKeyspace(QueryOptions optionsOrNull);

	/**
	 * Use system keyspace
	 * 
	 * @param optionsOrNull The Query Options Object is exists
	 * 
	 */
	void useSystemKeyspace(QueryOptions optionsOrNull);

	/**
	 * Gets the keyspace metadata.
	 */
	KeyspaceMetadata getKeyspaceMetadata();

}
