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
package org.springdata.cassandra.cql.core;

import com.datastax.driver.core.TableMetadata;

/**
 * Cassandra Schema Operations interface
 * 
 * TODO:
 * 
 * Add createTable, alterTable, dropIndex methods
 * 
 * @author Alex Shvid
 * 
 */
public interface CassandraSchemaOperations {

	/**
	 * Get the given table's metadata.
	 * 
	 * @param tableName The name of the table.
	 */
	TableMetadata getTableMetadata(String tableName);

}
