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
package org.springdata.cassandra.data.core;

import java.util.List;
import java.util.Map;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.TableMetadata;

/**
 * Operations for managing a Cassandra keyspace.
 * 
 * @author David Webb
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public interface CassandraAdminOperations {

	/**
	 * Creates Keyspace with given name
	 * 
	 * @param keyspace Name of the keyspace
	 * @param optionsByName Table options, given by the string option name and the appropriate option value.
	 * @return returns true if a keyspace was created, false if not.
	 */
	void createKeyspace(String keyspace, Map<String, Object> optionsByName);

	/**
	 * Alters Keyspace with given name and options
	 * 
	 * @param keyspace Name of the keyspace
	 * @param optionsByName Table options, given by the string option name and the appropriate option value.
	 */
	void alterKeyspace(String keyspace, Map<String, Object> optionsByName);

	/**
	 * Drop keyspace
	 * 
	 * @param keyspace Name of the keyspace
	 */
	void dropKeyspace(String keyspace);

	/**
	 * Use keyspace
	 * 
	 * @param keyspace Name of the keyspace
	 */
	void useKeyspace(String keyspace);

	/**
	 * Get the keyspace metadata.
	 */
	KeyspaceMetadata getKeyspaceMetadata();

	/**
	 * Get the given table's metadata.
	 * 
	 * @param tableName The name of the table.
	 */
	TableMetadata getTableMetadata(String tableName);

	/**
	 * Get table name defined in the entity class
	 * 
	 * @param entityClass
	 * @return String tableName
	 */
	String getTableName(Class<?> entityClass);

	/**
	 * Create a table with the name given and fields corresponding to the given class. If the table already exists and
	 * parameter <code>ifNotExists</code> is {@literal true}, this is a no-op and {@literal false} is returned. If the
	 * table doesn't exist, parameter <code>ifNotExists</code> is ignored, the table is created and {@literal true} is
	 * returned.
	 * 
	 * @param ifNotExists If true, will only create the table if it doesn't exist, else the create operation will be
	 *          ignored and the method will return {@literal false}.
	 * @param tableName The name of the table.
	 * @param entityClass The class whose fields determine the columns created.
	 * @param optionsByName Table options, given by the string option name and the appropriate option value.
	 * @return Returns true if a table was created, false if not.
	 */
	boolean createTable(boolean ifNotExists, String tableName, Class<?> entityClass, Map<String, Object> optionsByName);

	/**
	 * Add columns to the given table from the given class. If parameter dropRemovedAttributColumns is true, then this
	 * effectively becomes a synchronization operation between the class's fields and the existing table's columns.
	 * 
	 * @param tableName The name of the existing table.
	 * @param entityClass The class whose fields determine the columns added.
	 * @param dropRemovedAttributeColumns Whether to drop columns that exist on the table but that don't have
	 *          corresponding fields in the class. If true, this effectively becomes a synchronziation operation.
	 */
	void alterTable(String tableName, Class<?> entityClass, boolean dropRemovedAttributeColumns);

	/**
	 * Validate columns in the given table from the given class.
	 * 
	 * @param tableName The name of the existing table.
	 * @param entityClass The class whose fields determine the columns added.
	 * @return Returns alter table statement or null
	 */
	String validateTable(String tableName, Class<?> entityClass);

	/**
	 * Drops the existing table with the given name and creates a new one; basically a {@link #dropTable(String)} followed
	 * by a {@link #createTable(boolean, String, Class, Map)}.
	 * 
	 * @param tableName The name of the table.
	 * @param entityClass The class whose fields determine the new table's columns.
	 * @param optionsByName Table options, given by the string option name and the appropriate option value.
	 */
	void replaceTable(String tableName, Class<?> entityClass, Map<String, Object> optionsByName);

	/**
	 * Drops the named table.
	 * 
	 * @param tableName The name of the table.
	 */
	void dropTable(String tableName);

	/**
	 * Create all indexed annotated in entityClass
	 * 
	 * @param tableName The name of the table.
	 * @param entityClass The class whose fields determine the new table's columns.
	 */
	void createIndexes(String tableName, Class<?> entityClass);

	/**
	 * Create all indexed annotated in entityClass
	 * 
	 * @param tableName The name of the table.
	 * @param entityClass The class whose fields determine the new table's columns.
	 */
	void alterIndexes(String tableName, Class<?> entityClass);

	/**
	 * Create all indexed annotated in entityClass
	 * 
	 * @param tableName The name of the table.
	 * @param entityClass The class whose fields determine the new table's columns.
	 * @return List of the cql statement to change indexes
	 */
	List<String> validateIndexes(String tableName, Class<?> entityClass);

}
