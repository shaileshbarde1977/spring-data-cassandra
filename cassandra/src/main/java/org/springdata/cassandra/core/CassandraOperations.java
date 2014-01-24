/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springdata.cassandra.core;

import java.util.Iterator;
import java.util.List;

import org.springdata.cassandra.convert.CassandraConverter;
import org.springdata.cassandra.cql.core.CqlOperations;
import org.springdata.cassandra.cql.core.query.StatementOptions;

/**
 * Operations for interacting with Cassandra. These operations are also used by the SimpleCassandraRepository
 * implementation.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 * 
 */
public interface CassandraOperations {

	/**
	 * The table name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	String getTableName(Class<?> entityClass);

	/**
	 * Finds all entities in table
	 * 
	 * @param entityClass
	 * @return GetOperation
	 */
	<T> GetOperation<Iterator<T>> findAll(Class<T> entityClass);

	/**
	 * Finds all entities with specific ids in table
	 * 
	 * @param entityClass
	 * @param ids
	 * @return GetOperation
	 */
	<T> GetOperation<List<T>> findAll(Class<T> entityClass, Iterable<?> ids);

	/**
	 * 
	 * @param entityClass
	 * @param id
	 * @return
	 */
	<T> GetOperation<T> findById(Class<T> entityClass, Object id);

	/**
	 * Finds a list of instances with the specified partition part of the primary key
	 * 
	 * @param entityClass
	 * @param id
	 * @param <T>
	 * @return
	 */
	<T> GetOperation<Iterator<T>> findByPartitionKey(Class<T> entityClass, Object id);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param query must not be {@literal null}.
	 * @param entityClass must not be {@literal null}, mapped entity type.
	 * @param optionsOrNull
	 * @return
	 */
	<T> List<T> find(String cql, Class<T> entityClass, StatementOptions optionsOrNull);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param query must not be {@literal null}.
	 * @param entityClass must not be {@literal null}, mapped entity type.
	 * @param optionsOrNull
	 * @return
	 */
	<T> T findOne(String query, Class<T> entityClass, StatementOptions optionsOrNull);

	/**
	 * Counts rows for given query
	 * 
	 * @param query
	 * @return
	 */
	Long count(String query, StatementOptions optionsOrNull);

	/**
	 * Checks if entity exists in Cassandra
	 * 
	 * @param entity
	 * @return GetOperation
	 */
	<T> GetOperation<Boolean> exists(T entity);

	/**
	 * Checks if entity exists in Cassandra
	 * 
	 * @param entityClass
	 * @param id
	 * @return GetOperation
	 */
	<T> GetOperation<Boolean> exists(Class<T> entityClass, Object id);

	/**
	 * Insert the given object to the table.
	 * 
	 * @param entity
	 * @param optionsOrNull
	 */
	<T> SaveNewOperation saveNew(T entity);

	/**
	 * Insert the given list of objects to the table.
	 * 
	 * @param entities
	 * @param optionsOrNull
	 * @return
	 */
	<T> BatchOperation saveNewInBatch(Iterable<T> entities);

	/**
	 * Updates the given object in the table.
	 * 
	 * @param entity to save
	 * @return
	 */
	<T> SaveOperation save(T entity);

	/**
	 * Updates list of objects in the table.
	 * 
	 * @param entities
	 * @return
	 */
	<T> BatchOperation saveInBatch(Iterable<T> entities);

	/**
	 * Removes the given object by id from the given table.
	 * 
	 * @param entityClass
	 * @param id
	 */
	<T> DeleteOperation deleteById(Class<T> entityClass, Object id);

	/**
	 * Remove list of objects from the table by given ids.
	 * 
	 * @param entityClass
	 * @param ids
	 */
	<T> BatchOperation deleteByIdInBatch(Class<T> entityClass, Iterable<?> ids);

	/**
	 * Remove entity from the table
	 * 
	 * @param entity
	 */
	<T> DeleteOperation delete(T entity);

	/**
	 * @param entities
	 */
	<T> BatchOperation deleteInBatch(Iterable<T> entities);

	/**
	 * Returns the underlying keyspace.
	 * 
	 * @return
	 */
	String getKeyspace();

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 * 
	 * @return
	 */
	CassandraConverter getConverter();

	/**
	 * Returns table specific operations
	 * 
	 * @return TableDataOperations
	 */
	SchemaOperations schemaOps();

	/**
	 * Returns Cql specific operations
	 * 
	 * @return CassandraCqlOperations
	 */
	CqlOperations cqlOps();

}
