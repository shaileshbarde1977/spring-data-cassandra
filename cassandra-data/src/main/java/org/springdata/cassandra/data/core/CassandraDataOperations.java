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

import org.springdata.cassandra.base.core.query.QueryOptions;
import org.springdata.cassandra.data.convert.CassandraConverter;

import com.datastax.driver.core.querybuilder.Select;

/**
 * Operations for interacting with Cassandra. These operations are used by the Repository implementation, but can also
 * be used directly when that is desired by the developer.
 * 
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 * 
 */
public interface CassandraDataOperations {

	/**
	 * The table name used for the specified class by this template.
	 * 
	 * @param entityClass must not be {@literal null}.
	 * @return
	 */
	String getTableName(Class<?> entityClass);

	/**
	 * Find by id
	 * 
	 * @param id
	 * @param entityClass
	 * @return
	 */

	<T> T findById(Object id, Class<T> entityClass);

	<T> T findById(Object id, Class<T> entityClass, String tableName);

	<T> T findById(Object id, Class<T> entityClass, QueryOptions options);

	<T> T findById(Object id, Class<T> entityClass, String tableName, QueryOptions options);

	<T> T findById(Object id, Class<T> entityClass, String tableName, Map<String, Object> optionsByName);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param query must not be {@literal null}.
	 * @param selectClass must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> List<T> findByQuery(String cql, Class<T> selectClass);

	/**
	 * Execute query and convert ResultSet to the list of entities
	 * 
	 * @param selectQuery must not be {@literal null}.
	 * @param selectClass must not be {@literal null}, mapped entity type.
	 * @return
	 */

	<T> List<T> findByQuery(Select selectQuery, Class<T> selectClass);

	/**
	 * Execute query and convert ResultSet to the entity
	 * 
	 * @param query must not be {@literal null}.
	 * @param selectClass must not be {@literal null}, mapped entity type.
	 * @return
	 */
	<T> T findOneByQuery(String cql, Class<T> selectClass);

	<T> T findOneByQuery(Select selectQuery, Class<T> selectClass);

	/**
	 * Counts rows for given query
	 * 
	 * @param selectQuery
	 * @return
	 */

	Long countByQuery(Select selectQuery);

	/**
	 * Counts all rows for given table
	 * 
	 * @param tableName
	 * @return
	 */

	Long count(String tableName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param entity
	 */
	<T> T saveNew(T entity);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param entity
	 * @param tableName
	 * @return
	 */
	<T> T saveNew(T entity, String tableName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T saveNew(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T saveNew(T entity, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T saveNew(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T saveNew(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given list of objects to the table by annotation table name.
	 * 
	 * @param entities
	 * @return
	 */
	<T> List<T> saveNewList(List<T> entities);

	/**
	 * Insert the given list of objects to the table by name.
	 * 
	 * @param entities
	 * @param tableName
	 * @return
	 */
	<T> List<T> saveNewList(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> saveNewList(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> saveNewList(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> saveNewList(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> saveNewList(List<T> entities, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T saveNewAsynchronously(T entity);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T saveNewAsynchronously(T entity, String tableName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T saveNewAsynchronously(T entity, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T saveNewAsynchronously(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T saveNewAsynchronously(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T saveNewAsynchronously(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> saveNewAsynchronously(List<T> entities);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> saveNewAsynchronously(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> saveNewAsynchronously(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> saveNewAsynchronously(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> saveNewAsynchronously(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> saveNewAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T save(T entity);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T save(T entity, String tableName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T save(T entity, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T save(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T save(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T save(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> saveList(List<T> entities);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> saveList(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> saveList(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> saveList(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> saveList(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> saveList(List<T> entities, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T saveAsynchronously(T entity);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> T saveAsynchronously(T entity, String tableName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T saveAsynchronously(T entity, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T saveAsynchronously(T entity, Map<String, Object> optionsByName);

	/**
	 * @param entity
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> T saveAsynchronously(T entity, String tableName, QueryOptions options);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> T saveAsynchronously(T entity, String tableName, Map<String, Object> optionsByName);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> saveAsynchronously(List<T> entities);

	/**
	 * Insert the given object to the table by id.
	 * 
	 * @param object
	 */
	<T> List<T> saveAsynchronously(List<T> entities, String tableName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> saveAsynchronously(List<T> entities, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> saveAsynchronously(List<T> entities, Map<String, Object> optionsByName);

	/**
	 * @param entities
	 * @param tableName
	 * @param options
	 * @return
	 */
	<T> List<T> saveAsynchronously(List<T> entities, String tableName, QueryOptions options);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsByName
	 * @return
	 */
	<T> List<T> saveAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName);

	/**
	 * Removes the given object by id from the given table.
	 * 
	 * @param id
	 * @param entityClass
	 * @param optionsOrNull
	 */
	<T> void deleteById(boolean asychronously, T id, Class<?> entityClass, QueryOptions optionsOrNull);

	/**
	 * Removes the given object by id from the given table.
	 * 
	 * @param id
	 * @param entityClass
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void deleteById(boolean asychronously, T id, Class<?> entityClass, String tableName, QueryOptions optionsOrNull);

	/**
	 * Remove list of objects from the table by given ids.
	 * 
	 * @param ids
	 * @param entityClass
	 * @param optionsOrNull
	 */
	<T> void deleteById(boolean asychronously, List<T> ids, Class<?> entityClass, QueryOptions optionsOrNull);

	/**
	 * Remove list of objects from the table by given ids.
	 * 
	 * @param ids
	 * @param entityClass
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void deleteById(boolean asychronously, List<T> ids, Class<?> entityClass, String tableName,
			QueryOptions optionsOrNull);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void delete(boolean asychronously, T entity, QueryOptions optionsOrNull);

	/**
	 * @param entity
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void delete(boolean asychronously, T entity, String tableName, QueryOptions optionsOrNull);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void delete(boolean asychronously, List<T> entities, QueryOptions optionsOrNull);

	/**
	 * @param entities
	 * @param tableName
	 * @param optionsOrNull
	 */
	<T> void delete(boolean asychronously, List<T> entities, String tableName, QueryOptions optionsOrNull);

	/**
	 * Returns the underlying {@link CassandraConverter}.
	 * 
	 * @return
	 */
	CassandraConverter getConverter();
}
