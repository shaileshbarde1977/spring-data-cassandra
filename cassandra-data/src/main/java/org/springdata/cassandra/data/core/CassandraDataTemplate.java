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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springdata.cassandra.base.core.CassandraTemplate;
import org.springdata.cassandra.base.core.SessionCallback;
import org.springdata.cassandra.base.core.query.QueryOptions;
import org.springdata.cassandra.data.convert.CassandraConverter;
import org.springdata.cassandra.data.mapping.CassandraPersistentEntity;
import org.springdata.cassandra.data.mapping.CassandraPersistentProperty;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;

/**
 * The Cassandra Data Template is a convenience API for all Cassandra Operations using POJOs. This is the "Spring Data"
 * flavor of the template. For low level Cassandra Operations use the {@link CassandraTemplate}
 * 
 * @author Alex Shvid
 * @author David Webb
 */
public class CassandraDataTemplate extends CassandraTemplate implements CassandraDataOperations {

	/*
	 * List of iterable classes when testing POJOs for specific operations.
	 */
	public static final Collection<String> ITERABLE_CLASSES;
	static {

		Set<String> iterableClasses = new HashSet<String>();
		iterableClasses.add(List.class.getName());
		iterableClasses.add(Collection.class.getName());
		iterableClasses.add(Iterator.class.getName());

		ITERABLE_CLASSES = Collections.unmodifiableCollection(iterableClasses);

	}

	private String keyspace;
	private CassandraConverter cassandraConverter;
	private MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraDataTemplate(Session session, CassandraConverter converter, String keyspace) {
		super(session);
		Assert.notNull(converter);
		Assert.notNull(keyspace);
		this.keyspace = keyspace;
		this.cassandraConverter = converter;
		this.mappingContext = this.cassandraConverter.getMappingContext();
	}

	@Override
	public <T> T findById(Object id, Class<T> entityClass) {
		return findById(id, entityClass, getTableName(entityClass), Collections.<String, Object> emptyMap());
	}

	@Override
	public <T> T findById(Object id, Class<T> entityClass, String tableName) {
		return findById(id, entityClass, tableName, Collections.<String, Object> emptyMap());
	}

	@Override
	public <T> T findById(Object id, Class<T> entityClass, QueryOptions options) {
		return findById(id, entityClass, getTableName(entityClass), options);
	}

	@Override
	public <T> T findById(Object id, Class<T> entityClass, String tableName, QueryOptions options) {
		return doFindById(id, entityClass, tableName, options.toMap());
	}

	@Override
	public <T> T findById(Object id, Class<T> entityClass, String tableName, Map<String, Object> optionsByName) {
		return doFindById(id, entityClass, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#count(com.datastax.driver.core.querybuilder.Select)
	 */
	@Override
	public Long countByQuery(Select selectQuery) {
		return doSelectCount(selectQuery);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#count(java.lang.String)
	 */
	@Override
	public Long count(String tableName) {
		Select select = QueryBuilder.select().countAll().from(tableName);
		return doSelectCount(select);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.util.List)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, List<T> ids, Class<?> entityClass) {
		String tableName = getTableName(entityClass);
		Assert.notNull(tableName);
		deleteById(asychronously, ids, entityClass, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.util.List, java.util.Map)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, List<T> ids, Class<?> entityClass, Map<String, Object> optionsByName) {
		String tableName = getTableName(entityClass);
		Assert.notNull(tableName);
		deleteById(asychronously, ids, entityClass, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, List<T> ids, Class<?> entityClass, QueryOptions options) {
		String tableName = getTableName(entityClass);
		Assert.notNull(tableName);
		deleteById(asychronously, ids, entityClass, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.util.List, java.lang.String)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, List<T> ids, Class<?> entityClass, String tableName) {
		deleteById(asychronously, ids, entityClass, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, List<T> ids, Class<?> entityClass, String tableName,
			Map<String, Object> optionsByName) {
		Assert.notNull(ids);
		Assert.notEmpty(ids);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		doBatchDeleteById(asychronously, tableName, ids, entityClass, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, List<T> ids, Class<?> entityClass, String tableName,
			QueryOptions options) {
		deleteById(asychronously, ids, entityClass, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.lang.Object)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, T id, Class<?> entityClass) {
		String tableName = getTableName(entityClass);
		Assert.notNull(tableName);
		deleteById(asychronously, id, entityClass, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, T id, Class<?> entityClass, Map<String, Object> optionsByName) {
		String tableName = getTableName(entityClass);
		Assert.notNull(tableName);
		deleteById(asychronously, id, entityClass, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, T id, Class<?> entityClass, QueryOptions options) {
		String tableName = getTableName(entityClass);
		Assert.notNull(tableName);
		deleteById(asychronously, id, entityClass, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, T id, Class<?> entityClass, String tableName) {
		deleteById(asychronously, id, entityClass, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, T id, Class<?> entityClass, String tableName,
			Map<String, Object> optionsByName) {
		Assert.notNull(id);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		doDeleteById(asychronously, tableName, id, entityClass, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#deleteById(java.lang.Boolean, java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void deleteById(boolean asychronously, T id, Class<?> entityClass, String tableName, QueryOptions options) {
		deleteById(asychronously, id, entityClass, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.util.List)
	 */
	@Override
	public <T> void delete(boolean asychronously, List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		delete(asychronously, entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.util.List, java.util.Map)
	 */
	@Override
	public <T> void delete(boolean asychronously, List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		delete(asychronously, entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void delete(boolean asychronously, List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		delete(asychronously, entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.util.List, java.lang.String)
	 */
	@Override
	public <T> void delete(boolean asychronously, List<T> entities, String tableName) {
		Map<String, Object> defaultOptions = Collections.emptyMap();
		delete(asychronously, entities, tableName, defaultOptions);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> void delete(boolean asychronously, List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		doBatchDelete(asychronously, tableName, entities, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void delete(boolean asychronously, List<T> entities, String tableName, QueryOptions options) {
		delete(asychronously, entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.lang.Object)
	 */
	@Override
	public <T> void delete(boolean asychronously, T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		delete(asychronously, entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> void delete(boolean asychronously, T entity, Map<String, Object> optionsByName) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		delete(asychronously, entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void delete(boolean asychronously, T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		delete(asychronously, entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> void delete(boolean asychronously, T entity, String tableName) {
		Map<String, Object> defaultOptions = Collections.emptyMap();
		delete(asychronously, entity, tableName, defaultOptions);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> void delete(boolean asychronously, T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		doDelete(asychronously, tableName, entity, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#delete(java.lang.Boolean, java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void delete(boolean asychronously, T entity, String tableName, QueryOptions options) {
		delete(asychronously, entity, tableName, options.toMap());
	}

	/**
	 * @param entityClass
	 * @return
	 */
	public String determineTableName(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);
		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("No Persitent Entity information found for the class "
					+ entityClass.getName());
		}
		return entity.getTable();
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#getConverter()
	 */
	@Override
	public CassandraConverter getConverter() {
		return cassandraConverter;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#getTableName(java.lang.Class)
	 */
	@Override
	public String getTableName(Class<?> entityClass) {
		return determineTableName(entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.util.List)
	 */
	@Override
	public <T> List<T> saveNewList(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveNewList(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.util.List, java.util.Map)
	 */
	@Override
	public <T> List<T> saveNewList(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveNewList(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> saveNewList(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveNewList(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.util.List, java.lang.String)
	 */
	@Override
	public <T> List<T> saveNewList(List<T> entities, String tableName) {
		return saveNewList(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> List<T> saveNewList(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doBatchInsert(tableName, entities, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> saveNewList(List<T> entities, String tableName, QueryOptions options) {
		return saveNewList(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> T saveNew(T entity) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return saveNew(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> T saveNew(T entity, Map<String, Object> optionsByName) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return saveNew(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T saveNew(T entity, QueryOptions options) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return saveNew(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> T saveNew(T entity, String tableName) {
		return saveNew(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> T saveNew(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		ensureNotIterable(entity);
		return doInsert(tableName, entity, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insert(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T saveNew(T entity, String tableName, QueryOptions options) {
		return saveNew(entity, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.util.List)
	 */
	@Override
	public <T> List<T> saveNewAsynchronously(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveNewAsynchronously(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.util.List, java.util.Map)
	 */
	@Override
	public <T> List<T> saveNewAsynchronously(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveNewAsynchronously(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> saveNewAsynchronously(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveNewAsynchronously(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.util.List, java.lang.String)
	 */
	@Override
	public <T> List<T> saveNewAsynchronously(List<T> entities, String tableName) {
		return saveNewAsynchronously(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> List<T> saveNewAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doBatchInsert(tableName, entities, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> saveNewAsynchronously(List<T> entities, String tableName, QueryOptions options) {
		return saveNewAsynchronously(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.lang.Object)
	 */
	@Override
	public <T> T saveNewAsynchronously(T entity) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return saveNewAsynchronously(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> T saveNewAsynchronously(T entity, Map<String, Object> optionsByName) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return saveNewAsynchronously(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T saveNewAsynchronously(T entity, QueryOptions options) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return saveNewAsynchronously(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> T saveNewAsynchronously(T entity, String tableName) {
		return saveNewAsynchronously(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> T saveNewAsynchronously(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);

		ensureNotIterable(entity);

		return doInsert(tableName, entity, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#insertAsynchronously(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T saveNewAsynchronously(T entity, String tableName, QueryOptions options) {
		return saveNewAsynchronously(entity, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#select(com.datastax.driver.core.querybuilder.Select, java.lang.Class)
	 */
	@Override
	public <T> List<T> findByQuery(Select cql, Class<T> selectClass) {
		return findByQuery(cql.getQueryString(), selectClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#select(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> List<T> findByQuery(String cql, Class<T> selectClass) {
		return doSelect(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#selectOne(com.datastax.driver.core.querybuilder.Select, java.lang.Class)
	 */
	@Override
	public <T> T findOneByQuery(Select selectQuery, Class<T> selectClass) {
		return findOneByQuery(selectQuery.getQueryString(), selectClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#selectOne(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> T findOneByQuery(String cql, Class<T> selectClass) {
		return doSelectOne(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.util.List)
	 */
	@Override
	public <T> List<T> saveList(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveList(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.util.List, java.util.Map)
	 */
	@Override
	public <T> List<T> saveList(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveList(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> saveList(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveList(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.util.List, java.lang.String)
	 */
	@Override
	public <T> List<T> saveList(List<T> entities, String tableName) {
		return saveList(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> List<T> saveList(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doBatchUpdate(tableName, entities, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> saveList(List<T> entities, String tableName, QueryOptions options) {
		return saveList(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.lang.Object)
	 */
	@Override
	public <T> T save(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return save(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> T save(T entity, Map<String, Object> optionsByName) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return save(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T save(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return save(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> T save(T entity, String tableName) {

		return save(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> T save(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doUpdate(tableName, entity, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#update(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T save(T entity, String tableName, QueryOptions options) {
		return save(entity, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.util.List)
	 */
	@Override
	public <T> List<T> saveAsynchronously(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveAsynchronously(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.util.List, java.util.Map)
	 */
	@Override
	public <T> List<T> saveAsynchronously(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveAsynchronously(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> saveAsynchronously(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return saveAsynchronously(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.util.List, java.lang.String)
	 */
	@Override
	public <T> List<T> saveAsynchronously(List<T> entities, String tableName) {

		return saveAsynchronously(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> List<T> saveAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doBatchUpdate(tableName, entities, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> saveAsynchronously(List<T> entities, String tableName, QueryOptions options) {
		return saveAsynchronously(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.lang.Object)
	 */
	@Override
	public <T> T saveAsynchronously(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return saveAsynchronously(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> T saveAsynchronously(T entity, Map<String, Object> optionsByName) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return saveAsynchronously(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T saveAsynchronously(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return saveAsynchronously(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> T saveAsynchronously(T entity, String tableName) {

		return saveAsynchronously(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> T saveAsynchronously(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doUpdate(tableName, entity, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraDataOperations#updateAsynchronously(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T saveAsynchronously(T entity, String tableName, QueryOptions options) {
		return saveAsynchronously(entity, tableName, options.toMap());
	}

	/**
	 * @param obj
	 * @return
	 */
	private <T> String determineTableName(T obj) {
		if (null != obj) {
			return determineTableName(obj.getClass());
		}

		return null;
	}

	/**
	 * @param query
	 * @param readRowCallback
	 * @return
	 */
	private <T> List<T> doSelect(final String query, ReadRowCallback<T> readRowCallback) {

		ResultSet resultSet = doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(query);
			}
		});

		if (resultSet == null) {
			return null;
		}

		List<T> result = new ArrayList<T>();
		Iterator<Row> iterator = resultSet.iterator();
		while (iterator.hasNext()) {
			Row row = iterator.next();
			result.add(readRowCallback.doWith(row));
		}

		return result;
	}

	/**
	 * @param selectQuery
	 * @return
	 */
	private Long doSelectCount(final Select query) {

		Long count = null;

		ResultSet resultSet = doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(query);
			}
		});

		if (resultSet == null) {
			return null;
		}

		Iterator<Row> iterator = resultSet.iterator();
		while (iterator.hasNext()) {
			Row row = iterator.next();
			count = row.getLong(0);
		}

		return count;

	}

	/**
	 * @param query
	 * @param readRowCallback
	 * @return
	 */
	private <T> T doSelectOne(final String query, ReadRowCallback<T> readRowCallback) {

		logger.info(query);

		/*
		 * Run the Query
		 */
		ResultSet resultSet = doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) throws DataAccessException {
				return s.execute(query);
			}
		});

		if (resultSet == null) {
			return null;
		}

		Iterator<Row> iterator = resultSet.iterator();
		if (iterator.hasNext()) {
			Row row = iterator.next();
			T result = readRowCallback.doWith(row);
			if (iterator.hasNext()) {
				throw new DuplicateKeyException("found two or more results in query " + query);
			}
			return result;
		}

		return null;
	}

	/**
	 * Insert a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entities
	 * @param optionsByName
	 * @param insertAsychronously
	 * @return
	 */
	protected <T> List<T> doBatchInsert(final String tableName, final List<T> entities,
			Map<String, Object> optionsByName, final boolean insertAsychronously) {

		Assert.notEmpty(entities);

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		for (final T objectToSave : entities) {

			batch.add((Statement) toInsertQuery(tableName, objectToSave, optionsByName));

		}

		addQueryOptions(batch, optionsByName);

		logger.info(batch.getQueryString());

		return doExecute(new SessionCallback<List<T>>() {

			@Override
			public List<T> doInSession(Session s) throws DataAccessException {

				if (insertAsychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return entities;

			}
		});

	}

	/**
	 * Update a Batch of rows in a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entities
	 * @param optionsByName
	 * @param updateAsychronously
	 * @return
	 */
	protected <T> List<T> doBatchUpdate(final String tableName, final List<T> entities,
			Map<String, Object> optionsByName, final boolean updateAsychronously) {

		Assert.notEmpty(entities);

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		for (final T objectToSave : entities) {

			batch.add((Statement) toUpdateQuery(tableName, objectToSave, optionsByName));

		}

		addQueryOptions(batch, optionsByName);

		logger.info(batch.toString());

		return doExecute(new SessionCallback<List<T>>() {

			@Override
			public List<T> doInSession(Session s) throws DataAccessException {

				if (updateAsychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return entities;

			}
		});

	}

	/**
	 * Perform the removal of a Row.
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doDeleteById(final boolean asychronously, final String tableName, final T id,
			Class<?> entityClass, Map<String, Object> optionsByName) {

		final Query query = toDeleteQueryById(tableName, id, entityClass, optionsByName);

		logger.info(query.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {

				if (asychronously) {
					s.executeAsync(query);
				} else {
					s.execute(query);
				}

				return null;

			}
		});

	}

	/**
	 * Perform the removal of a Row.
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doDelete(final boolean asychronously, final String tableName, final T objectToRemove,
			Map<String, Object> optionsByName) {

		final Query query = toDeleteQuery(tableName, objectToRemove, optionsByName);

		logger.info(query.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {

				if (asychronously) {
					s.executeAsync(query);
				} else {
					s.execute(query);
				}

				return null;

			}
		});

	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T doExecute(SessionCallback<T> callback) {

		Assert.notNull(callback);

		try {

			return callback.doInSession(getSession());

		} catch (DataAccessException e) {
			throw translateIfPossible(e);
		}
	}

	/**
	 * Insert a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entity
	 */
	protected <T> T doInsert(final String tableName, final T entity, final Map<String, Object> optionsByName,
			final boolean insertAsychronously) {

		final Query query = toInsertQuery(tableName, entity, optionsByName);

		logger.info(query.toString());
		if (query.getConsistencyLevel() != null) {
			logger.info(query.getConsistencyLevel().name());
		}
		if (query.getRetryPolicy() != null) {
			logger.info(query.getRetryPolicy().toString());
		}

		return doExecute(new SessionCallback<T>() {

			@Override
			public T doInSession(Session s) throws DataAccessException {

				if (insertAsychronously) {
					s.executeAsync(query);
				} else {
					s.execute(query);
				}

				return entity;

			}
		});

	}

	/**
	 * Update a row into a Cassandra CQL Table
	 * 
	 * @param tableName
	 * @param entity
	 * @param optionsByName
	 * @param updateAsychronously
	 * @return
	 */
	protected <T> T doUpdate(final String tableName, final T entity, final Map<String, Object> optionsByName,
			final boolean updateAsychronously) {

		final Query q = toUpdateQuery(tableName, entity, optionsByName);

		logger.info(q.toString());

		return doExecute(new SessionCallback<T>() {

			@Override
			public T doInSession(Session s) throws DataAccessException {

				if (updateAsychronously) {
					s.executeAsync(q);
				} else {
					s.execute(q);
				}

				return entity;

			}
		});

	}

	/**
	 * Verify the object is not an iterable type
	 * 
	 * @param o
	 */
	protected void ensureNotIterable(Object o) {
		if (null != o) {
			if (o.getClass().isArray() || ITERABLE_CLASSES.contains(o.getClass().getName())) {
				throw new IllegalArgumentException("Cannot use a collection here.");
			}
		}
	}

	/**
	 * Generates a Query Object for an insert
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 */
	public Query toInsertQuery(String tableName, final Object objectToSave, Map<String, Object> optionsByName) {

		final Insert query = QueryBuilder.insertInto(keyspace, tableName);

		/*
		 * Write properties
		 */
		cassandraConverter.write(objectToSave, query);

		/*
		 * Add Query Options
		 */
		addQueryOptions(query, optionsByName);

		/*
		 * Add TTL to Insert object
		 */
		addInsertOptions(query, optionsByName);

		return query;

	}

	/**
	 * Generates a Query Object for an Update
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 */
	public Query toUpdateQuery(String tableName, final Object objectToSave, Map<String, Object> optionsByName) {

		final Update query = QueryBuilder.update(keyspace, tableName);

		/*
		 * Write properties
		 */
		cassandraConverter.write(objectToSave, query);

		/*
		 * Add Query Options
		 */
		addQueryOptions(query, optionsByName);

		/*
		 * Add TTL to Insert object
		 */
		addUpdateOptions(query, optionsByName);

		return query;

	}

	/**
	 * Create a Delete Query Object from an annotated POJO
	 * 
	 * @param keyspace
	 * @param tableName
	 * @param id
	 * @param entity
	 * @param optionsByName
	 * @return
	 */
	public Query toDeleteQueryById(String tableName, final Object id, Class<?> entityClass,
			Map<String, Object> optionsByName) {

		CassandraPersistentEntity<?> entity = getEntity(entityClass);

		final Delete.Selection ds = QueryBuilder.delete();
		final Delete query = ds.from(keyspace, tableName);
		final Where w = query.where();

		List<Clause> list = cassandraConverter.getPrimaryKey(entity, id);

		for (Clause c : list) {
			w.and(c);
		}

		addQueryOptions(query, optionsByName);

		addDeleteOptions(query, optionsByName);

		return query;

	}

	/**
	 * Perform the deletion on a list of objects
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doBatchDeleteById(final boolean asychronously, final String tableName, final List<T> ids,
			Class<?> entityClass, Map<String, Object> optionsByName) {

		Assert.notEmpty(ids);

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		for (final T id : ids) {

			batch.add((Statement) toDeleteQueryById(tableName, id, entityClass, optionsByName));

		}

		addQueryOptions(batch, optionsByName);

		logger.info(batch.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {

				if (asychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return null;

			}
		});

	}

	/**
	 * Create a Delete Query Object from an annotated POJO
	 * 
	 * @param keyspace
	 * @param tableName
	 * @param objectToRemove
	 * @param entity
	 * @param optionsByName
	 * @return
	 */
	public Query toDeleteQuery(String tableName, final Object objectToRemove, Map<String, Object> optionsByName) {

		final Delete.Selection ds = QueryBuilder.delete();
		final Delete query = ds.from(keyspace, tableName);
		final Where w = query.where();

		/*
		 * Write where condition to find by Id
		 */
		cassandraConverter.write(objectToRemove, w);

		addQueryOptions(query, optionsByName);

		addDeleteOptions(query, optionsByName);

		return query;

	}

	/**
	 * Perform the deletion on a list of objects
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doBatchDelete(final boolean asychronously, final String tableName, final List<T> entities,
			Map<String, Object> optionsByName) {

		Assert.notEmpty(entities);

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		for (final T objectToSave : entities) {

			batch.add((Statement) toDeleteQuery(tableName, objectToSave, optionsByName));

		}

		addQueryOptions(batch, optionsByName);

		logger.info(batch.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {

				if (asychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return null;

			}
		});

	}

	protected <T> T doFindById(Object id, Class<T> entityClass, String tableName, Map<String, Object> optionsByName) {
		Select select = QueryBuilder.select().all().from(tableName);
		Select.Where w = select.where();

		CassandraPersistentEntity<?> entity = getEntity(entityClass);

		List<Clause> list = cassandraConverter.getPrimaryKey(entity, id);

		for (Clause c : list) {
			w.and(c);
		}

		addQueryOptions(select, optionsByName);

		return doSelectOne(select.getQueryString(), new ReadRowCallback<T>(cassandraConverter, entityClass));

	}

	/**
	 * Service method for persistent entity lookup
	 * 
	 * @param entityClass
	 * @return CassandraPertistentEntity
	 */

	protected CassandraPersistentEntity<?> getEntity(Class<?> entityClass) {

		if (entityClass == null) {
			throw new InvalidDataAccessApiUsageException(
					"No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		if (entity == null) {
			throw new InvalidDataAccessApiUsageException("persistent entity not found for a given class " + entityClass);
		}

		return entity;
	}
}
