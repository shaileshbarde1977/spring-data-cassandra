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
package org.springframework.data.cassandra.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.springframework.cassandra.core.CassandraTemplate;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.core.query.QueryOptions;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
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

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#count(com.datastax.driver.core.querybuilder.Select)
	 */
	@Override
	public Long count(Select selectQuery) {
		return doSelectCount(selectQuery);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#count(java.lang.String)
	 */
	@Override
	public Long count(String tableName) {
		Select select = QueryBuilder.select().countAll().from(tableName);
		return doSelectCount(select);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.util.List)
	 */
	@Override
	public <T> void delete(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		delete(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.util.List, java.util.Map)
	 */
	@Override
	public <T> void delete(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		delete(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void delete(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		delete(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.util.List, java.lang.String)
	 */
	@Override
	public <T> void delete(List<T> entities, String tableName) {
		delete(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> void delete(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		doBatchDelete(tableName, entities, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void delete(List<T> entities, String tableName, QueryOptions options) {
		delete(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object)
	 */
	@Override
	public <T> void delete(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		delete(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> void delete(T entity, Map<String, Object> optionsByName) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		delete(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void delete(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		delete(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> void delete(T entity, String tableName) {
		delete(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> void delete(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		doDelete(tableName, entity, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#delete(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void delete(T entity, String tableName, QueryOptions options) {
		delete(entity, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.util.List)
	 */
	@Override
	public <T> void deleteAsynchronously(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.util.List, java.util.Map)
	 */
	@Override
	public <T> void deleteAsynchronously(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void deleteAsynchronously(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.util.List, java.lang.String)
	 */
	@Override
	public <T> void deleteAsynchronously(List<T> entities, String tableName) {
		insertAsynchronously(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> void deleteAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		doBatchDelete(tableName, entities, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void deleteAsynchronously(List<T> entities, String tableName, QueryOptions options) {
		deleteAsynchronously(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.lang.Object)
	 */
	@Override
	public <T> void deleteAsynchronously(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> void deleteAsynchronously(T entity, Map<String, Object> optionsByName) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void deleteAsynchronously(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		deleteAsynchronously(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> void deleteAsynchronously(T entity, String tableName) {
		deleteAsynchronously(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> void deleteAsynchronously(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		doDelete(tableName, entity, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#deleteAsynchronously(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> void deleteAsynchronously(T entity, String tableName, QueryOptions options) {
		deleteAsynchronously(entity, tableName, options.toMap());
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
	 * @see org.springframework.data.cassandra.core.CassandraOperations#getConverter()
	 */
	@Override
	public CassandraConverter getConverter() {
		return cassandraConverter;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#getTableName(java.lang.Class)
	 */
	@Override
	public String getTableName(Class<?> entityClass) {
		return determineTableName(entityClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.util.List)
	 */
	@Override
	public <T> List<T> insert(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insert(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.util.List, java.util.Map)
	 */
	@Override
	public <T> List<T> insert(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insert(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> insert(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insert(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.util.List, java.lang.String)
	 */
	@Override
	public <T> List<T> insert(List<T> entities, String tableName) {
		return insert(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> List<T> insert(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doBatchInsert(tableName, entities, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> insert(List<T> entities, String tableName, QueryOptions options) {
		return insert(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object)
	 */
	@Override
	public <T> T insert(T entity) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insert(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> T insert(T entity, Map<String, Object> optionsByName) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insert(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T insert(T entity, QueryOptions options) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insert(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> T insert(T entity, String tableName) {
		return insert(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> T insert(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		ensureNotIterable(entity);
		return doInsert(tableName, entity, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insert(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T insert(T entity, String tableName, QueryOptions options) {
		return insert(entity, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.util.List)
	 */
	@Override
	public <T> List<T> insertAsynchronously(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insertAsynchronously(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.util.List, java.util.Map)
	 */
	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insertAsynchronously(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return insertAsynchronously(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.util.List, java.lang.String)
	 */
	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, String tableName) {
		return insertAsynchronously(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doBatchInsert(tableName, entities, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> insertAsynchronously(List<T> entities, String tableName, QueryOptions options) {
		return insertAsynchronously(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.lang.Object)
	 */
	@Override
	public <T> T insertAsynchronously(T entity) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insertAsynchronously(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> T insertAsynchronously(T entity, Map<String, Object> optionsByName) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insertAsynchronously(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T insertAsynchronously(T entity, QueryOptions options) {
		String tableName = determineTableName(entity);
		Assert.notNull(tableName);
		return insertAsynchronously(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> T insertAsynchronously(T entity, String tableName) {
		return insertAsynchronously(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> T insertAsynchronously(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);

		ensureNotIterable(entity);

		return doInsert(tableName, entity, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#insertAsynchronously(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T insertAsynchronously(T entity, String tableName, QueryOptions options) {
		return insertAsynchronously(entity, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#select(com.datastax.driver.core.querybuilder.Select, java.lang.Class)
	 */
	@Override
	public <T> List<T> select(Select cql, Class<T> selectClass) {
		return select(cql.getQueryString(), selectClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#select(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> List<T> select(String cql, Class<T> selectClass) {
		return doSelect(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#selectOne(com.datastax.driver.core.querybuilder.Select, java.lang.Class)
	 */
	@Override
	public <T> T selectOne(Select selectQuery, Class<T> selectClass) {
		return selectOne(selectQuery.getQueryString(), selectClass);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#selectOne(java.lang.String, java.lang.Class)
	 */
	@Override
	public <T> T selectOne(String cql, Class<T> selectClass) {
		return doSelectOne(cql, new ReadRowCallback<T>(cassandraConverter, selectClass));
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.util.List)
	 */
	@Override
	public <T> List<T> update(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return update(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.util.List, java.util.Map)
	 */
	@Override
	public <T> List<T> update(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return update(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> update(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return update(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.util.List, java.lang.String)
	 */
	@Override
	public <T> List<T> update(List<T> entities, String tableName) {
		return update(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> List<T> update(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doBatchUpdate(tableName, entities, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> update(List<T> entities, String tableName, QueryOptions options) {
		return update(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object)
	 */
	@Override
	public <T> T update(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return update(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> T update(T entity, Map<String, Object> optionsByName) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return update(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T update(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return update(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> T update(T entity, String tableName) {

		return update(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> T update(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doUpdate(tableName, entity, optionsByName, false);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#update(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T update(T entity, String tableName, QueryOptions options) {
		return update(entity, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.util.List)
	 */
	@Override
	public <T> List<T> updateAsynchronously(List<T> entities) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entities, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.util.List, java.util.Map)
	 */
	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, Map<String, Object> optionsByName) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entities, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.util.List, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, QueryOptions options) {
		String tableName = getTableName(entities.get(0).getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entities, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.util.List, java.lang.String)
	 */
	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, String tableName) {

		return updateAsynchronously(entities, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.util.List, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entities);
		Assert.notEmpty(entities);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doBatchUpdate(tableName, entities, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.util.List, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> List<T> updateAsynchronously(List<T> entities, String tableName, QueryOptions options) {
		return updateAsynchronously(entities, tableName, options.toMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.lang.Object)
	 */
	@Override
	public <T> T updateAsynchronously(T entity) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entity, tableName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.lang.Object, java.util.Map)
	 */
	@Override
	public <T> T updateAsynchronously(T entity, Map<String, Object> optionsByName) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entity, tableName, optionsByName);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.lang.Object, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T updateAsynchronously(T entity, QueryOptions options) {
		String tableName = getTableName(entity.getClass());
		Assert.notNull(tableName);
		return updateAsynchronously(entity, tableName, options);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.lang.Object, java.lang.String)
	 */
	@Override
	public <T> T updateAsynchronously(T entity, String tableName) {

		return updateAsynchronously(entity, tableName, Collections.<String, Object> emptyMap());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.lang.Object, java.lang.String, java.util.Map)
	 */
	@Override
	public <T> T updateAsynchronously(T entity, String tableName, Map<String, Object> optionsByName) {
		Assert.notNull(entity);
		Assert.notNull(tableName);
		Assert.notNull(optionsByName);
		return doUpdate(tableName, entity, optionsByName, true);
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#updateAsynchronously(java.lang.Object, java.lang.String, org.springframework.data.cassandra.core.QueryOptions)
	 */
	@Override
	public <T> T updateAsynchronously(T entity, String tableName, QueryOptions options) {
		return updateAsynchronously(entity, tableName, options.toMap());
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
	 * Perform the deletion on a list of objects
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doBatchDelete(final String tableName, final List<T> entities, Map<String, Object> optionsByName,
			final boolean deleteAsynchronously) {

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

				if (deleteAsynchronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return null;

			}
		});

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
	protected <T> void doDelete(final String tableName, final T objectToRemove, Map<String, Object> optionsByName,
			final boolean deleteAsynchronously) {

		final Query query = toDeleteQuery(tableName, objectToRemove, optionsByName);

		logger.info(query.toString());

		doExecute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {

				if (deleteAsynchronously) {
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
}
