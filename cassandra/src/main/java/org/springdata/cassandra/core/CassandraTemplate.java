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
package org.springdata.cassandra.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.convert.CassandraConverter;
import org.springdata.cassandra.cql.core.CqlOperations;
import org.springdata.cassandra.cql.core.CqlTemplate;
import org.springdata.cassandra.cql.core.SessionCallback;
import org.springdata.cassandra.cql.core.query.ConsistencyLevelResolver;
import org.springdata.cassandra.cql.core.query.RetryPolicyResolver;
import org.springdata.cassandra.cql.core.query.StatementOptions;
import org.springdata.cassandra.mapping.CassandraPersistentEntity;
import org.springdata.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.util.Assert;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

/**
 * The CassandraTemplate is a convenience API for all CassandraOperations using POJOs. This is the "Spring Data" flavor
 * of the template. For low level CassandraOperations use the {@link CqlTemplate}
 * 
 * @author Alex Shvid
 * @author David Webb
 */
public class CassandraTemplate implements CassandraOperations {

	private final static Logger logger = LoggerFactory.getLogger(CassandraTemplate.class);

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

	private final CqlTemplate cqlTemplate;
	private final CassandraConverter cassandraConverter;
	private final String keyspace;
	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final SchemaOperations schemaDataOperations;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 * @param converter must not be {@literal null}.
	 */
	public CassandraTemplate(Session session, CassandraConverter converter, String keyspace) {
		Assert.notNull(session);
		Assert.notNull(converter);
		this.cqlTemplate = new CqlTemplate(session, keyspace);
		this.cassandraConverter = converter;
		this.keyspace = keyspace;
		this.mappingContext = this.cassandraConverter.getMappingContext();
		this.schemaDataOperations = new DefaultSchemaOperations(this);
	}

	@Override
	public <T> GetOperation<Iterator<T>> findAll(Class<T> entityClass) {
		Assert.notNull(entityClass);

		return new AbstractFindOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				Select select = QueryBuilder.select().all().from(cassandraTemplate.getKeyspace(), getTableName());
				return select;
			}

		};
	}

	@Override
	public <T> GetOperation<List<T>> findAll(final Class<T> entityClass, final Iterable<?> ids) {
		Assert.notNull(entityClass);
		Assert.notNull(ids);

		return new DefaultMultiFindOperation<T>(this, entityClass, ids.iterator());
	}

	@Override
	public <T> GetOperation<T> findById(Class<T> entityClass, final Object id) {
		Assert.notNull(entityClass);
		Assert.notNull(id);

		return new AbstractFindOneOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				Select select = QueryBuilder.select().all().from(cassandraTemplate.getKeyspace(), getTableName());
				Select.Where w = select.where();

				CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

				List<Clause> list = cassandraConverter.getPrimaryKey(entity, id);

				for (Clause c : list) {
					w.and(c);
				}

				return select;
			}

		};

	}

	@Override
	public <T> GetOperation<Iterator<T>> findByPartitionKey(Class<T> entityClass, final Object id) {
		Assert.notNull(entityClass);
		Assert.notNull(id);

		return new AbstractFindOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				Select select = QueryBuilder.select().all().from(cassandraTemplate.getKeyspace(), getTableName());
				Select.Where w = select.where();

				CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

				List<Clause> list = cassandraConverter.getPartitionKey(entity, id);

				for (Clause c : list) {
					w.and(c);
				}

				return select;
			}

		};

	}

	@Override
	public <T> GetOperation<Iterator<T>> find(Class<T> entityClass, final String cql) {
		Assert.notNull(entityClass);
		Assert.notNull(cql);

		return new AbstractFindOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				return new SimpleStatement(cql);
			}

		};

	}

	@Override
	public <T> GetOperation<T> findOne(Class<T> entityClass, final String cql) {
		Assert.notNull(entityClass);
		Assert.notNull(cql);

		return new AbstractFindOneOperation<T>(this, entityClass) {

			@Override
			public Query createQuery() {
				return new SimpleStatement(cql);
			}

		};

	}

	@Override
	public <T> BatchOperation deleteByIdInBatch(final Class<T> entityClass, Iterable<?> ids) {
		Assert.notNull(entityClass);
		Assert.notNull(ids);

		final CassandraTemplate cassandraTemplate = this;
		Iterator<BatchedStatementCreator> creators = Iterators.transform(ids.iterator(),
				new Function<Object, BatchedStatementCreator>() {

					@Override
					public BatchedStatementCreator apply(Object id) {

						Assert.notNull(id);
						assertNotIterable(id);

						return new DefaultDeleteOperation<T>(cassandraTemplate, entityClass, id);
					}

				});

		return new DefaultBatchOperation(this, creators);
	}

	@Override
	public <T> DefaultDeleteOperation<T> deleteById(Class<T> entityClass, Object id) {
		Assert.notNull(entityClass);
		Assert.notNull(id);
		assertNotIterable(id);
		return new DefaultDeleteOperation<T>(this, entityClass, id);
	}

	@Override
	public <T> BatchOperation deleteInBatch(Iterable<T> entities) {
		Assert.notNull(entities);

		final CassandraTemplate cassandraTemplate = this;
		Iterator<BatchedStatementCreator> creators = Iterators.transform(entities.iterator(),
				new Function<T, BatchedStatementCreator>() {

					@Override
					public BatchedStatementCreator apply(T entity) {

						Assert.notNull(entity);
						assertNotIterable(entity);

						return new DefaultDeleteOperation<T>(cassandraTemplate, entity);
					}

				});

		return new DefaultBatchOperation(this, creators);
	}

	@Override
	public <T> DeleteOperation delete(T entity) {
		Assert.notNull(entity);
		assertNotIterable(entity);
		return new DefaultDeleteOperation<T>(this, entity);
	}

	@Override
	public <T> GetOperation<Long> count(Class<T> entityClass) {
		Assert.notNull(entityClass);
		return new DefaultCountOperation<T>(this, entityClass);
	}

	@Override
	public <T> GetOperation<Boolean> exists(T entity) {
		Assert.notNull(entity);
		return new DefaultExistsOperation<T>(this, entity);
	}

	@Override
	public <T> GetOperation<Boolean> exists(Class<T> entityClass, Object id) {
		Assert.notNull(entityClass);
		Assert.notNull(id);
		return new DefaultExistsOperation<T>(this, entityClass, id);
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
		return entity.getTableName();
	}

	@Override
	public CassandraConverter getConverter() {
		return cassandraConverter;
	}

	@Override
	public String getKeyspace() {
		return keyspace;
	}

	@Override
	public String getTableName(Class<?> entityClass) {
		return determineTableName(entityClass);
	}

	@Override
	public <T> BatchOperation saveNewInBatch(Iterable<T> entities) {
		Assert.notNull(entities);

		final CassandraTemplate cassandraTemplate = this;
		Iterator<BatchedStatementCreator> creators = Iterators.transform(entities.iterator(),
				new Function<T, BatchedStatementCreator>() {

					@Override
					public BatchedStatementCreator apply(T entity) {

						Assert.notNull(entity);
						assertNotIterable(entity);

						return new DefaultSaveNewOperation<T>(cassandraTemplate, entity);
					}

				});

		return new DefaultBatchOperation(this, creators);

	}

	@Override
	public <T> SaveNewOperation saveNew(T entity) {
		Assert.notNull(entity);
		assertNotIterable(entity);
		return new DefaultSaveNewOperation<T>(this, entity);
	}

	@Override
	public <T> BatchOperation saveInBatch(Iterable<T> entities) {
		Assert.notNull(entities);

		final CassandraTemplate cassandraTemplate = this;
		Iterator<BatchedStatementCreator> creators = Iterators.transform(entities.iterator(),
				new Function<T, BatchedStatementCreator>() {

					@Override
					public BatchedStatementCreator apply(T entity) {

						Assert.notNull(entity);
						assertNotIterable(entity);

						return new DefaultSaveOperation<T>(cassandraTemplate, entity);
					}

				});

		return new DefaultBatchOperation(this, creators);
	}

	@Override
	public <T> SaveOperation save(T entity) {
		Assert.notNull(entity);
		assertNotIterable(entity);
		return new DefaultSaveOperation<T>(this, entity);
	}

	@Override
	public SchemaOperations schemaOps() {
		return schemaDataOperations;
	}

	@Override
	public CqlOperations cqlOps() {
		return cqlTemplate;
	}

	public CqlTemplate cqlTemplate() {
		return cqlTemplate;
	}

	/**
	 * @param obj
	 * @return
	 */
	protected <T> String determineTableName(T obj) {
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
	protected <T> List<T> doSelect(final String cql, ReadRowCallback<T> readRowCallback,
			final StatementOptions optionsOrNull) {

		ResultSet resultSet = cqlTemplate.execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {

				SimpleStatement statement = new SimpleStatement(cql);

				addQueryOptions(statement, optionsOrNull);

				return s.execute(statement);
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
	protected Long doSelectCount(final String cql, final StatementOptions optionsOrNull) {

		Long count = null;

		ResultSet resultSet = cqlTemplate.execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {

				SimpleStatement statement = new SimpleStatement(cql);

				addQueryOptions(statement, optionsOrNull);

				return s.execute(statement);
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
	protected <T> T doSelectOne(final String cql, ReadRowCallback<T> readRowCallback, final StatementOptions optionsOrNull) {

		logger.debug(cql);

		/*
		 * Run the Query
		 */
		ResultSet resultSet = cqlTemplate.execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {

				SimpleStatement statement = new SimpleStatement(cql);

				addQueryOptions(statement, optionsOrNull);

				return s.execute(statement);
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
				throw new DuplicateKeyException("found two or more results in query " + cql);
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
	protected <T> void doBatchInsert(final boolean insertAsychronously, String tableNameOrNull,
			final Iterable<T> entities, StatementOptions optionsOrNull) {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		boolean emptyBatch = true;
		for (final T objectToSave : entities) {

			Assert.notNull(objectToSave);
			assertNotIterable(objectToSave);

			if (tableNameOrNull == null) {
				tableNameOrNull = getTableName(objectToSave.getClass());
				Assert.notNull(tableNameOrNull);
			}

			batch.add((Statement) toInsertQuery(tableNameOrNull, objectToSave, optionsOrNull));
			emptyBatch = false;
		}

		if (emptyBatch) {
			throw new IllegalArgumentException("entities are empty");
		}

		addQueryOptions(batch, optionsOrNull);

		if (logger.isDebugEnabled()) {
			logger.debug(batch.getQueryString());
		}

		cqlTemplate.execute(new SessionCallback<Object>() {

			@Override
			public List<T> doInSession(Session s) {

				if (insertAsychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return null;

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
	protected <T> void doBatchUpdate(final boolean updateAsychronously, String tableNameOrNull,
			final Iterable<T> entities, StatementOptions optionsOrNull) {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		boolean emptyBatch = true;
		for (final T objectToSave : entities) {

			Assert.notNull(objectToSave);
			assertNotIterable(objectToSave);

			if (tableNameOrNull == null) {
				tableNameOrNull = getTableName(objectToSave.getClass());
				Assert.notNull(tableNameOrNull);
			}

			batch.add((Statement) toUpdateQuery(tableNameOrNull, objectToSave, optionsOrNull));
			emptyBatch = false;
		}

		if (emptyBatch) {
			throw new IllegalArgumentException("entities are empty");
		}

		addQueryOptions(batch, optionsOrNull);

		if (logger.isDebugEnabled()) {
			logger.debug(batch.toString());
		}

		cqlTemplate.execute(new SessionCallback<Object>() {

			@Override
			public List<T> doInSession(Session s) {

				if (updateAsychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
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
	protected <T> void doDeleteById(final boolean asychronously, final String tableName, final T id,
			Class<?> entityClass, StatementOptions optionsOrNull) {

		final Query query = toDeleteQueryById(tableName, id, entityClass, optionsOrNull);

		if (logger.isDebugEnabled()) {
			logger.debug(query.toString());
		}

		cqlTemplate.execute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) {

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
			StatementOptions optionsOrNull) {

		final Query query = toDeleteQuery(tableName, objectToRemove, optionsOrNull);

		if (logger.isDebugEnabled()) {
			logger.debug(query.toString());
		}

		cqlTemplate.execute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) {

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
	 * Verify the object is not an iterable type
	 * 
	 * @param o
	 */
	protected void assertNotIterable(Object o) {
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
	protected Query toInsertQuery(String tableName, final Object objectToSave, StatementOptions optionsOrNull) {

		final Insert query = QueryBuilder.insertInto(keyspace, tableName);

		/*
		 * Write properties
		 */
		cassandraConverter.write(objectToSave, query);

		/*
		 * Add Query Options
		 */
		addQueryOptions(query, optionsOrNull);

		/*
		 * Add TTL to Insert object
		 */
		addInsertOptions(query, optionsOrNull);

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
	protected Query toUpdateQuery(String tableName, final Object objectToSave, StatementOptions optionsOrNull) {

		final Update query = QueryBuilder.update(keyspace, tableName);

		/*
		 * Write properties
		 */
		cassandraConverter.write(objectToSave, query);

		/*
		 * Add Query Options
		 */
		addQueryOptions(query, optionsOrNull);

		/*
		 * Add TTL to Insert object
		 */
		addUpdateOptions(query, optionsOrNull);

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
	protected Query toDeleteQueryById(String tableName, final Object id, Class<?> entityClass,
			StatementOptions optionsOrNull) {

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

		final Delete.Selection ds = QueryBuilder.delete();
		final Delete query = ds.from(keyspace, tableName);
		final Where w = query.where();

		List<Clause> list = cassandraConverter.getPrimaryKey(entity, id);

		for (Clause c : list) {
			w.and(c);
		}

		addQueryOptions(query, optionsOrNull);

		addDeleteOptions(query, optionsOrNull);

		return query;

	}

	/**
	 * Perform the deletion on a list of objects
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doBatchDeleteById(final boolean asychronously, String tableName, final Iterable<T> ids,
			Class<?> entityClass, StatementOptions optionsOrNull) {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		boolean emptyBatch = true;
		for (final T id : ids) {

			Assert.notNull(id);
			assertNotIterable(id);

			batch.add((Statement) toDeleteQueryById(tableName, id, entityClass, optionsOrNull));
			emptyBatch = false;

		}

		if (emptyBatch) {
			throw new IllegalArgumentException("ids are empty");
		}

		addQueryOptions(batch, optionsOrNull);

		if (logger.isDebugEnabled()) {
			logger.debug(batch.toString());
		}

		cqlTemplate.execute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) {

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
	protected Query toDeleteQuery(String tableName, final Object objectToRemove, StatementOptions optionsOrNull) {

		final Delete.Selection ds = QueryBuilder.delete();
		final Delete query = ds.from(keyspace, tableName);
		final Where w = query.where();

		/*
		 * Write where condition to find by Id
		 */
		cassandraConverter.write(objectToRemove, w);

		addQueryOptions(query, optionsOrNull);

		addDeleteOptions(query, optionsOrNull);

		return query;

	}

	/**
	 * Perform the deletion on a list of objects
	 * 
	 * @param tableName
	 * @param objectToRemove
	 */
	protected <T> void doBatchDelete(final boolean asychronously, String tableNameOrNull, final Iterable<T> entities,
			StatementOptions optionsOrNull) {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch batch = QueryBuilder.batch();

		boolean emptyBatch = true;
		for (final T objectToSave : entities) {

			Assert.notNull(objectToSave);
			assertNotIterable(objectToSave);

			if (tableNameOrNull == null) {
				tableNameOrNull = getTableName(objectToSave.getClass());
				Assert.notNull(tableNameOrNull);
			}

			batch.add((Statement) toDeleteQuery(tableNameOrNull, objectToSave, optionsOrNull));
			emptyBatch = false;

		}

		if (emptyBatch) {
			throw new IllegalArgumentException("entities are empty");
		}

		addQueryOptions(batch, optionsOrNull);

		if (logger.isDebugEnabled()) {
			logger.debug(batch.toString());
		}

		cqlTemplate.execute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) {

				if (asychronously) {
					s.executeAsync(batch);
				} else {
					s.execute(batch);
				}

				return null;

			}
		});

	}

	protected <T> T doFindById(Object id, Class<T> entityClass, String tableName, StatementOptions optionsOrNull) {
		Select select = QueryBuilder.select().all().from(tableName);
		Select.Where w = select.where();

		CassandraPersistentEntity<?> entity = getPersistentEntity(entityClass);

		List<Clause> list = cassandraConverter.getPrimaryKey(entity, id);

		for (Clause c : list) {
			w.and(c);
		}

		return doSelectOne(select.getQueryString(), new ReadRowCallback<T>(cassandraConverter, entityClass), optionsOrNull);

	}

	/**
	 * Service method for persistent entity lookup
	 * 
	 * @param entityClass
	 * @return CassandraPertistentEntity
	 */

	protected CassandraPersistentEntity<?> getPersistentEntity(Class<?> entityClass) {

		if (entityClass == null) {
			throw new IllegalArgumentException("No class parameter provided, entity table name can't be determined!");
		}

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(entityClass);

		if (entity == null) {
			throw new MappingException("persistent entity not found for a given class " + entityClass);
		}

		return entity;
	}

	/**
	 * Add common delete options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */

	protected static void addDeleteOptions(Delete query, StatementOptions optionsOrNull) {

		if (optionsOrNull == null) {
			return;
		}

		/*
		 * Add TTL to Insert object
		 */
		if (optionsOrNull.getTimestamp() != null) {
			query.using(QueryBuilder.timestamp(optionsOrNull.getTimestamp()));
		}

	}

	/**
	 * Add common update options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */

	protected static void addUpdateOptions(Update query, StatementOptions optionsOrNull) {

		if (optionsOrNull == null) {
			return;
		}

		/*
		 * Add TTL to Insert object
		 */
		if (optionsOrNull.getTtl() != null) {
			query.using(QueryBuilder.ttl(optionsOrNull.getTtl()));
		}
		if (optionsOrNull.getTimestamp() != null) {
			query.using(QueryBuilder.timestamp(optionsOrNull.getTimestamp()));
		}

	}

	/**
	 * Add common insert options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */

	protected static void addInsertOptions(Insert query, StatementOptions optionsOrNull) {

		if (optionsOrNull == null) {
			return;
		}

		/*
		 * Add TTL to Insert object
		 */
		if (optionsOrNull.getTtl() != null) {
			query.using(QueryBuilder.ttl(optionsOrNull.getTtl()));
		}
		if (optionsOrNull.getTimestamp() != null) {
			query.using(QueryBuilder.timestamp(optionsOrNull.getTimestamp()));
		}

	}

	/**
	 * Add common Query options for all types of queries.
	 * 
	 * @param q
	 * @param optionsOrNull
	 */

	protected static void addQueryOptions(Query q, StatementOptions optionsOrNull) {

		if (optionsOrNull == null) {
			return;
		}

		/*
		 * Add Query Options
		 */
		if (optionsOrNull.getConsistencyLevel() != null) {
			q.setConsistencyLevel(ConsistencyLevelResolver.resolve(optionsOrNull.getConsistencyLevel()));
		}
		if (optionsOrNull.getRetryPolicy() != null) {
			q.setRetryPolicy(RetryPolicyResolver.resolve(optionsOrNull.getRetryPolicy()));
		}

	}

	/**
	 * Execute as a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected ResultSet doExecute(final String cql, final StatementOptions optionsOrNull) {

		logger.debug(cql);

		return cqlTemplate.execute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {

				SimpleStatement statement = new SimpleStatement(cql);

				addQueryOptions(statement, optionsOrNull);

				return s.execute(statement);
			}
		});
	}

	protected RuntimeException translateIfPossible(RuntimeException ex) {
		return cqlTemplate.translateIfPossible(ex);
	}
}
