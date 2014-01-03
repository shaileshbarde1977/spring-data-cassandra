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
package org.springdata.cassandra.base.core;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.base.core.query.ConsistencyLevelResolver;
import org.springdata.cassandra.base.core.query.ExecuteOptions;
import org.springdata.cassandra.base.core.query.RetryPolicyResolver;
import org.springdata.cassandra.base.support.CassandraExceptionTranslator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.util.Assert;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.querybuilder.Update;
import com.google.common.base.Function;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * <b>This is the Central class in the Cassandra core package.</b> It simplifies the use of Cassandra and helps to avoid
 * common errors. It executes the core Cassandra workflow, leaving application code to provide CQL and result
 * extraction. This class execute CQL Queries, provides different ways to extract/map results, and provides Exception
 * translation to the generic, more informative exception hierarchy defined in the <code>org.springframework.dao</code>
 * package.
 * 
 * <p>
 * For working with POJOs, use the CassandraDataTemplate.
 * </p>
 * 
 * @author David Webb
 * @author Matthew Adams
 * @author Alex Shvid
 */
public class CassandraTemplate implements CassandraOperations {

	/** Logger available to subclasses */
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private Session session;
	private String keyspace;

	private CassandraExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 * @param keyspace must not be {@literal null}.
	 */
	public CassandraTemplate(Session session, String keyspace) {
		setSession(session);
		setKeyspace(keyspace);
	}

	/**
	 * Ensure that the Cassandra Session has been set
	 */
	public void afterPropertiesSet() {
		if (getSession() == null) {
			throw new IllegalArgumentException("Property 'session' is required");
		}
		if (getKeyspace() == null) {
			throw new IllegalArgumentException("Property 'keyspace' is required");
		}
	}

	/**
	 * @return Returns the session.
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * @param session The session to set.
	 */
	public void setSession(Session session) {
		Assert.notNull(session);
		this.session = session;
	}

	/**
	 * @return Returns the keyspace.
	 */
	public String getKeyspace() {
		return keyspace;
	}

	/**
	 * @param keyspace The keyspace to set.
	 */
	public void setKeyspace(String keyspace) {
		Assert.notNull(session);
		this.keyspace = keyspace;
	}

	/**
	 * Set the exception translator for this instance.
	 * 
	 * @see org.springdata.cassandra.base.support.CassandraExceptionTranslator
	 */
	public void setExceptionTranslator(CassandraExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator;
	}

	/**
	 * Return the exception translator for this instance.
	 */
	public CassandraExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	@Override
	public <T> T execute(SessionCallback<T> sessionCallback) {
		Assert.notNull(sessionCallback);
		return doExecute(sessionCallback);
	}

	@Override
	public void execute(boolean asynchronously, String cql, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		if (asynchronously) {
			doExecuteAsync(cql, optionsOrNull);
		} else {
			doExecute(cql, optionsOrNull);
		}
	}

	@Override
	public <T> T select(String cql, final ResultSetCallback<T> rsc, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(rsc);
		ResultSet resultSet = doExecute(cql, optionsOrNull);
		return doProcess(resultSet, rsc);
	}

	@Override
	public CassandraFuture<ResultSet> selectAsync(final String cql, final ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		ResultSetFuture resultSetFuture = doExecuteAsync(cql, optionsOrNull);
		return new CassandraFuture<ResultSet>(resultSetFuture, getExceptionTranslator());
	}

	@Override
	public <T> T selectNonstop(final String cql, final ResultSetCallback<T> rsc, final int timeoutMls,
			final ExecuteOptions optionsOrNull) throws TimeoutException {
		Assert.notNull(cql);
		Assert.notNull(rsc);

		ResultSetFuture resultSetFuture = doExecuteAsync(cql, optionsOrNull);
		CassandraFuture<ResultSet> wrappedFuture = new CassandraFuture<ResultSet>(resultSetFuture, getExceptionTranslator());
		ResultSet resultSet = wrappedFuture.getUninterruptibly(timeoutMls, TimeUnit.MILLISECONDS);
		return doProcess(resultSet, rsc);
	}

	@Override
	public void select(String cql, final RowCallbackHandler rch, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(rch);
		process(doExecute(cql, optionsOrNull), rch);
	}

	@Override
	public void selectAsync(String cql, final RowCallbackHandler.Async rch, Executor executor, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(rch);
		Assert.notNull(executor);

		ResultSetFuture future = doExecuteAsync(cql, optionsOrNull);

		Futures.addCallback(future, new FutureCallback<ResultSet>() {

			@Override
			public void onFailure(Throwable t) {
				if (t instanceof RuntimeException) {
					t = translateIfPossible((RuntimeException) t);
				}
				rch.onFailure(t);
			}

			public void onSuccess(ResultSet rs) {
				process(rs, rch);
			}

		}, executor);

	}

	@Override
	public <T> Iterator<T> select(String cql, RowMapper<T> rowMapper, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(rowMapper);
		return process(doExecute(cql, optionsOrNull), rowMapper);
	}

	@Override
	public <T> CassandraFuture<Iterator<T>> selectAsync(String cql, final RowMapper<T> rowMapper,
			ExecuteOptions optionsOrNull) {

		Assert.notNull(cql);
		Assert.notNull(rowMapper);

		ResultSetFuture resultSetFuture = doExecuteAsync(cql, optionsOrNull);

		ListenableFuture<Iterator<T>> future = Futures.transform(resultSetFuture, new Function<ResultSet, Iterator<T>>() {

			@Override
			public Iterator<T> apply(ResultSet resultSet) {
				return process(resultSet, rowMapper);
			}

		});

		return new CassandraFuture<Iterator<T>>(future, getExceptionTranslator());
	}

	@Override
	public List<Map<String, Object>> selectAsListOfMap(String cql, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		return processAsListOfMap(doExecute(cql, optionsOrNull));
	}

	@Override
	public <T> List<T> selectFirstColumnAsList(String cql, Class<T> elementType, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(elementType);
		return processFirstColumnAsList(doExecute(cql, optionsOrNull), elementType);
	}

	@Override
	public Map<String, Object> selectOneAsMap(String cql, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		return processOneAsMap(doExecute(cql, optionsOrNull));
	}

	@Override
	public <T> T selectOne(String cql, Class<T> elementType, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(elementType);
		return processOne(doExecute(cql, optionsOrNull), elementType);
	}

	@Override
	public <T> T selectOne(String cql, RowMapper<T> rowMapper, ExecuteOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(rowMapper);
		return processOne(doExecute(cql, optionsOrNull), rowMapper);
	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T doExecute(SessionCallback<T> callback) {

		try {

			return callback.doInSession(getSession());

		} catch (RuntimeException e) {
			throw translateIfPossible(e);
		}
	}

	/**
	 * Execute as a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	public ResultSet doExecute(final String cql, final ExecuteOptions optionsOrNull) {

		logger.info(cql);

		return doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {

				SimpleStatement statement = new SimpleStatement(cql);

				addQueryOptions(statement, optionsOrNull);

				return s.execute(statement);
			}
		});
	}

	/**
	 * Execute asynchronously a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	public ResultSetFuture doExecuteAsync(final String cql, final ExecuteOptions optionsOrNull) {

		logger.info(cql);

		return doExecute(new SessionCallback<ResultSetFuture>() {

			@Override
			public ResultSetFuture doInSession(Session s) {

				SimpleStatement statement = new SimpleStatement(cql);

				addQueryOptions(statement, optionsOrNull);

				return s.executeAsync(statement);
			}
		});
	}

	/**
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected ResultSet doExecute(final BoundStatement bs, final ExecuteOptions optionsOrNull) {

		return doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {
				addQueryOptions(bs, optionsOrNull);
				return s.execute(bs);
			}
		});
	}

	/**
	 * Deserializes first column in the row.
	 * 
	 * @param row
	 * @return
	 */
	protected Object firstColumnToObject(Row row) {
		if (row == null) {
			return null;
		}

		ColumnDefinitions cols = row.getColumnDefinitions();
		if (cols.size() == 0) {
			return null;
		}
		return cols.getType(0).deserialize(row.getBytesUnsafe(0));
	}

	/**
	 * Deserializes row to the map.
	 * 
	 * @param row
	 * @return
	 */
	protected Map<String, Object> toMap(Row row) {
		if (row == null) {
			return null;
		}

		ColumnDefinitions cols = row.getColumnDefinitions();
		Map<String, Object> map = new HashMap<String, Object>(cols.size());

		for (Definition def : cols.asList()) {
			String name = def.getName();
			DataType dataType = def.getType();
			map.put(name, dataType.deserialize(row.getBytesUnsafe(name)));
		}

		return map;
	}

	@Override
	public Collection<RingMember> describeRing() {
		return Collections.unmodifiableCollection(describeRing(new RingMemberHostMapper()));
	}

	/**
	 * Pulls the list of Hosts for the current Session
	 * 
	 * @return
	 */
	protected Set<Host> getHosts() {

		/*
		 * Get the cluster metadata for this session
		 */
		Metadata clusterMetadata = doExecute(new SessionCallback<Metadata>() {

			@Override
			public Metadata doInSession(Session s) {
				return s.getCluster().getMetadata();
			}

		});

		/*
		 * Get all hosts in the cluster
		 */
		Set<Host> hosts = clusterMetadata.getAllHosts();

		return hosts;

	}

	/**
	 * Process result and handle exceptions
	 * 
	 * @param callback
	 * @return
	 */
	protected <T> T doProcess(final ResultSet resultSet, final ResultSetCallback<T> callback) {

		try {

			return callback.doWithResultSet(resultSet);

		} catch (RuntimeException e) {
			throw translateIfPossible(e);
		}
	}

	/**
	 * Service iterator
	 * 
	 * @author Alex Shvid
	 * 
	 * @param <T>
	 */

	protected class MappedRowIterator<T> implements Iterator<T> {

		final Iterator<Row> backingIterator;
		final RowMapper<T> mapper;
		int row = 0;

		MappedRowIterator(Iterator<Row> backingIterator, RowMapper<T> mapper) {
			this.backingIterator = backingIterator;
			this.mapper = mapper;
		}

		@Override
		public final boolean hasNext() {
			return backingIterator.hasNext();
		}

		@Override
		public final T next() {
			try {
				return mapper.mapRow(backingIterator.next(), ++row);
			} catch (RuntimeException e) {
				throw translateIfPossible(e);
			}
		}

		@Override
		public final void remove() {
			throw new UnsupportedOperationException("can not remove row from the ResultSet");
		}
	}

	@Override
	public <T> Collection<T> describeRing(HostMapper<T> hostMapper) {
		Assert.notNull(hostMapper);
		Set<Host> hosts = getHosts();
		return hostMapper.mapHosts(hosts);
	}

	@Override
	public void process(ResultSet resultSet, final RowCallbackHandler rch) {
		Assert.notNull(resultSet);
		Assert.notNull(rch);

		doProcess(resultSet, new ResultSetCallback<Object>() {

			@Override
			public Object doWithResultSet(ResultSet resultSet) {

				List<Row> rows = resultSet.all();
				if (rows == null || rows.isEmpty()) {
					return null;
				}

				for (Row row : rows) {
					rch.processRow(row);
				}

				return null;
			}

		});
	}

	@Override
	public <T> Iterator<T> process(ResultSet resultSet, final RowMapper<T> rowMapper) {
		Assert.notNull(resultSet);
		Assert.notNull(rowMapper);

		return doProcess(resultSet, new ResultSetCallback<Iterator<T>>() {

			@Override
			public Iterator<T> doWithResultSet(ResultSet resultSet) {
				return new MappedRowIterator<T>(resultSet.iterator(), rowMapper);
			}

		});

	}

	@Override
	public <T> T processOne(ResultSet resultSet, final RowMapper<T> rowMapper) {
		Assert.notNull(resultSet);
		Assert.notNull(rowMapper);

		return doProcess(resultSet, new ResultSetCallback<T>() {

			@Override
			public T doWithResultSet(ResultSet resultSet) {

				Iterator<Row> iterator = resultSet.iterator();
				if (!iterator.hasNext()) {
					return null;
				}

				Row firstRow = iterator.next();

				if (iterator.hasNext()) {
					throw new DataIntegrityViolationException("expected single row in the resultSet");
				}

				return rowMapper.mapRow(firstRow, 0);
			}

		});

	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> T processOne(ResultSet resultSet, Class<T> elementType) {
		Assert.notNull(resultSet);
		Assert.notNull(elementType);

		return doProcess(resultSet, new ResultSetCallback<T>() {

			@Override
			public T doWithResultSet(ResultSet resultSet) {

				Row row = resultSet.one();
				if (row == null) {
					return null;
				}

				return (T) firstColumnToObject(row);

			}

		});

	}

	@Override
	public Map<String, Object> processOneAsMap(ResultSet resultSet) {
		Assert.notNull(resultSet);

		return doProcess(resultSet, new ResultSetCallback<Map<String, Object>>() {

			@Override
			public Map<String, Object> doWithResultSet(ResultSet resultSet) {

				Row row = resultSet.one();
				if (row == null) {
					return Collections.emptyMap();
				}
				return toMap(row);

			}

		});

	}

	@SuppressWarnings("unchecked")
	@Override
	public <T> List<T> processFirstColumnAsList(ResultSet resultSet, Class<T> elementType) {
		Assert.notNull(resultSet);
		Assert.notNull(elementType);

		return doProcess(resultSet, new ResultSetCallback<List<T>>() {

			@Override
			public List<T> doWithResultSet(ResultSet resultSet) {

				List<T> list = new ArrayList<T>();
				for (Row row : resultSet) {
					list.add((T) firstColumnToObject(row));
				}
				return list;

			}

		});

	}

	@Override
	public List<Map<String, Object>> processAsListOfMap(ResultSet resultSet) {
		Assert.notNull(resultSet);

		return doProcess(resultSet, new ResultSetCallback<List<Map<String, Object>>>() {

			@Override
			public List<Map<String, Object>> doWithResultSet(ResultSet resultSet) {

				List<Row> rows = resultSet.all();
				if (rows == null || rows.isEmpty()) {
					return Collections.emptyList();
				}
				List<Map<String, Object>> list = new ArrayList<Map<String, Object>>(rows.size());
				for (Row row : rows) {
					list.add(toMap(row));
				}
				return list;

			}

		});

	}

	/**
	 * Attempt to translate a Runtime Exception to a Spring Data Exception
	 * 
	 * @param ex
	 * @return
	 */
	public RuntimeException translateIfPossible(RuntimeException ex) {
		RuntimeException resolved = getExceptionTranslator().translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

	@Override
	public PreparedStatement prepareStatement(final String cql, final ExecuteOptions optionsOrNull) {

		Assert.notNull(cql);

		return doExecute(new SessionCallback<PreparedStatement>() {

			@Override
			public PreparedStatement doInSession(Session session) {

				PreparedStatementCreator psc = new SimplePreparedStatementCreator(cql);

				PreparedStatement ps = psc.createPreparedStatement(session);

				addPreparedStatementOptions(ps, optionsOrNull);

				return ps;
			}

		});

	}

	@Override
	public PreparedStatement prepareStatement(final PreparedStatementCreator psc, final ExecuteOptions optionsOrNull) {

		Assert.notNull(psc);

		return doExecute(new SessionCallback<PreparedStatement>() {

			@Override
			public PreparedStatement doInSession(Session session) {

				PreparedStatement ps = psc.createPreparedStatement(session);

				addPreparedStatementOptions(ps, optionsOrNull);

				return ps;
			}

		});

	}

	@Override
	public <T> T execute(final PreparedStatement ps, final PreparedStatementCallback<T> rsc) {

		Assert.notNull(ps);
		Assert.notNull(rsc);

		return doExecute(ps, rsc);

	}

	@Override
	public TableOperations tableOps(String tableName) {
		return new DefaultTableOperations(this, keyspace, tableName);
	}

	@Override
	public IndexOperations indexOps(String tableName) {
		return new DefaultIndexOperations(this, keyspace, tableName);
	}

	/**
	 * Service method to deal with PreparedStatements
	 * 
	 * @param psc
	 * @param rsc
	 * @param optionsOrNull
	 * @return
	 */

	protected <T> T doExecute(final PreparedStatement ps, final PreparedStatementCallback<T> rsc) {

		return doExecute(new SessionCallback<T>() {

			@Override
			public T doInSession(Session session) {
				return rsc.doWithPreparedStatement(session, ps);
			}

		});
	}

	@Override
	public <T> T select(PreparedStatement ps, ResultSetCallback<T> rsc, ExecuteOptions optionsOrNull) {
		Assert.notNull(ps);
		Assert.notNull(rsc);
		return select(ps, null, rsc, optionsOrNull);
	}

	@Override
	public void select(PreparedStatement ps, RowCallbackHandler rch, ExecuteOptions optionsOrNull) {
		Assert.notNull(ps);
		Assert.notNull(rch);
		select(ps, null, rch, optionsOrNull);
	}

	@Override
	public <T> Iterator<T> select(PreparedStatement ps, RowMapper<T> rowMapper, ExecuteOptions optionsOrNull) {
		Assert.notNull(ps);
		Assert.notNull(rowMapper);
		return select(ps, null, rowMapper, optionsOrNull);
	}

	@Override
	public <T> T select(PreparedStatement ps, final PreparedStatementBinder psbOrNull, final ResultSetCallback<T> rsc,
			final ExecuteOptions optionsOrNull) {

		Assert.notNull(ps);
		Assert.notNull(rsc);

		return doExecute(ps, new PreparedStatementCallback<T>() {
			public T doWithPreparedStatement(Session session, PreparedStatement ps) {

				BoundStatement bs = null;
				if (psbOrNull != null) {
					bs = psbOrNull.bindValues(ps);
				} else {
					bs = ps.bind();
				}

				addQueryOptions(bs, optionsOrNull);

				return rsc.doWithResultSet(session.execute(bs));
			}
		});
	}

	@Override
	public void select(PreparedStatement ps, final PreparedStatementBinder psbOrNull, final RowCallbackHandler rch,
			final ExecuteOptions optionsOrNull) {
		Assert.notNull(ps);
		Assert.notNull(rch);

		doExecute(ps, new PreparedStatementCallback<Object>() {

			public Object doWithPreparedStatement(Session session, PreparedStatement ps) {

				BoundStatement bs = null;
				if (psbOrNull != null) {
					bs = psbOrNull.bindValues(ps);
				} else {
					bs = ps.bind();
				}

				addQueryOptions(bs, optionsOrNull);

				ResultSet rs = session.execute(bs);
				for (Row row : rs) {
					rch.processRow(row);
				}
				return null;
			}
		});

	}

	@Override
	public <T> Iterator<T> select(PreparedStatement ps, final PreparedStatementBinder psbOrNull,
			final RowMapper<T> rowMapper, final ExecuteOptions optionsOrNull) {

		Assert.notNull(ps);
		Assert.notNull(rowMapper);

		return doExecute(ps, new PreparedStatementCallback<Iterator<T>>() {

			public Iterator<T> doWithPreparedStatement(Session session, PreparedStatement ps) {

				BoundStatement bs = null;
				if (psbOrNull != null) {
					bs = psbOrNull.bindValues(ps);
				} else {
					bs = ps.bind();
				}

				addQueryOptions(bs, optionsOrNull);

				ResultSet rs = session.execute(bs);

				return new MappedRowIterator<T>(rs.iterator(), rowMapper);

			}
		});

	}

	@Override
	public void ingest(String cql, Iterable<Object[]> rowIterator, ExecuteOptions optionsOrNull) {

		Assert.notNull(cql);
		Assert.notNull(rowIterator);

		PreparedStatement preparedStatement = getSession().prepare(cql);
		addPreparedStatementOptions(preparedStatement, optionsOrNull);

		for (Object[] values : rowIterator) {
			getSession().execute(preparedStatement.bind(values));
		}

	}

	@Override
	public void ingest(String cql, final Object[][] rows, final ExecuteOptions optionsOrNull) {

		Assert.notNull(cql);
		Assert.notNull(rows);
		Assert.notEmpty(rows);

		ingest(cql, new Iterable<Object[]>() {

			public Iterator<Object[]> iterator() {
				return new ArrayIterator<Object[]>(rows);
			}
		}, optionsOrNull);
	}

	/**
	 * Service iterator based on array
	 * 
	 * @author Alex Shvid
	 * 
	 * @param <T>
	 */

	private static class ArrayIterator<T> implements Iterator<T> {

		private T[] array;
		private int pos = 0;

		public ArrayIterator(T[] array) {
			this.array = array;
		}

		public boolean hasNext() {
			return array.length > pos;
		}

		public T next() {
			return array[pos++];
		}

		public void remove() {
			throw new UnsupportedOperationException("Cannot remove an element of an array.");
		}

	}

	@Override
	public void truncate(boolean asynchronously, String tableName, ExecuteOptions optionsOrNull) throws DataAccessException {
		Truncate truncate = QueryBuilder.truncate(tableName);
		if (asynchronously) {
			doExecuteAsync(truncate.getQueryString(), optionsOrNull);
		} else {
			doExecute(truncate.getQueryString(), optionsOrNull);
		}
	}

	@Override
	public KeyspaceOperations keyspaceOps() {
		return new DefaultKeyspaceOperations(this);
	}

	/**
	 * Add common Query options for all types of queries.
	 * 
	 * @param q
	 * @param optionsOrNull
	 */

	public static void addQueryOptions(Query q, ExecuteOptions optionsOrNull) {

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
	 * Add common insert options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */

	public static void addInsertOptions(Insert query, ExecuteOptions optionsOrNull) {

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
	 * Add common update options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */

	public static void addUpdateOptions(Update query, ExecuteOptions optionsOrNull) {

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
	 * Add common delete options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */

	public static void addDeleteOptions(Delete query, ExecuteOptions optionsOrNull) {

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
	 * Add common Query options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */
	public static void addPreparedStatementOptions(PreparedStatement ps, ExecuteOptions optionsOrNull) {

		if (optionsOrNull == null) {
			return;
		}

		/*
		 * Add Query Options if exists
		 */

		if (optionsOrNull.getConsistencyLevel() != null) {
			ps.setConsistencyLevel(ConsistencyLevelResolver.resolve(optionsOrNull.getConsistencyLevel()));
		}
		if (optionsOrNull.getRetryPolicy() != null) {
			ps.setRetryPolicy(RetryPolicyResolver.resolve(optionsOrNull.getRetryPolicy()));
		}

	}

}
