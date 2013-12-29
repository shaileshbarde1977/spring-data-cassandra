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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.base.core.query.ConsistencyLevelResolver;
import org.springdata.cassandra.base.core.query.QueryOptions;
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
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Truncate;
import com.datastax.driver.core.querybuilder.Update;

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

	private CassandraExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 */
	public CassandraTemplate(Session session) {
		setSession(session);
	}

	/**
	 * Ensure that the Cassandra Session has been set
	 */
	public void afterPropertiesSet() {
		if (getSession() == null) {
			throw new IllegalArgumentException("Property 'session' is required");
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
	public void execute(boolean asynchronously, String cql, QueryOptions optionsOrNull) {
		Assert.notNull(cql);
		doExecute(asynchronously, cql, optionsOrNull);
	}

	@Override
	public <T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> rse, final QueryOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(rse);
		return rse.extractData(execute(new SessionCallback<ResultSetFuture>() {
			@Override
			public ResultSetFuture doInSession(Session s) {
				Statement statement = new SimpleStatement(cql);
				addQueryOptions(statement, optionsOrNull);
				return s.executeAsync(statement);
			}
		}));
	}

	@Override
	public <T> T query(String cql, ResultSetExtractor<T> rse, QueryOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(rse);
		ResultSet rs = doExecute(cql, optionsOrNull);
		return rse.extractData(rs);
	}

	@Override
	public void query(String cql, RowCallbackHandler rch, QueryOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(rch);
		process(doExecute(cql, optionsOrNull), rch);
	}

	@Override
	public <T> Iterator<T> select(String cql, RowMapper<T> rowMapper, QueryOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(rowMapper);
		return process(doExecute(cql, optionsOrNull), rowMapper);
	}

	@Override
	public List<Map<String, Object>> queryForListOfMap(String cql, QueryOptions optionsOrNull) {
		Assert.notNull(cql);
		return processListOfMap(doExecute(cql, optionsOrNull));
	}

	@Override
	public <T> List<T> queryForList(String cql, Class<T> elementType, QueryOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(elementType);
		return processList(doExecute(cql, optionsOrNull), elementType);
	}

	@Override
	public Map<String, Object> queryForMap(String cql, QueryOptions optionsOrNull) {
		Assert.notNull(cql);
		return processMap(doExecute(cql, optionsOrNull));
	}

	@Override
	public <T> T queryForObject(String cql, Class<T> elementType, QueryOptions optionsOrNull) {
		Assert.notNull(cql);
		Assert.notNull(elementType);
		return processOne(doExecute(cql, optionsOrNull), elementType);
	}

	@Override
	public <T> T queryForObject(String cql, RowMapper<T> rowMapper, QueryOptions optionsOrNull) {
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
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected void doExecute(final boolean asynchronously, final String cql, final QueryOptions optionsOrNull) {

		logger.info(cql);

		doExecute(new SessionCallback<Object>() {

			@Override
			public ResultSet doInSession(Session s) {

				SimpleStatement statement = new SimpleStatement(cql);

				addQueryOptions(statement, optionsOrNull);

				if (asynchronously) {
					s.executeAsync(statement);
				} else {
					s.execute(statement);
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
	protected ResultSet doExecute(final String cql, final QueryOptions optionsOrNull) {

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
	 * Execute a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	protected ResultSet doExecute(final BoundStatement bs, final QueryOptions optionsOrNull) {

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
			try {
				return backingIterator.hasNext();
			} catch (RuntimeException e) {
				throw translateIfPossible(e);
			}
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
			try {
				backingIterator.remove();
			} catch (RuntimeException e) {
				throw translateIfPossible(e);
			}
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

				Iterator<Row> iterator = resultSet.iterator();
				if (iterator == null) {
					return Collections.<T> emptyList().iterator();
				}

				return new MappedRowIterator<T>(iterator, rowMapper);
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

				List<Row> rows = resultSet.all();
				if (rows == null || rows.isEmpty()) {
					return null;
				}

				if (rows.size() > 1) {
					throw new DataIntegrityViolationException("expected single row in the resultSet");
				}

				return rowMapper.mapRow(rows.get(0), 0);
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
	public Map<String, Object> processMap(ResultSet resultSet) {
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
	public <T> List<T> processList(ResultSet resultSet, Class<T> elementType) {
		Assert.notNull(resultSet);
		Assert.notNull(elementType);

		return doProcess(resultSet, new ResultSetCallback<List<T>>() {

			@Override
			public List<T> doWithResultSet(ResultSet resultSet) {

				List<Row> rows = resultSet.all();
				if (rows == null || rows.isEmpty()) {
					return Collections.emptyList();
				}
				List<T> list = new ArrayList<T>(rows.size());
				for (Row row : rows) {
					list.add((T) firstColumnToObject(row));
				}
				return list;

			}

		});

	}

	@Override
	public List<Map<String, Object>> processListOfMap(ResultSet resultSet) {
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
	protected RuntimeException translateIfPossible(RuntimeException ex) {
		RuntimeException resolved = getExceptionTranslator().translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}

	@Override
	public <T> T execute(final PreparedStatementCreator psc, final PreparedStatementCallback<T> action,
			final QueryOptions optionsOrNull) {

		Assert.notNull(psc);
		Assert.notNull(action);

		return doExecute(new SessionCallback<T>() {

			@Override
			public T doInSession(Session session) {
				PreparedStatement ps = psc.createPreparedStatement(getSession());
				addPreparedStatementOptions(ps, optionsOrNull);
				return action.doInPreparedStatement(ps);
			}

		});

	}

	@Override
	public <T> T execute(String cql, PreparedStatementCallback<T> action, QueryOptions optionsOrNull) {
		return execute(new SimplePreparedStatementCreator(cql), action, optionsOrNull);
	}

	@Override
	public <T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse, QueryOptions optionsOrNull) {
		Assert.notNull(psc);
		Assert.notNull(rse);
		return query(psc, null, rse, optionsOrNull);
	}

	@Override
	public void query(PreparedStatementCreator psc, RowCallbackHandler rch, QueryOptions optionsOrNull) {
		Assert.notNull(psc);
		Assert.notNull(rch);
		query(psc, null, rch, optionsOrNull);
	}

	@Override
	public <T> Iterator<T> select(PreparedStatementCreator psc, RowMapper<T> rowMapper, QueryOptions optionsOrNull) {
		Assert.notNull(psc);
		Assert.notNull(rowMapper);
		return select(psc, null, rowMapper, optionsOrNull);
	}

	@Override
	public <T> T query(PreparedStatementCreator psc, final PreparedStatementBinder psbOrNull,
			final ResultSetExtractor<T> rse, final QueryOptions optionsOrNull) {

		Assert.notNull(psc);
		Assert.notNull(rse);

		return execute(psc, new PreparedStatementCallback<T>() {
			public T doInPreparedStatement(PreparedStatement ps) {
				ResultSet rs = null;
				BoundStatement bs = null;
				if (psbOrNull != null) {
					bs = psbOrNull.bindValues(ps);
				} else {
					bs = ps.bind();
				}
				rs = doExecute(bs, optionsOrNull);
				return rse.extractData(rs);
			}
		}, optionsOrNull);
	}

	@Override
	public <T> T query(String cql, PreparedStatementBinder psbOrNull, ResultSetExtractor<T> rse,
			QueryOptions optionsOrNull) {
		return query(new SimplePreparedStatementCreator(cql), psbOrNull, rse, optionsOrNull);
	}

	@Override
	public void query(String cql, PreparedStatementBinder psbOrNull, RowCallbackHandler rch, QueryOptions optionsOrNull) {
		query(new SimplePreparedStatementCreator(cql), psbOrNull, rch, optionsOrNull);
	}

	@Override
	public <T> Iterator<T> select(String cql, PreparedStatementBinder psbOrNull, RowMapper<T> rowMapper,
			QueryOptions optionsOrNull) {
		return select(new SimplePreparedStatementCreator(cql), psbOrNull, rowMapper, optionsOrNull);
	}

	@Override
	public void query(PreparedStatementCreator psc, final PreparedStatementBinder psbOrNull,
			final RowCallbackHandler rch, final QueryOptions optionsOrNull) {
		Assert.notNull(psc);
		Assert.notNull(rch);

		execute(psc, new PreparedStatementCallback<Object>() {
			public Object doInPreparedStatement(PreparedStatement ps) {
				ResultSet rs = null;
				BoundStatement bs = null;
				if (psbOrNull != null) {
					bs = psbOrNull.bindValues(ps);
				} else {
					bs = ps.bind();
				}
				rs = doExecute(bs, optionsOrNull);
				process(rs, rch);
				return null;
			}
		}, optionsOrNull);
	}

	@Override
	public <T> Iterator<T> select(PreparedStatementCreator psc, final PreparedStatementBinder psbOrNull,
			final RowMapper<T> rowMapper, final QueryOptions optionsOrNull) {

		Assert.notNull(psc);
		Assert.notNull(rowMapper);

		return execute(psc, new PreparedStatementCallback<Iterator<T>>() {
			public Iterator<T> doInPreparedStatement(PreparedStatement ps) {
				ResultSet rs = null;
				BoundStatement bs = null;
				if (psbOrNull != null) {
					bs = psbOrNull.bindValues(ps);
				} else {
					bs = ps.bind();
				}
				rs = doExecute(bs, optionsOrNull);

				return process(rs, rowMapper);
			}
		}, optionsOrNull);
	}

	@Override
	public void ingest(String cql, Iterable<Object[]> rowIterator, QueryOptions optionsOrNull) {

		Assert.notNull(cql);
		Assert.notNull(rowIterator);

		PreparedStatement preparedStatement = getSession().prepare(cql);
		addPreparedStatementOptions(preparedStatement, optionsOrNull);

		for (Object[] values : rowIterator) {
			getSession().execute(preparedStatement.bind(values));
		}

	}

	@Override
	public void ingest(String cql, final Object[][] rows, final QueryOptions optionsOrNull) {

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
	public void truncate(boolean asynchronously, String tableName, QueryOptions optionsOrNull) throws DataAccessException {
		Truncate truncate = QueryBuilder.truncate(tableName);
		doExecute(asynchronously, truncate.getQueryString(), optionsOrNull);
	}

	/**
	 * Add common Query options for all types of queries.
	 * 
	 * @param q
	 * @param optionsOrNull
	 */

	public static void addQueryOptions(Query q, QueryOptions optionsOrNull) {

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

	public static void addInsertOptions(Insert query, QueryOptions optionsOrNull) {

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

	public static void addUpdateOptions(Update query, QueryOptions optionsOrNull) {

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

	public static void addDeleteOptions(Delete query, QueryOptions optionsOrNull) {

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
	public static void addPreparedStatementOptions(PreparedStatement s, QueryOptions optionsOrNull) {

		if (optionsOrNull == null) {
			return;
		}

		/*
		 * Add Query Options if exists
		 */

		if (optionsOrNull.getConsistencyLevel() != null) {
			s.setConsistencyLevel(ConsistencyLevelResolver.resolve(optionsOrNull.getConsistencyLevel()));
		}
		if (optionsOrNull.getRetryPolicy() != null) {
			s.setRetryPolicy(RetryPolicyResolver.resolve(optionsOrNull.getRetryPolicy()));
		}

	}

}
