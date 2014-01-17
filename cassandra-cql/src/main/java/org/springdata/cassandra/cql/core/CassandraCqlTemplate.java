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
package org.springdata.cassandra.cql.core;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.cql.support.CassandraExceptionTranslator;
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
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.base.Function;
import com.google.common.collect.Iterators;

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
 * @author Alex Shvid
 * @author David Webb
 * @author Matthew Adams
 */
public class CassandraCqlTemplate implements CassandraCqlOperations {

	private final static Logger logger = LoggerFactory.getLogger(CassandraCqlTemplate.class);

	private Session session;
	private String keyspace;

	private CassandraExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	private AdminCqlOperations adminOperations;
	private SchemaCqlOperations schemaOperations;

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param session must not be {@literal null}.
	 * @param keyspace must not be {@literal null}.
	 */
	public CassandraCqlTemplate(Session session, String keyspace) {
		setSession(session);
		setKeyspace(keyspace);
		this.adminOperations = new DefaultAdminCqlOperations(this);
		this.schemaOperations = new DefaultSchemaCqlOperations(this, keyspace);
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
	 * @see org.springdata.cassandra.cql.support.CassandraExceptionTranslator
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
	public Query createQuery(QueryCreator qc) {
		Assert.notNull(qc);
		return doCreateQuery(qc);
	}

	@Override
	public <T> T execute(SessionCallback<T> sessionCallback) {
		Assert.notNull(sessionCallback);
		return doExecute(sessionCallback);
	}

	@Override
	public UpdateOperation update(final String cql) {
		Assert.notNull(cql);
		return new DefaultUpdateOperation(this, cql);
	}

	@Override
	public UpdateOperation update(PreparedStatement ps, PreparedStatementBinder psb) {
		Assert.notNull(ps);
		return new DefaultUpdateOperation(this, new SimplePreparedStatementQueryCreator(ps, psb));
	}

	@Override
	public UpdateOperation update(final BoundStatement bs) {
		Assert.notNull(bs);
		return new DefaultUpdateOperation(this, new QueryCreator() {

			@Override
			public Query createQuery() {
				return bs;
			}

		});
	}

	@Override
	public UpdateOperation update(final QueryCreator qc) {
		Assert.notNull(qc);
		return new DefaultUpdateOperation(this, qc);
	}

	@Override
	public UpdateOperation batchUpdate(final String[] cqls) {
		Assert.notNull(cqls);

		Iterator<Statement> statements = Iterators.transform(new ArrayIterator<String>(cqls),
				new Function<String, Statement>() {

					@Override
					public Statement apply(String cql) {
						return new SimpleStatement(cql);
					}

				});

		return batchUpdate(statements);
	}

	@Override
	public UpdateOperation batchUpdate(final Iterator<Statement> statements) {
		Assert.notNull(statements);

		return new DefaultUpdateOperation(this, new QueryCreator() {

			@Override
			public Query createQuery() {

				/*
				 * Return variable is a Batch statement
				 */
				final Batch batch = QueryBuilder.batch();

				boolean emptyBatch = true;
				while (statements.hasNext()) {

					Statement statement = statements.next();
					Assert.notNull(statement);

					batch.add(statement);
					emptyBatch = false;
				}

				if (emptyBatch) {
					throw new IllegalArgumentException("statements are empty");
				}

				return batch;
			}

		});
	}

	@Override
	public SelectOperation<ResultSet> select(String cql) {
		Assert.notNull(cql);
		Query query = new SimpleStatement(cql);
		return new DefaultSelectOperation(this, query);
	}

	@Override
	public SelectOperation<ResultSet> select(PreparedStatement ps, PreparedStatementBinder psb) {
		Assert.notNull(ps);
		BoundStatement bs = doBind(ps, psb);
		return new DefaultSelectOperation(this, bs);
	}

	@Override
	public SelectOperation<ResultSet> select(BoundStatement bs) {
		Assert.notNull(bs);
		return new DefaultSelectOperation(this, bs);
	}

	@Override
	public SelectOperation<ResultSet> select(QueryCreator qc) {
		Assert.notNull(qc);
		Query query = doCreateQuery(qc);
		return new DefaultSelectOperation(this, query);
	}

	/**
	 * Execute a query creator
	 * 
	 * @param callback
	 * @return
	 */
	protected Query doCreateQuery(QueryCreator qc) {

		try {

			return qc.createQuery();

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
	public <T> T doExecute(SessionCallback<T> callback) {

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
	public ResultSet doExecute(final Query query) {

		if (logger.isDebugEnabled()) {
			logger.debug(query.toString());
		}

		return doExecute(new SessionCallback<ResultSet>() {

			@Override
			public ResultSet doInSession(Session s) {

				return s.execute(query);
			}
		});
	}

	/**
	 * Execute as a command at the Session Level
	 * 
	 * @param callback
	 * @return
	 */
	public ResultSetFuture doExecuteAsync(final Query query) {

		if (logger.isDebugEnabled()) {
			logger.debug(query.toString());
		}

		return doExecute(new SessionCallback<ResultSetFuture>() {

			@Override
			public ResultSetFuture doInSession(Session s) {

				return s.executeAsync(query);
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

				for (Row row : resultSet) {
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
	public <T> T processOneFirstColumn(ResultSet resultSet, Class<T> elementType) {
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
	public <T> Iterator<T> processFirstColumn(ResultSet resultSet, Class<T> elementType) {
		Assert.notNull(resultSet);
		Assert.notNull(elementType);

		return doProcess(resultSet, new ResultSetCallback<Iterator<T>>() {

			@Override
			public Iterator<T> doWithResultSet(ResultSet resultSet) {

				return Iterators.transform(resultSet.iterator(), new Function<Row, T>() {

					@Override
					public T apply(Row row) {
						return (T) firstColumnToObject(row);
					}

				});

			}

		});

	}

	@Override
	public Iterator<Map<String, Object>> processAsMap(ResultSet resultSet) {
		Assert.notNull(resultSet);

		return doProcess(resultSet, new ResultSetCallback<Iterator<Map<String, Object>>>() {

			@Override
			public Iterator<Map<String, Object>> doWithResultSet(ResultSet resultSet) {

				return Iterators.transform(resultSet.iterator(), new Function<Row, Map<String, Object>>() {

					@Override
					public Map<String, Object> apply(Row row) {
						return toMap(row);
					}

				});

			}

		});

	}

	@Override
	public PreparedStatement prepareStatement(String cql) {
		Assert.notNull(cql);
		return doPrepareStatement(new SimplePreparedStatementCreator(cql));
	}

	@Override
	public PreparedStatement prepareStatement(PreparedStatementCreator psc) {
		Assert.notNull(psc);
		return doPrepareStatement(psc);
	}

	/**
	 * Service method to prepare statement
	 * 
	 * @param cql
	 * @param consistency
	 * @param retryPolicy
	 * @param traceQuery
	 * @return
	 */

	protected PreparedStatement doPrepareStatement(final PreparedStatementCreator psc) {

		return doExecute(new SessionCallback<PreparedStatement>() {

			@Override
			public PreparedStatement doInSession(Session session) {

				PreparedStatement ps = psc.createPreparedStatement(session);

				return ps;
			}

		});

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
	public <T> T execute(final PreparedStatement ps, final PreparedStatementCallback<T> rsc) {

		Assert.notNull(ps);
		Assert.notNull(rsc);

		return doExecute(ps, rsc);

	}

	/**
	 * Service method to bind PreparedStatement
	 * 
	 * @param ps
	 * @param psbOrNull
	 * @return
	 */

	protected BoundStatement doBind(final PreparedStatement ps, final PreparedStatementBinder psbOrNull) {

		Assert.notNull(ps);

		return doExecute(new SessionCallback<BoundStatement>() {

			@Override
			public BoundStatement doInSession(Session session) {

				BoundStatement bs = null;
				if (psbOrNull != null) {
					bs = psbOrNull.bindValues(ps);
				} else {
					bs = ps.bind();
				}

				return bs;
			}

		});

	}

	@Override
	public BoundStatement bind(PreparedStatement ps) {
		return doBind(ps, null);
	}

	@Override
	public BoundStatement bind(PreparedStatement ps, PreparedStatementBinder psb) {
		return doBind(ps, psb);
	}

	@Override
	public IngestOperation ingest(final PreparedStatement ps, Iterator<Object[]> rows) {

		Assert.notNull(ps);
		Assert.notNull(rows);

		Iterator<Query> queryIterator = Iterators.transform(rows, new Function<Object[], Query>() {

			@Override
			public Query apply(final Object[] values) {

				BoundStatement bs = doBind(ps, new PreparedStatementBinder() {

					@Override
					public BoundStatement bindValues(PreparedStatement ps) {
						return ps.bind(values);
					}
				});

				return bs;
			}
		});

		return new DefaultIngestOperation(this, queryIterator);

	}

	@Override
	public IngestOperation ingest(PreparedStatement ps, final Object[][] rows) {

		Assert.notNull(ps);
		Assert.notNull(rows);
		Assert.notEmpty(rows);

		return ingest(ps, new ArrayIterator<Object[]>(rows));
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
	public ProcessOperation<Long> countAll(final String tableName) {
		Assert.notNull(tableName);

		return select(new QueryCreator() {

			@Override
			public Query createQuery() {
				Select select = QueryBuilder.select().countAll().from(tableName);
				return select;
			}

		}).firstColumnOne(Long.class);
	}

	@Override
	public UpdateOperation truncate(final String tableName) {
		Assert.notNull(tableName);
		return new DefaultUpdateOperation(this, new QueryCreator() {

			@Override
			public Query createQuery() {
				return QueryBuilder.truncate(tableName);
			}

		});
	}

	@Override
	public AdminCqlOperations adminOps() {
		return adminOperations;
	}

	@Override
	public SchemaCqlOperations schemaOps() {
		return schemaOperations;
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

}
