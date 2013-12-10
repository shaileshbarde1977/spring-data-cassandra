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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.SessionCallback;
import org.springframework.cassandra.core.cql.generator.AlterKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.AlterTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateIndexCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropIndexCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.UseKeyspaceCqlGenerator;
import org.springframework.cassandra.core.cql.spec.AlterKeyspaceSpecification;
import org.springframework.cassandra.core.cql.spec.AlterTableSpecification;
import org.springframework.cassandra.core.cql.spec.CreateIndexSpecification;
import org.springframework.cassandra.core.cql.spec.CreateKeyspaceSpecification;
import org.springframework.cassandra.core.cql.spec.CreateTableSpecification;
import org.springframework.cassandra.core.cql.spec.DropIndexSpecification;
import org.springframework.cassandra.core.cql.spec.DropKeyspaceSpecification;
import org.springframework.cassandra.core.cql.spec.DropTableSpecification;
import org.springframework.cassandra.core.cql.spec.UseKeyspaceSpecification;
import org.springframework.cassandra.core.cql.spec.WithNameSpecification;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.cassandra.support.exception.CassandraTableExistsException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Default implementation of {@link CassandraAdminOperations}.
 * 
 * @author Alex Shvid
 */
public class CassandraAdminTemplate implements CassandraAdminOperations {

	private static final Logger log = LoggerFactory.getLogger(CassandraAdminTemplate.class);

	private String keyspace;
	private Session session;
	private CassandraConverter converter;
	private MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param keyspace must not be {@literal null}.
	 */
	public CassandraAdminTemplate(Session session, CassandraConverter converter, String keyspace) {
		setKeyspace(keyspace).setSession(session).setCassandraConverter(converter);
	}

	protected CassandraAdminTemplate setKeyspace(String keyspace) {
		Assert.notNull(keyspace);
		this.keyspace = keyspace;
		return this;
	}

	protected CassandraAdminTemplate setSession(Session session) {
		Assert.notNull(session);
		this.session = session;
		return this;
	}

	protected CassandraAdminTemplate setCassandraConverter(CassandraConverter converter) {
		Assert.notNull(converter);
		this.converter = converter;
		return setMappingContext(converter.getMappingContext());
	}

	protected CassandraAdminTemplate setMappingContext(
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {
		Assert.notNull(mappingContext);
		this.mappingContext = mappingContext;
		return this;
	}

	public void createKeyspace(final String keyspace, final Map<String, Object> optionsByName) {

		Assert.notNull(keyspace);
		Assert.notNull(optionsByName);

		CreateKeyspaceSpecification spec = new CreateKeyspaceSpecification().name(keyspace).with(optionsByName);

		CreateKeyspaceCqlGenerator generator = new CreateKeyspaceCqlGenerator(spec);

		final String cql = generator.toCql();

		doExecute(cql);

	}

	public void alterKeyspace(String keyspace, Map<String, Object> optionsByName) {

		Assert.notNull(keyspace);
		Assert.notNull(optionsByName);

		AlterKeyspaceSpecification spec = new AlterKeyspaceSpecification().name(keyspace).with(optionsByName);

		AlterKeyspaceCqlGenerator generator = new AlterKeyspaceCqlGenerator(spec);

		final String cql = generator.toCql();

		doExecute(cql);

	}

	public void dropKeyspace(String keyspace) {

		Assert.notNull(keyspace);

		DropKeyspaceSpecification spec = new DropKeyspaceSpecification().name(keyspace);
		DropKeyspaceCqlGenerator generator = new DropKeyspaceCqlGenerator(spec);

		final String cql = generator.toCql();

		doExecute(cql);

	}

	public void useKeyspace(String keyspace) {

		Assert.notNull(keyspace);

		UseKeyspaceSpecification spec = new UseKeyspaceSpecification().name(keyspace);
		UseKeyspaceCqlGenerator generator = new UseKeyspaceCqlGenerator(spec);

		final String cql = generator.toCql();

		doExecute(cql);

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#createTable(boolean, java.lang.String, java.lang.Class, java.util.Map)
	 */
	@Override
	public boolean createTable(boolean ifNotExists, final String tableName, Class<?> entityClass,
			Map<String, Object> optionsByName) {

		Assert.notNull(tableName);
		Assert.notNull(entityClass);
		Assert.notNull(optionsByName);

		try {

			final CassandraPersistentEntity<?> entity = getEntity(entityClass);
			CreateTableSpecification spec = converter.getCreateTableSpecification(entity);
			spec.name(tableName);

			CreateTableCqlGenerator generator = new CreateTableCqlGenerator(spec);

			String cql = generator.toCql();

			doExecute(cql);

			return true;

		} catch (CassandraTableExistsException ctex) {
			return !ifNotExists;
		} catch (RuntimeException x) {
			throw tryToConvert(x);
		}
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#alterTable(java.lang.String, java.lang.Class, boolean)
	 */
	@Override
	public void alterTable(String tableName, Class<?> entityClass, boolean dropRemovedAttributeColumns) {

		Assert.notNull(tableName);
		Assert.notNull(entityClass);

		String cql = alterTableCql(tableName, entityClass, dropRemovedAttributeColumns);

		if (cql != null) {
			doExecute(cql);
		}
	}

	@Override
	public String validateTable(String tableName, Class<?> entityClass) {

		Assert.notNull(tableName);
		Assert.notNull(entityClass);

		return alterTableCql(tableName, entityClass, true);

	}

	/**
	 * Service method to generate cql query for the given table
	 * 
	 * @param tableName
	 * @param entityClass
	 * @param dropRemovedAttributeColumns
	 * @return Cql query string or null if no changes
	 */
	protected String alterTableCql(String tableName, Class<?> entityClass, boolean dropRemovedAttributeColumns) {

		final CassandraPersistentEntity<?> entity = getEntity(entityClass);

		AlterTableSpecification spec = converter.getAlterTableSpecification(entity, getTableMetadata(tableName),
				dropRemovedAttributeColumns);

		if (!spec.hasChanges()) {
			return null;
		}

		AlterTableCqlGenerator generator = new AlterTableCqlGenerator(spec);

		String cql = generator.toCql();

		return cql;

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraAdminOperations#replaceTable(java.lang.String, java.lang.Class)
	 */
	@Override
	public void replaceTable(String tableName, Class<?> entityClass, Map<String, Object> optionsByName) {

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#dropTable(java.lang.Class)
	 */
	public void dropTable(Class<?> entityClass) {

		Assert.notNull(entityClass);

		dropTable(getTableName(entityClass));

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#dropTable(java.lang.String)
	 */
	@Override
	public void dropTable(String tableName) {

		Assert.notNull(tableName);

		DropTableSpecification spec = new DropTableSpecification().name(tableName);
		String cql = new DropTableCqlGenerator(spec).toCql();

		doExecute(cql);

	}

	@Override
	public void createIndexes(String tableName, Class<?> entityClass) {

		Assert.notNull(tableName);
		Assert.notNull(entityClass);

		CassandraPersistentEntity<?> entity = getEntity(entityClass);

		List<CreateIndexSpecification> specList = converter.getCreateIndexSpecifications(entity);

		for (CreateIndexSpecification spec : specList) {
			String cql = new CreateIndexCqlGenerator(spec).toCql();

			doExecute(cql);
		}

	}

	@Override
	public void alterIndexes(String tableName, Class<?> entityClass) {

		Assert.notNull(tableName);
		Assert.notNull(entityClass);

		List<String> cqlList = alterIndexesCql(tableName, entityClass);

		for (String cql : cqlList) {
			doExecute(cql);
		}

	}

	@Override
	public List<String> validateIndexes(String tableName, Class<?> entityClass) {

		Assert.notNull(tableName);
		Assert.notNull(entityClass);

		return alterIndexesCql(tableName, entityClass);
	}

	/**
	 * Service method to generate cql queries to alter indexes in table
	 * 
	 * @param tableName
	 * @param entityClass
	 * @return List of cql queries
	 */

	protected List<String> alterIndexesCql(String tableName, Class<?> entityClass) {

		CassandraPersistentEntity<?> entity = getEntity(entityClass);

		List<WithNameSpecification<?>> specList = converter.getIndexChangeSpecifications(entity,
				getTableMetadata(tableName));

		if (specList.isEmpty()) {
			return Collections.emptyList();
		}

		List<String> result = new ArrayList<String>(specList.size());

		for (WithNameSpecification<?> spec : specList) {

			if (spec instanceof CreateIndexSpecification) {
				result.add(new CreateIndexCqlGenerator((CreateIndexSpecification) spec).toCql());
			} else if (spec instanceof DropIndexSpecification) {
				result.add(new DropIndexCqlGenerator((DropIndexSpecification) spec).toCql());
			} else {
				throw new MappingException("unexpected index operation " + spec + " for " + entityClass);
			}

		}

		return result;
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

	/**
	 * Get the given keyspace metadata.
	 * 
	 * @param keyspace The name of the table.
	 */
	@Override
	public KeyspaceMetadata getKeyspaceMetadata() {

		return execute(new SessionCallback<KeyspaceMetadata>() {

			public KeyspaceMetadata doInSession(Session s) throws DataAccessException {

				return s.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase());
			}
		});

	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.core.CassandraOperations#getTableMetadata(java.lang.Class)
	 */
	@Override
	public TableMetadata getTableMetadata(final String tableName) {

		Assert.notNull(tableName);

		return execute(new SessionCallback<TableMetadata>() {

			public TableMetadata doInSession(Session s) throws DataAccessException {

				log.info("getTableMetadata keyspace => " + keyspace + ", table => " + tableName);

				return s.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase()).getTable(tableName.toLowerCase());
			}
		});
	}

	@Override
	public String getTableName(Class<?> entityClass) {

		Assert.notNull(entityClass);

		final CassandraPersistentEntity<?> entity = getEntity(entityClass);

		return entity.getTable();
	}

	/**
	 * Service method to execute command
	 * 
	 * @param callback
	 * @return
	 */
	protected void doExecute(final String cql) {

		execute(new SessionCallback<Object>() {
			public Object doInSession(Session s) throws DataAccessException {

				log.info("EXECUTE CQL -> " + cql);
				s.execute(cql);
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
	protected <T> T execute(SessionCallback<T> callback) {

		Assert.notNull(callback);

		try {
			return callback.doInSession(session);
		} catch (RuntimeException x) {
			throw tryToConvert(x);
		}
	}

	protected RuntimeException tryToConvert(RuntimeException x) {
		RuntimeException resolved = exceptionTranslator.translateExceptionIfPossible(x);
		return resolved == null ? x : resolved;
	}

}
