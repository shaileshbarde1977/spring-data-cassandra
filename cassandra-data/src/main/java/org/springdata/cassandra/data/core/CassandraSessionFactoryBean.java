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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.base.core.CassandraTemplate;
import org.springdata.cassandra.base.core.CassandraAdminOperations;
import org.springdata.cassandra.base.core.cql.options.KeyspaceOptions;
import org.springdata.cassandra.base.core.cql.options.KeyspaceReplicationOptions;
import org.springdata.cassandra.base.core.cql.spec.KeyspaceOption;
import org.springdata.cassandra.base.core.cql.spec.KeyspaceOption.ReplicationOption;
import org.springdata.cassandra.base.support.CassandraExceptionTranslator;
import org.springdata.cassandra.data.config.KeyspaceAttributes;
import org.springdata.cassandra.data.config.TableAttributes;
import org.springdata.cassandra.data.convert.CassandraConverter;
import org.springdata.cassandra.data.convert.MappingCassandraConverter;
import org.springdata.cassandra.data.mapping.CassandraMappingContext;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Convenient factory for configuring a Cassandra Session. It is enough to have one session per application.
 * 
 * @author Alex Shvid
 */

public class CassandraSessionFactoryBean implements FactoryBean<Session>, InitializingBean, DisposableBean,
		BeanClassLoaderAware, PersistenceExceptionTranslator {

	private static final Logger log = LoggerFactory.getLogger(CassandraSessionFactoryBean.class);

	public static final String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
	public static final int DEFAULT_REPLICATION_FACTOR = 1;

	private ClassLoader beanClassLoader;

	private Cluster cluster;
	private Session session;
	private String keyspace;

	private CassandraConverter converter;

	private KeyspaceAttributes keyspaceAttributes;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	public Session getObject() {
		return session;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<? extends Session> getObjectType() {
		return Session.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	public boolean isSingleton() {
		return true;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws ClassNotFoundException {

		if (this.converter == null) {
			this.converter = getDefaultCassandraConverter();
		}

		if (cluster == null) {
			throw new IllegalArgumentException("at least one cluster is required");
		}

		Session session = null;
		session = cluster.connect();

		if (StringUtils.hasText(keyspace)) {

			CassandraTemplate cassandraTemplate = new CassandraTemplate(session, keyspace);
			CassandraAdminOperations adminOps = cassandraTemplate.adminOps();

			CassandraDataTemplate cassandraDataTemplate = new CassandraDataTemplate(session, converter, keyspace);

			KeyspaceMetadata keyspaceMetadata = adminOps.getKeyspaceMetadata(keyspace);
			boolean keyspaceExists = keyspaceMetadata != null;
			boolean keyspaceCreated = false;

			if (keyspaceExists) {
				log.info("keyspace exists " + keyspaceMetadata.asCQLQuery());
			}

			if (keyspaceAttributes == null) {
				keyspaceAttributes = new KeyspaceAttributes();
			}

			// drop the old keyspace if needed
			if (keyspaceExists && (keyspaceAttributes.isCreate() || keyspaceAttributes.isCreateDrop())) {

				log.info("Drop keyspace " + keyspace + " on afterPropertiesSet");
				adminOps.dropKeyspace(keyspace).execute();
				keyspaceExists = false;

			}

			// create the new keyspace if needed
			if (!keyspaceExists
					&& (keyspaceAttributes.isCreate() || keyspaceAttributes.isCreateDrop() || keyspaceAttributes.isUpdate())) {

				log.info("Create keyspace " + keyspace + " on afterPropertiesSet");

				adminOps.createKeyspace(keyspace, createKeyspaceOptions()).execute();
				keyspaceCreated = true;
			}

			// update keyspace if needed
			if (keyspaceAttributes.isUpdate() && !keyspaceCreated) {

				if (compareKeyspaceAttributes(keyspaceAttributes, keyspaceMetadata) != null) {

					log.info("Update keyspace " + keyspace + " on afterPropertiesSet");

					adminOps.alterKeyspace(keyspace, createKeyspaceOptions()).execute();

				}

			}

			// validate keyspace if needed
			if (keyspaceAttributes.isValidate()) {

				if (!keyspaceExists) {
					throw new InvalidDataAccessApiUsageException("keyspace '" + keyspace + "' not found in the Cassandra");
				}

				String errorField = compareKeyspaceAttributes(keyspaceAttributes, keyspaceMetadata);
				if (errorField != null) {
					throw new InvalidDataAccessApiUsageException(errorField + " attribute is not much in the keyspace '"
							+ keyspace + "'");
				}

			}

			adminOps.useKeyspace(keyspace).execute();

			if (!CollectionUtils.isEmpty(keyspaceAttributes.getTables())) {

				for (TableAttributes tableAttributes : keyspaceAttributes.getTables()) {

					String entityClassName = tableAttributes.getEntity();
					Class<?> entityClass = ClassUtils.forName(entityClassName, this.beanClassLoader);

					String useTableName = tableAttributes.getName() != null ? tableAttributes.getName() : cassandraDataTemplate
							.getTableName(entityClass);

					if (keyspaceCreated) {
						createNewTable(cassandraDataTemplate, useTableName, entityClass);
					} else if (keyspaceAttributes.isUpdate()) {
						TableMetadata table = cassandraTemplate.schemaOps().getTableMetadata(useTableName);
						if (table == null) {
							createNewTable(cassandraDataTemplate, useTableName, entityClass);
						} else {

							cassandraDataTemplate.schemaDataOps().alterTable(useTableName, entityClass, true, null);

							cassandraDataTemplate.schemaDataOps().alterIndexes(useTableName, entityClass, null);

						}
					} else if (keyspaceAttributes.isValidate()) {

						TableMetadata table = cassandraTemplate.schemaOps().getTableMetadata(useTableName);
						if (table == null) {
							throw new InvalidDataAccessApiUsageException("not found table " + useTableName + " for entity "
									+ entityClassName);
						}

						String query = cassandraDataTemplate.schemaDataOps().validateTable(useTableName, entityClass);

						if (query != null) {
							throw new InvalidDataAccessApiUsageException("invalid table " + useTableName + " for entity "
									+ entityClassName + ". modify it by " + query);
						}

						List<String> queryList = cassandraDataTemplate.schemaDataOps().validateIndexes(useTableName, entityClass);

						if (!queryList.isEmpty()) {
							throw new InvalidDataAccessApiUsageException("invalid indexes in table " + useTableName + " for entity "
									+ entityClassName + ". modify it by " + queryList);
						}

					}

				}
			}

		}

		// initialize property
		this.session = session;

	}

	private KeyspaceOptions createKeyspaceOptions() {
		KeyspaceReplicationOptions keyspaceReplicationOptions = new KeyspaceReplicationOptions().with(
				ReplicationOption.CLASS, keyspaceAttributes.getReplicationStrategy()).with(
				ReplicationOption.REPLICATION_FACTOR, keyspaceAttributes.getReplicationFactor());

		KeyspaceOptions keyspaceOptions = new KeyspaceOptions()
				.with(KeyspaceOption.REPLICATION, keyspaceReplicationOptions).with(KeyspaceOption.DURABLE_WRITES,
						keyspaceAttributes.isDurableWrites());
		return keyspaceOptions;
	}

	private void createNewTable(CassandraDataTemplate cassandraDataTemplate, String useTableName, Class<?> entityClass) {

		cassandraDataTemplate.schemaDataOps().createTable(false, useTableName, entityClass, null);

		cassandraDataTemplate.schemaDataOps().createIndexes(useTableName, entityClass, null);

	}

	public void destroy() throws Exception {

		if (StringUtils.hasText(keyspace) && keyspaceAttributes != null && keyspaceAttributes.isCreateDrop()) {

			log.info("Drop keyspace " + keyspace + " on destroy");

			CassandraTemplate casandraTemplate = new CassandraTemplate(session, keyspace);
			CassandraAdminOperations keyspaceOps = casandraTemplate.adminOps();

			keyspaceOps.useSystemKeyspace().execute();
			keyspaceOps.dropKeyspace(keyspace).execute();

		}
		this.session.shutdown();
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public void setKeyspaceAttributes(KeyspaceAttributes keyspaceAttributes) {
		this.keyspaceAttributes = keyspaceAttributes;
	}

	public void setConverter(CassandraConverter converter) {
		this.converter = converter;
	}

	private static String compareKeyspaceAttributes(KeyspaceAttributes keyspaceAttributes,
			KeyspaceMetadata keyspaceMetadata) {
		if (keyspaceAttributes.isDurableWrites() != keyspaceMetadata.isDurableWrites()) {
			return "durableWrites";
		}
		Map<String, String> replication = keyspaceMetadata.getReplication();
		String replicationFactorStr = replication.get("replication_factor");
		if (replicationFactorStr == null) {
			return "replication_factor";
		}
		try {
			int replicationFactor = Integer.parseInt(replicationFactorStr);
			if (keyspaceAttributes.getReplicationFactor() != replicationFactor) {
				return "replication_factor";
			}
		} catch (NumberFormatException e) {
			return "replication_factor";
		}

		String attributesStrategy = keyspaceAttributes.getReplicationStrategy();
		if (attributesStrategy.indexOf('.') == -1) {
			attributesStrategy = "org.apache.cassandra.locator." + attributesStrategy;
		}
		String replicationStrategy = replication.get("class");
		if (!attributesStrategy.equals(replicationStrategy)) {
			return "replication_class";
		}
		return null;
	}

	private static final CassandraConverter getDefaultCassandraConverter() {
		MappingCassandraConverter converter = new MappingCassandraConverter(new CassandraMappingContext());
		converter.afterPropertiesSet();
		return converter;
	}
}
