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

import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.cassandra.core.cql.options.KeyspaceOptions;
import org.springframework.cassandra.core.cql.options.KeyspaceReplicationOptions;
import org.springframework.cassandra.core.cql.spec.KeyspaceOption;
import org.springframework.cassandra.core.cql.spec.KeyspaceOption.ReplicationOption;
import org.springframework.cassandra.support.CassandraExceptionTranslator;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.config.KeyspaceAttributes;
import org.springframework.data.cassandra.config.TableAttributes;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

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
	private MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

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
		this.mappingContext = this.converter.getMappingContext();

		if (cluster == null) {
			throw new IllegalArgumentException("at least one cluster is required");
		}

		Session session = null;
		session = cluster.connect();

		if (StringUtils.hasText(keyspace)) {

			CassandraAdminTemplate cassandraAdminTemplate = new CassandraAdminTemplate(session, converter, keyspace);

			KeyspaceMetadata keyspaceMetadata = cassandraAdminTemplate.getKeyspaceMetadata();
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
				cassandraAdminTemplate.dropKeyspace(keyspace);
				keyspaceExists = false;

			}

			// create the new keyspace if needed
			if (!keyspaceExists
					&& (keyspaceAttributes.isCreate() || keyspaceAttributes.isCreateDrop() || keyspaceAttributes.isUpdate())) {

				log.info("Create keyspace " + keyspace + " on afterPropertiesSet");

				cassandraAdminTemplate.createKeyspace(keyspace, createKeyspaceOptions().getOptions());
				keyspaceCreated = true;
			}

			// update keyspace if needed
			if (keyspaceAttributes.isUpdate() && !keyspaceCreated) {

				if (compareKeyspaceAttributes(keyspaceAttributes, keyspaceMetadata) != null) {

					log.info("Update keyspace " + keyspace + " on afterPropertiesSet");

					cassandraAdminTemplate.alterKeyspace(keyspace, createKeyspaceOptions().getOptions());

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

			cassandraAdminTemplate.useKeyspace(keyspace);

			if (!CollectionUtils.isEmpty(keyspaceAttributes.getTables())) {

				for (TableAttributes tableAttributes : keyspaceAttributes.getTables()) {

					String entityClassName = tableAttributes.getEntity();
					Class<?> entityClass = ClassUtils.forName(entityClassName, this.beanClassLoader);

					String useTableName = tableAttributes.getName() != null ? tableAttributes.getName() : cassandraAdminTemplate
							.getTableName(entityClass);

					if (keyspaceCreated) {
						createNewTable(cassandraAdminTemplate, useTableName, entityClass);
					} else if (keyspaceAttributes.isUpdate()) {
						TableMetadata table = cassandraAdminTemplate.getTableMetadata(useTableName);
						if (table == null) {
							createNewTable(cassandraAdminTemplate, useTableName, entityClass);
						} else {

							cassandraAdminTemplate.alterTable(useTableName, entityClass, true);

							cassandraAdminTemplate.alterIndexes(useTableName, entityClass);

						}
					} else if (keyspaceAttributes.isValidate()) {

						TableMetadata table = cassandraAdminTemplate.getTableMetadata(useTableName);
						if (table == null) {
							throw new InvalidDataAccessApiUsageException("not found table " + useTableName + " for entity "
									+ entityClassName);
						}

						String query = cassandraAdminTemplate.validateTable(useTableName, entityClass);

						if (query != null) {
							throw new InvalidDataAccessApiUsageException("invalid table " + useTableName + " for entity "
									+ entityClassName + ". modify it by " + query);
						}

						List<String> queryList = cassandraAdminTemplate.validateIndexes(useTableName, entityClass);

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

	private void createNewTable(CassandraAdminTemplate cassandraAdminTemplate, String useTableName, Class<?> entityClass)
			throws NoHostAvailableException {

		cassandraAdminTemplate.createTable(false, useTableName, entityClass, Collections.<String, Object> emptyMap());

		cassandraAdminTemplate.createIndexes(useTableName, entityClass);

	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {

		if (StringUtils.hasText(keyspace) && keyspaceAttributes != null && keyspaceAttributes.isCreateDrop()) {
			log.info("Drop keyspace " + keyspace + " on destroy");
			session.execute("USE system");
			session.execute("DROP KEYSPACE " + keyspace);
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
