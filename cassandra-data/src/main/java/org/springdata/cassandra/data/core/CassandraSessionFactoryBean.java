/*
 * Copyright 2013-2014 the original author or authors.
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

import java.util.Collection;
import java.util.List;

import org.springdata.cassandra.base.core.CassandraCqlSessionFactoryBean;
import org.springdata.cassandra.base.core.CassandraCqlTemplate;
import org.springdata.cassandra.data.config.TableAttributes;
import org.springdata.cassandra.data.convert.CassandraConverter;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.util.ClassUtils;
import org.springframework.util.CollectionUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Convenient factory for configuring a Cassandra Session.
 * 
 * @author Alex Shvid
 */

public class CassandraSessionFactoryBean extends CassandraCqlSessionFactoryBean implements FactoryBean<Session>,
		InitializingBean, DisposableBean, BeanClassLoaderAware, PersistenceExceptionTranslator {

	private ClassLoader beanClassLoader;

	private CassandraConverter converter;

	private Collection<TableAttributes> tables;

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

	@Override
	public void afterPropertiesSet() {

		if (converter == null) {
			throw new IllegalArgumentException("converter is required");
		}

		super.afterPropertiesSet();

		if (StringUtils.hasText(keyspace)) {

			CassandraCqlTemplate cassandraTemplate = new CassandraCqlTemplate(session, keyspace);
			CassandraTemplate cassandraDataTemplate = new CassandraTemplate(session, converter, keyspace);

			if (!CollectionUtils.isEmpty(tables)) {

				for (TableAttributes tableAttributes : tables) {

					String entityClassName = tableAttributes.getEntityClass();
					Class<?> entityClass = loadClass(entityClassName);

					String useTableName = tableAttributes.getTableName() != null ? tableAttributes.getTableName()
							: cassandraDataTemplate.getTableName(entityClass);

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

	}

	private Class<?> loadClass(String className) {
		try {
			return ClassUtils.forName(className, this.beanClassLoader);
		} catch (Exception e) {
			throw new IllegalArgumentException("class not found " + className, e);
		}
	}

	private void createNewTable(CassandraTemplate cassandraDataTemplate, String useTableName, Class<?> entityClass) {
		cassandraDataTemplate.schemaDataOps().createTable(false, useTableName, entityClass, null);
		cassandraDataTemplate.schemaDataOps().createIndexes(useTableName, entityClass, null);
	}

	public void setConverter(CassandraConverter converter) {
		this.converter = converter;
	}

	public void setTables(Collection<TableAttributes> tables) {
		this.tables = tables;
	}

}
