/*
 * Copyright 2014 the original author or authors.
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

import org.springdata.cassandra.base.core.cql.generator.AlterTableCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.CreateTableCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.DropTableCqlGenerator;
import org.springdata.cassandra.base.core.cql.spec.AlterTableSpecification;
import org.springdata.cassandra.base.core.cql.spec.CreateTableSpecification;
import org.springdata.cassandra.base.core.cql.spec.DropTableSpecification;
import org.springdata.cassandra.base.support.exception.CassandraTableExistsException;
import org.springdata.cassandra.data.mapping.CassandraPersistentEntity;
import org.springframework.util.Assert;

import com.datastax.driver.core.TableMetadata;

/**
 * Default TableDataOperations implementation
 * 
 * @author Alex Shvid
 * 
 */
public class DefaultTableDataOperations implements TableDataOperations {

	private CassandraDataTemplate dataTemplate;
	private String tableName;

	protected DefaultTableDataOperations(CassandraDataTemplate dataTemplate, String tableName) {
		Assert.notNull(dataTemplate);
		Assert.notNull(tableName);
		this.dataTemplate = dataTemplate;
		this.tableName = tableName;
	}

	@Override
	public boolean createTable(boolean ifNotExists, Class<?> entityClass) {

		Assert.notNull(entityClass);

		try {

			final CassandraPersistentEntity<?> entity = dataTemplate.getEntity(entityClass);
			CreateTableSpecification spec = dataTemplate.getConverter().getCreateTableSpecification(entity);
			spec.name(tableName);

			CreateTableCqlGenerator generator = new CreateTableCqlGenerator(spec);

			String cql = generator.toCql();

			dataTemplate.doExecute(cql, null);

			return true;

		} catch (CassandraTableExistsException ctex) {
			return !ifNotExists;
		} catch (RuntimeException x) {
			throw dataTemplate.translateIfPossible(x);
		}
	}

	@Override
	public void alterTable(Class<?> entityClass, boolean dropRemovedAttributeColumns) {

		Assert.notNull(entityClass);

		String cql = alterTableCql(entityClass, dropRemovedAttributeColumns);

		if (cql != null) {
			dataTemplate.doExecute(cql, null);
		}
	}

	@Override
	public String validateTable(Class<?> entityClass) {

		Assert.notNull(entityClass);

		return alterTableCql(entityClass, true);

	}

	/**
	 * Service method to generate cql query for the given table
	 * 
	 * @param tableName
	 * @param entityClass
	 * @param dropRemovedAttributeColumns
	 * @return Cql query string or null if no changes
	 */
	protected String alterTableCql(Class<?> entityClass, boolean dropRemovedAttributeColumns) {

		final CassandraPersistentEntity<?> entity = dataTemplate.getEntity(entityClass);

		TableMetadata tableMetadata = dataTemplate.tableOps(tableName).getTableMetadata();

		AlterTableSpecification spec = dataTemplate.getConverter().getAlterTableSpecification(entity, tableMetadata,
				dropRemovedAttributeColumns);

		if (!spec.hasChanges()) {
			return null;
		}

		AlterTableCqlGenerator generator = new AlterTableCqlGenerator(spec);

		String cql = generator.toCql();

		return cql;

	}

	@Override
	public void dropTable() {

		DropTableSpecification spec = new DropTableSpecification().name(tableName);
		String cql = new DropTableCqlGenerator(spec).toCql();

		dataTemplate.doExecute(cql, null);

	}

}
