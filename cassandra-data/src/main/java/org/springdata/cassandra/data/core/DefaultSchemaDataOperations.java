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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springdata.cassandra.base.core.cql.generator.AlterTableCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.CreateIndexCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.CreateTableCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.DropIndexCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.DropTableCqlGenerator;
import org.springdata.cassandra.base.core.cql.spec.AlterTableSpecification;
import org.springdata.cassandra.base.core.cql.spec.CreateIndexSpecification;
import org.springdata.cassandra.base.core.cql.spec.CreateTableSpecification;
import org.springdata.cassandra.base.core.cql.spec.DropIndexSpecification;
import org.springdata.cassandra.base.core.cql.spec.DropTableSpecification;
import org.springdata.cassandra.base.core.cql.spec.WithNameSpecification;
import org.springdata.cassandra.base.core.query.ExecuteOptions;
import org.springdata.cassandra.base.support.exception.CassandraTableExistsException;
import org.springdata.cassandra.data.mapping.CassandraPersistentEntity;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.util.Assert;

import com.datastax.driver.core.TableMetadata;

/**
 * CassandraSchemaDataOperations implementation
 * 
 * @author Alex Shvid
 * 
 */
public class DefaultSchemaDataOperations implements CassandraSchemaDataOperations {

	private CassandraDataTemplate dataTemplate;

	protected DefaultSchemaDataOperations(CassandraDataTemplate dataTemplate) {
		Assert.notNull(dataTemplate);

		this.dataTemplate = dataTemplate;
	}

	@Override
	public boolean createTable(boolean ifNotExists, String tableName, Class<?> entityClass, ExecuteOptions optionsOrNull) {

		Assert.notNull(entityClass);

		try {

			final CassandraPersistentEntity<?> entity = dataTemplate.getEntity(entityClass);
			CreateTableSpecification spec = dataTemplate.getConverter().getCreateTableSpecification(entity);
			spec.name(tableName);

			CreateTableCqlGenerator generator = new CreateTableCqlGenerator(spec);

			String cql = generator.toCql();

			dataTemplate.doExecute(cql, optionsOrNull);

			return true;

		} catch (CassandraTableExistsException ctex) {
			return !ifNotExists;
		} catch (RuntimeException x) {
			throw dataTemplate.translateIfPossible(x);
		}
	}

	@Override
	public void alterTable(String tableName, Class<?> entityClass, boolean dropRemovedAttributeColumns,
			ExecuteOptions optionsOrNull) {

		Assert.notNull(entityClass);

		String cql = alterTableCql(tableName, entityClass, dropRemovedAttributeColumns);

		if (cql != null) {
			dataTemplate.doExecute(cql, optionsOrNull);
		}
	}

	@Override
	public String validateTable(String tableName, Class<?> entityClass) {

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

		final CassandraPersistentEntity<?> entity = dataTemplate.getEntity(entityClass);

		TableMetadata tableMetadata = dataTemplate.schemaOps().getTableMetadata(tableName);

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
	public void dropTable(String tableName, ExecuteOptions optionsOrNull) {

		DropTableSpecification spec = new DropTableSpecification().name(tableName);
		String cql = new DropTableCqlGenerator(spec).toCql();

		dataTemplate.doExecute(cql, optionsOrNull);

	}

	@Override
	public void createIndexes(String tableName, Class<?> entityClass, ExecuteOptions optionsOrNull) {

		Assert.notNull(entityClass);

		CassandraPersistentEntity<?> entity = dataTemplate.getEntity(entityClass);

		List<CreateIndexSpecification> specList = dataTemplate.getConverter().getCreateIndexSpecifications(entity);

		for (CreateIndexSpecification spec : specList) {
			String cql = new CreateIndexCqlGenerator(spec).toCql();

			dataTemplate.doExecute(cql, optionsOrNull);
		}

	}

	@Override
	public void alterIndexes(String tableName, Class<?> entityClass, ExecuteOptions optionsOrNull) {

		Assert.notNull(entityClass);

		List<String> cqlList = alterIndexesCql(tableName, entityClass);

		for (String cql : cqlList) {
			dataTemplate.doExecute(cql, optionsOrNull);
		}

	}

	@Override
	public List<String> validateIndexes(String tableName, Class<?> entityClass) {

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

		CassandraPersistentEntity<?> entity = dataTemplate.getEntity(entityClass);

		TableMetadata tableMetadata = dataTemplate.schemaOps().getTableMetadata(tableName);

		List<WithNameSpecification<?>> specList = dataTemplate.getConverter().getIndexChangeSpecifications(entity,
				tableMetadata);

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

}
