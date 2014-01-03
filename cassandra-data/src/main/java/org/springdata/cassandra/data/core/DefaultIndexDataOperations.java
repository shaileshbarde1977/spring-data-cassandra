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

import org.springdata.cassandra.base.core.cql.generator.CreateIndexCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.DropIndexCqlGenerator;
import org.springdata.cassandra.base.core.cql.spec.CreateIndexSpecification;
import org.springdata.cassandra.base.core.cql.spec.DropIndexSpecification;
import org.springdata.cassandra.base.core.cql.spec.WithNameSpecification;
import org.springdata.cassandra.data.mapping.CassandraPersistentEntity;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.util.Assert;

import com.datastax.driver.core.TableMetadata;

/**
 * Default IndexDataOperations implementation
 * 
 * @author Alex Shvid
 * 
 */
public class DefaultIndexDataOperations implements IndexDataOperations {

	private CassandraDataTemplate dataTemplate;
	private String keyspace;
	private String tableName;

	protected DefaultIndexDataOperations(CassandraDataTemplate dataTemplate, String keyspace, String tableName) {

		Assert.notNull(dataTemplate);
		Assert.notNull(keyspace);
		Assert.notNull(tableName);

		this.dataTemplate = dataTemplate;
		this.keyspace = keyspace;
		this.tableName = tableName;
	}

	@Override
	public void createIndexes(Class<?> entityClass) {

		Assert.notNull(entityClass);

		CassandraPersistentEntity<?> entity = dataTemplate.getEntity(entityClass);

		List<CreateIndexSpecification> specList = dataTemplate.getConverter().getCreateIndexSpecifications(entity);

		for (CreateIndexSpecification spec : specList) {
			String cql = new CreateIndexCqlGenerator(spec).toCql();

			dataTemplate.doExecute(cql, null);
		}

	}

	@Override
	public void alterIndexes(Class<?> entityClass) {

		Assert.notNull(entityClass);

		List<String> cqlList = alterIndexesCql(entityClass);

		for (String cql : cqlList) {
			dataTemplate.doExecute(cql, null);
		}

	}

	@Override
	public List<String> validateIndexes(Class<?> entityClass) {

		Assert.notNull(entityClass);

		return alterIndexesCql(entityClass);
	}

	/**
	 * Service method to generate cql queries to alter indexes in table
	 * 
	 * @param tableName
	 * @param entityClass
	 * @return List of cql queries
	 */

	protected List<String> alterIndexesCql(Class<?> entityClass) {

		CassandraPersistentEntity<?> entity = dataTemplate.getEntity(entityClass);

		TableMetadata tableMetadata = dataTemplate.tableOps(tableName).getTableMetadata();

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
