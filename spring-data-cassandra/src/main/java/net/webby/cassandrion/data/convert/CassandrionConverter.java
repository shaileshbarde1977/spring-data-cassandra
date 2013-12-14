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
package net.webby.cassandrion.data.convert;

import java.util.List;

import net.webby.cassandrion.core.cql.spec.AlterTableSpecification;
import net.webby.cassandrion.core.cql.spec.CreateIndexSpecification;
import net.webby.cassandrion.core.cql.spec.CreateTableSpecification;
import net.webby.cassandrion.core.cql.spec.WithNameSpecification;
import net.webby.cassandrion.data.mapping.CassandraPersistentEntity;
import net.webby.cassandrion.data.mapping.CassandraPersistentProperty;

import org.springframework.data.convert.EntityConverter;

import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Clause;

/**
 * Central Cassandra specific converter interface from Object to Row.
 * 
 * @author Alex Shvid
 */
public interface CassandrionConverter extends
		EntityConverter<CassandraPersistentEntity<?>, CassandraPersistentProperty, Object, Object> {

	/**
	 * Creates table specification for a given entity
	 * 
	 * @param entity
	 * @return CreateTableSpecification for this entity
	 */

	CreateTableSpecification getCreateTableSpecification(CassandraPersistentEntity<?> entity);

	/**
	 * Checks existing table in Cassandra with entity information. Creates alter table specification if has differences.
	 * 
	 * @param entity
	 * @param table
	 * @return AlterTableSpecification or null
	 */

	AlterTableSpecification getAlterTableSpecification(CassandraPersistentEntity<?> entity, TableMetadata table,
			boolean dropRemovedAttributeColumns);

	/**
	 * Get all create index specifications for the entity
	 * 
	 * @param entity
	 * @return list of all CreateIndexSpecifications in the indexes
	 */

	List<CreateIndexSpecification> getCreateIndexSpecifications(CassandraPersistentEntity<?> entity);

	/**
	 * Get index change specifications for the entity
	 * 
	 * @param entity
	 * @return list of all CreateIndexSpecifications in the indexes
	 */

	List<WithNameSpecification<?>> getIndexChangeSpecifications(CassandraPersistentEntity<?> entity, TableMetadata table);

	/**
	 * Get the primary key from entity
	 * 
	 * @param entity
	 * @param id
	 * @return
	 */
	List<Clause> getPrimaryKey(CassandraPersistentEntity<?> entity, Object id);

}
