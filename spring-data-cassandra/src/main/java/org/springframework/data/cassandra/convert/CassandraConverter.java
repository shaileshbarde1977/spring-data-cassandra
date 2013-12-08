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
package org.springframework.data.cassandra.convert;

import org.springframework.cassandra.core.keyspace.AlterTableSpecification;
import org.springframework.cassandra.core.keyspace.CreateTableSpecification;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.cassandra.mapping.CassandraPersistentProperty;
import org.springframework.data.convert.EntityConverter;

import com.datastax.driver.core.TableMetadata;

/**
 * Central Cassandra specific converter interface from Object to Row.
 * 
 * @author Alex Shvid
 */
public interface CassandraConverter extends
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

	AlterTableSpecification getAlterTableSpecificationIfDifferent(CassandraPersistentEntity<?> entity, TableMetadata table);

}
