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
package org.springdata.cassandra.base.core;

import org.springdata.cassandra.base.core.cql.generator.AlterKeyspaceCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.CreateKeyspaceCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.DropKeyspaceCqlGenerator;
import org.springdata.cassandra.base.core.cql.generator.UseKeyspaceCqlGenerator;
import org.springdata.cassandra.base.core.cql.options.KeyspaceOptions;
import org.springdata.cassandra.base.core.cql.spec.AlterKeyspaceSpecification;
import org.springdata.cassandra.base.core.cql.spec.CreateKeyspaceSpecification;
import org.springdata.cassandra.base.core.cql.spec.DropKeyspaceSpecification;
import org.springdata.cassandra.base.core.cql.spec.UseKeyspaceSpecification;
import org.springdata.cassandra.base.core.query.ExecuteOptions;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

/**
 * Default implementation of {@link KeyspaceOperations}.
 * 
 * @author Alex Shvid
 */
public class DefaultKeyspaceOperations implements KeyspaceOperations {

	private static final String SYSTEM_KEYSPACE = "system";

	private final CassandraTemplate cassandraTemplate;

	protected DefaultKeyspaceOperations(CassandraTemplate cassandraTemplate) {
		Assert.notNull(cassandraTemplate);

		this.cassandraTemplate = cassandraTemplate;
	}

	@Override
	public void createKeyspace(final KeyspaceOptions keyspaceOptions, ExecuteOptions optionsOrNull) {

		Assert.notNull(keyspaceOptions);

		CreateKeyspaceSpecification spec = new CreateKeyspaceSpecification().name(cassandraTemplate.getKeyspace()).with(
				keyspaceOptions.getOptions());

		CreateKeyspaceCqlGenerator generator = new CreateKeyspaceCqlGenerator(spec);

		final String cql = generator.toCql();

		cassandraTemplate.doExecute(cql, optionsOrNull);

	}

	@Override
	public void alterKeyspace(KeyspaceOptions keyspaceOptions, ExecuteOptions optionsOrNull) {

		Assert.notNull(keyspaceOptions);

		AlterKeyspaceSpecification spec = new AlterKeyspaceSpecification().name(cassandraTemplate.getKeyspace()).with(
				keyspaceOptions.getOptions());

		AlterKeyspaceCqlGenerator generator = new AlterKeyspaceCqlGenerator(spec);

		final String cql = generator.toCql();

		cassandraTemplate.doExecute(cql, optionsOrNull);

	}

	@Override
	public void dropKeyspace(ExecuteOptions optionsOrNull) {

		DropKeyspaceSpecification spec = new DropKeyspaceSpecification().name(cassandraTemplate.getKeyspace());
		DropKeyspaceCqlGenerator generator = new DropKeyspaceCqlGenerator(spec);

		final String cql = generator.toCql();

		cassandraTemplate.doExecute(cql, optionsOrNull);

	}

	@Override
	public void useKeyspace(ExecuteOptions optionsOrNull) {

		UseKeyspaceSpecification spec = new UseKeyspaceSpecification().name(cassandraTemplate.getKeyspace());
		UseKeyspaceCqlGenerator generator = new UseKeyspaceCqlGenerator(spec);

		final String cql = generator.toCql();

		cassandraTemplate.doExecute(cql, optionsOrNull);

	}

	@Override
	public void useSystemKeyspace(ExecuteOptions optionsOrNull) {

		UseKeyspaceSpecification spec = new UseKeyspaceSpecification().name(SYSTEM_KEYSPACE);
		UseKeyspaceCqlGenerator generator = new UseKeyspaceCqlGenerator(spec);

		final String cql = generator.toCql();

		cassandraTemplate.doExecute(cql, optionsOrNull);

	}

	/**
	 * Get the given keyspace metadata.
	 * 
	 * @param keyspace The name of the table.
	 */
	@Override
	public KeyspaceMetadata getKeyspaceMetadata() {

		return cassandraTemplate.doExecute(new SessionCallback<KeyspaceMetadata>() {

			public KeyspaceMetadata doInSession(Session s) {

				return s.getCluster().getMetadata().getKeyspace(cassandraTemplate.getKeyspace().toLowerCase());
			}
		});

	}

}
