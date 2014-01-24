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
package org.springdata.cassandra.cql.core;

import org.springdata.cassandra.cql.generator.AlterKeyspaceCqlGenerator;
import org.springdata.cassandra.cql.generator.CreateKeyspaceCqlGenerator;
import org.springdata.cassandra.cql.generator.DropKeyspaceCqlGenerator;
import org.springdata.cassandra.cql.generator.UseKeyspaceCqlGenerator;
import org.springdata.cassandra.cql.option.KeyspaceOptions;
import org.springdata.cassandra.cql.spec.AlterKeyspaceSpecification;
import org.springdata.cassandra.cql.spec.CreateKeyspaceSpecification;
import org.springdata.cassandra.cql.spec.DropKeyspaceSpecification;
import org.springdata.cassandra.cql.spec.UseKeyspaceSpecification;
import org.springframework.util.Assert;

import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

/**
 * Default implementation of {@link AdminCqlOperations}.
 * 
 * @author Alex Shvid
 */
public class DefaultAdminCqlOperations implements AdminCqlOperations {

	private static final String SYSTEM_KEYSPACE = "system";

	private final CqlTemplate cassandraTemplate;

	protected DefaultAdminCqlOperations(CqlTemplate cassandraTemplate) {
		Assert.notNull(cassandraTemplate);

		this.cassandraTemplate = cassandraTemplate;
	}

	@Override
	public UpdateOperation createKeyspace(String keyspace, final KeyspaceOptions keyspaceOptions) {

		Assert.notNull(keyspace);
		Assert.notNull(keyspaceOptions);

		CreateKeyspaceSpecification spec = new CreateKeyspaceSpecification().name(keyspace).with(
				keyspaceOptions.getOptions());

		CreateKeyspaceCqlGenerator generator = new CreateKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cassandraTemplate, generator.toCql());

	}

	@Override
	public UpdateOperation alterKeyspace(String keyspace, KeyspaceOptions keyspaceOptions) {

		Assert.notNull(keyspace);
		Assert.notNull(keyspaceOptions);

		AlterKeyspaceSpecification spec = new AlterKeyspaceSpecification().name(keyspace)
				.with(keyspaceOptions.getOptions());

		AlterKeyspaceCqlGenerator generator = new AlterKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cassandraTemplate, generator.toCql());

	}

	@Override
	public UpdateOperation dropKeyspace(String keyspace) {

		Assert.notNull(keyspace);

		DropKeyspaceSpecification spec = new DropKeyspaceSpecification().name(keyspace);
		DropKeyspaceCqlGenerator generator = new DropKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cassandraTemplate, generator.toCql());

	}

	@Override
	public UpdateOperation useKeyspace(String keyspace) {

		Assert.notNull(keyspace);

		UseKeyspaceSpecification spec = new UseKeyspaceSpecification().name(keyspace);
		UseKeyspaceCqlGenerator generator = new UseKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cassandraTemplate, generator.toCql());

	}

	@Override
	public UpdateOperation useSystemKeyspace() {

		UseKeyspaceSpecification spec = new UseKeyspaceSpecification().name(SYSTEM_KEYSPACE);
		UseKeyspaceCqlGenerator generator = new UseKeyspaceCqlGenerator(spec);

		return new DefaultUpdateOperation(cassandraTemplate, generator.toCql());

	}

	/**
	 * Get the given keyspace metadata.
	 * 
	 * @param keyspace The name of the table.
	 */
	@Override
	public KeyspaceMetadata getKeyspaceMetadata(final String keyspace) {

		Assert.notNull(keyspace);

		return cassandraTemplate.doExecute(new SessionCallback<KeyspaceMetadata>() {

			public KeyspaceMetadata doInSession(Session s) {

				return s.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase());
			}
		});

	}

}
