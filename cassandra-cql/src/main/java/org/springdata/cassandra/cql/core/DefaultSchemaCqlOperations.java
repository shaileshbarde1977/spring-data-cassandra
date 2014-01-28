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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;

/**
 * Default Table Operations implementation
 * 
 * @author Alex Shvid
 * 
 */
public class DefaultSchemaCqlOperations implements SchemaCqlOperations {

	private static final Logger log = LoggerFactory.getLogger(DefaultSchemaCqlOperations.class);

	private final CqlTemplate cqlTemplate;
	private final String keyspace;

	protected DefaultSchemaCqlOperations(CqlTemplate cqlTemplate, String keyspace) {

		Assert.notNull(cqlTemplate);
		Assert.notNull(keyspace);

		this.cqlTemplate = cqlTemplate;
		this.keyspace = keyspace;
	}

	@Override
	public TableMetadata getTableMetadata(final String tableName) {

		return cqlTemplate.doExecute(new SessionCallback<TableMetadata>() {

			public TableMetadata doInSession(Session s) {

				log.debug("getTableMetadata keyspace => " + keyspace + ", table => " + tableName);

				return s.getCluster().getMetadata().getKeyspace(keyspace.toLowerCase()).getTable(tableName.toLowerCase());
			}
		});
	}

}
