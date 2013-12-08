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
package org.springframework.data.cassandra.test.integration.config;

import org.springframework.cassandra.core.CassandraOperations;
import org.springframework.cassandra.core.CassandraTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.cassandra.config.AbstractCassandraConfiguration;
import org.springframework.data.cassandra.core.CassandraDataOperations;
import org.springframework.data.cassandra.core.CassandraDataTemplate;
import org.springframework.data.cassandra.core.CassandraKeyspaceFactoryBean;
import org.springframework.data.cassandra.core.SessionFactoryBean;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Cluster.Builder;

/**
 * Setup any spring configuration for unit tests
 * 
 * @author David Webb
 * 
 */
@Configuration
public class TestConfig extends AbstractCassandraConfiguration {

	public static final String keyspace = "test";

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.config.AbstractCassandraConfiguration#getKeyspaceName()
	 */
	@Override
	protected String getKeyspaceName() {
		return keyspace;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.cassandra.config.AbstractCassandraConfiguration#cluster()
	 */
	@Override
	@Bean
	public Cluster cluster() {

		Builder builder = Cluster.builder();
		builder.addContactPoint("127.0.0.1");
		return builder.build();
	}

	@Bean
	public CassandraKeyspaceFactoryBean keyspaceFactoryBean() {

		CassandraKeyspaceFactoryBean bean = new CassandraKeyspaceFactoryBean();
		bean.setCluster(cluster());
		bean.setKeyspace("test");

		return bean;

	}

	@Bean
	public SessionFactoryBean sessionFactoryBean() {

		SessionFactoryBean bean = new SessionFactoryBean(keyspaceFactoryBean().getObject());
		return bean;

	}

	@Bean
	public CassandraOperations cassandraTemplate() {

		CassandraOperations template = new CassandraTemplate(sessionFactoryBean().getObject());
		return template;
	}

	@Bean
	public CassandraDataOperations cassandraDataTemplate() {

		CassandraDataOperations template = new CassandraDataTemplate(keyspaceFactoryBean().getObject().getSession(),
				keyspaceFactoryBean().getObject().getCassandraConverter(), keyspaceFactoryBean().getObject().getKeyspace());

		return template;

	}
}
