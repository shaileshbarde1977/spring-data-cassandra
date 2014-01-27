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
package org.springdata.cassandra.cql.config.java;

import org.springdata.cassandra.cql.config.CompressionType;
import org.springdata.cassandra.cql.config.KeyspaceAttributes;
import org.springdata.cassandra.cql.config.PoolingOptions;
import org.springdata.cassandra.cql.config.SocketOptions;
import org.springdata.cassandra.cql.core.CassandraClusterFactoryBean;
import org.springdata.cassandra.cql.core.CqlOperations;
import org.springdata.cassandra.cql.core.CqlTemplate;
import org.springdata.cassandra.cql.core.SessionFactoryBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.policies.LoadBalancingPolicy;
import com.datastax.driver.core.policies.ReconnectionPolicy;
import com.datastax.driver.core.policies.RetryPolicy;

/**
 * Base class for Spring Data Cassandra configuration using JavaConfig.
 * 
 * @author Alex Shvid
 */
@Configuration
public abstract class AbstractCassandraCqlConfiguration {

	/**
	 * Return the name of the keyspace to connect to.
	 * 
	 * @return for {@literal null} or empty keyspace will be used SYSTEM keyspace by default.
	 */
	protected abstract String keyspace();

	/**
	 * Return the {@link Cluster} instance to connect to.
	 * 
	 * @return Cluster object
	 */
	@Bean
	public CassandraClusterFactoryBean cluster() {

		CassandraClusterFactoryBean bean = new CassandraClusterFactoryBean();
		bean.setAuthProvider(authProvider());
		bean.setCompressionType(compressionType());
		bean.setContactPoints(contactPoints());
		bean.setPort(port());
		bean.setMetricsEnabled(metricsEnabled());
		bean.setLoadBalancingPolicy(loadBalancingPolicy());
		bean.setReconnectionPolicy(reconnectionPolicy());
		bean.setLocalPoolingOptions(localPoolingOptions());
		bean.setRemotePoolingOptions(remotePoolingOptions());
		bean.setRetryPolicy(retryPolicy());
		bean.setSocketOptions(socketOptions());

		return bean;
	}

	protected AuthProvider authProvider() {
		return null;
	}

	protected CompressionType compressionType() {
		return null;
	}

	protected String contactPoints() {
		return CassandraClusterFactoryBean.DEFAULT_CONTACT_POINTS;
	}

	protected int port() {
		return CassandraClusterFactoryBean.DEFAULT_PORT;
	}

	protected boolean metricsEnabled() {
		return CassandraClusterFactoryBean.DEFAULT_METRICS_ENABLED;
	}

	protected LoadBalancingPolicy loadBalancingPolicy() {
		return null;
	}

	protected ReconnectionPolicy reconnectionPolicy() {
		return null;
	}

	protected PoolingOptions localPoolingOptions() {
		return null;
	}

	protected PoolingOptions remotePoolingOptions() {
		return null;
	}

	protected RetryPolicy retryPolicy() {
		return null;
	}

	protected SocketOptions socketOptions() {
		return null;
	}

	/**
	 * Return keyspace attributes
	 * 
	 * @return KeyspaceAttributes
	 */
	@Bean
	public KeyspaceAttributes keyspaceAttributes() {
		return new KeyspaceAttributes();
	}

	/**
	 * Creates a {@link Session} to be used by the {@link CqlTemplate}. Will use the {@link Cluster} instance configured
	 * in {@link #cluster()}.
	 * 
	 * @see #cluster()
	 * @see #Keyspace()
	 * @return Session
	 */
	@Bean
	public SessionFactoryBean session() throws Exception {
		SessionFactoryBean factory = new SessionFactoryBean();
		factory.setKeyspace(keyspace());
		factory.setCluster(cluster().getObject());
		factory.setKeyspaceAttributes(keyspaceAttributes());
		return factory;
	}

	/**
	 * Creates a {@link CqlTemplate}.
	 * 
	 * @return CqlOperations
	 */
	@Bean
	public CqlOperations cqlTemplate() throws Exception {
		return new CqlTemplate(session().getObject(), keyspace());
	}

}
