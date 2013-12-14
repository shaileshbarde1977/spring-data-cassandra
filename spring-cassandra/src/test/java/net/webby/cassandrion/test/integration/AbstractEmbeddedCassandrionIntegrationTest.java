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
package net.webby.cassandrion.test.integration;

import java.io.IOException;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

public abstract class AbstractEmbeddedCassandrionIntegrationTest {

	protected final static String CASSANDRA_CONFIG = "cassandra.yaml";
	protected final static String CASSANDRA_HOST = "localhost";
	protected final static int CASSANDRA_NATIVE_PORT = 9042;

	@BeforeClass
	public static void beforeClass() throws ConfigurationException, TTransportException, IOException,
			InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG);
	}

	/**
	 * Whether to clear the cluster before the next test.
	 */
	protected boolean clear = false;
	/**
	 * Whether to connect to Cassandra.
	 */
	protected boolean connect = true;
	/**
	 * The {@link Cluster} that's connected to Cassandra.
	 */
	protected Cluster cluster;
	/**
	 * If not <code>null</code>, get a {@link Session} for the from the {@link #cluster}.
	 */
	protected String keyspace = "ks" + UUID.randomUUID().toString().replace("-", "");
	/**
	 * The {@link Session} for the {@link #keyspace} from the {@link #cluster}.
	 */
	protected Session session;

	/**
	 * Returns whether we're currently connected to the cluster.
	 */
	public boolean connected() {
		return session != null;
	}

	public Cluster cluster() {
		return Cluster.builder().addContactPoint(CASSANDRA_HOST).withPort(CASSANDRA_NATIVE_PORT).build();
	}

	@Before
	public void before() {
		if (connect && !connected()) {
			cluster = cluster();

			if (keyspace == null) {
				session = cluster.connect();
			} else {

				KeyspaceMetadata kmd = cluster.getMetadata().getKeyspace(keyspace);
				if (kmd == null) { // then create keyspace
					session = cluster.connect();
					session.execute("CREATE KEYSPACE " + keyspace
							+ " WITH replication = {'class': 'SimpleStrategy', 'replication_factor' : 1};");
					session.execute("USE " + keyspace + ";");
				} else {// else keyspace already exists
					session = cluster.connect(keyspace);
				}
			}
		}
	}

	@After
	public void after() {
		if (clear && connected()) {
			EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
		}
	}
}
