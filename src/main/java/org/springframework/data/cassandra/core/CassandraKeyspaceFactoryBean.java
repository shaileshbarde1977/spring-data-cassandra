/*
 * Copyright 2011-2013 the original author or authors.
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
package org.springframework.data.cassandra.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;
import org.springframework.data.cassandra.config.KeyspaceAttributes;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.Session;

/**
 * Convenient factory for configuring a Cassandra Session.
 * Session is a thread safe singleton and created per a keyspace.
 * So, it is enough to have one session per application.
 * 
 * @author Alex Shvid
 */

public class CassandraKeyspaceFactoryBean implements FactoryBean<Session>,
InitializingBean, DisposableBean, PersistenceExceptionTranslator  {

    private static final Logger log = LoggerFactory.getLogger(CassandraKeyspaceFactoryBean.class);
    
	public static final String DEFAULT_REPLICATION_STRATEGY = "SimpleStrategy";
	public static final int DEFAULT_REPLICATION_FACTOR = 1;
	
	private Cluster cluster;
	private Session session;
	private String keyspace;
	
	private KeyspaceAttributes keyspaceAttributes;
	
	private PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	public Session getObject() throws Exception {
		return session;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<? extends Session> getObjectType() {
		return Session.class;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#isSingleton()
	 */
	public boolean isSingleton() {
		return true;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException ex) {
		return exceptionTranslator.translateExceptionIfPossible(ex);
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
	 */
	public void afterPropertiesSet() throws Exception {
		
		if (cluster == null) {
			throw new IllegalArgumentException(
					"at least one cluster is required");
		}

		Session session = null;
		session = cluster.connect();
		
		if (StringUtils.hasText(keyspace)) {
			
			KeyspaceMetadata keyspaceMetadata = cluster.getMetadata().getKeyspace(keyspace.toLowerCase());
			boolean keyspaceExists = keyspaceMetadata != null;
			
			if (keyspaceExists) {
				log.info("keyspace exists " + keyspaceMetadata.asCQLQuery());
			}
			
			if (keyspaceAttributes == null) {
				keyspaceAttributes = new KeyspaceAttributes();
			}
				
			// drop the old keyspace if needed
			if (keyspaceExists && (keyspaceAttributes.isCreate() || keyspaceAttributes.isCreateDrop())) {
				log.info("Drop keyspace " + keyspace + " on afterPropertiesSet");
				session.execute("DROP KEYSPACE " + keyspace);
				keyspaceExists = false;
			}	
			
			boolean keyspaceCreated = false;
			// create the new keyspace if needed
			if (!keyspaceExists && (keyspaceAttributes.isCreate() || keyspaceAttributes.isCreateDrop() || keyspaceAttributes.isUpdate())) {

				String query = String.format("CREATE KEYSPACE %1$s WITH replication = { 'class' : '%2$s', 'replication_factor' : %3$d } AND DURABLE_WRITES = %4$b", 
						keyspace, 
						keyspaceAttributes.getReplicationStrategy(), 
						keyspaceAttributes.getReplicationFactor(), 
						keyspaceAttributes.isDurableWrites());
				
				log.info("Create keyspace " + keyspace + " on afterPropertiesSet " + query);
				
				session.execute(query);
				keyspaceCreated = true;
			}
			
			// update keyspace if needed
			if (keyspaceAttributes.isUpdate() && !keyspaceCreated) {
				
				
				
			}
			
			// validate keyspace if needed
			if (keyspaceAttributes.isValidate()) {
				
				if (!keyspaceExists) {
					throw new IllegalStateException("keyspace '" + keyspace + "' not found in the Cassandra");
				}
			}
			
			session.execute("USE " + keyspace);
	    }
			
		// initialize property
		this.session = session;
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {
		
		if (StringUtils.hasText(keyspace) && keyspaceAttributes != null && keyspaceAttributes.isCreateDrop()) {
			log.info("Drop keyspace " + keyspace + " on destroy");
			session.execute("USE system");
			session.execute("DROP KEYSPACE " + keyspace);
		}
		this.session.shutdown();
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}

	public void setKeyspaceAttributes(KeyspaceAttributes keyspaceAttributes) {
		this.keyspaceAttributes = keyspaceAttributes;
	}

}
