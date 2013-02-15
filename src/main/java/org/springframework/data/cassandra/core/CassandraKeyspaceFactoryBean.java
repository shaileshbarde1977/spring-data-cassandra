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

import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.datastax.driver.core.Cluster;
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

	private Cluster cluster;
	private Session session;
	private String keyspace;
	
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
		if (keyspace != null) {
			session = cluster.connect(keyspace);
		}
		else {
			session = cluster.connect();
		}
		
		// initialize property
		this.session = session;
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {
		this.session.shutdown();
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void setCluster(Cluster cluster) {
		this.cluster = cluster;
	}
	
	
}
