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
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;

/**
 * Convenient factory for configuring a Cassandra Cluster.
 * 
 * @author Alex Shvid
 */

public class CassandraClusterFactoryBean implements FactoryBean<Cluster>,
		InitializingBean, DisposableBean, PersistenceExceptionTranslator {

	private static final int DEFAULT_PORT = 9042;
	
	private Cluster cluster;
	
	private String contactPoints;
	private int port = DEFAULT_PORT;
	
	private PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	public Cluster getObject() throws Exception {
		return cluster;
	}
	
	/*
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.FactoryBean#getObjectType()
	 */
	public Class<? extends Cluster> getObjectType() {
		return Cluster.class;
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
		
		if (!StringUtils.hasText(contactPoints)) {
			throw new IllegalArgumentException(
					"at least one server is required");
		}
		
		Cluster.Builder builder = Cluster.builder();

		builder.addContactPoints(StringUtils.commaDelimitedListToStringArray(contactPoints)).withPort(port);
		
		Cluster cluster = builder.build();
		
		// initialize property
		this.cluster = cluster;
	}
	
	/* 
	 * (non-Javadoc)
	 * @see org.springframework.beans.factory.DisposableBean#destroy()
	 */
	public void destroy() throws Exception {
		this.cluster.shutdown();
	}

	public void setContactPoints(String contactPoints) {
		this.contactPoints = contactPoints;
	}

	public void setPort(int port) {
		this.port = port;
	}
	

}
