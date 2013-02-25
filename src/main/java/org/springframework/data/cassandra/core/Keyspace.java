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

import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.convert.MappingCassandraConverter;
import org.springframework.data.cassandra.mapping.CassandraMappingContext;

import com.datastax.driver.core.Session;

/**
 * Simple Cassandra Keyspace object 
 * 
 * @author Alex Shvid
 */
public class Keyspace {

	private final String keyspace;
	private final Session session;
	private final CassandraConverter cassandraConverter;
	
	/**
	 * Constructor used for a basic Keyspace configuration with Cassandra
	 * Session associated with Cassandra Keyspace
	 * 
	 * @param keyspace, system if {@literal null}.
	 * @param session must not be {@literal null}.
	 */
	public Keyspace(String keyspace, Session session) {
		this(keyspace, session, null);
	}
	
	/**
	 * Constructor used for a basic keyspace configuration
	 * 
	 * @param keyspace, system if {@literal null}.
	 * @param session must not be {@literal null}.
	 * @param cassandraConverter, create default if {@literal null}.
	 */
	public Keyspace(String keyspace, Session session, CassandraConverter cassandraConverter) {
		this.keyspace = keyspace;
		this.session = session;
		this.cassandraConverter = cassandraConverter != null ? cassandraConverter : getDefaultCassandraConverter();
	}
	
	public String getKeyspace() {
		return keyspace;
	}

	public Session getSession() {
		return session;
	}

	public CassandraConverter getCassandraConverter() {
		return cassandraConverter;
	}

	private static final CassandraConverter getDefaultCassandraConverter() {
		MappingCassandraConverter converter = new MappingCassandraConverter(new CassandraMappingContext());
		converter.afterPropertiesSet();
		return converter;
	}

}
