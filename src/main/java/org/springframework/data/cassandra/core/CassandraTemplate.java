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

import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * @author Alex Shvid
 */
public class CassandraTemplate implements CassandraOperations {

	private final Session session;

	private final PersistenceExceptionTranslator exceptionTranslator = new CassandraExceptionTranslator();

	/**
	 * Constructor used for a basic template configuration
	 * 
	 * @param factory
	 *            must not be {@literal null}.
	 */
	public CassandraTemplate(Session session) {
		this.session = session;
	}

	public ResultSet executeQuery(String query) {
		try {
			return session.execute(query);
		} catch (NoHostAvailableException e) {
			throw new CassandraConnectionFailureException("no host available", e);
		} catch (RuntimeException e) {
			throw potentiallyConvertRuntimeException(e);
		}
	}

	private RuntimeException potentiallyConvertRuntimeException(
			RuntimeException ex) {
		RuntimeException resolved = this.exceptionTranslator.translateExceptionIfPossible(ex);
		return resolved == null ? ex : resolved;
	}
}
