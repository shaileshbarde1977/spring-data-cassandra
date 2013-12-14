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
package net.webby.cassandrion.support;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

/**
 * @author David Webb
 * @author Alex Shvid
 * 
 */
public class CassandraAccessor implements InitializingBean {

	/** Logger available to subclasses */
	protected final Logger logger = LoggerFactory.getLogger(getClass());

	private Session session;

	private CassandraExceptionTranslator exceptionTranslator;

	/**
	 * Set the exception translator for this instance.
	 * 
	 * @see net.webby.cassandrion.support.CassandraExceptionTranslator
	 */
	public void setExceptionTranslator(CassandraExceptionTranslator exceptionTranslator) {
		this.exceptionTranslator = exceptionTranslator;
	}

	/**
	 * Return the exception translator for this instance.
	 */
	public CassandraExceptionTranslator getExceptionTranslator() {
		return this.exceptionTranslator;
	}

	/**
	 * Ensure that the Cassandra Session has been set
	 */
	public void afterPropertiesSet() {
		if (getSession() == null) {
			throw new IllegalArgumentException("Property 'session' is required");
		}
	}

	/**
	 * @return Returns the session.
	 */
	public Session getSession() {
		return session;
	}

	/**
	 * @param session The session to set.
	 */
	public void setSession(Session session) {
		Assert.notNull(session);
		this.session = session;
	}

}
