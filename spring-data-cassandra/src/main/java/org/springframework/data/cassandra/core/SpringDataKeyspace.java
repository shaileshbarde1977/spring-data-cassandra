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
import org.springframework.util.Assert;

import com.datastax.driver.core.Session;

/**
 * @deprecated This needs more thought.
 */
@Deprecated
public class SpringDataKeyspace extends Keyspace {

	private CassandraConverter converter;

	public SpringDataKeyspace(String keyspace, Session session, CassandraConverter converter) {
		super(keyspace, session);
		setCassandraConverter(converter);
	}

	public CassandraConverter getCassandraConverter() {
		return converter;
	}

	private void setCassandraConverter(CassandraConverter converter) {
		Assert.notNull(converter);
		this.converter = converter;
	}
}
