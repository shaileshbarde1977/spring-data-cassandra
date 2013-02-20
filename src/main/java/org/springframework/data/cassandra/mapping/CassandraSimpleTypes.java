/*
 * Copyright 2010-2013 the original author or authors.
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
package org.springframework.data.cassandra.mapping;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.springframework.data.mapping.model.SimpleTypeHolder;

import com.datastax.driver.core.DataType;

/**
 * Simple constant holder for a {@link SimpleTypeHolder} enriched with Cassandra specific simple types.
 * 
 * @author Alex Shvid
 */
public class CassandraSimpleTypes {

	static {
		Set<Class<?>> simpleTypes = new HashSet<Class<?>>();
		for (DataType dataType : DataType.allPrimitiveTypes()) {
			simpleTypes.add(dataType.asJavaClass());
		}
		CASSANDRA_SIMPLE_TYPES = Collections.unmodifiableSet(simpleTypes);
	}

	private static final Set<Class<?>> CASSANDRA_SIMPLE_TYPES;
	public static final SimpleTypeHolder HOLDER = new SimpleTypeHolder(CASSANDRA_SIMPLE_TYPES, true);

	private CassandraSimpleTypes() {
	}
	
}
