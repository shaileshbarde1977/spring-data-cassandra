/*
 * Copyright 2014 the original author or authors.
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
package org.springdata.cassandra.data.core;

import java.util.List;

import org.springdata.cassandra.base.core.query.QueryOptions;

/**
 * IndexDataOperations interface
 * 
 * @author Alex Shvid
 * 
 */
public interface IndexDataOperations {

	/**
	 * Create all indexed annotated in entityClass
	 * 
	 * @param entityClass The class whose fields determine the new table's columns.
	 * @param optionsOrNull The Query Options Object is exists.
	 */
	void createIndexes(Class<?> entityClass, QueryOptions optionsOrNull);

	/**
	 * Create all indexed annotated in entityClass
	 * 
	 * @param entityClass The class whose fields determine the new table's columns.
	 * @param optionsOrNull The Query Options Object is exists.
	 */
	void alterIndexes(Class<?> entityClass, QueryOptions optionsOrNull);

	/**
	 * Create all indexed annotated in entityClass
	 * 
	 * @param entityClass The class whose fields determine the new table's columns.
	 * @return List of the cql statement to change indexes
	 */
	List<String> validateIndexes(Class<?> entityClass);

}
