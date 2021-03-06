/*
 * Copyright 2013-2014 the original author or authors.
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
package org.springdata.cassandra.config;

import org.springdata.cassandra.cql.config.xml.ConfigCqlConstants;

/**
 * 
 * @author Alex Shvid
 * 
 */

public interface ConfigConstants extends ConfigCqlConstants {

	public static final String CASSANDRA_CONVERTER = "cassandra-converter";
	public static final String CASSANDRA_TEMPLATE = "cassandra-template";

	public static final String CASSANDRA_MAPPING_CONVERTER_ELEMENT = "mapping-converter";
	public static final String CASSANDRA_TEMPLATE_ELEMENT = "template";

}
