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
package net.webby.cassandrion.core.cql.generator;

import static net.webby.cassandrion.core.cql.CqlStringUtils.noNull;
import net.webby.cassandrion.core.cql.spec.AlterKeyspaceSpecification;
import net.webby.cassandrion.core.cql.spec.KeyspaceOption;

/**
 * CQL generator for generating a <code>ALTER KEYSPACE</code> statement.
 * 
 * @author Alex Shvid
 */
public class AlterKeyspaceCqlGenerator extends WithOptionsCqlGenerator<KeyspaceOption, AlterKeyspaceSpecification> {

	public AlterKeyspaceCqlGenerator(AlterKeyspaceSpecification spec) {
		super(spec);
	}

	public StringBuilder toCql(StringBuilder cql) {
		return noNull(cql).append("ALTER KEYSPACE ").append(spec().getNameAsIdentifier()).append(";");
	}

}
