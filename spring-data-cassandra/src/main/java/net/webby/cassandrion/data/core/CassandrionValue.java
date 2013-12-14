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
package net.webby.cassandrion.data.core;

import java.nio.ByteBuffer;

import com.datastax.driver.core.DataType;

/**
 * Simple Cassandra value of the ByteBuffer with DataType
 * 
 * @author Alex Shvid
 */
public class CassandrionValue {

	private final ByteBuffer value;
	private final DataType type;

	public CassandrionValue(ByteBuffer value, DataType type) {
		this.value = value;
		this.type = type;
	}

	public ByteBuffer getValue() {
		return value;
	}

	public DataType getType() {
		return type;
	}

}
