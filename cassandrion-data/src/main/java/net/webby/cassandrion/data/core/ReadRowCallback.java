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

import net.webby.cassandrion.core.RowCallback;

import org.springframework.data.convert.EntityReader;
import org.springframework.util.Assert;

import com.datastax.driver.core.Row;

/**
 * Simple {@link RowCallback} that will transform {@link Row} into the given target type using the given
 * {@link EntityReader}.
 * 
 * @author Alex Shvid
 */
public class ReadRowCallback<T> implements RowCallback<T> {

	private final EntityReader<? super T, Object> reader;
	private final Class<T> type;

	public ReadRowCallback(EntityReader<? super T, Object> reader, Class<T> type) {
		Assert.notNull(reader);
		Assert.notNull(type);
		this.reader = reader;
		this.type = type;
	}

	@Override
	public T doWith(Row row) {
		T source = reader.read(type, row);
		return source;
	}
}