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
package org.springframework.cassandra.core.cql.spec;

/**
 * Abstract builder class to support keyspace operations creation.
 * 
 * @author Alex Shvid
 * @param <T> The subtype of the {@link KeyspaceOptionsSpecification}.
 */
public abstract class KeyspaceOptionsSpecification<T extends KeyspaceOptionsSpecification<T>> extends
		WithOptionsSpecification<KeyspaceOption, KeyspaceOptionsSpecification<T>> {

	@SuppressWarnings("unchecked")
	public T name(String name) {
		return (T) super.name(name);
	}

	/**
	 * Convenience method that calls <code>with(option, null)</code>.
	 * 
	 * @return this
	 */
	public T with(KeyspaceOption option) {
		return with(option, null);
	}

	/**
	 * Sets the given table option. This is a convenience method that calls
	 * {@link #with(String, Object, boolean, boolean)} appropriately from the given {@link TableOption} and value for that
	 * option.
	 * 
	 * @param option The option to set.
	 * @param value The value of the option. Must be type-compatible with the {@link TableOption}.
	 * @return this
	 * @see #with(String, Object, boolean, boolean)
	 */
	public T with(KeyspaceOption option, Object value) {
		option.checkValue(value);
		return (T) with(option.getName(), value, option.escapesValue(), option.quotesValue());
	}

}
