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
package org.springdata.cassandra.data.repository.query;

import java.util.List;

import org.springdata.cassandra.data.core.CassandraDataOperations;
import org.springframework.core.convert.ConversionService;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.repository.query.ParameterAccessor;
import org.springframework.data.repository.query.RepositoryQuery;
import org.springframework.util.Assert;

/**
 * Base class for {@link RepositoryQuery} implementations for Cassandra.
 * 
 * @author Alex Shvid
 */
public abstract class AbstractCassandraQuery implements RepositoryQuery {

	private static final ConversionService CONVERSION_SERVICE = new DefaultConversionService();

	private final CassandraQueryMethod method;
	private final CassandraDataOperations dataOperations;

	/**
	 * Creates a new {@link AbstractMongoQuery} from the given {@link CassandraQueryMethod} and
	 * {@link CassandraDataOperations}.
	 * 
	 * @param method must not be {@literal null}.
	 * @param operations must not be {@literal null}.
	 */
	public AbstractCassandraQuery(CassandraQueryMethod method, CassandraDataOperations dataOperations) {

		Assert.notNull(method);
		Assert.notNull(dataOperations);

		this.method = method;
		this.dataOperations = dataOperations;
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#getQueryMethod()
	 */
	public CassandraQueryMethod getQueryMethod() {
		return method;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.query.RepositoryQuery#execute(java.lang.Object[])
	 */
	public Object execute(Object[] parameters) {

		CassandraEntityMetadata<?> metadata = method.getEntityInformation();

		CassandraParameterAccessor accessor = new CassandraParametersParameterAccessor(method, parameters);
		String query = createQuery(accessor, metadata.getTableName());

		if (method.isCollectionQuery()) {
			return new CollectionExecution().execute(query);
		}

		Object result = new SingleEntityExecution(isCountQuery()).execute(query);

		if (result == null) {
			return result;
		}

		Class<?> expectedReturnType = method.getReturnType().getType();

		if (expectedReturnType.isAssignableFrom(result.getClass())) {
			return result;
		}

		return CONVERSION_SERVICE.convert(result, expectedReturnType);
	}

	/**
	 * Creates a Cql count query using the given {@link ParameterAccessor} and tableName
	 * 
	 * @param accessor must not be {@literal null}.
	 * @param tableName
	 * @return
	 */
	protected String createCountQuery(CassandraParameterAccessor accessor, String tableName) {
		return createQuery(accessor, tableName);
	}

	/**
	 * Creates a Cql query using the given {@link ParameterAccessor} and tableName
	 * 
	 * @param accessor must not be {@literal null}.
	 * @param tableName
	 * @return
	 */
	protected abstract String createQuery(CassandraParameterAccessor accessor, String tableName);

	/**
	 * Returns whether the query should get a count projection applied.
	 * 
	 * @return
	 */
	protected abstract boolean isCountQuery();

	private abstract class Execution {

		abstract Object execute(String query);

		protected List<?> readCollection(String query) {

			CassandraEntityMetadata<?> metadata = method.getEntityInformation();

			return dataOperations.find(query, metadata.getJavaType(), null);
		}
	}

	/**
	 * {@link Execution} for collection returning queries.
	 * 
	 * @author Alex Shvid
	 */
	class CollectionExecution extends Execution {

		CollectionExecution() {
		}

		@Override
		public Object execute(String query) {
			return readCollection(query);
		}
	}

	/**
	 * {@link Execution} to return a single entity.
	 * 
	 * @author Alex Shvid
	 */
	class SingleEntityExecution extends Execution {

		private final boolean countProjection;

		private SingleEntityExecution(boolean countProjection) {
			this.countProjection = countProjection;
		}

		@Override
		Object execute(String query) {

			CassandraEntityMetadata<?> metadata = method.getEntityInformation();
			return countProjection ? dataOperations.count(query, null) : dataOperations.findOne(query,
					metadata.getJavaType(), null);
		}
	}

}
