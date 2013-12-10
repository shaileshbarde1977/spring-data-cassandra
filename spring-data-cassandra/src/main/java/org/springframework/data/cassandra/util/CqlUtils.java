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
package org.springframework.data.cassandra.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cassandra.core.ConsistencyLevel;
import org.springframework.cassandra.core.ConsistencyLevelResolver;
import org.springframework.cassandra.core.QueryOptions;
import org.springframework.cassandra.core.RetryPolicy;
import org.springframework.cassandra.core.RetryPolicyResolver;
import org.springframework.cassandra.core.cql.generator.AlterTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateIndexCqlGenerator;
import org.springframework.cassandra.core.cql.generator.CreateTableCqlGenerator;
import org.springframework.cassandra.core.cql.generator.DropTableCqlGenerator;
import org.springframework.cassandra.core.cql.spec.AlterTableSpecification;
import org.springframework.cassandra.core.cql.spec.CreateIndexSpecification;
import org.springframework.cassandra.core.cql.spec.CreateTableSpecification;
import org.springframework.cassandra.core.cql.spec.DropTableSpecification;
import org.springframework.data.cassandra.convert.CassandraConverter;
import org.springframework.data.cassandra.exception.EntityWriterException;
import org.springframework.data.cassandra.mapping.CassandraPersistentEntity;
import org.springframework.data.convert.EntityWriter;

import com.datastax.driver.core.Query;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Delete.Where;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;

/**
 * Utilties to convert Cassandra Annotated objects to Queries and CQL.
 * 
 * @author Alex Shvid
 * @author David Webb
 * 
 */
public abstract class CqlUtils {

	private static Logger log = LoggerFactory.getLogger(CqlUtils.class);

	/**
	 * Generates the CQL String to create a table in Cassandra
	 * 
	 * @param tableName
	 * @param entity
	 * @return The CQL that can be passed to session.execute()
	 */
	public static String createTable(String tableName, final CassandraPersistentEntity<?> entity,
			CassandraConverter cassandraConverter) {

		CreateTableSpecification spec = cassandraConverter.getCreateTableSpecification(entity);
		spec.name(tableName);

		CreateTableCqlGenerator generator = new CreateTableCqlGenerator(spec);

		return generator.toCql();
	}

	/**
	 * Create the List of CQL for the indexes required for Cassandra mapped Table.
	 * 
	 * @param tableName
	 * @param entity
	 * @return The list of CQL statements to run with session.execute()
	 */
	public static List<String> createIndexes(final String tableName, final CassandraPersistentEntity<?> entity,
			CassandraConverter cassandraConverter) {

		List<CreateIndexSpecification> specList = cassandraConverter.getCreateIndexSpecifications(entity);

		List<String> result = new ArrayList<String>(specList.size());

		for (CreateIndexSpecification spec : specList) {
			result.add(new CreateIndexCqlGenerator(spec).toCql());
		}

		return result;
	}

	/**
	 * Alter the table to refelct the entity annotations
	 * 
	 * @param tableName
	 * @param entity
	 * @param table
	 * @return
	 */
	public static String alterTable(final String tableName, final CassandraPersistentEntity<?> entity,
			final TableMetadata table, CassandraConverter cassandraConverter) {

		AlterTableSpecification spec = cassandraConverter.getAlterTableSpecification(entity, table);

		if (!spec.hasChanges()) {
			return null;
		}

		spec.name(tableName);

		AlterTableCqlGenerator generator = new AlterTableCqlGenerator(spec);

		return generator.toCql();

	}

	/**
	 * Generates a Query Object for an insert
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 * @throws EntityWriterException
	 */
	public static Query toInsertQuery(String keyspaceName, String tableName, final Object objectToSave,
			Map<String, Object> optionsByName, EntityWriter<Object, Object> entityWriter) throws EntityWriterException {

		final Insert q = QueryBuilder.insertInto(keyspaceName, tableName);

		/*
		 * Write properties
		 */
		entityWriter.write(objectToSave, q);

		/*
		 * Add Query Options
		 */
		addQueryOptions(q, optionsByName);

		/*
		 * Add TTL to Insert object
		 */
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.TTL) != null) {
			q.using(QueryBuilder.ttl((Integer) optionsByName.get(QueryOptions.QueryOptionMapKeys.TTL)));
		}

		return q;

	}

	/**
	 * Generates a Query Object for an Update
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param objectToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 * @throws EntityWriterException
	 */
	public static Query toUpdateQuery(String keyspaceName, String tableName, final Object objectToSave,
			Map<String, Object> optionsByName, EntityWriter<Object, Object> entityWriter) throws EntityWriterException {

		final Update q = QueryBuilder.update(keyspaceName, tableName);

		/*
		 * Write properties
		 */
		entityWriter.write(objectToSave, q);

		/*
		 * Add Query Options
		 */
		addQueryOptions(q, optionsByName);

		/*
		 * Add TTL to Insert object
		 */
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.TTL) != null) {
			q.using(QueryBuilder.ttl((Integer) optionsByName.get(QueryOptions.QueryOptionMapKeys.TTL)));
		}

		return q;

	}

	/**
	 * Generates a Batch Object for multiple Updates
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param objectsToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 * @throws EntityWriterException
	 */
	public static <T> Batch toUpdateBatchQuery(final String keyspaceName, final String tableName,
			final List<T> objectsToSave, Map<String, Object> optionsByName, EntityWriter<Object, Object> entityWriter)
			throws EntityWriterException {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch b = QueryBuilder.batch();

		for (final T objectToSave : objectsToSave) {

			b.add((Statement) toUpdateQuery(keyspaceName, tableName, objectToSave, optionsByName, entityWriter));

		}

		addQueryOptions(b, optionsByName);

		return b;

	}

	/**
	 * Generates a Batch Object for multiple inserts
	 * 
	 * @param keyspaceName
	 * @param tableName
	 * @param objectsToSave
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return The Query object to run with session.execute();
	 * @throws EntityWriterException
	 */
	public static <T> Batch toInsertBatchQuery(final String keyspaceName, final String tableName,
			final List<T> objectsToSave, Map<String, Object> optionsByName, EntityWriter<Object, Object> entityWriter)
			throws EntityWriterException {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch b = QueryBuilder.batch();

		for (final T objectToSave : objectsToSave) {

			b.add((Statement) toInsertQuery(keyspaceName, tableName, objectToSave, optionsByName, entityWriter));

		}

		addQueryOptions(b, optionsByName);

		return b;

	}

	/**
	 * Create a Delete Query Object from an annotated POJO
	 * 
	 * @param keyspace
	 * @param tableName
	 * @param objectToRemove
	 * @param entity
	 * @param optionsByName
	 * @return
	 * @throws EntityWriterException
	 */
	public static Query toDeleteQuery(String keyspace, String tableName, final Object objectToRemove,
			Map<String, Object> optionsByName, EntityWriter<Object, Object> entityWriter) throws EntityWriterException {

		final Delete.Selection ds = QueryBuilder.delete();
		final Delete q = ds.from(keyspace, tableName);
		final Where w = q.where();

		/*
		 * Write where condition to find by Id
		 */
		entityWriter.write(objectToRemove, w);

		addQueryOptions(q, optionsByName);

		return q;

	}

	/**
	 * @param tableName
	 * @return
	 */
	public static String dropTable(String tableName) {

		if (tableName == null) {
			return null;
		}

		DropTableSpecification spec = new DropTableSpecification().name(tableName);
		return new DropTableCqlGenerator(spec).toCql();

	}

	/**
	 * Create a Batch Query object for multiple deletes.
	 * 
	 * @param keyspace
	 * @param tableName
	 * @param entities
	 * @param entity
	 * @param optionsByName
	 * 
	 * @return
	 * @throws EntityWriterException
	 */
	public static <T> Batch toDeleteBatchQuery(String keyspaceName, String tableName, List<T> entities,
			Map<String, Object> optionsByName, EntityWriter<Object, Object> entityWriter) throws EntityWriterException {

		/*
		 * Return variable is a Batch statement
		 */
		final Batch b = QueryBuilder.batch();

		for (final T objectToSave : entities) {

			b.add((Statement) toDeleteQuery(keyspaceName, tableName, objectToSave, optionsByName, entityWriter));

		}

		addQueryOptions(b, optionsByName);

		return b;

	}

	/**
	 * Add common Query options for all types of queries.
	 * 
	 * @param q
	 * @param optionsByName
	 */
	private static void addQueryOptions(Query q, Map<String, Object> optionsByName) {

		if (optionsByName == null) {
			return;
		}

		/*
		 * Add Query Options
		 */
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL) != null) {
			q.setConsistencyLevel(ConsistencyLevelResolver.resolve((ConsistencyLevel) optionsByName
					.get(QueryOptions.QueryOptionMapKeys.CONSISTENCY_LEVEL)));
		}
		if (optionsByName.get(QueryOptions.QueryOptionMapKeys.RETRY_POLICY) != null) {
			q.setRetryPolicy(RetryPolicyResolver.resolve((RetryPolicy) optionsByName
					.get(QueryOptions.QueryOptionMapKeys.RETRY_POLICY)));
		}

	}

}
