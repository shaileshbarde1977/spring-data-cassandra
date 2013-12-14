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
package net.webby.cassandrion.core;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import net.webby.cassandrion.core.query.QueryOptions;

import org.springframework.dao.DataAccessException;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

/**
 * Operations for interacting with Cassandra at the lowest level. This interface provides Exception Translation.
 * 
 * @author David Webb
 * @author Matthew Adams
 */
public interface CassandrionOperations {

	/**
	 * Executes the supplied {@link SessionCallback} in the current Template Session. The implementation of
	 * SessionCallback can decide whether or not to <code>execute()</code> or <code>executeAsync()</code> the operation.
	 * 
	 * @param sessionCallback
	 * @return Type<T> defined in the SessionCallback
	 */
	<T> T execute(SessionCallback<T> sessionCallback) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query and returns nothing.
	 * 
	 * @param cql
	 */
	void execute(final String cql) throws DataAccessException;

	/**
	 * Executes the supplied CQL Query Asynchronously and returns nothing.
	 * 
	 * @param cql The CQL Statement to execute
	 */
	void executeAsynchronously(final String cql) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and extracts the results with the ResultSetExtractor.
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the ResultSet
	 * 
	 * @return Type <T> specified in the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(final String cql, ResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and extracts the results with the ResultSetExtractor.
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the ResultSet
	 * @param optionsByName Query Options Map
	 * 
	 * @return
	 * @throws DataAccessException
	 */
	<T> T query(final String cql, ResultSetExtractor<T> rse, final Map<String, Object> optionsByName)
			throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and extracts the results with the ResultSetExtractor.
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the ResultSet
	 * @param options Query Options Object
	 * 
	 * @return
	 * @throws DataAccessException
	 */
	<T> T query(final String cql, ResultSetExtractor<T> rse, final QueryOptions options) throws DataAccessException;

	/**
	 * Executes the provided CQL Query asynchronously, and extracts the results with the ResultSetFutureExtractor
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the future results
	 * @return
	 * @throws DataAccessException
	 */
	<T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> rse) throws DataAccessException;

	/**
	 * Executes the provided CQL Query asynchronously, and extracts the results with the ResultSetFutureExtractor
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the future results
	 * @param optionsByName Query Options Map
	 * @return
	 * @throws DataAccessException
	 */
	<T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> rse, final Map<String, Object> optionsByName)
			throws DataAccessException;

	/**
	 * Executes the provided CQL Query asynchronously, and extracts the results with the ResultSetFutureExtractor
	 * 
	 * @param cql The Query
	 * @param rse The implementation for extracting the future results
	 * @param options Query Options Object
	 * @return
	 * @throws DataAccessException
	 */
	<T> T queryAsynchronously(final String cql, ResultSetFutureExtractor<T> rse, final QueryOptions options)
			throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and then processes the results with the <code>RowCallbackHandler</code>.
	 * 
	 * @param cql The Query
	 * @param rch The implementation for processing the rows returned.
	 * @throws DataAccessException
	 */
	void query(final String cql, RowCallbackHandler rch) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and then processes the results with the <code>RowCallbackHandler</code>.
	 * 
	 * @param cql The Query
	 * @param rch The implementation for processing the rows returned.
	 * @param options Query Options Map
	 * @throws DataAccessException
	 */
	void query(final String cql, RowCallbackHandler rch, final Map<String, Object> optionsByName)
			throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and then processes the results with the <code>RowCallbackHandler</code>.
	 * 
	 * @param cql The Query
	 * @param rch The implementation for processing the rows returned.
	 * @param options Query Options Object
	 * @throws DataAccessException
	 */
	void query(final String cql, RowCallbackHandler rch, final QueryOptions options) throws DataAccessException;

	/**
	 * Processes the ResultSet through the RowCallbackHandler and return nothing. This is used internal to the Template
	 * for core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param rch RowCallbackHandler with the processing implementation
	 * @throws DataAccessException
	 */
	void process(ResultSet resultSet, RowCallbackHandler rch) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and maps all Rows returned with the supplied RowMapper.
	 * 
	 * @param cql The Query
	 * @param rowMapper The implementation for mapping all rows
	 * @return List of <T> processed by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> query(final String cql, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and maps all Rows returned with the supplied RowMapper.
	 * 
	 * @param cql The Query
	 * @param rowMapper The implementation for mapping all rows
	 * @param optionsByName Query Options Map
	 * @return List of <T> processed by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> query(final String cql, RowMapper<T> rowMapper, final Map<String, Object> optionsByName)
			throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and maps all Rows returned with the supplied RowMapper.
	 * 
	 * @param cql The Query
	 * @param rowMapper The implementation for mapping all rows
	 * @param options Query Options Object
	 * @return List of <T> processed by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> query(final String cql, RowMapper<T> rowMapper, final QueryOptions options) throws DataAccessException;

	/**
	 * Processes the ResultSet through the RowMapper and returns the List of mapped Rows. This is used internal to the
	 * Template for core operations, but is made available through Operations in the event you have a ResultSet to
	 * process. The ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet Results to process
	 * @param rowMapper RowMapper with the processing implementation
	 * @return List of <T> generated by the RowMapper
	 * @throws DataAccessException
	 */
	<T> List<T> process(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Executes the provided CQL Query, and maps <b>ONE</b> Row returned with the supplied RowMapper.
	 * 
	 * <p>
	 * This expects only ONE row to be returned. More than one Row will cause an Exception to be thrown.
	 * </p>
	 * 
	 * @param cql The Query
	 * @param rowMapper The implementation for convert the Row to <T>
	 * @return Object<T>
	 * @throws DataAccessException
	 */
	<T> T queryForObject(final String cql, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Process a ResultSet through a RowMapper. This is used internal to the Template for core operations, but is made
	 * available through Operations in the event you have a ResultSet to process. The ResultsSet could come from a
	 * ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param rowMapper
	 * @return
	 * @throws DataAccessException
	 */
	<T> T processOne(ResultSet resultSet, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Executes the provided query and tries to return the first column of the first Row as a Class<T>.
	 * 
	 * @param cql The Query
	 * @param requiredType Valid Class that Cassandra Data Types can be converted to.
	 * @return The Object<T> - item [0,0] in the result table of the query.
	 * @throws DataAccessException
	 */
	<T> T queryForObject(final String cql, Class<T> requiredType) throws DataAccessException;

	/**
	 * Process a ResultSet, trying to convert the first columns of the first Row to Class<T>. This is used internal to the
	 * Template for core operations, but is made available through Operations in the event you have a ResultSet to
	 * process. The ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param requiredType
	 * @return
	 * @throws DataAccessException
	 */
	<T> T processOne(ResultSet resultSet, Class<T> requiredType) throws DataAccessException;

	/**
	 * Executes the provided CQL Query and maps <b>ONE</b> Row to a basic Map of Strings and Objects. If more than one Row
	 * is returned from the Query, an exception will be thrown.
	 * 
	 * @param cql The Query
	 * @return Map representing the results of the Query
	 * @throws DataAccessException
	 */
	Map<String, Object> queryForMap(final String cql) throws DataAccessException;

	/**
	 * Process a ResultSet with <b>ONE</b> Row and convert to a Map. This is used internal to the Template for core
	 * operations, but is made available through Operations in the event you have a ResultSet to process. The ResultsSet
	 * could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @return
	 * @throws DataAccessException
	 */
	Map<String, Object> processMap(ResultSet resultSet) throws DataAccessException;

	/**
	 * Executes the provided CQL and returns all values in the first column of the Results as a List of the Type in the
	 * second argument.
	 * 
	 * @param cql The Query
	 * @param elementType Type to cast the data values to
	 * @return List of elementType
	 * @throws DataAccessException
	 */
	<T> List<T> queryForList(final String cql, Class<T> elementType) throws DataAccessException;

	/**
	 * Process a ResultSet and convert the first column of the results to a List. This is used internal to the Template
	 * for core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @param elementType
	 * @return
	 * @throws DataAccessException
	 */
	<T> List<T> processList(ResultSet resultSet, Class<T> elementType) throws DataAccessException;

	/**
	 * Executes the provided CQL and converts the results to a basic List of Maps. Each element in the List represents a
	 * Row returned from the Query. Each Row's columns are put into the map as column/value.
	 * 
	 * @param cql The Query
	 * @return List of Maps with the query results
	 * @throws DataAccessException
	 */
	List<Map<String, Object>> queryForListOfMap(final String cql) throws DataAccessException;

	/**
	 * Process a ResultSet and convert it to a List of Maps with column/value. This is used internal to the Template for
	 * core operations, but is made available through Operations in the event you have a ResultSet to process. The
	 * ResultsSet could come from a ResultSetFuture after an asynchronous query.
	 * 
	 * @param resultSet
	 * @return
	 * @throws DataAccessException
	 */
	List<Map<String, Object>> processListOfMap(ResultSet resultSet) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * PreparedStatementCallback implementation provided by the Application Code.
	 * 
	 * @param cql The CQL Statement to Execute
	 * @param action What to do with the results of the PreparedStatement
	 * @return Type<T> as determined by the supplied Callback.
	 * @throws DataAccessException
	 */
	<T> T execute(String cql, PreparedStatementCallback<T> action) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call, then executes the statement and processes
	 * the statement using the provided Callback. <b>This can only be used for CQL Statements that do not have data
	 * binding.</b> The results of the PreparedStatement are processed with PreparedStatementCallback implementation
	 * provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param action What to do with the results of the PreparedStatement
	 * @return Type<T> as determined by the supplied Callback.
	 * @throws DataAccessException
	 */
	<T> T execute(PreparedStatementCreator psc, PreparedStatementCallback<T> action) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the ResultSetExtractor implementation provided by the Application Code. The can return any object,
	 * including a List of Objects to support the ResultSet processing.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rse The implementation for extracting the results of the query.
	 * @return Type<T> generated by the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(final String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse) throws DataAccessException;

	<T> T query(final String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse,
			final Map<String, Object> optionsByName) throws DataAccessException;

	<T> T query(final String cql, PreparedStatementBinder psb, ResultSetExtractor<T> rse, final QueryOptions options)
			throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowCallbackHandler implementation provided and nothing is returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rch The RowCallbackHandler for processing the ResultSet
	 * @throws DataAccessException
	 */
	void query(final String cql, PreparedStatementBinder psb, RowCallbackHandler rch) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowCallbackHandler implementation provided and nothing is returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rch The RowCallbackHandler for processing the ResultSet
	 * @param optionsByName The Query Options Map
	 * @throws DataAccessException
	 */
	void query(final String cql, PreparedStatementBinder psb, RowCallbackHandler rch,
			final Map<String, Object> optionsByName) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowCallbackHandler implementation provided and nothing is returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rch The RowCallbackHandler for processing the ResultSet
	 * @param options The Query Options Object
	 * @throws DataAccessException
	 */
	void query(final String cql, PreparedStatementBinder psb, RowCallbackHandler rch, final QueryOptions options)
			throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowMapper implementation provided and a List is returned with elements of Type <T> for each Row
	 * returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rowMapper The implementation for Mapping a Row to Type <T>
	 * @return List of <T> for each Row returned from the Query.
	 * @throws DataAccessException
	 */
	<T> List<T> query(final String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowMapper implementation provided and a List is returned with elements of Type <T> for each Row
	 * returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rowMapper The implementation for Mapping a Row to Type <T>
	 * @param optionsByName The Query Options Map
	 * @return List of <T> for each Row returned from the Query.
	 * @throws DataAccessException
	 */
	<T> List<T> query(final String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper,
			final Map<String, Object> optionsByName) throws DataAccessException;

	/**
	 * Converts the CQL provided into a {@link SimplePreparedStatementCreator}. Then, the PreparedStatementBinder will
	 * bind its values to the bind variables in the provided CQL String. The results of the PreparedStatement are
	 * processed with the RowMapper implementation provided and a List is returned with elements of Type <T> for each Row
	 * returned.
	 * 
	 * @param cql The Query to Prepare
	 * @param psb The Binding implementation
	 * @param rowMapper The implementation for Mapping a Row to Type <T>
	 * @param options The Query Options Object
	 * @return List of <T> for each Row returned from the Query.
	 * @throws DataAccessException
	 */
	<T> List<T> query(final String cql, PreparedStatementBinder psb, RowMapper<T> rowMapper, final QueryOptions options)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rse Implementation for extracting from the ResultSet
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rse Implementation for extracting from the ResultSet
	 * @param optionsByName The Query Options Map
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse, final Map<String, Object> optionsByName)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rse Implementation for extracting from the ResultSet
	 * @param options The Query Options Object
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, ResultSetExtractor<T> rse, final QueryOptions options)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rch The implementation to process Results
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, RowCallbackHandler rch) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rch The implementation to process Results
	 * @param optionsByName The Query Options Map
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, RowCallbackHandler rch, final Map<String, Object> optionsByName)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rch The implementation to process Results
	 * @param options The Query Options Object
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, RowCallbackHandler rch, final QueryOptions options)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with RowMapper
	 * implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @return List of Type <T> mapped from each Row in the Results
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with RowMapper
	 * implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @param optionsByName The Query Options Map
	 * @return List of Type <T> mapped from each Row in the Results
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper, final Map<String, Object> optionsByName)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. <b>This can only be used for CQL
	 * Statements that do not have data binding.</b> The results of the PreparedStatement are processed with RowMapper
	 * implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @param options The Query Options Object
	 * @return List of Type <T> mapped from each Row in the Results
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, RowMapper<T> rowMapper, final QueryOptions options)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rse Implementation for extracting from the ResultSet
	 * @param optionsByName The Query Options Map
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final ResultSetExtractor<T> rse,
			final Map<String, Object> optionsByName) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rse Implementation for extracting from the ResultSet
	 * @param options The Query Options Object
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final ResultSetExtractor<T> rse,
			final QueryOptions options) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * ResultSetExtractor implementation provided by the Application Code.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rse Implementation for extracting from the ResultSet
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> T query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final ResultSetExtractor<T> rse)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rch The implementation to process Results
	 * @param optionsByName The Query Options Map
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowCallbackHandler rch,
			final Map<String, Object> optionsByName) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rch The implementation to process Results
	 * @param options The Query Options Object
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowCallbackHandler rch,
			final QueryOptions options) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowCallbackHandler and nothing is returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rch The implementation to process Results
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	void query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowCallbackHandler rch)
			throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowMapper implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @param optionsByName The Query Options Map
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowMapper<T> rowMapper,
			final Map<String, Object> optionsByName) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowMapper implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @param options The Query Options Object
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowMapper<T> rowMapper,
			final QueryOptions options) throws DataAccessException;

	/**
	 * Uses the provided PreparedStatementCreator to prepare a new Session call. Binds the values from the
	 * PreparedStatementBinder to the available bind variables. The results of the PreparedStatement are processed with
	 * RowMapper implementation provided and a List is returned with elements of Type <T> for each Row returned.
	 * 
	 * @param psc The implementation to create the PreparedStatement
	 * @param psb The implementation to bind variables to values
	 * @param rowMapper The implementation for mapping each Row returned.
	 * @return Type <T> which is the output of the ResultSetExtractor
	 * @throws DataAccessException
	 */
	<T> List<T> query(PreparedStatementCreator psc, final PreparedStatementBinder psb, final RowMapper<T> rowMapper)
			throws DataAccessException;

	/**
	 * Describe the current Ring. This uses the provided {@link RingMemberHostMapper} to provide the basics of the
	 * Cassandra Ring topology.
	 * 
	 * @return The list of ring tokens that are active in the cluster
	 */
	List<RingMember> describeRing() throws DataAccessException;

	/**
	 * Describe the current Ring. Application code must provide its own {@link HostMapper} implementation to process the
	 * lists of hosts returned by the Cassandra Cluster Metadata.
	 * 
	 * @param hostMapper The implementation to use for host mapping.
	 * @return Collection generated by the provided HostMapper.
	 * @throws DataAccessException
	 */
	<T> Collection<T> describeRing(HostMapper<T> hostMapper) throws DataAccessException;

	/**
	 * Get the current Session used for operations in the implementing class.
	 * 
	 * @return The DataStax Driver Session Object
	 */
	Session getSession();

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * This is used internally by the other ingest() methods, but can be used if you want to write your own RowIterator.
	 * The Object[] length returned by the next() implementation must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rowIterator Implementation to provide the Object[] to be bound to the CQL.
	 * @param optionsByName The Query Options Map
	 */
	void ingest(String cql, RowIterator rowIterator, Map<String, Object> optionsByName);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * This is used internally by the other ingest() methods, but can be used if you want to write your own RowIterator.
	 * The Object[] length returned by the next() implementation must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rowIterator Implementation to provide the Object[] to be bound to the CQL.
	 * @param options The Query Options Object
	 */
	void ingest(String cql, RowIterator rowIterator, QueryOptions options);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * This is used internally by the other ingest() methods, but can be used if you want to write your own RowIterator.
	 * The Object[] length returned by the next() implementation must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rowIterator Implementation to provide the Object[] to be bound to the CQL.
	 */
	void ingest(String cql, RowIterator rowIterator);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * The List<?> length must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows List of List<?> with data to bind to the CQL.
	 * @param optionsByName The Query Options Map
	 */
	void ingest(String cql, List<List<?>> rows, Map<String, Object> optionsByName);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * The List<?> length must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows List of List<?> with data to bind to the CQL.
	 * @param options The Query Options Object
	 */
	void ingest(String cql, List<List<?>> rows, QueryOptions options);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * The List<?> length must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows List of List<?> with data to bind to the CQL.
	 */
	void ingest(String cql, List<List<?>> rows);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * The Object[] length of the nested array must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows Object array of Object array of values to bind to the CQL.
	 * @param optionsByName The Query Options Map
	 */
	void ingest(String cql, Object[][] rows, Map<String, Object> optionsByName);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * The Object[] length of the nested array must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows Object array of Object array of values to bind to the CQL.
	 * @param options The Query Options Object
	 */
	void ingest(String cql, Object[][] rows, QueryOptions options);

	/**
	 * This is an operation designed for high performance writes. The cql is used to create a PreparedStatement once, then
	 * all row values are bound to the single PreparedStatement and executed against the Session.
	 * 
	 * <p>
	 * The Object[] length of the nested array must match the number of bind variables in the CQL.
	 * </p>
	 * 
	 * @param cql The CQL
	 * @param rows Object array of Object array of values to bind to the CQL.
	 */
	void ingest(String cql, Object[][] rows);

	/**
	 * Delete all rows in the table
	 * 
	 * @param tableName
	 */
	void truncate(String tableName);

}
