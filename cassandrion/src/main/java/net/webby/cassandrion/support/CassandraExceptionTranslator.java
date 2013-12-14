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
package net.webby.cassandrion.support;

import net.webby.cassandrion.support.exception.CassandraAuthenticationException;
import net.webby.cassandrion.support.exception.CassandraConnectionFailureException;
import net.webby.cassandrion.support.exception.CassandraInsufficientReplicasAvailableException;
import net.webby.cassandrion.support.exception.CassandraInternalException;
import net.webby.cassandrion.support.exception.CassandraInvalidConfigurationInQueryException;
import net.webby.cassandrion.support.exception.CassandraInvalidQueryException;
import net.webby.cassandrion.support.exception.CassandraKeyspaceExistsException;
import net.webby.cassandrion.support.exception.CassandraQuerySyntaxException;
import net.webby.cassandrion.support.exception.CassandraReadTimeoutException;
import net.webby.cassandrion.support.exception.CassandraTableExistsException;
import net.webby.cassandrion.support.exception.CassandraTraceRetrievalException;
import net.webby.cassandrion.support.exception.CassandraTruncateException;
import net.webby.cassandrion.support.exception.CassandraTypeMismatchException;
import net.webby.cassandrion.support.exception.CassandraUnauthorizedException;
import net.webby.cassandrion.support.exception.CassandraUncategorizedException;
import net.webby.cassandrion.support.exception.CassandraWriteTimeoutException;

import org.springframework.dao.DataAccessException;
import org.springframework.dao.support.PersistenceExceptionTranslator;

import com.datastax.driver.core.WriteType;
import com.datastax.driver.core.exceptions.AlreadyExistsException;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.DriverInternalError;
import com.datastax.driver.core.exceptions.InvalidConfigurationInQueryException;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.driver.core.exceptions.TraceRetrievalException;
import com.datastax.driver.core.exceptions.TruncateException;
import com.datastax.driver.core.exceptions.UnauthorizedException;
import com.datastax.driver.core.exceptions.UnavailableException;
import com.datastax.driver.core.exceptions.WriteTimeoutException;

/**
 * Simple {@link PersistenceExceptionTranslator} for Cassandra. Convert the given runtime exception to an appropriate
 * exception from the {@code org.springframework.dao} hierarchy. Return {@literal null} if no translation is
 * appropriate: any other exception may have resulted from user code, and should not be translated.
 * 
 * @author Alex Shvid
 * @author Matthew T. Adams
 */

public class CassandraExceptionTranslator implements PersistenceExceptionTranslator {

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.springframework.dao.support.PersistenceExceptionTranslator#
	 * translateExceptionIfPossible(java.lang.RuntimeException)
	 */
	public DataAccessException translateExceptionIfPossible(RuntimeException x) {

		if (x instanceof DataAccessException) {
			return (DataAccessException) x;
		}

		if (!(x instanceof DriverException)) {
			return null;
		}

		// Remember: subclasses must come before superclasses, otherwise the
		// superclass would match before the subclass!

		if (x instanceof AuthenticationException) {
			return new CassandraAuthenticationException(((AuthenticationException) x).getHost(), x.getMessage(), x);
		}
		if (x instanceof DriverInternalError) {
			return new CassandraInternalException(x.getMessage(), x);
		}
		if (x instanceof InvalidTypeException) {
			return new CassandraTypeMismatchException(x.getMessage(), x);
		}
		if (x instanceof NoHostAvailableException) {
			return new CassandraConnectionFailureException(((NoHostAvailableException) x).getErrors(), x.getMessage(), x);
		}
		if (x instanceof ReadTimeoutException) {
			return new CassandraReadTimeoutException(((ReadTimeoutException) x).wasDataRetrieved(), x.getMessage(), x);
		}
		if (x instanceof WriteTimeoutException) {
			WriteType writeType = ((WriteTimeoutException) x).getWriteType();
			return new CassandraWriteTimeoutException(writeType == null ? null : writeType.name(), x.getMessage(), x);
		}
		if (x instanceof TruncateException) {
			return new CassandraTruncateException(x.getMessage(), x);
		}
		if (x instanceof UnavailableException) {
			UnavailableException ux = (UnavailableException) x;
			return new CassandraInsufficientReplicasAvailableException(ux.getRequiredReplicas(), ux.getAliveReplicas(),
					x.getMessage(), x);
		}
		if (x instanceof AlreadyExistsException) {
			AlreadyExistsException aex = (AlreadyExistsException) x;

			return aex.wasTableCreation() ? new CassandraTableExistsException(aex.getTable(), x.getMessage(), x)
					: new CassandraKeyspaceExistsException(aex.getKeyspace(), x.getMessage(), x);
		}
		if (x instanceof InvalidConfigurationInQueryException) {
			return new CassandraInvalidConfigurationInQueryException(x.getMessage(), x);
		}
		if (x instanceof InvalidQueryException) {
			return new CassandraInvalidQueryException(x.getMessage(), x);
		}
		if (x instanceof SyntaxError) {
			return new CassandraQuerySyntaxException(x.getMessage(), x);
		}
		if (x instanceof UnauthorizedException) {
			return new CassandraUnauthorizedException(x.getMessage(), x);
		}
		if (x instanceof TraceRetrievalException) {
			return new CassandraTraceRetrievalException(x.getMessage(), x);
		}

		// unknown or unhandled exception
		return new CassandraUncategorizedException(x.getMessage(), x);
	}
}
