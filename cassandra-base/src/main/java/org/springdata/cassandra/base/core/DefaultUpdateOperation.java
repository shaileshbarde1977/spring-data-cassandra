package org.springdata.cassandra.base.core;

import com.datastax.driver.core.Query;

public class DefaultUpdateOperation extends AbstractUpdateOperation {

	private Query query;

	protected DefaultUpdateOperation(CassandraTemplate cassandraTemplate, Query query) {
		super(cassandraTemplate);
		this.query = query;
	}

	@Override
	Query getQuery() {
		return query;
	}

}
