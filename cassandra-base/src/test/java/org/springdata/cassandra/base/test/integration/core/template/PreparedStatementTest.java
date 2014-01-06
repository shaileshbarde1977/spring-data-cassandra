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
package org.springdata.cassandra.base.test.integration.core.template;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.junit.Test;
import org.springdata.cassandra.base.core.PreparedStatementBinder;
import org.springdata.cassandra.base.core.PreparedStatementCallback;
import org.springdata.cassandra.base.core.PreparedStatementCreator;
import org.springdata.cassandra.base.core.PreparedStatementQueryCreator;
import org.springdata.cassandra.base.core.ResultSetCallback;
import org.springdata.cassandra.base.core.RowCallbackHandler;
import org.springdata.cassandra.base.core.RowMapper;
import org.springdata.cassandra.base.core.SimpleQueryCreator;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;

/**
 * @author David Webb
 * @author Alex Shvid
 */
public class PreparedStatementTest extends AbstractCassandraOperations {

	@Test
	public void executeTestCqlStringPreparedStatementCallback() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		PreparedStatement ps = cassandraTemplate.prepareStatement(cql);

		BoundStatement statement = cassandraTemplate.execute(ps, new PreparedStatementCallback<BoundStatement>() {

			@Override
			public BoundStatement doWithPreparedStatement(Session session, PreparedStatement ps) {
				BoundStatement bs = ps.bind();
				return bs;
			}
		});

		assertNotNull(statement);

	}

	@Test
	public void executeTestPreparedStatementCreatorPreparedStatementCallback() {

		final String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement statement = cassandraTemplate.execute(ps, new PreparedStatementCallback<BoundStatement>() {

			@Override
			public BoundStatement doWithPreparedStatement(Session session, PreparedStatement ps) {
				BoundStatement bs = ps.bind();
				return bs;
			}
		});

		assertNotNull(statement);

	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderResultSetCallback() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cassandraTemplate.prepareStatement(cql);

		BoundStatement bs = cassandraTemplate.bind(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		});

		Book b1 = cassandraTemplate.select(new SimpleQueryCreator(bs), new ResultSetCallback<Book>() {

			@Override
			public Book doWithResultSet(ResultSet rs) {
				Row r = rs.one();
				assertNotNull(r);

				Book b = rowToBook(r);

				return b;
			}
		});

		Book b2 = getBook(isbn);

		assertBook(b1, b2);
	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderRowCallbackHandler() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cassandraTemplate.prepareStatement(cql);

		cassandraTemplate.select(new PreparedStatementQueryCreator(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}), new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {

				Book b = rowToBook(row);

				Book b2 = getBook(isbn);

				assertBook(b, b2);

			}
		});

	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderRowMapper() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cassandraTemplate.prepareStatement(cql);

		Iterator<Book> ibooks = cassandraTemplate.select(new PreparedStatementQueryCreator(ps,
				new PreparedStatementBinder() {

					@Override
					public BoundStatement bindValues(PreparedStatement ps) {
						return ps.bind(isbn);
					}
				}), new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				return rowToBook(row);
			}
		});

		List<Book> books = Lists.newArrayList(ibooks);

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	@Test
	public void queryTestPreparedStatementCreatorResultSetCallback() {

		insertBooks();

		final String cql = "select * from book";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cassandraTemplate.bind(ps);

		List<Book> books = cassandraTemplate.select(new SimpleQueryCreator(bs), new ResultSetCallback<List<Book>>() {

			@Override
			public List<Book> doWithResultSet(ResultSet rs) {

				List<Book> books = new LinkedList<Book>();

				for (Row row : rs.all()) {
					books.add(rowToBook(row));
				}

				return books;
			}
		});

		log.debug("Size of all Books -> " + books.size());

		assertTrue(books.size() > 0);
	}

	@Test
	public void queryTestPreparedStatementCreatorRowCallbackHandler() {

		insertBooks();

		final String cql = "select * from book";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cassandraTemplate.bind(ps);

		cassandraTemplate.select(new SimpleQueryCreator(bs), new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {

				Book b = rowToBook(row);

				log.debug("Title -> " + b.getTitle());

			}
		});

	}

	@Test
	public void queryTestPreparedStatementCreatorRowMapper() {

		insertBooks();

		final String cql = "select * from book";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cassandraTemplate.bind(ps);

		Iterator<Book> ibooks = cassandraTemplate.select(new SimpleQueryCreator(bs), new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				return rowToBook(row);
			}
		});

		List<Book> books = Lists.newArrayList(ibooks);

		log.debug("Size of all Books -> " + books.size());

		assertTrue(books.size() > 0);
	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderResultSetCallback() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cassandraTemplate.bind(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		});

		List<Book> books = cassandraTemplate.select(new SimpleQueryCreator(bs), new ResultSetCallback<List<Book>>() {

			@Override
			public List<Book> doWithResultSet(ResultSet rs) {
				List<Book> books = new LinkedList<Book>();

				for (Row row : rs.all()) {
					books.add(rowToBook(row));
				}

				return books;
			}
		});

		Book b2 = getBook(isbn);

		log.debug("Book list Size -> " + books.size());

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderRowCallbackHandler() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cassandraTemplate.bind(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		});

		cassandraTemplate.select(new SimpleQueryCreator(bs), new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {
				Book b = rowToBook(row);
				Book b2 = getBook(isbn);
				assertBook(b, b2);
			}
		});

	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderRowMapper() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		});

		BoundStatement bs = cassandraTemplate.bind(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		});

		Iterator<Book> ibooks = cassandraTemplate.select(new SimpleQueryCreator(bs), new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				return rowToBook(row);
			}
		});

		List<Book> books = Lists.newArrayList(ibooks);

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

}
