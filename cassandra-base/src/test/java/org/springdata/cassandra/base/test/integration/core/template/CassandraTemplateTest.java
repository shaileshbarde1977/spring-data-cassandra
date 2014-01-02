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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.junit.Test;
import org.springdata.cassandra.base.core.HostMapper;
import org.springdata.cassandra.base.core.ResultSetCallback;
import org.springdata.cassandra.base.core.RingMember;
import org.springdata.cassandra.base.core.RowCallbackHandler;
import org.springdata.cassandra.base.core.RowMapper;
import org.springdata.cassandra.base.core.SessionCallback;
import org.springframework.dao.DataIntegrityViolationException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;

/**
 * @author David Webb
 * @author Alex Shvid
 */
public class CassandraTemplateTest extends AbstractCassandraOperations {

	@Test
	public void ringTest() {

		Collection<RingMember> ring = cassandraTemplate.describeRing();

		/*
		 * There must be 1 node in the cluster if the embedded server is
		 * running.
		 */
		assertNotNull(ring);

		for (RingMember h : ring) {
			log.info("ringTest Host -> " + h.address);
		}
	}

	@Test
	public void hostMapperTest() {

		List<MyHost> ring = (List<MyHost>) cassandraTemplate.describeRing(new HostMapper<MyHost>() {

			@Override
			public Collection<MyHost> mapHosts(Set<Host> host) {

				List<MyHost> list = new LinkedList<MyHost>();

				for (Host h : host) {
					MyHost mh = new MyHost();
					mh.someName = h.getAddress().getCanonicalHostName();
					list.add(mh);
				}

				return list;
			}

		});

		assertNotNull(ring);
		assertTrue(ring.size() > 0);

		for (MyHost h : ring) {
			log.info("hostMapperTest Host -> " + h.someName);
		}

	}

	@Test
	public void executeTestSessionCallback() {

		final String isbn = UUID.randomUUID().toString();
		final String title = "Spring Data Cassandra Cookbook";
		final String author = "David Webb";
		final Integer pages = 1;

		cassandraTemplate.execute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) {

				String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

				PreparedStatement ps = s.prepare(cql);
				BoundStatement bs = ps.bind(isbn, title, author, pages);

				s.execute(bs);

				return null;

			}
		});

		Book b = getBook(isbn);

		assertBook(b, isbn, title, author, pages);

	}

	@Test
	public void executeTestCqlString() {

		final String isbn = UUID.randomUUID().toString();
		final String title = "Spring Data Cassandra Cookbook";
		final String author = "David Webb";
		final Integer pages = 1;

		cassandraTemplate.execute(false, "insert into book (isbn, title, author, pages) values ('" + isbn + "', '" + title
				+ "', '" + author + "', " + pages + ")", null);

		Book b = getBook(isbn);

		assertBook(b, isbn, title, author, pages);

	}

	@Test
	public void queryTestCqlStringResultSetCallback() {

		final String isbn = "999999999";

		Book b1 = cassandraTemplate.select("select * from book where isbn='" + isbn + "'", new ResultSetCallback<Book>() {

			@Override
			public Book doWithResultSet(ResultSet rs) {
				Row r = rs.one();
				assertNotNull(r);

				Book b = rowToBook(r);

				return b;
			}
		}, null);

		Book b2 = getBook(isbn);

		assertBook(b1, b2);

	}

	@Test
	public void queryTestCqlStringRowCallbackHandler() {

		final String isbn = "999999999";

		final Book b1 = getBook(isbn);

		cassandraTemplate.select("select * from book where isbn='" + isbn + "'", new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {

				assertNotNull(row);

				Book b = rowToBook(row);

				assertBook(b1, b);

			}
		}, null);

	}

	@Test
	public void processTestResultSetRowCallbackHandler() {

		final String isbn = "999999999";

		final Book b1 = getBook(isbn);

		ResultSet rs = cassandraTemplate.selectAsync("select * from book where isbn='" + isbn + "'", null)
				.getUninterruptibly();

		assertNotNull(rs);

		cassandraTemplate.process(rs, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {

				assertNotNull(row);

				Book b = rowToBook(row);

				assertBook(b1, b);

			}

		});

	}

	@Test
	public void queryTestCqlStringRowMapper() {

		// Insert our 3 test books.
		insertBooks();

		Iterator<Book> ibooks = cassandraTemplate.select("select * from book where isbn in ('1234','2345','3456')",
				new RowMapper<Book>() {

					@Override
					public Book mapRow(Row row, int rowNum) {
						Book b = rowToBook(row);
						return b;
					}
				}, null);

		List<Book> books = Lists.newArrayList(ibooks);

		log.debug("Size of Book List -> " + books.size());
		assertEquals(books.size(), 3);
		assertBook(books.get(0), getBook(books.get(0).getIsbn()));
		assertBook(books.get(1), getBook(books.get(1).getIsbn()));
		assertBook(books.get(2), getBook(books.get(2).getIsbn()));
	}

	@Test
	public void processTestResultSetRowMapper() {

		// Insert our 3 test books.
		insertBooks();

		ResultSet rs = cassandraTemplate.selectAsync("select * from book where isbn in ('1234','2345','3456')", null)
				.getUninterruptibly();

		assertNotNull(rs);

		Iterator<Book> ibooks = cassandraTemplate.process(rs, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				Book b = rowToBook(row);
				return b;
			}
		});

		List<Book> books = Lists.newArrayList(ibooks);

		log.debug("Size of Book List -> " + books.size());
		assertEquals(books.size(), 3);
		assertBook(books.get(0), getBook(books.get(0).getIsbn()));
		assertBook(books.get(1), getBook(books.get(1).getIsbn()));
		assertBook(books.get(2), getBook(books.get(2).getIsbn()));

	}

	@Test
	public void queryForObjectTestCqlStringRowMapper() {

		Book book = cassandraTemplate.selectOne("select * from book where isbn in ('" + ISBN_NINES + "')",
				new RowMapper<Book>() {
					@Override
					public Book mapRow(Row row, int rowNum) {
						Book b = rowToBook(row);
						return b;
					}
				}, null);

		assertNotNull(book);
		assertBook(book, getBook(ISBN_NINES));
	}

	/**
	 * Test that CQL for QueryForObject must only return 1 row or an IllegalArgumentException is thrown.
	 */
	@Test(expected = DataIntegrityViolationException.class)
	public void queryForObjectTestCqlStringRowMapperNotOneRowReturned() {

		// Insert our 3 test books.
		insertBooks();

		Book book = cassandraTemplate.selectOne("select * from book where isbn in ('1234','2345','3456')",
				new RowMapper<Book>() {
					@Override
					public Book mapRow(Row row, int rowNum) {
						Book b = rowToBook(row);
						return b;
					}
				}, null);
	}

	@Test
	public void processOneTestResultSetRowMapper() {

		// Insert our 3 test books.
		insertBooks();

		ResultSet rs = cassandraTemplate.selectAsync("select * from book where isbn in ('" + ISBN_NINES + "')", null)
				.getUninterruptibly();

		assertNotNull(rs);

		Book book = cassandraTemplate.processOne(rs, new RowMapper<Book>() {
			@Override
			public Book mapRow(Row row, int rowNum) {
				Book b = rowToBook(row);
				return b;
			}
		});

		assertNotNull(book);
		assertBook(book, getBook(ISBN_NINES));
	}

	@Test
	public void quertForObjectTestCqlStringRequiredType() {

		String title = cassandraTemplate.selectOne("select title from book where isbn in ('" + ISBN_NINES + "')",
				String.class, null);

		assertEquals(title, TITLE_NINES);

	}

	@Test(expected = ClassCastException.class)
	public void queryForObjectTestCqlStringRequiredTypeInvalid() {

		Float title = cassandraTemplate.selectOne("select title from book where isbn in ('" + ISBN_NINES + "')",
				Float.class, null);

	}

	@Test
	public void processOneTestResultSetType() {

		ResultSet rs = cassandraTemplate.selectAsync("select title from book where isbn in ('" + ISBN_NINES + "')", null)
				.getUninterruptibly();

		assertNotNull(rs);

		String title = cassandraTemplate.processOne(rs, String.class);

		assertNotNull(title);
		assertEquals(title, TITLE_NINES);
	}

	@Test
	public void queryForMapTestCqlString() {

		Map<String, Object> rsMap = cassandraTemplate.selectOneAsMap("select * from book where isbn in ('" + ISBN_NINES
				+ "')", null);

		log.debug(rsMap.toString());

		Book b1 = objectToBook(rsMap.get("isbn"), rsMap.get("title"), rsMap.get("author"), rsMap.get("pages"));

		Book b2 = getBook(ISBN_NINES);

		assertBook(b1, b2);

	}

	@Test
	public void processMapTestResultSet() {

		ResultSet rs = cassandraTemplate.selectAsync("select * from book where isbn in ('" + ISBN_NINES + "')", null)
				.getUninterruptibly();

		assertNotNull(rs);

		Map<String, Object> rsMap = cassandraTemplate.processOneAsMap(rs);

		log.debug("Size of Book List -> " + rsMap.size());

		Book b1 = objectToBook(rsMap.get("isbn"), rsMap.get("title"), rsMap.get("author"), rsMap.get("pages"));

		Book b2 = getBook(ISBN_NINES);

		assertBook(b1, b2);

	}

	@Test
	public void queryForListTestCqlStringType() {

		// Insert our 3 test books.
		insertBooks();

		List<String> titles = cassandraTemplate.selectFirstColumnAsList(
				"select title from book where isbn in ('1234','2345','3456')", String.class, null);

		log.debug(titles.toString());

		assertNotNull(titles);
		assertEquals(titles.size(), 3);

	}

	@Test
	public void processListTestResultSetType() {

		// Insert our 3 test books.
		insertBooks();

		ResultSet rs = cassandraTemplate.selectAsync("select * from book where isbn in ('1234','2345','3456')", null)
				.getUninterruptibly();

		assertNotNull(rs);

		List<String> titles = cassandraTemplate.processFirstColumnAsList(rs, String.class);

		log.debug(titles.toString());

		assertNotNull(titles);
		assertEquals(titles.size(), 3);
	}

	@Test
	public void queryForListOfMapCqlString() {

		// Insert our 3 test books.
		insertBooks();

		List<Map<String, Object>> results = cassandraTemplate.selectAsListOfMap(
				"select * from book where isbn in ('1234','2345','3456')", null);

		log.debug(results.toString());

		assertEquals(results.size(), 3);

	}

	@Test
	public void processListOfMapTestResultSet() {

		// Insert our 3 test books.
		insertBooks();

		ResultSet rs = cassandraTemplate.selectAsync("select * from book where isbn in ('1234','2345','3456')", null)
				.getUninterruptibly();

		assertNotNull(rs);

		List<Map<String, Object>> results = cassandraTemplate.processAsListOfMap(rs);

		log.debug(results.toString());

		assertEquals(results.size(), 3);

	}

	/**
	 * For testing a HostMapper Implementation
	 */
	public class MyHost {
		public String someName;
	}

}
