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
package org.springdata.cassandra.base.test.integration.core.template;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.base.core.CassandraOperations;
import org.springdata.cassandra.base.core.CassandraTemplate;
import org.springdata.cassandra.base.core.HostMapper;
import org.springdata.cassandra.base.core.PreparedStatementBinder;
import org.springdata.cassandra.base.core.PreparedStatementCallback;
import org.springdata.cassandra.base.core.PreparedStatementCreator;
import org.springdata.cassandra.base.core.ResultSetCallback;
import org.springdata.cassandra.base.core.RingMember;
import org.springdata.cassandra.base.core.RowCallbackHandler;
import org.springdata.cassandra.base.core.RowMapper;
import org.springdata.cassandra.base.core.SessionCallback;
import org.springdata.cassandra.base.test.integration.AbstractEmbeddedCassandraIntegrationTest;
import org.springframework.dao.DataIntegrityViolationException;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.collect.Lists;

/**
 * Unit Tests for CassandraTemplate
 * 
 * @author David Webb
 * @author Alex Shvid
 * 
 */
public class CassandraOperationsTest extends AbstractEmbeddedCassandraIntegrationTest {

	private CassandraOperations cassandraTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandraOperationsTest.class);

	/*
	 * Objects used for test data
	 */
	final String ISBN_NINES = "999999999";
	final String TITLE_NINES = "Book of Nines";
	final Object[] o1 = new Object[] { "1234", "Moby Dick", "Herman Manville", new Integer(456) };
	final Object[] o2 = new Object[] { "2345", "War and Peace", "Russian Dude", new Integer(456) };
	final Object[] o3 = new Object[] { "3456", "Jane Ayre", "Charlotte", new Integer(456) };

	/**
	 * This loads any test specific Cassandra objects
	 */
	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet(
			"cassandraOperationsTest-cql-dataload.cql", this.keyspace), CASSANDRA_CONFIG, CASSANDRA_HOST,
			CASSANDRA_NATIVE_PORT);

	@Before
	public void setupTemplate() {
		cassandraTemplate = new CassandraTemplate(session);
	}

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

				List<MyHost> list = new LinkedList<CassandraOperationsTest.MyHost>();

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
	public void ingestionTestObjectArray() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		Object[][] values = new Object[3][];
		values[0] = o1;
		values[1] = o2;
		values[2] = o3;

		cassandraTemplate.ingest(cql, values, null);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook("1234");
		Book b2 = getBook("2345");
		Book b3 = getBook("3456");

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));
	}

	@Test
	public void ingestionTestRowIterator() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		final Object[][] v = new Object[3][];
		v[0] = o1;
		v[1] = o2;
		v[2] = o3;

		cassandraTemplate.ingest(cql, Arrays.asList(v), null);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook("1234");
		Book b2 = getBook("2345");
		Book b3 = getBook("3456");

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));

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
	public void executeAsynchronouslyTestCqlString() {

		final String isbn = UUID.randomUUID().toString();
		final String title = "Spring Data Cassandra Cookbook";
		final String author = "David Webb";
		final Integer pages = 1;

		cassandraTemplate.execute(true, "insert into book (isbn, title, author, pages) values ('" + isbn + "', '" + title
				+ "', '" + author + "', " + pages + ")", null);

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

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
	public void queryAsynchronouslyTestCqlStringResultSetCallback() {

		final String isbn = "999999999";

		ResultSet frs = cassandraTemplate.selectAsync("select * from book where isbn='" + isbn + "'", null)
				.getUninterruptibly();

		Row r = frs.one();
		assertNotNull(r);

		Book b1 = rowToBook(r);

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
		ingestionTestObjectArray();

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
		ingestionTestObjectArray();

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
		ingestionTestObjectArray();

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
		ingestionTestObjectArray();

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
		ingestionTestObjectArray();

		List<String> titles = cassandraTemplate.selectFirstColumnAsList(
				"select title from book where isbn in ('1234','2345','3456')", String.class, null);

		log.debug(titles.toString());

		assertNotNull(titles);
		assertEquals(titles.size(), 3);

	}

	@Test
	public void processListTestResultSetType() {

		// Insert our 3 test books.
		ingestionTestObjectArray();

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
		ingestionTestObjectArray();

		List<Map<String, Object>> results = cassandraTemplate.selectAsListOfMap(
				"select * from book where isbn in ('1234','2345','3456')", null);

		log.debug(results.toString());

		assertEquals(results.size(), 3);

	}

	@Test
	public void processListOfMapTestResultSet() {

		// Insert our 3 test books.
		ingestionTestObjectArray();

		ResultSet rs = cassandraTemplate.selectAsync("select * from book where isbn in ('1234','2345','3456')", null)
				.getUninterruptibly();

		assertNotNull(rs);

		List<Map<String, Object>> results = cassandraTemplate.processAsListOfMap(rs);

		log.debug(results.toString());

		assertEquals(results.size(), 3);

	}

	@Test
	public void executeTestCqlStringPreparedStatementCallback() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		PreparedStatement ps = cassandraTemplate.prepareStatement(cql, null);

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
		}, null);

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

		PreparedStatement ps = cassandraTemplate.prepareStatement(cql, null);

		Book b1 = cassandraTemplate.select(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}, new ResultSetCallback<Book>() {

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
	public void queryTestCqlStringPreparedStatementBinderRowCallbackHandler() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cassandraTemplate.prepareStatement(cql, null);

		cassandraTemplate.select(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {

				Book b = rowToBook(row);

				Book b2 = getBook(isbn);

				assertBook(b, b2);

			}
		}, null);

	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderRowMapper() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		PreparedStatement ps = cassandraTemplate.prepareStatement(cql, null);

		Iterator<Book> ibooks = cassandraTemplate.select(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				return rowToBook(row);
			}
		}, null);

		List<Book> books = Lists.newArrayList(ibooks);

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	@Test
	public void queryTestPreparedStatementCreatorResultSetCallback() {

		ingestionTestObjectArray();

		final String cql = "select * from book";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		}, null);

		List<Book> books = cassandraTemplate.select(ps, new ResultSetCallback<List<Book>>() {

			@Override
			public List<Book> doWithResultSet(ResultSet rs) {

				List<Book> books = new LinkedList<Book>();

				for (Row row : rs.all()) {
					books.add(rowToBook(row));
				}

				return books;
			}
		}, null);

		log.debug("Size of all Books -> " + books.size());

		assertTrue(books.size() > 0);
	}

	@Test
	public void queryTestPreparedStatementCreatorRowCallbackHandler() {

		ingestionTestObjectArray();

		final String cql = "select * from book";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		}, null);

		cassandraTemplate.select(ps, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {

				Book b = rowToBook(row);

				log.debug("Title -> " + b.getTitle());

			}
		}, null);

	}

	@Test
	public void queryTestPreparedStatementCreatorRowMapper() {

		ingestionTestObjectArray();

		final String cql = "select * from book";

		PreparedStatement ps = cassandraTemplate.prepareStatement(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) {
				return session.prepare(cql);
			}
		}, null);

		Iterator<Book> ibooks = cassandraTemplate.select(ps, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				return rowToBook(row);
			}
		}, null);

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
		}, null);

		List<Book> books = cassandraTemplate.select(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}, new ResultSetCallback<List<Book>>() {

			@Override
			public List<Book> doWithResultSet(ResultSet rs) {
				List<Book> books = new LinkedList<Book>();

				for (Row row : rs.all()) {
					books.add(rowToBook(row));
				}

				return books;
			}
		}, null);

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
		}, null);

		cassandraTemplate.select(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) {
				Book b = rowToBook(row);
				Book b2 = getBook(isbn);
				assertBook(b, b2);
			}
		}, null);

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
		}, null);

		Iterator<Book> ibooks = cassandraTemplate.select(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) {
				return rowToBook(row);
			}
		}, null);

		List<Book> books = Lists.newArrayList(ibooks);

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	/**
	 * Assert that a Book matches the arguments expected
	 * 
	 * @param b
	 * @param orderedElements
	 */
	private void assertBook(Book b, Object... orderedElements) {

		assertEquals(b.getIsbn(), orderedElements[0]);
		assertEquals(b.getTitle(), orderedElements[1]);
		assertEquals(b.getAuthor(), orderedElements[2]);
		assertEquals(b.getPages(), orderedElements[3]);

	}

	private Book rowToBook(Row row) {
		Book b = new Book();
		b.setIsbn(row.getString("isbn"));
		b.setTitle(row.getString("title"));
		b.setAuthor(row.getString("author"));
		b.setPages(row.getInt("pages"));
		return b;
	}

	/**
	 * Convert Object[] to a Book
	 * 
	 * @param bookElements
	 * @return
	 */
	private Book objectToBook(Object... bookElements) {
		Book b = new Book();
		b.setIsbn((String) bookElements[0]);
		b.setTitle((String) bookElements[1]);
		b.setAuthor((String) bookElements[2]);
		b.setPages((Integer) bookElements[3]);
		return b;
	}

	/**
	 * Convert List<Object> to a Book
	 * 
	 * @param bookElements
	 * @return
	 */
	private Book listToBook(List<Object> bookElements) {
		Book b = new Book();
		b.setIsbn((String) bookElements.get(0));
		b.setTitle((String) bookElements.get(1));
		b.setAuthor((String) bookElements.get(2));
		b.setPages((Integer) bookElements.get(3));
		return b;

	}

	/**
	 * Assert that 2 Book objects are the same
	 * 
	 * @param b
	 * @param orderedElements
	 */
	private void assertBook(Book b1, Book b2) {

		assertEquals(b1.getIsbn(), b2.getIsbn());
		assertEquals(b1.getTitle(), b2.getTitle());
		assertEquals(b1.getAuthor(), b2.getAuthor());
		assertEquals(b1.getPages(), b2.getPages());

	}

	/**
	 * Get a Book from Cassandra for assertions.
	 * 
	 * @param isbn
	 * @return
	 */
	public Book getBook(final String isbn) {

		PreparedStatement ps = cassandraTemplate.prepareStatement("select * from book where isbn = ?", null);

		Book b = this.cassandraTemplate.select(ps, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) {
				return ps.bind(isbn);
			}
		}, new ResultSetCallback<Book>() {

			@Override
			public Book doWithResultSet(ResultSet rs) {
				Book b = new Book();
				Row r = rs.one();
				b.setIsbn(r.getString("isbn"));
				b.setTitle(r.getString("title"));
				b.setAuthor(r.getString("author"));
				b.setPages(r.getInt("pages"));
				return b;
			}
		}, null);

		return b;

	}

	/**
	 * For testing a HostMapper Implementation
	 */
	public class MyHost {
		public String someName;
	}
}
