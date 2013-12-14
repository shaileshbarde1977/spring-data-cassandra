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
package net.webby.cassandrion.test.integration.core.template;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import net.webby.cassandrion.core.CassandrionOperations;
import net.webby.cassandrion.core.CassandrionTemplate;
import net.webby.cassandrion.core.HostMapper;
import net.webby.cassandrion.core.PreparedStatementBinder;
import net.webby.cassandrion.core.PreparedStatementCallback;
import net.webby.cassandrion.core.PreparedStatementCreator;
import net.webby.cassandrion.core.ResultSetExtractor;
import net.webby.cassandrion.core.ResultSetFutureExtractor;
import net.webby.cassandrion.core.RingMember;
import net.webby.cassandrion.core.RowCallbackHandler;
import net.webby.cassandrion.core.RowIterator;
import net.webby.cassandrion.core.RowMapper;
import net.webby.cassandrion.core.SessionCallback;
import net.webby.cassandrion.test.integration.AbstractEmbeddedCassandrionIntegrationTest;

import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.util.CollectionUtils;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.DriverException;

/**
 * Unit Tests for CassandraTemplate
 * 
 * @author David Webb
 * 
 */
public class CassandrionOperationsTest extends AbstractEmbeddedCassandrionIntegrationTest {

	private CassandrionOperations cassandrionTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandrionOperationsTest.class);

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
			"cassandrionOperationsTest-cql-dataload.cql", this.keyspace), CASSANDRA_CONFIG, CASSANDRA_HOST,
			CASSANDRA_NATIVE_PORT);

	@Before
	public void setupTemplate() {
		cassandrionTemplate = new CassandrionTemplate(session);
	}

	@Test
	public void ringTest() {

		List<RingMember> ring = cassandrionTemplate.describeRing();

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

		List<MyHost> ring = (List<MyHost>) cassandrionTemplate.describeRing(new HostMapper<MyHost>() {

			@Override
			public Collection<MyHost> mapHosts(Set<Host> host) throws DriverException {

				List<MyHost> list = new LinkedList<CassandrionOperationsTest.MyHost>();

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
	public void ingestionTestListOfList() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		List<List<?>> values = new LinkedList<List<?>>();

		values.add(new LinkedList<Object>(CollectionUtils.arrayToList(o1)));
		values.add(new LinkedList<Object>(CollectionUtils.arrayToList(o2)));
		values.add(new LinkedList<Object>(CollectionUtils.arrayToList(o3)));

		cassandrionTemplate.ingest(cql, values);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook((String) o1[0]);
		Book b2 = getBook((String) o2[0]);
		Book b3 = getBook((String) o3[0]);

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));
	}

	@Test
	public void ingestionTestObjectArray() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		Object[][] values = new Object[3][];
		values[0] = o1;
		values[1] = o2;
		values[2] = o3;

		cassandrionTemplate.ingest(cql, values);

		// Assert that the rows were inserted into Cassandra
		Book b1 = getBook("1234");
		Book b2 = getBook("2345");
		Book b3 = getBook("3456");

		assertBook(b1, objectToBook(o1));
		assertBook(b2, objectToBook(o2));
		assertBook(b3, objectToBook(o3));
	}

	/**
	 * This is an implementation of RowIterator for the purposes of testing passing your own Impl to CassandraTemplate
	 * 
	 * @author David Webb
	 */
	final class MyRowIterator implements RowIterator {

		private Object[][] values;

		public MyRowIterator(Object[][] values) {
			this.values = values;
		}

		int index = 0;

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.RowIterator#next()
		 */
		@Override
		public Object[] next() {
			return values[index++];
		}

		/* (non-Javadoc)
		 * @see org.springframework.cassandra.core.RowIterator#hasNext()
		 */
		@Override
		public boolean hasNext() {
			return index < values.length;
		}

	}

	@Test
	public void ingestionTestRowIterator() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		final Object[][] v = new Object[3][];
		v[0] = o1;
		v[1] = o2;
		v[2] = o3;
		RowIterator ri = new MyRowIterator(v);

		cassandrionTemplate.ingest(cql, ri);

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

		cassandrionTemplate.execute(new SessionCallback<Object>() {

			@Override
			public Object doInSession(Session s) throws DataAccessException {

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

		cassandrionTemplate.execute("insert into book (isbn, title, author, pages) values ('" + isbn + "', '" + title
				+ "', '" + author + "', " + pages + ")");

		Book b = getBook(isbn);

		assertBook(b, isbn, title, author, pages);

	}

	@Test
	public void executeAsynchronouslyTestCqlString() {

		final String isbn = UUID.randomUUID().toString();
		final String title = "Spring Data Cassandra Cookbook";
		final String author = "David Webb";
		final Integer pages = 1;

		cassandrionTemplate.executeAsynchronously("insert into book (isbn, title, author, pages) values ('" + isbn + "', '"
				+ title + "', '" + author + "', " + pages + ")");

		try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		Book b = getBook(isbn);

		assertBook(b, isbn, title, author, pages);

	}

	@Test
	public void queryTestCqlStringResultSetExtractor() {

		final String isbn = "999999999";

		Book b1 = cassandrionTemplate.query("select * from book where isbn='" + isbn + "'", new ResultSetExtractor<Book>() {

			@Override
			public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
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
	public void queryAsynchronouslyTestCqlStringResultSetExtractor() {

		final String isbn = "999999999";

		Book b1 = cassandrionTemplate.queryAsynchronously("select * from book where isbn='" + isbn + "'",
				new ResultSetFutureExtractor<Book>() {

					@Override
					public Book extractData(ResultSetFuture rs) throws DriverException, DataAccessException {

						ResultSet frs = rs.getUninterruptibly();
						Row r = frs.one();
						assertNotNull(r);

						Book b = rowToBook(r);

						return b;
					}
				});

		Book b2 = getBook(isbn);

		assertBook(b1, b2);

	}

	@Test
	public void queryTestCqlStringRowCallbackHandler() {

		final String isbn = "999999999";

		final Book b1 = getBook(isbn);

		cassandrionTemplate.query("select * from book where isbn='" + isbn + "'", new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {

				assertNotNull(row);

				Book b = rowToBook(row);

				assertBook(b1, b);

			}
		});

	}

	@Test
	public void processTestResultSetRowCallbackHandler() {

		final String isbn = "999999999";

		final Book b1 = getBook(isbn);

		ResultSet rs = cassandrionTemplate.queryAsynchronously("select * from book where isbn='" + isbn + "'",
				new ResultSetFutureExtractor<ResultSet>() {

					@Override
					public ResultSet extractData(ResultSetFuture rs) throws DriverException, DataAccessException {

						ResultSet frs = rs.getUninterruptibly();
						return frs;
					}
				});

		assertNotNull(rs);

		cassandrionTemplate.process(rs, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {

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

		List<Book> books = cassandrionTemplate.query("select * from book where isbn in ('1234','2345','3456')",
				new RowMapper<Book>() {

					@Override
					public Book mapRow(Row row, int rowNum) throws DriverException {
						Book b = rowToBook(row);
						return b;
					}
				});

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

		ResultSet rs = cassandrionTemplate.queryAsynchronously("select * from book where isbn in ('1234','2345','3456')",
				new ResultSetFutureExtractor<ResultSet>() {

					@Override
					public ResultSet extractData(ResultSetFuture rs) throws DriverException, DataAccessException {

						ResultSet frs = rs.getUninterruptibly();
						return frs;
					}
				});

		assertNotNull(rs);

		List<Book> books = cassandrionTemplate.process(rs, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				Book b = rowToBook(row);
				return b;
			}
		});

		log.debug("Size of Book List -> " + books.size());
		assertEquals(books.size(), 3);
		assertBook(books.get(0), getBook(books.get(0).getIsbn()));
		assertBook(books.get(1), getBook(books.get(1).getIsbn()));
		assertBook(books.get(2), getBook(books.get(2).getIsbn()));

	}

	@Test
	public void queryForObjectTestCqlStringRowMapper() {

		Book book = cassandrionTemplate.queryForObject("select * from book where isbn in ('" + ISBN_NINES + "')",
				new RowMapper<Book>() {
					@Override
					public Book mapRow(Row row, int rowNum) throws DriverException {
						Book b = rowToBook(row);
						return b;
					}
				});

		assertNotNull(book);
		assertBook(book, getBook(ISBN_NINES));
	}

	/**
	 * Test that CQL for QueryForObject must only return 1 row or an IllegalArgumentException is thrown.
	 */
	@Test(expected = IllegalArgumentException.class)
	public void queryForObjectTestCqlStringRowMapperNotOneRowReturned() {

		// Insert our 3 test books.
		ingestionTestObjectArray();

		Book book = cassandrionTemplate.queryForObject("select * from book where isbn in ('1234','2345','3456')",
				new RowMapper<Book>() {
					@Override
					public Book mapRow(Row row, int rowNum) throws DriverException {
						Book b = rowToBook(row);
						return b;
					}
				});
	}

	@Test
	public void processOneTestResultSetRowMapper() {

		// Insert our 3 test books.
		ingestionTestObjectArray();

		ResultSet rs = cassandrionTemplate.queryAsynchronously("select * from book where isbn in ('" + ISBN_NINES + "')",
				new ResultSetFutureExtractor<ResultSet>() {

					@Override
					public ResultSet extractData(ResultSetFuture rs) throws DriverException, DataAccessException {

						ResultSet frs = rs.getUninterruptibly();
						return frs;
					}
				});

		assertNotNull(rs);

		Book book = cassandrionTemplate.processOne(rs, new RowMapper<Book>() {
			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				Book b = rowToBook(row);
				return b;
			}
		});

		assertNotNull(book);
		assertBook(book, getBook(ISBN_NINES));
	}

	@Test
	public void quertForObjectTestCqlStringRequiredType() {

		String title = cassandrionTemplate.queryForObject("select title from book where isbn in ('" + ISBN_NINES + "')",
				String.class);

		assertEquals(title, TITLE_NINES);

	}

	@Test(expected = ClassCastException.class)
	public void queryForObjectTestCqlStringRequiredTypeInvalid() {

		Float title = cassandrionTemplate.queryForObject("select title from book where isbn in ('" + ISBN_NINES + "')",
				Float.class);

	}

	@Test
	public void processOneTestResultSetType() {

		ResultSet rs = cassandrionTemplate.queryAsynchronously("select title from book where isbn in ('" + ISBN_NINES
				+ "')", new ResultSetFutureExtractor<ResultSet>() {

			@Override
			public ResultSet extractData(ResultSetFuture rs) throws DriverException, DataAccessException {

				ResultSet frs = rs.getUninterruptibly();
				return frs;
			}
		});

		assertNotNull(rs);

		String title = cassandrionTemplate.processOne(rs, String.class);

		assertNotNull(title);
		assertEquals(title, TITLE_NINES);
	}

	@Test
	public void queryForMapTestCqlString() {

		Map<String, Object> rsMap = cassandrionTemplate.queryForMap("select * from book where isbn in ('" + ISBN_NINES
				+ "')");

		log.debug(rsMap.toString());

		Book b1 = objectToBook(rsMap.get("isbn"), rsMap.get("title"), rsMap.get("author"), rsMap.get("pages"));

		Book b2 = getBook(ISBN_NINES);

		assertBook(b1, b2);

	}

	@Test
	public void processMapTestResultSet() {

		ResultSet rs = cassandrionTemplate.queryAsynchronously("select * from book where isbn in ('" + ISBN_NINES + "')",
				new ResultSetFutureExtractor<ResultSet>() {

					@Override
					public ResultSet extractData(ResultSetFuture rs) throws DriverException, DataAccessException {

						ResultSet frs = rs.getUninterruptibly();
						return frs;
					}
				});

		assertNotNull(rs);

		Map<String, Object> rsMap = cassandrionTemplate.processMap(rs);

		log.debug("Size of Book List -> " + rsMap.size());

		Book b1 = objectToBook(rsMap.get("isbn"), rsMap.get("title"), rsMap.get("author"), rsMap.get("pages"));

		Book b2 = getBook(ISBN_NINES);

		assertBook(b1, b2);

	}

	@Test
	public void queryForListTestCqlStringType() {

		// Insert our 3 test books.
		ingestionTestObjectArray();

		List<String> titles = cassandrionTemplate.queryForList(
				"select title from book where isbn in ('1234','2345','3456')", String.class);

		log.debug(titles.toString());

		assertNotNull(titles);
		assertEquals(titles.size(), 3);

	}

	@Test
	public void processListTestResultSetType() {

		// Insert our 3 test books.
		ingestionTestObjectArray();

		ResultSet rs = cassandrionTemplate.queryAsynchronously("select * from book where isbn in ('1234','2345','3456')",
				new ResultSetFutureExtractor<ResultSet>() {

					@Override
					public ResultSet extractData(ResultSetFuture rs) throws DriverException, DataAccessException {

						ResultSet frs = rs.getUninterruptibly();
						return frs;
					}
				});

		assertNotNull(rs);

		List<String> titles = cassandrionTemplate.processList(rs, String.class);

		log.debug(titles.toString());

		assertNotNull(titles);
		assertEquals(titles.size(), 3);
	}

	@Test
	public void queryForListOfMapCqlString() {

		// Insert our 3 test books.
		ingestionTestObjectArray();

		List<Map<String, Object>> results = cassandrionTemplate
				.queryForListOfMap("select * from book where isbn in ('1234','2345','3456')");

		log.debug(results.toString());

		assertEquals(results.size(), 3);

	}

	@Test
	public void processListOfMapTestResultSet() {

		// Insert our 3 test books.
		ingestionTestObjectArray();

		ResultSet rs = cassandrionTemplate.queryAsynchronously("select * from book where isbn in ('1234','2345','3456')",
				new ResultSetFutureExtractor<ResultSet>() {

					@Override
					public ResultSet extractData(ResultSetFuture rs) throws DriverException, DataAccessException {

						ResultSet frs = rs.getUninterruptibly();
						return frs;
					}
				});

		assertNotNull(rs);

		List<Map<String, Object>> results = cassandrionTemplate.processListOfMap(rs);

		log.debug(results.toString());

		assertEquals(results.size(), 3);

	}

	@Test
	public void executeTestCqlStringPreparedStatementCallback() {

		String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		BoundStatement statement = cassandrionTemplate.execute(cql, new PreparedStatementCallback<BoundStatement>() {

			@Override
			public BoundStatement doInPreparedStatement(PreparedStatement ps) throws DriverException, DataAccessException {
				BoundStatement bs = ps.bind();
				return bs;
			}
		});

		assertNotNull(statement);

	}

	@Test
	public void executeTestPreparedStatementCreatorPreparedStatementCallback() {

		final String cql = "insert into book (isbn, title, author, pages) values (?, ?, ?, ?)";

		BoundStatement statement = cassandrionTemplate.execute(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new PreparedStatementCallback<BoundStatement>() {

			@Override
			public BoundStatement doInPreparedStatement(PreparedStatement ps) throws DriverException, DataAccessException {
				BoundStatement bs = ps.bind();
				return bs;
			}
		});

		assertNotNull(statement);

	}

	@Test
	public void queryTestCqlStringPreparedStatementBinderResultSetExtractor() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		Book b1 = cassandrionTemplate.query(cql, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new ResultSetExtractor<Book>() {

			@Override
			public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
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

		cassandrionTemplate.query(cql, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {

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

		List<Book> books = cassandrionTemplate.query(cql, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				return rowToBook(row);
			}
		});

		Book b2 = getBook(isbn);

		assertEquals(books.size(), 1);
		assertBook(books.get(0), b2);
	}

	@Test
	public void queryTestPreparedStatementCreatorResultSetExtractor() {

		ingestionTestObjectArray();

		final String cql = "select * from book";

		List<Book> books = cassandrionTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new ResultSetExtractor<List<Book>>() {

			@Override
			public List<Book> extractData(ResultSet rs) throws DriverException, DataAccessException {

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

		ingestionTestObjectArray();

		final String cql = "select * from book";

		cassandrionTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {

				Book b = rowToBook(row);

				log.debug("Title -> " + b.getTitle());

			}
		});

	}

	@Test
	public void queryTestPreparedStatementCreatorRowMapper() {

		ingestionTestObjectArray();

		final String cql = "select * from book";

		List<Book> books = cassandrionTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				return rowToBook(row);
			}
		});

		log.debug("Size of all Books -> " + books.size());

		assertTrue(books.size() > 0);
	}

	@Test
	public void queryTestPreparedStatementCreatorPreparedStatementBinderResultSetExtractor() {

		final String cql = "select * from book where isbn = ?";
		final String isbn = "999999999";

		List<Book> books = cassandrionTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new ResultSetExtractor<List<Book>>() {

			@Override
			public List<Book> extractData(ResultSet rs) throws DriverException, DataAccessException {
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

		cassandrionTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new RowCallbackHandler() {

			@Override
			public void processRow(Row row) throws DriverException {
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

		List<Book> books = cassandrionTemplate.query(new PreparedStatementCreator() {

			@Override
			public PreparedStatement createPreparedStatement(Session session) throws DriverException {
				return session.prepare(cql);
			}
		}, new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new RowMapper<Book>() {

			@Override
			public Book mapRow(Row row, int rowNum) throws DriverException {
				return rowToBook(row);
			}
		});

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

		Book b = this.cassandrionTemplate.query("select * from book where isbn = ?", new PreparedStatementBinder() {

			@Override
			public BoundStatement bindValues(PreparedStatement ps) throws DriverException {
				return ps.bind(isbn);
			}
		}, new ResultSetExtractor<Book>() {

			@Override
			public Book extractData(ResultSet rs) throws DriverException, DataAccessException {
				Book b = new Book();
				Row r = rs.one();
				b.setIsbn(r.getString("isbn"));
				b.setTitle(r.getString("title"));
				b.setAuthor(r.getString("author"));
				b.setPages(r.getInt("pages"));
				return b;
			}
		});

		return b;

	}

	/**
	 * For testing a HostMapper Implementation
	 */
	public class MyHost {
		public String someName;
	}
}
