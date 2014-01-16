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
package org.springdata.cassandra.test.integration.core;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.DataLoader;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.cassandraunit.dataset.yaml.ClassPathYamlDataSet;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.core.CassandraOperations;
import org.springdata.cassandra.cql.core.query.ConsistencyLevel;
import org.springdata.cassandra.cql.core.query.RetryPolicy;
import org.springdata.cassandra.cql.core.query.StatementOptions;
import org.springdata.cassandra.test.integration.config.JavaConfig;
import org.springdata.cassandra.test.integration.table.Book;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.AnnotationConfigContextLoader;

import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Unit Tests for CassandraTemplate
 * 
 * @author David Webb
 * @author Alex Shvid
 * 
 */
@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = { JavaConfig.class }, loader = AnnotationConfigContextLoader.class)
public class CassandraDataOperationsTest {

	@Autowired
	private CassandraOperations cassandraTemplate;

	private static Logger log = LoggerFactory.getLogger(CassandraDataOperationsTest.class);

	private final static String CASSANDRA_CONFIG = "cassandra.yaml";
	private final static String KEYSPACE_NAME = "test";
	private final static String CASSANDRA_HOST = "localhost";
	private final static int CASSANDRA_NATIVE_PORT = 9042;
	private final static int CASSANDRA_THRIFT_PORT = 9160;

	@Rule
	public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("cql-dataload.cql",
			KEYSPACE_NAME), CASSANDRA_CONFIG, CASSANDRA_HOST, CASSANDRA_NATIVE_PORT);

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {

		EmbeddedCassandraServerHelper.startEmbeddedCassandra(CASSANDRA_CONFIG);

		/*
		 * Load data file to creat the test keyspace before we init the template
		 */
		DataLoader dataLoader = new DataLoader("Test Cluster", CASSANDRA_HOST + ":" + CASSANDRA_THRIFT_PORT);
		dataLoader.load(new ClassPathYamlDataSet("cassandra-keyspace.yaml"));
	}

	@Test
	public void insertTest() {

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.saveNew(b1).execute();

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraTemplate.saveNew(b2).toTable("book_alt").execute();

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Guide");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		cassandraTemplate.saveNew(b3).toTable("book").withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).execute();

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraTemplate.saveNew(b5).withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).execute();

	}

	@Test
	public void insertAsynchronouslyTest() {

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.saveNew(b1).executeAsync();

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Guide");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraTemplate.saveNew(b2).toTable("book_alt").executeAsync();

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Guide");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		cassandraTemplate.saveNew(b3).toTable("book").withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).executeAsync();

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Guide");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraTemplate.saveNew(b5).withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).executeAsync();

	}

	@Test
	public void insertBatchTest() {

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book_alt", null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book", options);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, options);

	}

	@Test
	public void insertBatchAsynchronouslyTest() {

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(true, books, null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(true, books, "book_alt", null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(true, books, "book", options);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(true, books, options);

	}

	/**
	 * @return
	 */
	private List<Book> getBookList(int numBooks) {

		List<Book> books = new ArrayList<Book>();

		Book b = null;
		for (int i = 0; i < numBooks; i++) {
			b = new Book();
			b.setIsbn(UUID.randomUUID().toString());
			b.setTitle("Spring Data Cassandra Guide");
			b.setAuthor("Cassandra Guru");
			b.setPages(i * 10 + 5);
			books.add(b);
		}

		return books;
	}

	@Test
	public void updateTest() {

		insertTest();

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Book");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.save(b1).execute();

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Book");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraTemplate.save(b2).toTable("book_alt").execute();

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Book");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		cassandraTemplate.save(b3).toTable("book").withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).execute();

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Book");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraTemplate.save(b5).withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).execute();

	}

	@Test
	public void updateAsynchronouslyTest() {

		insertTest();

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Book");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.save(b1).executeAsync();

		Book b2 = new Book();
		b2.setIsbn("123456-2");
		b2.setTitle("Spring Data Cassandra Book");
		b2.setAuthor("Cassandra Guru");
		b2.setPages(521);

		cassandraTemplate.save(b2).toTable("book_alt").executeAsync();

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");
		b3.setTitle("Spring Data Cassandra Book");
		b3.setAuthor("Cassandra Guru");
		b3.setPages(265);

		cassandraTemplate.save(b3).toTable("book").withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).executeAsync();

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");
		b5.setTitle("Spring Data Cassandra Book");
		b5.setAuthor("Cassandra Guru");
		b5.setPages(265);

		cassandraTemplate.save(b5).withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).executeAsync();
		;

	}

	@Test
	public void updateBatchTest() {

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, null);

		alterBooks(books);

		cassandraTemplate.saveInBatch(false, books, null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book_alt", null);

		alterBooks(books);

		cassandraTemplate.saveInBatch(false, books, "book_alt", null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book", options);

		alterBooks(books);

		cassandraTemplate.saveInBatch(false, books, "book", options);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, options);

		alterBooks(books);

		cassandraTemplate.saveInBatch(false, books, options);

	}

	@Test
	public void updateBatchAsynchronouslyTest() {

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, null);

		alterBooks(books);

		cassandraTemplate.saveInBatch(true, books, null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book_alt", null);

		alterBooks(books);

		cassandraTemplate.saveInBatch(true, books, "book_alt", null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book", options);

		alterBooks(books);

		cassandraTemplate.saveInBatch(true, books, "book", options);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, options);

		alterBooks(books);

		cassandraTemplate.saveInBatch(true, books, options);

	}

	/**
	 * @param books
	 */
	private void alterBooks(List<Book> books) {

		for (Book b : books) {
			b.setAuthor("Ernest Hemmingway");
			b.setTitle("The Old Man and the Sea");
			b.setPages(115);
		}
	}

	@Test
	public void deleteTest() {

		insertTest();

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");

		cassandraTemplate.delete(b1).execute();

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		cassandraTemplate.delete(b2).fromTable("book_alt").execute();

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		cassandraTemplate.delete(b3).fromTable("book").withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).execute();

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		cassandraTemplate.delete(b5).withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).execute();

	}

	@Test
	public void deleteAsynchronouslyTest() {

		insertTest();

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");

		cassandraTemplate.delete(b1).executeAsync();

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		cassandraTemplate.delete(b2).fromTable("book_alt").executeAsync();

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		cassandraTemplate.delete(b3).fromTable("book").withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).executeAsync();

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		cassandraTemplate.delete(b5).withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).executeAsync();
	}

	@Test
	public void deleteBatchTest() {

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, null);

		cassandraTemplate.deleteInBatch(false, books, null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book_alt", null);

		cassandraTemplate.deleteInBatch(false, books, "book_alt", null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book", options);

		cassandraTemplate.deleteInBatch(false, books, "book", options);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, options);

		cassandraTemplate.deleteInBatch(false, books, options);

	}

	@Test
	public void deleteBatchAsynchronouslyTest() {

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, null);

		cassandraTemplate.deleteInBatch(true, books, null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book_alt", null);

		cassandraTemplate.deleteInBatch(true, books, "book_alt", null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book", options);

		cassandraTemplate.deleteInBatch(true, books, "book", options);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, options);

		cassandraTemplate.deleteInBatch(true, books, options);

	}

	@Test
	public void deleteByIdTest() {

		insertTest();

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");

		cassandraTemplate.deleteById(Book.class, b1.getIsbn()).execute();

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		cassandraTemplate.deleteById(Book.class, b2.getIsbn()).fromTable("book_alt").execute();

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		cassandraTemplate.deleteById(Book.class, b3.getIsbn()).fromTable("book").withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).execute();

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		cassandraTemplate.deleteById(Book.class, b5.getIsbn()).withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).execute();

	}

	@Test
	public void deleteByIdAsynchronouslyTest() {

		insertTest();

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");

		cassandraTemplate.deleteById(Book.class, b1.getIsbn()).executeAsync();

		Book b2 = new Book();
		b2.setIsbn("123456-2");

		cassandraTemplate.deleteById(Book.class, b2.getIsbn()).fromTable("book_alt").executeAsync();

		/*
		 * Test Single Insert with entity
		 */
		Book b3 = new Book();
		b3.setIsbn("123456-3");

		cassandraTemplate.deleteById(Book.class, b3.getIsbn()).fromTable("book").withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).executeAsync();

		/*
		 * Test Single Insert with entity
		 */
		Book b5 = new Book();
		b5.setIsbn("123456-5");

		cassandraTemplate.deleteById(Book.class, b5.getIsbn()).withConsistencyLevel(ConsistencyLevel.ONE)
				.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY).executeAsync();

	}

	@Test
	public void deleteByIdBatchTest() {

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, null);

		cassandraTemplate.deleteInBatchById(false, ids(books), Book.class, null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book_alt", null);

		cassandraTemplate.deleteInBatchById(false, ids(books), Book.class, "book_alt", null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book", options);

		cassandraTemplate.deleteInBatchById(false, ids(books), Book.class, "book", options);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, options);

		cassandraTemplate.deleteInBatchById(false, ids(books), Book.class, options);

	}

	private List<String> ids(List<Book> books) {
		List<String> ids = new ArrayList<String>(books.size());
		for (Book book : books) {
			ids.add(book.getIsbn());
		}
		return ids;
	}

	@Test
	public void deleteByIdBatchAsynchronouslyTest() {

		StatementOptions options = new StatementOptions();
		options.withConsistencyLevel(ConsistencyLevel.ONE);
		options.withRetryPolicy(RetryPolicy.DOWNGRADING_CONSISTENCY);

		List<Book> books = null;

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, null);

		cassandraTemplate.deleteInBatchById(true, ids(books), Book.class, null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book_alt", null);

		cassandraTemplate.deleteInBatchById(true, ids(books), Book.class, "book_alt", null);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, "book", options);

		cassandraTemplate.deleteInBatchById(true, ids(books), Book.class, "book", options);

		books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, options);

		cassandraTemplate.deleteInBatchById(true, ids(books), Book.class, options);

	}

	@Test
	public void selectOneTest() {

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.saveNew(b1).execute();

		Select select = QueryBuilder.select().all().from("book");
		select.where(QueryBuilder.eq("isbn", "123456-1"));

		Book b = cassandraTemplate.findOne(select.getQueryString(), Book.class, null);

		log.info("SingleSelect Book Title -> " + b.getTitle());
		log.info("SingleSelect Book Author -> " + b.getAuthor());

		assertEquals(b.getTitle(), "Spring Data Cassandra Guide");
		assertEquals(b.getAuthor(), "Cassandra Guru");

	}

	@Test
	public void findByIdTest() {

		/*
		 * Test Single Insert with entity
		 */
		Book b1 = new Book();
		b1.setIsbn("123456-1");
		b1.setTitle("Spring Data Cassandra Guide");
		b1.setAuthor("Cassandra Guru");
		b1.setPages(521);

		cassandraTemplate.saveNew(b1).execute();

		Book b = cassandraTemplate.findById("123456-1", Book.class, null);

		log.info("SingleSelect Book Title -> " + b.getTitle());
		log.info("SingleSelect Book Author -> " + b.getAuthor());

		assertEquals(b.getTitle(), "Spring Data Cassandra Guide");
		assertEquals(b.getAuthor(), "Cassandra Guru");
	}

	@Test
	public void selectTest() {

		List<Book> books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, null);

		Select select = QueryBuilder.select().all().from("book");

		List<Book> b = cassandraTemplate.find(select.getQueryString(), Book.class, null);

		log.info("Book Count -> " + b.size());

		assertEquals(b.size(), 20);

	}

	@Test
	public void selectCountTest() {

		List<Book> books = getBookList(20);

		cassandraTemplate.saveNewInBatch(false, books, null);

		Select select = QueryBuilder.select().countAll().from("book");

		Long count = cassandraTemplate.count(select.getQueryString(), null);

		log.info("Book Count -> " + count);

		assertEquals(count, new Long(20));

	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();

	}

	@SuppressWarnings("deprecation")
	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}
}
