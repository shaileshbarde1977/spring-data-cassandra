/*
 * Copyright 2013 by the original author(s).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.webby.cassandrion.data.test.integration.mapping;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Date;

import net.webby.cassandrion.data.mapping.BasicCassandrionPersistentEntity;
import net.webby.cassandrion.data.mapping.BasicCassandrionPersistentProperty;
import net.webby.cassandrion.data.mapping.CassandraPersistentEntity;
import net.webby.cassandrion.data.mapping.CassandraPersistentProperty;
import net.webby.cassandrion.data.mapping.Column;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.thrift.transport.TTransportException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.springframework.data.annotation.Id;
import org.springframework.data.mapping.model.SimpleTypeHolder;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.util.ReflectionUtils;

/**
 * Integration test for {@link BasicCassandrionPersistentProperty}.
 * 
 * @author Alex Shvid
 */
public class BasicCassandrionPersistentPropertyIntegrationTests {

	CassandraPersistentEntity<Timeline> entity;

	@BeforeClass
	public static void startCassandra() throws IOException, TTransportException, ConfigurationException,
			InterruptedException {
		EmbeddedCassandraServerHelper.startEmbeddedCassandra("cassandra.yaml");
	}

	@Before
	public void setup() {
		entity = new BasicCassandrionPersistentEntity<Timeline>(ClassTypeInformation.from(Timeline.class));
	}

	@Test
	public void usesAnnotatedColumnName() {

		Field field = ReflectionUtils.findField(Timeline.class, "text");
		assertThat(getPropertyFor(field).getColumnName(), is("message"));
	}

	@Test
	public void checksIdProperty() {
		Field field = ReflectionUtils.findField(Timeline.class, "id");
		CassandraPersistentProperty property = getPropertyFor(field);
		assertThat(property.isIdProperty(), is(true));
	}

	@Test
	public void returnsPropertyNameForUnannotatedProperties() {
		Field field = ReflectionUtils.findField(Timeline.class, "time");
		assertThat(getPropertyFor(field).getColumnName(), is("time"));
	}

	@After
	public void clearCassandra() {
		EmbeddedCassandraServerHelper.cleanEmbeddedCassandra();
	}

	@AfterClass
	public static void stopCassandra() {
		EmbeddedCassandraServerHelper.stopEmbeddedCassandra();
	}

	private CassandraPersistentProperty getPropertyFor(Field field) {
		return new BasicCassandrionPersistentProperty(field, null, entity, new SimpleTypeHolder());
	}

	class Timeline {

		@Id
		String id;

		Date time;

		@Column("message")
		String text;

	}

}
