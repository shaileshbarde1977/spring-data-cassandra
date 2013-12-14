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
package net.webby.cassandrion.test.unit.core.keyspace;

import static org.junit.Assert.*;

import java.lang.annotation.RetentionPolicy;

import net.webby.cassandrion.core.cql.spec.DefaultOption;
import net.webby.cassandrion.core.cql.spec.Option;

import org.junit.Test;

public class OptionTest {

	@Test(expected = IllegalArgumentException.class)
	public void testOptionWithNullName() {
		new DefaultOption(null, Object.class, true, true, true);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testOptionWithEmptyName() {
		new DefaultOption("", Object.class, true, true, true);
	}

	@Test
	public void testOptionWithNullType() {
		new DefaultOption("opt", null, true, true, true);
		new DefaultOption("opt", null, false, true, true);
	}

	@Test
	public void testOptionWithNullTypeIsCoerceable() {
		Option op = new DefaultOption("opt", null, true, true, true);
		assertTrue(op.isCoerceable(""));
		assertTrue(op.isCoerceable(null));
	}

	@Test
	public void testOptionValueCoercion() {
		String name = "my_option";
		Class<?> type = String.class;
		boolean requires = true;
		boolean escapes = true;
		boolean quotes = true;

		Option op = new DefaultOption(name, type, requires, escapes, quotes);

		assertTrue(op.isCoerceable("opt"));
		assertEquals("'opt'", op.toString("opt"));
		assertEquals("'opt''n'", op.toString("opt'n"));

		type = Long.class;
		escapes = false;
		quotes = false;
		op = new DefaultOption(name, type, requires, escapes, quotes);

		String expected = "1";
		for (Object value : new Object[] { 1, "1" }) {
			assertTrue(op.isCoerceable(value));
			assertEquals(expected, op.toString(value));
		}
		assertFalse(op.isCoerceable("x"));
		assertTrue(op.isCoerceable(null));

		type = Long.class;
		escapes = false;
		quotes = true;
		op = new DefaultOption(name, type, requires, escapes, quotes);

		expected = "'1'";
		for (Object value : new Object[] { 1, "1" }) {
			assertTrue(op.isCoerceable(value));
			assertEquals(expected, op.toString(value));
		}
		assertFalse(op.isCoerceable("x"));
		assertTrue(op.isCoerceable(null));

		type = Double.class;
		escapes = false;
		quotes = false;
		op = new DefaultOption(name, type, requires, escapes, quotes);

		String[] expecteds = new String[] { "1", "1.0", "1.0", "1", "1.0", null };
		Object[] values = new Object[] { 1, 1.0F, 1.0D, "1", "1.0", null };
		for (int i = 0; i < values.length; i++) {
			assertTrue(op.isCoerceable(values[i]));
			assertEquals(expecteds[i], op.toString(values[i]));
		}
		assertFalse(op.isCoerceable("x"));
		assertTrue(op.isCoerceable(null));

		type = RetentionPolicy.class;
		escapes = false;
		quotes = false;
		op = new DefaultOption(name, type, requires, escapes, quotes);

		assertTrue(op.isCoerceable(null));
		assertTrue(op.isCoerceable(RetentionPolicy.CLASS));
		assertTrue(op.isCoerceable("CLASS"));
		assertFalse(op.isCoerceable("x"));
		assertEquals("CLASS", op.toString("CLASS"));
		assertEquals("CLASS", op.toString(RetentionPolicy.CLASS));
	}
}
