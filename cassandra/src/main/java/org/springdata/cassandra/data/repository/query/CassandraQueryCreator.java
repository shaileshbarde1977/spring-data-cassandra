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
package org.springdata.cassandra.data.repository.query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springdata.cassandra.data.mapping.CassandraPersistentProperty;
import org.springframework.data.domain.Sort;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.context.PersistentPropertyPath;
import org.springframework.data.repository.query.parser.AbstractQueryCreator;
import org.springframework.data.repository.query.parser.Part;
import org.springframework.data.repository.query.parser.PartTree;
import org.springframework.util.Assert;

import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

/**
 * Custom query creator to create Cassandra cql query.
 * 
 * @author Alex Shvid
 */
class CassandraQueryCreator extends AbstractQueryCreator<Select, List<Clause>> {

	private static final Logger LOG = LoggerFactory.getLogger(CassandraQueryCreator.class);
	private final CassandraParameterAccessor accessor;
	private final MappingContext<?, CassandraPersistentProperty> context;
	private final String tableName;
	private final boolean countQuery;

	/**
	 * Creates a new {@link CassandraQueryCreator} from the given {@link PartTree}, {@link CassandraParameterAccessor} and
	 * {@link MappingContext}.
	 * 
	 * @param tree
	 * @param accessor
	 * @param context
	 */
	public CassandraQueryCreator(PartTree tree, CassandraParameterAccessor accessor,
			MappingContext<?, CassandraPersistentProperty> context, String tableName, boolean countQuery) {

		super(tree, accessor);

		Assert.notNull(context);

		this.accessor = accessor;
		this.context = context;
		this.tableName = tableName;
		this.countQuery = countQuery;

	}

	@Override
	protected List<Clause> create(Part part, Iterator<Object> iterator) {

		PersistentPropertyPath<CassandraPersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CassandraPersistentProperty property = path.getLeafProperty();

		List<Clause> criteria = new ArrayList<Clause>();

		return criteria;
	}

	@Override
	protected List<Clause> and(Part part, List<Clause> base, Iterator<Object> iterator) {

		if (base == null) {
			return create(part, iterator);
		}

		PersistentPropertyPath<CassandraPersistentProperty> path = context.getPersistentPropertyPath(part.getProperty());
		CassandraPersistentProperty property = path.getLeafProperty();

		String s = path.toDotPath(CassandraPersistentProperty.PropertyToColumnNameConverter.INSTANCE);

		return base;
	}

	@Override
	protected List<Clause> or(List<Clause> base, List<Clause> criteria) {
		throw new IllegalArgumentException("Unsupported or operation!");
	}

	@Override
	protected Select complete(List<Clause> criteria, Sort sort) {

		Select select = countQuery ? QueryBuilder.select().countAll().from(tableName) : QueryBuilder.select().all()
				.from(tableName);

		Select.Where w = select.where();

		if (criteria != null) {
			for (Clause c : criteria) {
				w.and(c);
			}
		}

		return select;
	}

}
