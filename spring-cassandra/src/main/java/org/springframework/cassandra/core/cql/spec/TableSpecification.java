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
package org.springframework.cassandra.core.cql.spec;

import static org.springframework.cassandra.core.PrimaryKey.CLUSTERED;
import static org.springframework.cassandra.core.PrimaryKey.PARTITIONED;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.springframework.cassandra.core.Ordering;
import org.springframework.cassandra.core.PrimaryKey;

import com.datastax.driver.core.DataType;

/**
 * Builder class to support the construction of table specifications that have columns. This class can also be used as a
 * standalone {@link TableDescriptor}, independent of {@link CreateTableSpecification}.
 * 
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public class TableSpecification<T> extends TableOptionsSpecification<TableSpecification<T>> implements TableDescriptor {

	/**
	 * Ordered List of only those columns that comprise the partition key.
	 */
	private List<ColumnSpecification> partitionedKeyColumns = new ArrayList<ColumnSpecification>();

	/**
	 * Ordered List of only those columns that comprise the primary key that are not also part of the partition key.
	 */
	private List<ColumnSpecification> clusteredKeyColumns = new ArrayList<ColumnSpecification>();

	/**
	 * List of only those columns that are not partition or primary key columns.
	 */
	private List<ColumnSpecification> nonKeyColumns = new ArrayList<ColumnSpecification>();

	/**
	 * Adds the given non-key column to the table.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 */
	public T column(String name, DataType type) {
		return column(name, type, null, null);
	}

	/**
	 * Adds the given partition key column to the table.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 * @return this
	 */
	public T partitionedKeyColumn(String name, DataType type) {
		return column(name, type, PARTITIONED, null);
	}

	/**
	 * Adds the given primary key column to the table with ascending ordering.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 * @return this
	 */
	public T clusteredKeyColumn(String name, DataType type) {
		return clusteredKeyColumn(name, type, null);
	}

	/**
	 * Adds the given primary key column to the table with the given ordering (<code>null</code> meaning ascending).
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 * @param ordering If the given {@link PrimaryKey} is {@link PrimaryKey#CLUSTERED}, then the given ordering is used,
	 *          else ignored.
	 * @return this
	 */
	public T clusteredKeyColumn(String name, DataType type, Ordering ordering) {
		return column(name, type, CLUSTERED, ordering);
	}

	/**
	 * Adds the given info as a new column to the table.
	 * 
	 * @param name The column name; must be a valid unquoted or quoted identifier without the surrounding double quotes.
	 * @param type The data type of the column.
	 * @param keyType Indicates key type. Null means that the column is not a key column.
	 * @param ordering If the given {@link PrimaryKey} is {@link PrimaryKey#CLUSTERED}, then the given ordering is used,
	 *          else ignored.
	 * @return this
	 */
	@SuppressWarnings("unchecked")
	protected T column(String name, DataType type, PrimaryKey keyType, Ordering ordering) {

		ColumnSpecification column = new ColumnSpecification().name(name).type(type).primary(keyType)
				.ordering(keyType == CLUSTERED ? ordering : null);

		if (keyType == PrimaryKey.PARTITIONED) {
			partitionedKeyColumns.add(column);
		}

		if (keyType == PrimaryKey.CLUSTERED) {
			clusteredKeyColumns.add(column);
		}

		if (keyType == null) {
			nonKeyColumns.add(column);
		}

		return (T) this;
	}

	/**
	 * Returns an unmodifiable list of all columns. Order is important: partitioned columns, clustered columns, non key
	 * columns
	 */
	public List<ColumnSpecification> getAllColumns() {

		ArrayList<ColumnSpecification> allKeyColumns = new ArrayList<ColumnSpecification>(partitionedKeyColumns.size()
				+ clusteredKeyColumns.size() + nonKeyColumns.size());
		allKeyColumns.addAll(partitionedKeyColumns);
		allKeyColumns.addAll(clusteredKeyColumns);
		allKeyColumns.addAll(nonKeyColumns);

		return Collections.unmodifiableList(allKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all partition key columns.
	 */
	public List<ColumnSpecification> getPartitionedKeyColumns() {
		return Collections.unmodifiableList(partitionedKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all primary key columns that are not also partition key columns.
	 */
	public List<ColumnSpecification> getClusteredKeyColumns() {
		return Collections.unmodifiableList(clusteredKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all primary key columns that are not also partition key columns.
	 */
	public List<ColumnSpecification> getPrimaryKeyColumns() {

		ArrayList<ColumnSpecification> primaryKeyColumns = new ArrayList<ColumnSpecification>(partitionedKeyColumns.size()
				+ clusteredKeyColumns.size());
		primaryKeyColumns.addAll(partitionedKeyColumns);
		primaryKeyColumns.addAll(clusteredKeyColumns);

		return Collections.unmodifiableList(primaryKeyColumns);
	}

	/**
	 * Returns an unmodifiable list of all non-key columns.
	 */
	public List<ColumnSpecification> getNonKeyColumns() {
		return Collections.unmodifiableList(nonKeyColumns);
	}
}
