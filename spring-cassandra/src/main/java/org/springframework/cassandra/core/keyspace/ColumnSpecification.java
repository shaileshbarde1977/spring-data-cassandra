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
package org.springframework.cassandra.core.keyspace;

import static org.springframework.cassandra.core.cql.CqlStringUtils.checkIdentifier;
import static org.springframework.cassandra.core.cql.CqlStringUtils.identifize;
import static org.springframework.cassandra.core.cql.CqlStringUtils.noNull;
import static org.springframework.cassandra.core.PrimaryKey.PARTITIONED;
import static org.springframework.cassandra.core.PrimaryKey.CLUSTERED;
import static org.springframework.cassandra.core.Ordering.ASCENDING;

import org.springframework.cassandra.core.PrimaryKey;
import org.springframework.cassandra.core.Ordering;

import com.datastax.driver.core.DataType;

/**
 * Builder class to help construct CQL statements that involve column manipulation. Not threadsafe.
 * <p/>
 * Use {@link #name(String)} and {@link #type(String)} to set the name and type of the column, respectively. To specify
 * a <code>PRIMARY KEY</code> column, use {@link #clustered()} or {@link #clustered(Ordering)}. To specify that the
 * <code>PRIMARY KEY</code> column is or is part of the partition key, use {@link #partition()} instead of
 * {@link #clustered()} or {@link #clustered(Ordering)}.
 * 
 * @author Matthew T. Adams
 * @author Alex Shvid
 */
public class ColumnSpecification {

	/**
	 * Default ordering of primary key fields; value is {@link Ordering#ASCENDING}.
	 */
	public static final Ordering DEFAULT_ORDERING = ASCENDING;

	private String name;
	private DataType type; // TODO: determining if we should be coupling this to Datastax Java Driver type?
	private PrimaryKey primary;
	private Integer ordinal;
	private Ordering ordering;

	/**
	 * Sets the column's name.
	 * 
	 * @return this
	 */
	public ColumnSpecification name(String name) {
		checkIdentifier(name);
		this.name = name;
		return this;
	}

	/**
	 * Sets the column's type.
	 * 
	 * @return this
	 */
	public ColumnSpecification type(DataType type) {
		this.type = type;
		return this;
	}

	/**
	 * Identifies this column as a primary key column.
	 * 
	 * @return this
	 */
	public ColumnSpecification primary(PrimaryKey primary, Integer ordinal) {
		return (primary == PrimaryKey.PARTITIONED) ? partitioned(ordinal, true)
				: clustered(DEFAULT_ORDERING, ordinal, true);
	}

	/**
	 * Identifies this column as a primary key column that is also part of a partition key. Sets the column's
	 * {@link #primary} to {@link PrimaryKey#PARTITIONED} and its {@link #ordering} to <code>null</code>.
	 * 
	 * @return this
	 */
	public ColumnSpecification partitioned(Integer ordinal) {
		return partitioned(ordinal, true);
	}

	/**
	 * Toggles the identification of this column as a primary key column that also is or is part of a partition key. Sets
	 * {@link #ordering} to <code>null</code> and, if the given boolean is <code>true</code>, then sets the column's
	 * {@link #primary} to {@link PrimaryKey#PARTITIONED}, else sets it to <code>null</code>.
	 * 
	 * @return this
	 */
	public ColumnSpecification partitioned(Integer ordinal, boolean primaryKey) {
		this.primary = primaryKey ? PARTITIONED : null;
		this.ordinal = primaryKey ? ordinal : null;
		this.ordering = null;
		return this;
	}

	/**
	 * Identifies this column as a primary key column with default ordering. Sets the column's {@link #primary} to
	 * {@link PrimaryKey#CLUSTERED} and its {@link #ordering} to {@link #DEFAULT_ORDERING}.
	 * 
	 * @return this
	 */
	public ColumnSpecification clustered(Integer ordinal) {
		return clustered(DEFAULT_ORDERING, ordinal);
	}

	/**
	 * Identifies this column as a primary key column with the given ordering. Sets the column's {@link #primary} to
	 * {@link PrimaryKey#CLUSTERED} and its {@link #ordering} to the given {@link Ordering}.
	 * 
	 * @return this
	 */
	public ColumnSpecification clustered(Ordering order, Integer ordinal) {
		return clustered(order, ordinal, true);
	}

	/**
	 * Toggles the identification of this column as a primary key column. If the given boolean is <code>true</code>, then
	 * sets the column's {@link #primary} to {@link PrimaryKey#PARTITIONED} and {@link #ordering} to the given
	 * {@link Ordering} , else sets both {@link #primary} and {@link #ordering} to <code>null</code>.
	 * 
	 * @return this
	 */
	public ColumnSpecification clustered(Ordering ordering, Integer ordinal, boolean primaryKey) {
		this.primary = primaryKey ? CLUSTERED : null;
		this.ordinal = primaryKey ? ordinal : null;
		this.ordering = primaryKey ? ordering : null;
		return this;
	}

	/**
	 * Sets the column's {@link #primary}.
	 * 
	 * @return this
	 */
	/* package */ColumnSpecification primary(PrimaryKey primaryKey) {
		this.primary = primaryKey;
		return this;
	}

	/**
	 * Sets the column's {@link #ordinal}.
	 * 
	 * @return this
	 */
	/* package */ColumnSpecification ordinal(Integer ordinal) {
		this.ordinal = ordinal;
		return this;
	}

	/**
	 * Sets the column's {@link #ordering}.
	 * 
	 * @return this
	 */
	/* package */ColumnSpecification ordering(Ordering ordering) {
		this.ordering = ordering;
		return this;
	}

	public String getName() {
		return name;
	}

	public String getNameAsIdentifier() {
		return identifize(name);
	}

	public DataType getType() {
		return type;
	}

	public PrimaryKey getPrimary() {
		return primary;
	}

	public Integer getOrdinal() {
		return ordinal;
	}

	public Ordering getOrdering() {
		return ordering;
	}

	public String toCql() {
		return toCql(null).toString();
	}

	public StringBuilder toCql(StringBuilder cql) {
		return (cql = noNull(cql)).append(name).append(" ").append(type);
	}

	@Override
	public String toString() {
		return toCql(null).append(" /* primary=").append(primary).append(", ordinal=").append(ordinal)
				.append(", ordering=").append(ordering).append(" */ ").toString();
	}
}