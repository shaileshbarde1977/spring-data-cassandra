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
package net.webby.cassandrion.data.convert;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import net.webby.cassandrion.core.KeyPart;
import net.webby.cassandrion.core.cql.spec.AlterTableSpecification;
import net.webby.cassandrion.core.cql.spec.CreateIndexSpecification;
import net.webby.cassandrion.core.cql.spec.CreateTableSpecification;
import net.webby.cassandrion.core.cql.spec.DropIndexSpecification;
import net.webby.cassandrion.core.cql.spec.WithNameSpecification;
import net.webby.cassandrion.data.mapping.CassandraPersistentEntity;
import net.webby.cassandrion.data.mapping.CassandraPersistentProperty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.core.convert.support.DefaultConversionService;
import org.springframework.data.convert.EntityInstantiator;
import org.springframework.data.mapping.PropertyHandler;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.BeanWrapper;
import org.springframework.data.mapping.model.DefaultSpELExpressionEvaluator;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.mapping.model.PersistentEntityParameterValueProvider;
import org.springframework.data.mapping.model.PropertyValueProvider;
import org.springframework.data.mapping.model.SpELContext;
import org.springframework.data.util.ClassTypeInformation;
import org.springframework.data.util.TypeInformation;
import org.springframework.util.ClassUtils;

import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.querybuilder.Update.Where;

/**
 * {@link CassandrionConverter} that uses a {@link MappingContext} to do sophisticated mapping of domain objects to
 * {@link Row}.
 * 
 * @author Alex Shvid
 */
public class MappingCassandrionConverter extends AbstractCassandrionConverter implements ApplicationContextAware,
		BeanClassLoaderAware {

	protected static final Logger log = LoggerFactory.getLogger(MappingCassandrionConverter.class);

	protected final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;
	protected ApplicationContext applicationContext;
	private SpELContext spELContext;
	private boolean useFieldAccessOnly = true;

	private ClassLoader beanClassLoader;

	/**
	 * Creates a new {@link MappingCassandrionConverter} given the new {@link MappingContext}.
	 * 
	 * @param mappingContext must not be {@literal null}.
	 */
	public MappingCassandrionConverter(
			MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext) {
		super(new DefaultConversionService());
		this.mappingContext = mappingContext;
		this.spELContext = new SpELContext(RowReaderPropertyAccessor.INSTANCE);
	}

	@SuppressWarnings("unchecked")
	public <R> R readRow(Class<R> clazz, Row row) {

		Class<R> beanClassLoaderClass = transformClassToBeanClassLoaderClass(clazz);

		TypeInformation<? extends R> type = ClassTypeInformation.from(beanClassLoaderClass);
		// TypeInformation<? extends R> typeToUse = typeMapper.readType(row, type);
		TypeInformation<? extends R> typeToUse = type;
		Class<? extends R> rawType = typeToUse.getType();

		if (Row.class.isAssignableFrom(rawType)) {
			return (R) row;
		}

		CassandraPersistentEntity<R> persistentEntity = (CassandraPersistentEntity<R>) mappingContext
				.getPersistentEntity(typeToUse);
		if (persistentEntity == null) {
			throw new MappingException("No mapping metadata found for " + rawType.getName());
		}

		return readRowInternal(persistentEntity, row);
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.convert.EntityConverter#getMappingContext()
	 */
	public MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> getMappingContext() {
		return mappingContext;
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
	 */
	public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
		this.applicationContext = applicationContext;
		this.spELContext = new SpELContext(this.spELContext, applicationContext);
	}

	private <S extends Object> S readRowInternal(final CassandraPersistentEntity<S> entity, final Row row) {

		final DefaultSpELExpressionEvaluator evaluator = new DefaultSpELExpressionEvaluator(row, spELContext);

		final PropertyValueProvider<CassandraPersistentProperty> propertyProvider = new CassandraPropertyValueProvider(row,
				evaluator);
		PersistentEntityParameterValueProvider<CassandraPersistentProperty> parameterProvider = new PersistentEntityParameterValueProvider<CassandraPersistentProperty>(
				entity, propertyProvider, null);

		EntityInstantiator instantiator = instantiators.getInstantiatorFor(entity);
		S instance = instantiator.createInstance(entity, parameterProvider);

		final BeanWrapper<CassandraPersistentEntity<S>, S> wrapper = BeanWrapper.create(instance, conversionService);
		final S result = wrapper.getBean();

		// Set properties not already set in the constructor
		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				boolean isConstructorProperty = entity.isConstructorArgument(prop);
				boolean hasValueForProperty = row.getColumnDefinitions().contains(prop.getColumnName());

				if (!hasValueForProperty || isConstructorProperty) {
					return;
				}

				Object obj = propertyProvider.getPropertyValue(prop);
				wrapper.setProperty(prop, obj, useFieldAccessOnly);
			}
		});

		return result;
	}

	public void setUseFieldAccessOnly(boolean useFieldAccessOnly) {
		this.useFieldAccessOnly = useFieldAccessOnly;
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public <R> R read(Class<R> type, Object row) {
		if (row instanceof Row) {
			return readRow(type, (Row) row);
		}
		throw new MappingException("Unknown row object " + row.getClass().getName());
	}

	/* (non-Javadoc)
	 * @see org.springframework.data.convert.EntityWriter#write(java.lang.Object, java.lang.Object)
	 */
	@Override
	public void write(Object obj, Object builtStatement) {

		if (obj == null) {
			return;
		}

		Class<?> beanClassLoaderClass = transformClassToBeanClassLoaderClass(obj.getClass());
		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(beanClassLoaderClass);

		if (entity == null) {
			throw new MappingException("No mapping metadata found for " + obj.getClass());
		}

		if (builtStatement instanceof Insert) {
			writeInsertInternal(obj, (Insert) builtStatement, entity);
		} else if (builtStatement instanceof Update) {
			writeUpdateInternal(obj, (Update) builtStatement, entity);
		} else if (builtStatement instanceof Delete.Where) {
			writeDeleteWhereInternal(obj, (Delete.Where) builtStatement, entity);
		} else {
			throw new MappingException("Unknown buildStatement " + builtStatement.getClass().getName());
		}
	}

	private void writeInsertInternal(final Object objectToSave, final Insert insert, CassandraPersistentEntity<?> entity) {

		final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(objectToSave,
				conversionService);

		// Write the properties
		doWithAllProperties(entity, new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				Object propertyObj = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);

				if (propertyObj != null) {
					insert.value(prop.getColumnName(), propertyObj);
				}

			}
		});

	}

	private void writeUpdateInternal(final Object objectToSave, final Update update, CassandraPersistentEntity<?> entity) {

		final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(objectToSave,
				conversionService);

		final Where w = update.where();

		// Write the properties
		doWithAllProperties(entity, new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				Object propertyObj = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);

				if (propertyObj != null) {
					if (prop.isIdProperty() || prop.getKeyPart() != null) {
						w.and(QueryBuilder.eq(prop.getColumnName(), propertyObj));
					} else {
						update.with(QueryBuilder.set(prop.getColumnName(), propertyObj));
					}
				}

			}
		});

	}

	private void writeDeleteWhereInternal(final Object objectToSave, final Delete.Where whereId,
			CassandraPersistentEntity<?> entity) {

		final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(objectToSave,
				conversionService);

		// Write the properties
		doWithAllProperties(entity, new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.isIdProperty() || prop.getKeyPart() != null) {

					Object propertyObj = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);

					if (propertyObj != null) {
						whereId.and(QueryBuilder.eq(prop.getColumnName(), propertyObj));
					}
				}

			}
		});

	}

	public CreateTableSpecification getCreateTableSpecification(CassandraPersistentEntity<?> entity) {

		final CreateTableSpecification spec = new CreateTableSpecification();

		spec.name(entity.getTable());

		final List<CassandraPersistentProperty> partitionKeyProperties = new ArrayList<CassandraPersistentProperty>(5);
		final List<CassandraPersistentProperty> clusteringKeyProperties = new ArrayList<CassandraPersistentProperty>(5);

		doWithAllProperties(entity, new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.isIdProperty()) {
					partitionKeyProperties.add(prop);
				} else if (prop.getKeyPart() == KeyPart.PARTITION) {
					partitionKeyProperties.add(prop);
				} else if (prop.getKeyPart() == KeyPart.CLUSTERING) {
					clusteringKeyProperties.add(prop);
				} else {
					spec.column(prop.getColumnName(), prop.getDataType());
				}

			}
		});

		if (partitionKeyProperties.isEmpty()) {
			throw new MappingException("not found partition key in the entity " + entity.getType());
		}

		/*
		 * Sort primary key properties by ordinal
		 */

		Collections.sort(partitionKeyProperties, OrdinalBasedPropertyComparator.INSTANCE);
		Collections.sort(clusteringKeyProperties, OrdinalBasedPropertyComparator.INSTANCE);

		/*
		 * Add ordered primary key columns to the specification
		 */

		for (CassandraPersistentProperty prop : partitionKeyProperties) {
			spec.partitionKeyColumn(prop.getColumnName(), prop.getDataType());
		}

		for (CassandraPersistentProperty prop : clusteringKeyProperties) {
			spec.clusteringKeyColumn(prop.getColumnName(), prop.getDataType(), prop.getOrdering());
		}

		return spec;

	}

	public AlterTableSpecification getAlterTableSpecification(final CassandraPersistentEntity<?> entity,
			final TableMetadata table, final boolean dropRemovedAttributeColumns) {

		final AlterTableSpecification spec = new AlterTableSpecification();

		spec.name(entity.getTable());

		final Set<String> definedColumns = dropRemovedAttributeColumns ? new HashSet<String>() : Collections
				.<String> emptySet();

		doWithAllProperties(entity, new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				String columnName = prop.getColumnName();
				DataType columnDataType = prop.getDataType();

				String tableColumnName = columnName.toLowerCase();

				if (dropRemovedAttributeColumns) {
					definedColumns.add(tableColumnName);
				}

				ColumnMetadata columnMetadata = table.getColumn(tableColumnName);

				if (columnMetadata != null && columnDataType.equals(columnMetadata.getType())) {
					return;
				}

				if (prop.isIdProperty() || prop.getKeyPart() != null) {
					throw new MappingException("unable to add or alter column in the primary index " + columnName
							+ " for entity " + entity.getName());
				} else {

					if (columnMetadata == null) {
						spec.add(columnName, columnDataType);
					} else {
						spec.alter(columnName, columnDataType);
					}

				}

			}
		});

		if (dropRemovedAttributeColumns) {
			for (ColumnMetadata columnMetadata : table.getColumns()) {

				String columnName = columnMetadata.getName();

				if (!definedColumns.contains(columnName)) {
					spec.drop(columnName);
				}

			}
		}

		return spec;

	}

	public List<CreateIndexSpecification> getCreateIndexSpecifications(final CassandraPersistentEntity<?> entity) {

		final List<CreateIndexSpecification> indexList = new ArrayList<CreateIndexSpecification>();

		doWithAllProperties(entity, new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.isIdProperty() || prop.getKeyPart() != null) {
					if (prop.isIndexed()) {
						throw new MappingException("unable to create index on column in the primary key " + prop.getColumnName()
								+ " for entity " + entity.getName());
					}
					return;
				}

				if (prop.isIndexed()) {
					indexList.add(new CreateIndexSpecification().optionalName(prop.getIndexName()).on(entity.getTable())
							.column(prop.getColumnName()));
				}
			}
		});

		return indexList;
	}

	public List<WithNameSpecification<?>> getIndexChangeSpecifications(final CassandraPersistentEntity<?> entity,
			final TableMetadata table) {

		final List<WithNameSpecification<?>> list = new ArrayList<WithNameSpecification<?>>();

		final Set<String> definedColumns = new HashSet<String>();

		doWithAllProperties(entity, new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.isIdProperty() || prop.getKeyPart() != null) {
					throw new MappingException("unable to create index on column in the primary key " + prop.getColumnName()
							+ " for entity " + entity.getName());
				}

				String columnName = prop.getColumnName();

				String tableColumnName = columnName.toLowerCase();
				definedColumns.add(tableColumnName);

				ColumnMetadata columnMetadata = table.getColumn(tableColumnName);

				if (prop.isIndexed() && (columnMetadata == null || columnMetadata.getIndex() == null)) {
					list.add(new CreateIndexSpecification().optionalName(prop.getIndexName()).on(entity.getTable())
							.column(prop.getColumnName()));
				} else if (!prop.isIndexed() && columnMetadata != null && columnMetadata.getIndex() != null) {
					list.add(new DropIndexSpecification().name(columnMetadata.getIndex().getName()));
				}

			}
		});

		for (ColumnMetadata columnMetadata : table.getColumns()) {

			String columnName = columnMetadata.getName();

			if (!definedColumns.contains(columnName) && columnMetadata.getIndex() != null) {
				list.add(new DropIndexSpecification().name(columnMetadata.getIndex().getName()));
			}

		}

		return list;

	}

	public List<Clause> getPrimaryKey(final CassandraPersistentEntity<?> entity, final Object id) {

		final List<Clause> result = new LinkedList<Clause>();

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.isIdProperty()) {

					if (prop.hasEmbeddableType()) {

						if (!prop.getRawType().isAssignableFrom(id.getClass())) {
							throw new MappingException("id class " + id.getClass() + " can not be converted to embeddedid property "
									+ prop.getColumnName() + " in the entity " + entity.getName());
						}

						embeddedPrimaryKey(prop.getRawType(), id, result);

					} else {
						result.add(QueryBuilder.eq(prop.getColumnName(), id));
						return;
					}

				}

			}
		});

		return result;
	}

	private void embeddedPrimaryKey(Class<?> idClass, Object id, final List<Clause> result) {

		final BeanWrapper<CassandraPersistentEntity<Object>, Object> wrapper = BeanWrapper.create(id, conversionService);

		final CassandraPersistentEntity<?> idEntity = mappingContext.getPersistentEntity(idClass);

		if (idEntity == null) {
			throw new MappingException("id entity not found for " + idClass);
		}

		// Write the properties
		doWithAllProperties(idEntity, new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.getKeyPart() != null) {

					Object propertyObj = wrapper.getProperty(prop, prop.getType(), useFieldAccessOnly);

					if (propertyObj == null) {
						throw new MappingException("null primary key column " + prop.getColumnName() + " in entity "
								+ idEntity.getName());
					}

					result.add(QueryBuilder.eq(prop.getColumnName(), propertyObj));
				}

			}
		});

	}

	private void doWithAllProperties(final CassandraPersistentEntity<?> entity,
			final PropertyHandler<CassandraPersistentProperty> handler) {

		entity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty prop) {

				if (prop.hasEmbeddableType()) {

					final CassandraPersistentEntity<?> pkEntity = mappingContext.getPersistentEntity(prop.getRawType());

					if (pkEntity == null) {
						throw new MappingException("entity not found for " + prop.getRawType());
					}

					if (prop.isEmbeddedIdProperty()) {
						validatePkEntity(pkEntity);
					}

					doWithAllProperties(pkEntity, handler);

				}

				else {

					handler.doWithPersistentProperty(prop);

				}

			}
		});

	}

	private void validatePkEntity(final CassandraPersistentEntity<?> pkEntity) {

		pkEntity.doWithProperties(new PropertyHandler<CassandraPersistentProperty>() {
			public void doWithPersistentProperty(CassandraPersistentProperty pkProp) {

				if (pkProp.getKeyPart() == null) {
					throw new MappingException(
							"all properties in composite private key must be annotated by PartitionKey or ClusteringKey annotations "
									+ pkEntity.getType());
				}

			}
		});

	}

	@SuppressWarnings("unchecked")
	private <T> Class<T> transformClassToBeanClassLoaderClass(Class<T> entity) {
		try {
			return (Class<T>) ClassUtils.forName(entity.getName(), beanClassLoader);
		} catch (ClassNotFoundException e) {
			return entity;
		} catch (LinkageError e) {
			return entity;
		}
	}

	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;

	}

	/**
	 * Ordinal based column comparator is used for column ordering in partition and clustering key parts of the primary
	 * key
	 * 
	 * @author Alex Shvid
	 * 
	 */

	static enum OrdinalBasedPropertyComparator implements Comparator<CassandraPersistentProperty> {

		INSTANCE;

		/*
		 * (non-Javadoc)
		 * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
		 */
		public int compare(CassandraPersistentProperty o1, CassandraPersistentProperty o2) {

			Integer ordinal1 = o1.getOrdinal();
			Integer ordinal2 = o1.getOrdinal();

			if (ordinal1 == null) {
				if (ordinal2 == null) {
					return 0;
				}
				return -1;
			}

			if (ordinal2 == null) {
				return 1;
			}

			return ordinal1.compareTo(ordinal2);
		}

	}

}
