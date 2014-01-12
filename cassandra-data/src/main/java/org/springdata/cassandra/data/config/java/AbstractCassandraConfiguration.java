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
package org.springdata.cassandra.data.config.java;

import java.util.HashSet;
import java.util.Set;

import org.springdata.cassandra.base.config.KeyspaceAttributes;
import org.springdata.cassandra.base.core.CassandraCqlOperations;
import org.springdata.cassandra.base.core.CassandraCqlTemplate;
import org.springdata.cassandra.data.convert.CassandraConverter;
import org.springdata.cassandra.data.convert.MappingCassandraConverter;
import org.springdata.cassandra.data.core.CassandraDataOperations;
import org.springdata.cassandra.data.core.CassandraDataTemplate;
import org.springdata.cassandra.data.core.CassandraSessionFactoryBean;
import org.springdata.cassandra.data.mapping.CassandraMappingContext;
import org.springdata.cassandra.data.mapping.CassandraPersistentEntity;
import org.springdata.cassandra.data.mapping.CassandraPersistentProperty;
import org.springdata.cassandra.data.mapping.Table;
import org.springframework.beans.factory.BeanClassLoaderAware;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ClassPathScanningCandidateComponentProvider;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.type.filter.AnnotationTypeFilter;
import org.springframework.data.annotation.Persistent;
import org.springframework.data.mapping.context.MappingContext;
import org.springframework.util.ClassUtils;
import org.springframework.util.StringUtils;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

/**
 * Base class for Spring Data Cassandra configuration using JavaConfig.
 * 
 * @author Alex Shvid
 */
@Configuration
public abstract class AbstractCassandraConfiguration implements BeanClassLoaderAware {

	/**
	 * Used by CassandraConverter
	 */
	private ClassLoader beanClassLoader;

	/**
	 * Return the name of the keyspace to connect to.
	 * 
	 * @return for {@literal null} value will be used SYSTEM keyspace.
	 */
	protected abstract String keyspace();

	/**
	 * Return the {@link Cluster} instance to connect to.
	 * 
	 * @return
	 * @throws Exception
	 */
	@Bean
	public abstract Cluster cluster();

	/**
	 * Return keyspace attributes
	 * 
	 * @return KeyspaceAttributes
	 */
	@Bean
	public KeyspaceAttributes keyspaceAttributes() {
		return new KeyspaceAttributes();
	}

	/**
	 * Creates a {@link Session}. Will create, verify or drop tables in Cassandra on creation/destroy stage.
	 * 
	 * @see #cluster()
	 * @see #Keyspace()
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public Session session() throws ClassNotFoundException {
		CassandraSessionFactoryBean factory = new CassandraSessionFactoryBean();
		factory.setKeyspace(keyspace());
		factory.setCluster(cluster());
		factory.setConverter(converter());
		factory.setKeyspaceAttributes(keyspaceAttributes());
		factory.setBeanClassLoader(beanClassLoader);
		factory.afterPropertiesSet();
		return factory.getObject();
	}

	/**
	 * Return the base package to scan for mapped {@link Table}s. Will return the package name of the configuration class'
	 * (the concrete class, not this one here) by default. So if you have a {@code com.acme.AppConfig} extending
	 * {@link AbstractCassandraConfiguration} the base package will be considered {@code com.acme} unless the method is
	 * overriden to implement alternate behaviour.
	 * 
	 * @return the base package to scan for mapped {@link Table} classes or {@literal null} to not enable scanning for
	 *         entities.
	 */
	protected String getMappingBasePackage() {
		return getClass().getPackage().getName();
	}

	/**
	 * Creates a {@link CassandraCqlTemplate}.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public CassandraCqlOperations cassandraTemplate() throws ClassNotFoundException {
		return new CassandraCqlTemplate(session(), keyspace());
	}

	/**
	 * Creates a {@link CassandraDataTemplate}.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public CassandraDataOperations cassandraDataTemplate() throws ClassNotFoundException {
		return new CassandraDataTemplate(session(), converter(), keyspace());
	}

	/**
	 * Return the {@link MappingContext} instance to map Entities to properties.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext()
			throws ClassNotFoundException {
		CassandraMappingContext context = new CassandraMappingContext();
		context.setInitialEntitySet(getInitialEntitySet());
		return context;
	}

	/**
	 * Return the {@link CassandraConverter} instance to convert Rows to Objects, Objects to BuiltStatements
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public CassandraConverter converter() throws ClassNotFoundException {
		MappingCassandraConverter converter = new MappingCassandraConverter(mappingContext());
		converter.setBeanClassLoader(beanClassLoader);
		return converter;
	}

	/**
	 * Scans the mapping base package for classes annotated with {@link Table}.
	 * 
	 * @see #getMappingBasePackage()
	 * @return
	 * @throws ClassNotFoundException
	 */
	protected Set<Class<?>> getInitialEntitySet() throws ClassNotFoundException {

		String basePackage = getMappingBasePackage();
		Set<Class<?>> initialEntitySet = new HashSet<Class<?>>();

		if (StringUtils.hasText(basePackage)) {
			ClassPathScanningCandidateComponentProvider componentProvider = new ClassPathScanningCandidateComponentProvider(
					false);
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Table.class));
			componentProvider.addIncludeFilter(new AnnotationTypeFilter(Persistent.class));

			for (BeanDefinition candidate : componentProvider.findCandidateComponents(basePackage)) {
				initialEntitySet.add(ClassUtils.forName(candidate.getBeanClassName(), beanClassLoader));
			}
		}

		return initialEntitySet;
	}

	/**
	 * Bean ClassLoader Aware for CassandraTemplate/CassandraAdminTemplate
	 */
	@Override
	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

}
