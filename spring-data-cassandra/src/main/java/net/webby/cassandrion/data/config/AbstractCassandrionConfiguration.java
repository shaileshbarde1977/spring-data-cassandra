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
package net.webby.cassandrion.data.config;

import java.util.HashSet;
import java.util.Set;

import net.webby.cassandrion.core.CassandrionOperations;
import net.webby.cassandrion.core.CassandrionTemplate;
import net.webby.cassandrion.data.convert.CassandrionConverter;
import net.webby.cassandrion.data.convert.MappingCassandrionConverter;
import net.webby.cassandrion.data.core.CassandrionAdminOperations;
import net.webby.cassandrion.data.core.CassandrionAdminTemplate;
import net.webby.cassandrion.data.core.CassandrionDataOperations;
import net.webby.cassandrion.data.core.CassandrionDataTemplate;
import net.webby.cassandrion.data.core.CassandrionSessionFactoryBean;
import net.webby.cassandrion.data.mapping.CassandrionMappingContext;
import net.webby.cassandrion.data.mapping.CassandraPersistentEntity;
import net.webby.cassandrion.data.mapping.CassandraPersistentProperty;
import net.webby.cassandrion.data.mapping.Table;

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
public abstract class AbstractCassandrionConfiguration implements BeanClassLoaderAware {

	/**
	 * Used by CassandraTemplate and CassandraAdminTemplate
	 */

	private ClassLoader beanClassLoader;

	/**
	 * Return the name of the keyspace to connect to.
	 * 
	 * @return must not be {@literal null}.
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

	@Bean
	public KeyspaceAttributes keyspaceAttributes() {
		return new KeyspaceAttributes();
	}

	/**
	 * Creates a {@link Session} to be used by the {@link SpringDataKeyspace}. Will use the {@link Cluster} instance
	 * configured in {@link #cluster()}.
	 * 
	 * @see #cluster()
	 * @see #Keyspace()
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public Session session() throws ClassNotFoundException {
		CassandrionSessionFactoryBean factory = new CassandrionSessionFactoryBean();
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
	 * {@link AbstractCassandrionConfiguration} the base package will be considered {@code com.acme} unless the method is
	 * overriden to implement alternate behaviour.
	 * 
	 * @return the base package to scan for mapped {@link Table} classes or {@literal null} to not enable scanning for
	 *         entities.
	 */
	protected String getMappingBasePackage() {
		return getClass().getPackage().getName();
	}

	/**
	 * Creates a {@link CassandrionTemplate}.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public CassandrionOperations cassandraTemplate() throws ClassNotFoundException {
		return new CassandrionTemplate(session());
	}

	/**
	 * Creates a {@link CassandrionDataTemplate}.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public CassandrionDataOperations cassandraDataTemplate() throws ClassNotFoundException {
		return new CassandrionDataTemplate(session(), converter(), keyspace());
	}

	/**
	 * Creates a {@link CassandrionAdminTemplate}.
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public CassandrionAdminOperations cassandraAdminTemplate() throws ClassNotFoundException {
		return new CassandrionAdminTemplate(session(), converter(), keyspace());
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
		CassandrionMappingContext context = new CassandrionMappingContext();
		context.setInitialEntitySet(getInitialEntitySet());
		return context;
	}

	/**
	 * Return the {@link CassandrionConverter} instance to convert Rows to Objects, Objects to BuiltStatements
	 * 
	 * @return
	 * @throws ClassNotFoundException
	 * @throws Exception
	 */
	@Bean
	public CassandrionConverter converter() throws ClassNotFoundException {
		MappingCassandrionConverter converter = new MappingCassandrionConverter(mappingContext());
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

	public void setBeanClassLoader(ClassLoader classLoader) {
		this.beanClassLoader = classLoader;
	}

}
