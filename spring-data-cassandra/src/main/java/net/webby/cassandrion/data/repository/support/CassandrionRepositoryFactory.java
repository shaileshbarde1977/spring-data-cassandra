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
package net.webby.cassandrion.data.repository.support;

import java.io.Serializable;

import net.webby.cassandrion.data.core.CassandrionDataTemplate;
import net.webby.cassandrion.data.mapping.CassandraPersistentEntity;
import net.webby.cassandrion.data.mapping.CassandraPersistentProperty;
import net.webby.cassandrion.data.repository.CassandrionRepository;
import net.webby.cassandrion.data.repository.query.CassandrionEntityInformation;

import org.springframework.data.mapping.context.MappingContext;
import org.springframework.data.mapping.model.MappingException;
import org.springframework.data.repository.core.RepositoryMetadata;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

/**
 * Factory to create {@link CassandrionRepository} instances.
 * 
 * @author Alex Shvid
 * 
 */

public class CassandrionRepositoryFactory extends RepositoryFactorySupport {

	private final CassandrionDataTemplate cassandraDataTemplate;
	private final MappingContext<? extends CassandraPersistentEntity<?>, CassandraPersistentProperty> mappingContext;

	/**
	 * Creates a new {@link MongoRepositoryFactory} with the given {@link MongoOperations}.
	 * 
	 * @param mongoOperations must not be {@literal null}
	 */
	public CassandrionRepositoryFactory(CassandrionDataTemplate cassandraDataTemplate) {

		Assert.notNull(cassandraDataTemplate);

		this.cassandraDataTemplate = cassandraDataTemplate;
		this.mappingContext = cassandraDataTemplate.getConverter().getMappingContext();
	}

	@Override
	protected Class<?> getRepositoryBaseClass(RepositoryMetadata metadata) {
		return SimpleCassandrionRepository.class;
	}

	@Override
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected Object getTargetRepository(RepositoryMetadata metadata) {

		CassandrionEntityInformation<?, Serializable> entityInformation = getEntityInformation(metadata.getDomainType());

		return new SimpleCassandrionRepository(entityInformation, cassandraDataTemplate);

	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.core.support.RepositoryFactorySupport#getEntityInformation(java.lang.Class)
	 */
	@Override
	@SuppressWarnings("unchecked")
	public <T, ID extends Serializable> CassandrionEntityInformation<T, ID> getEntityInformation(Class<T> domainClass) {

		CassandraPersistentEntity<?> entity = mappingContext.getPersistentEntity(domainClass);

		if (entity == null) {
			throw new MappingException(String.format("Could not lookup mapping metadata for domain class %s!",
					domainClass.getName()));
		}

		return new MappingCassandrionEntityInformation<T, ID>((CassandraPersistentEntity<T>) entity);
	}
}
