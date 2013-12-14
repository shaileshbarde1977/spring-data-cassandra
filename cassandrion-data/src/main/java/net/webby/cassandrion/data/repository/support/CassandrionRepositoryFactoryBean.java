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
import net.webby.cassandrion.data.repository.CassandrionRepository;

import org.springframework.data.repository.Repository;
import org.springframework.data.repository.core.support.RepositoryFactoryBeanSupport;
import org.springframework.data.repository.core.support.RepositoryFactorySupport;
import org.springframework.util.Assert;

/**
 * {@link org.springframework.beans.factory.FactoryBean} to create {@link CassandrionRepository} instances.
 * 
 * @author Alex Shvid
 * 
 */
public class CassandrionRepositoryFactoryBean<T extends Repository<S, ID>, S, ID extends Serializable> extends
		RepositoryFactoryBeanSupport<T, S, ID> {

	private CassandrionDataTemplate cassandrionDataTemplate;

	@Override
	protected RepositoryFactorySupport createRepositoryFactory() {
		return new CassandrionRepositoryFactory(cassandrionDataTemplate);
	}

	/**
	 * Configures the {@link CassandrionDataTemplate} to be used.
	 * 
	 * @param operations the operations to set
	 */
	public void setCassandrionDataTemplate(CassandrionDataTemplate cassandrionDataTemplate) {
		this.cassandrionDataTemplate = cassandrionDataTemplate;
		setMappingContext(cassandrionDataTemplate.getConverter().getMappingContext());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see
	 * org.springframework.data.repository.support.RepositoryFactoryBeanSupport
	 * #afterPropertiesSet()
	 */
	@Override
	public void afterPropertiesSet() {
		super.afterPropertiesSet();
		Assert.notNull(cassandrionDataTemplate, "cassandrionDataTemplate must not be null!");
	}

}
