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
package net.webby.cassandrion.data.repository.config;

import net.webby.cassandrion.data.repository.support.CassandrionRepositoryFactoryBean;

import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.core.annotation.AnnotationAttributes;
import org.springframework.data.config.ParsingUtils;
import org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource;
import org.springframework.data.repository.config.RepositoryConfigurationExtension;
import org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport;
import org.springframework.data.repository.config.XmlRepositoryConfigurationSource;
import org.springframework.util.StringUtils;
import org.w3c.dom.Element;

/**
 * {@link RepositoryConfigurationExtension} for Cassandra.
 * 
 * @author Alex Shvid
 * 
 */
public class CassandrionRepositoryConfigurationExtension extends RepositoryConfigurationExtensionSupport {

	private static final String CASSANDRA_DATA_TEMPLATE_REF = "cassandra-data-template-ref";
	private static final String CREATE_QUERY_INDEXES = "create-query-indexes";

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#getModulePrefix()
	 */
	@Override
	protected String getModulePrefix() {
		return "cassandra";
	}

	/*
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtension#getRepositoryFactoryClassName()
	 */
	public String getRepositoryFactoryClassName() {
		return CassandrionRepositoryFactoryBean.class.getName();
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#postProcess(org.springframework.beans.factory.support.BeanDefinitionBuilder, org.springframework.data.repository.config.XmlRepositoryConfigurationSource)
	 */
	@Override
	public void postProcess(BeanDefinitionBuilder builder, XmlRepositoryConfigurationSource config) {

		Element element = config.getElement();

		ParsingUtils.setPropertyReference(builder, element, CASSANDRA_DATA_TEMPLATE_REF, "cassandraDataTemplate");
		ParsingUtils.setPropertyValue(builder, element, CREATE_QUERY_INDEXES, "createIndexesForQueryMethods");
	}

	/* 
	 * (non-Javadoc)
	 * @see org.springframework.data.repository.config.RepositoryConfigurationExtensionSupport#postProcess(org.springframework.beans.factory.support.BeanDefinitionBuilder, org.springframework.data.repository.config.AnnotationRepositoryConfigurationSource)
	 */
	@Override
	public void postProcess(BeanDefinitionBuilder builder, AnnotationRepositoryConfigurationSource config) {

		AnnotationAttributes attributes = config.getAttributes();

		String cassandraDataTemplateRef = attributes.getString("cassandraDataTemplateRef");
		if (StringUtils.hasText(cassandraDataTemplateRef)) {
			builder.addPropertyReference("cassandraDataTemplate", cassandraDataTemplateRef);
		}
		builder.addPropertyValue("createIndexesForQueryMethods", attributes.getBoolean("createIndexesForQueryMethods"));
	}

}
