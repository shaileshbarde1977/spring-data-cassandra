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
package net.webby.cassandrion.data.test.integration.table;

import java.util.Date;

import net.webby.cassandrion.core.KeyPart;
import net.webby.cassandrion.data.mapping.Embeddable;
import net.webby.cassandrion.data.mapping.KeyColumn;
import net.webby.cassandrion.data.mapping.Qualify;

import com.datastax.driver.core.DataType;

/**
 * This is an example of dynamic table that creates each time new column with Notification timestamp.
 * 
 * By default it is active Notification until user deactivate it. This table uses index on the field active to access in
 * WHERE cause only for active notifications.
 * 
 * @author Alex Shvid
 */
@Embeddable
public class NotificationPK {

	/*
	 * Row ID
	 */
	@KeyColumn(keyPart = KeyPart.PARTITION, ordinal = 1)
	private String username;

	/*
	 * Clustered Column
	 */
	@KeyColumn(keyPart = KeyPart.CLUSTERING, ordinal = 1)
	@Qualify(type = DataType.Name.TIMESTAMP)
	private Date time;

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public Date getTime() {
		return time;
	}

	public void setTime(Date time) {
		this.time = time;
	}

}
