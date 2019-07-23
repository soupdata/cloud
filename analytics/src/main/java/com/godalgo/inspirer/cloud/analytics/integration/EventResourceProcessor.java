/*
 * Copyright 2014 the original author or authors.
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
 *//*

package com.godalgo.inspirer.cloud.analytics.integration;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Provider;
import javax.servlet.http.HttpServletRequest;

import com.godalgo.inspirer.cloud.analytics.Location;
import com.godalgo.inspirer.cloud.analytics.domain.Event;
import lombok.RequiredArgsConstructor;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.hateoas.Link;
import org.springframework.hateoas.Resource;
import org.springframework.hateoas.ResourceProcessor;
import org.springframework.stereotype.Component;

*/
/**
 * @author Oliver Gierke
 *//*

@Component
@RequiredArgsConstructor(onConstructor = @__(@Autowired))
public class EventResourceProcessor implements ResourceProcessor<Resource<Event>> {

	private static final String X_FORWARDED_HOST = "X-Forwarded-Host";
	private final ClientIntegration clientIntegration;
	private final Provider<HttpServletRequest> request;

	@Override
	public Resource<Event> process(Resource<Event> resource) {

		Event event = resource.getContent();
		Location location = event.getAddress().getLocation();

		Map<String, Object> parameters = new HashMap<>();
		parameters.put("location", String.format("%s,%s", location.getLatitude(), location.getLongitude()));
		parameters.put("distance", "50km");
		String host = this.request.get().getHeader(X_FORWARDED_HOST);
		Link link = this.clientIntegration.getStoresByLocationLink(parameters, host);
		if (link != null) {
			resource.add(link.withRel("stores-nearby"));
		}

		return resource;
	}
}
*/
