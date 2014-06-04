/*
Copyright 2013-2014 eBay Software Foundation
 
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 
    http://www.apache.org/licenses/LICENSE-2.0
 
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.pig.oink.common.service.impl;


import java.net.URI;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.apache.log4j.Logger;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class HttpNotification implements Runnable {

	private static final Logger logger = Logger.getLogger(HttpNotification.class);
	private ClientConfig config = new DefaultClientConfig();
	private Client client = Client.create(config);
	private String url;
	private String responseCode, responseBody;
	
	public HttpNotification(String url) {
		this.url = url;
	}
	
	@Override
	public void run() {
		int maxRetry = 3;
		while(maxRetry > 0) {
			try {
				client.setReadTimeout(30000);
				client.setConnectTimeout(30000);
				URI uri = UriBuilder.fromUri(url).build();
				WebResource service = client.resource(uri);
				responseBody = service.accept(MediaType.TEXT_PLAIN).get(String.class);
				responseCode = "200";
				logger.info("Successfully called service for " + url);
				break;
			} catch(Exception e) {
				responseCode = "500";
				responseBody = e.getMessage();
				logger.error("Error occurred while contacting service " + url + " Msg :" + e.getMessage(), e);
				maxRetry--;
			}
		}
	}
	
	public String getResponseCode() {
		return responseCode;
	}
	
	public String getResponseBody() {
		return responseBody;
	}
}
