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

import org.pig.oink.testutils.ServletTestContext;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestHttpNotification {
	private ServletTestContext testContext = null;
	private String hostport;
	
	@BeforeClass
	public void setUp() throws Exception {
		testContext = new ServletTestContext("/");
		testContext.addServletEndpoint("/regex/aggregate/*", MockServlet.class);
		testContext.startServer();
		hostport = testContext.getHost() + ":" + testContext.getPort();
	}
	
	@AfterClass
	public void tearDown() {
		testContext.stopServer();
	}

	@Test
	public void testHttpNotification() {
		HttpNotification notification = new HttpNotification("http://" + hostport + "/regex/aggregate");
		notification.run();
		Assert.assertEquals(notification.getResponseCode(), "200");
		Assert.assertEquals(notification.getResponseBody(), "");
	}
}
