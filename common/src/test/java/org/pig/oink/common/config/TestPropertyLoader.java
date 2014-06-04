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

package org.pig.oink.common.config;

import org.pig.oink.common.config.PropertyLoader;
import org.testng.Assert;
import org.testng.annotations.Test;

public class TestPropertyLoader {

	@Test
	public void testPropertyLoader() {
		PropertyLoader.getInstance().init("TraverserTests.properties");
		Assert.assertNotNull(PropertyLoader.getInstance().getProperty("FileTraverser.basePath"));
		Assert.assertNotNull(PropertyLoader.getInstance().getProperty("HDFSTraverser.basePath"));
		PropertyLoader.getInstance().setProperty("test", "test1");
		Assert.assertEquals(PropertyLoader.getInstance().getProperty("test"), "test1");
		System.setProperty("test1", "test2");
		Assert.assertEquals(PropertyLoader.getInstance().getProperty("test1"), "test2");
		Assert.assertNull(PropertyLoader.getInstance().getProperty("test2"));
		
		try {
			PropertyLoader.getInstance().init(null);
			Assert.fail();
		} catch(IllegalArgumentException e) {
		}
		try {
			PropertyLoader.getInstance().init("abc");
			Assert.fail();
		} catch(Exception e) {
		}

	}
}
