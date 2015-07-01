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

package org.pig.oink.operation;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.Response;

import junit.framework.Assert;

import org.apache.hadoop.conf.Configuration;
import org.pig.oink.bean.PigRequestParameters;
import org.pig.oink.common.config.PropertyLoader;
import org.pig.oink.operation.impl.SubmitTask;
import org.pig.oink.rest.PigResource;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestSubmitTask {
	private SubmitTask submitTask;
	private static final String HADOOP_TMP_PATH = "target/hadoop";

    static {
        System.setProperty("java.tmp.dir", HADOOP_TMP_PATH);
    }
    
    @BeforeClass
    protected void setUp() throws Exception {
    	PropertyLoader.getInstance().init("/test.properties");
        PropertyLoader.getInstance().setProperty("scripts.basepath", HADOOP_TMP_PATH + "/pig/scripts/");
        PropertyLoader.getInstance().setProperty("jars.basepath",  HADOOP_TMP_PATH + "/pig/jars/");
        PropertyLoader.getInstance().setProperty("requests.basepath",  HADOOP_TMP_PATH + "/pig/requests/");
    	PropertyLoader.getInstance().setProperty("fs.defaultFS", "file:///");
        new File(HADOOP_TMP_PATH).mkdirs();
    }
    
    @Test
    public void testSubmitTask() throws Exception {
    	PigRequestParameters params= new PigRequestParameters();
    	Map<String, String> map= new HashMap<String, String>();
    	map.put("a", "val");
    	params.setInputParameters(map);
    	
    	submitTask= new  SubmitTask("123", "a.pig", params);
    	submitTask.run();
    	
    	PigResource resource= new PigResource();
    	Response resp= resource.getRequestStats("123");
    	
    	Assert.assertEquals(resp.getEntity().toString().contains("FAILED"), true);
    }
	
}
