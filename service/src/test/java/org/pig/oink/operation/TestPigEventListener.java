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

import junit.framework.Assert;

import org.pig.oink.common.config.PropertyLoader;
import org.pig.oink.operation.impl.PigEventListener;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

public class TestPigEventListener {

	private PigEventListener listener= new PigEventListener(null, "abc");
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
    	PropertyLoader.getInstance().setProperty("fs.default.name", "file:///");
        new File(HADOOP_TMP_PATH).mkdirs();
    }
    
    @Test
    public void testPigEventListener() throws Exception {
    	listener.initialPlanNotification("abc", null);
    	listener.launchCompletedNotification("abc", 1);
    	listener.launchStartedNotification("abc", 1);
    	listener.outputCompletedNotification("abc", null);
    	
    	
    	listener.progressUpdatedNotification("abc", 50);
    	Assert.assertEquals(listener.getStats().getProgress(), 50);
    	
    	listener.jobsSubmittedNotification("abc", 1);
    	Assert.assertEquals(listener.getStats().getNumberOfJobs(), 1);
    	
    	listener.jobStartedNotification("abc", "job_123");
    	Assert.assertEquals(listener.getStats().getJobs().size(), 1);
    	
    }
	
}
