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

package org.pig.oink.bean;

import junit.framework.Assert;

import org.pig.oink.bean.PigRequestStats;
import org.testng.annotations.Test;

import com.google.gson.Gson;

public class TestRequestStats {
	@Test
	public void testRequestStats(){
		PigRequestStats stats= new PigRequestStats(123, 1234, "ErrorMsg", 5);
		
		Assert.assertEquals(stats.getBytesWritten(), 123);
		Assert.assertEquals(stats.getDuration(), 1234);
		Assert.assertEquals(stats.getErrorMessage(), "ErrorMsg");
		Assert.assertEquals(stats.getNumberOfJobs(), 5);
		
		stats.setBytesWritten(12);
		stats.setDuration(123);
		stats.setErrorMessage("ErrMsg");
		stats.setNumberOfJobs(10);
		
		Assert.assertEquals(stats.getBytesWritten(), 12);
		Assert.assertEquals(stats.getDuration(), 123);
		Assert.assertEquals(stats.getErrorMessage(), "ErrMsg");
		Assert.assertEquals(stats.getNumberOfJobs(), 10);
		
		stats.setProgress(100);
		stats.setStatus("SUCCEEDED");
		
		Assert.assertEquals(stats.getStatus(), "SUCCEEDED");
		Assert.assertEquals(stats.getProgress(), 100);
		
		Gson gson= new Gson();
		String jsonStr= gson.toJson(stats, PigRequestStats.class);
		
		Assert.assertEquals(jsonStr.contains("SUCCEEDED"), true);
		
		jsonStr= jsonStr.replace("SUCCEEDED", "SUBMITTED");
		stats= gson.fromJson(jsonStr, PigRequestStats.class);
		
		Assert.assertEquals(stats.getStatus(), "SUBMITTED");
		
	}

}
