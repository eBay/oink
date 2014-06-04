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

import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

import org.pig.oink.bean.PigRequestParameters;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.Gson;

public class TestPigRequestParamters {
	@Test
	public void testPigRequestParameters(){
		PigRequestParameters params= new PigRequestParameters();
		params.setHttpCallback("123");
		Assert.assertEquals(params.getHttpCallback(), "123");
		
		Map<String, String> inputParams= new HashMap<String, String>();
		inputParams.put("env", "prod");
		params.setInputParameters(inputParams);
		
		params.setPigScript("hello.pig");
		Assert.assertEquals(params.getPigScript(), "hello.pig");
		
		params.setRequestIp("123.124.124.12");
		Assert.assertEquals(params.getRequestIp(), "123.124.124.12");
		
		params.setRequestStartTime(Calendar.getInstance().getTime());
		Assert.assertNotNull(params.getRequestStartTime());
		
		Gson gson= new Gson();
		String jsonStr= gson.toJson(params, PigRequestParameters.class);
		
		Assert.assertEquals(jsonStr.contains("hello.pig"), true);
		
		jsonStr= jsonStr.replace("hello.pig", "Hello.pig");
		
		params= gson.fromJson(jsonStr, PigRequestParameters.class);
		Assert.assertEquals(params.getPigScript(), "Hello.pig");
	}

}
