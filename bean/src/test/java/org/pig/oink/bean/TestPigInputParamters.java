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

import java.util.HashMap;
import java.util.Map;

import org.pig.oink.bean.PigInputParameters;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.Gson;

public class TestPigInputParamters {
	@Test
	public void testPigInputParamters(){
		PigInputParameters input= new PigInputParameters();
		
		input.setHttpCallback("123");
		Assert.assertEquals(input.getHttpCallback(), "123");
		
		Map<String, String> inputParams= new HashMap<String, String>();
		inputParams.put("env", "prod");
		input.setInputParameters(inputParams);
		
		Assert.assertEquals(input.getInputParameters().get("env"), "prod");
		
		Gson gson= new Gson();
		String jsonStr= gson.toJson(input, PigInputParameters.class);
		
		Assert.assertEquals(jsonStr.contains("prod"), true);
		Assert.assertEquals(jsonStr.contains("123"), true);
		
		jsonStr= jsonStr.replace("prod", "preprod");
		jsonStr= jsonStr.replace("123"	, "245");
		
		input= gson.fromJson(jsonStr, PigInputParameters.class);
		Assert.assertEquals(input.getHttpCallback(), "245");
		
		Assert.assertEquals(input.getInputParameters().get("env"), "preprod");
		
	}
}
