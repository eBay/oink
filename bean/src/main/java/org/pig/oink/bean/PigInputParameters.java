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

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PigInputParameters {
	private Map<String, String> inputParameters = new HashMap<String, String>();
	private String httpCallback;
	
	public void setInputParameters(Map<String, String> inputParameters) {
		this.inputParameters = inputParameters;
	}
	public Map<String, String> getInputParameters() {
		return inputParameters;
	}
	public void setHttpCallback(String httpCallback) {
		this.httpCallback = httpCallback;
	}
	public String getHttpCallback() {
		return httpCallback;
	}
	
}
