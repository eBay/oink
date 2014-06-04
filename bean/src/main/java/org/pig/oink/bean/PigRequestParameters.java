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

import java.util.Date;
import java.util.Map;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class PigRequestParameters {
	private Map<String, String> inputParameters;
	private String httpCallback;
	private String pigScript;
	private Date requestStartTime;
	private String requestIp;
	
	public void setPigScript(String pigScript) {
		this.pigScript = pigScript;
	}

	public String getPigScript() {
		return pigScript;
	}

	public void setInputParameters(Map<String, String> inputParameters) {
		this.inputParameters = inputParameters;
	}

	public Map<String, String> getInputParameters() {
		return inputParameters;
	}

	public void setRequestStartTime(Date requestStartTime) {
		this.requestStartTime = requestStartTime;
	}

	public Date getRequestStartTime() {
		return requestStartTime;
	}

	public void setRequestIp(String requestIp) {
		this.requestIp = requestIp;
	}

	public String getRequestIp() {
		return requestIp;
	}

	public void setHttpCallback(String httpCallback) {
		this.httpCallback = httpCallback;
	}

	public String getHttpCallback() {
		return httpCallback;
	}
	
}
