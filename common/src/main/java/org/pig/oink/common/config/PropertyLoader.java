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

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;


public class PropertyLoader {

	private static final Logger logger = Logger.getLogger(PropertyLoader.class);
	private static Properties properties = new Properties();
	private static final PropertyLoader instance = new  PropertyLoader();
	
	private PropertyLoader() {
	}
	
	public static PropertyLoader getInstance() {
		return instance;
	}

	public void init(String path) {
		if(path == null) {
		    throw  new IllegalArgumentException("Property file can not be null");
		}
		InputStream in = getClass().getResourceAsStream(path);
		if(in == null) {
			in = ClassLoader.getSystemResourceAsStream(path);
		}
		try {
			properties.load(in);
		} catch(IOException ie) {
			logger.error("Configuration not loaded properly. Error : " + ie.getMessage(), ie);
		}
	}
	
	public String getProperty(String key) {
		return System.getProperty(key, properties.getProperty(key));
	}
	
	//this is mainly for test method
	public void setProperty(String key, String value) {
		properties.setProperty(key, value);
	}
	
	public Set<Object> getKeySet() {
		return properties.keySet();
	}
}
