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

import java.io.InputStream;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.PropertyConfigurator;

public class ConfigurationLoader implements ServletContextListener {
	@Override
	public void contextInitialized(ServletContextEvent se) {
		String logFileName = se.getServletContext().getInitParameter("log4jFileName");
		String env= System.getenv("env");
		
		String confFileName;
		if (env == null){
			confFileName = se.getServletContext().getInitParameter("configFileName");
		}else{
			confFileName= "/" + env + ".properties";
		}
		
		if(logFileName != null) {
			InputStream in = getClass().getResourceAsStream(logFileName);
			if(in == null) {
				in = ClassLoader.getSystemResourceAsStream(logFileName);
			}
			try {
				PropertyConfigurator.configure(in);
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		if (confFileName != null){
			PropertyLoader.getInstance().init(confFileName);
		}
	}

	@Override
	public void contextDestroyed(ServletContextEvent arg0) {		
	}
}
