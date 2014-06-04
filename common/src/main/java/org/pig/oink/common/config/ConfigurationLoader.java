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
