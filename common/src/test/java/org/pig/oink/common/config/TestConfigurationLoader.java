package org.pig.oink.common.config;

import static org.easymock.EasyMock.createMock;
import static org.easymock.EasyMock.expect;
import static org.easymock.EasyMock.replay;
import static org.easymock.EasyMock.verify;

import javax.servlet.ServletContext;
import javax.servlet.ServletContextEvent;

import org.pig.oink.common.config.ConfigurationLoader;
import org.testng.annotations.Test;

public class TestConfigurationLoader {

	@Test
	public void testConfigurationLoader() {
		ServletContextEvent event = org.easymock.classextension.EasyMock.createMock(ServletContextEvent.class);
		ServletContext ctx = createMock(ServletContext.class);
		expect(event.getServletContext()).andReturn(ctx);
		expect(ctx.getInitParameter("log4jFileName")).andReturn("log4j.xml");
		expect(event.getServletContext()).andReturn(ctx);
		expect(ctx.getInitParameter("configFileName")).andReturn("/TraverserTests.properties");
		org.easymock.classextension.EasyMock.replay(event);
		replay(ctx);
		ConfigurationLoader loader = new ConfigurationLoader();
		loader.contextInitialized(event);
		loader.contextDestroyed(event);
		verify(ctx);
		org.easymock.classextension.EasyMock.verify(event);

	}
	
	@Test
	public void testConfigurationLoader1() {
		ServletContextEvent event = org.easymock.classextension.EasyMock.createMock(ServletContextEvent.class);
		ServletContext ctx = createMock(ServletContext.class);
		expect(event.getServletContext()).andReturn(ctx);
		expect(ctx.getInitParameter("log4jFileName")).andReturn("log4j.xml1");
		expect(event.getServletContext()).andReturn(ctx);
		expect(ctx.getInitParameter("configFileName")).andReturn("/TraverserTests.properties");
		org.easymock.classextension.EasyMock.replay(event);
		replay(ctx);
		ConfigurationLoader loader = new ConfigurationLoader();
		loader.contextInitialized(event);
		loader.contextDestroyed(event);
		verify(ctx);
		org.easymock.classextension.EasyMock.verify(event);

	}
}
