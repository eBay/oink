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

package org.pig.oink.testutils;

import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;


/**
 * Tests a Servlet using Servlet context.
 * 
 * @see ServletTestContext
 * <p/>
 * The servlet is started and stopped for each test* method.
 */
public abstract class ServletTestCase {

    private ServletTestContext servletContext = null;
    private String contextPath;
    private String servletPath;
    private Class servletClass;
    
    /**
     * Creates a servlet test case.
     *
     * @param contextPath  context path for the servlet, it must be prefixed with "/"
     *                     for the default context use "/"
     * @param servletPath  servlet path for the servlet, it should be prefixed with '/", it may
     *                     contain a wild card at the end.
     * @param servletClass servlet class
     */
    protected ServletTestCase(String contextPath, String servletPath, Class servletClass) {
        this.contextPath = contextPath;
        this.servletPath = servletPath;
        this.servletClass = servletClass;
    }

    /**
     * Invoked before each test* method, starts the embedded servlet container and binds the
     * configured servlet to it.
     *
     * @throws Exception
     */
    @BeforeTest
    protected void setUp() throws Exception {
        servletContext = new ServletTestContext(contextPath);
        servletContext.addServletEndpoint(servletPath, servletClass);
        servletContext.startServer();
    }

    /**
     * Returns the hostname the servlet container is bound to.
     *
     * @return the hostname
     */
    protected String getHost() {
        return servletContext.getHost();
    }

    /**
     * Returns the port number the servlet container is bound to.
     *
     * @return the port number
     */
    protected int getPort() {
        return servletContext.getPort();
    }

    /**
     * Returns the full URL (including protocol, host, port, context path, servlet path) to the
     * servlet.
     *
     * @return URL to the servlet
     */
    protected String getServletURL() {
        return servletContext.getServletURL(servletPath);
    }

    /**
     * Invoked after each test* method, stops the embedded servlet container.
     *
     * @throws Exception
     */
    @AfterTest
    protected void tearDown() throws Exception {
        servletContext.stopServer();
    }
}
