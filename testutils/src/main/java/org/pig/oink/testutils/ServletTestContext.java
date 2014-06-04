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

import org.mortbay.jetty.Server;
import org.mortbay.jetty.bio.SocketConnector;
import org.mortbay.jetty.servlet.Context;


/**
 * This class provides wrapper over embedded servlet container. 
 * <p/>
 * Provides ability add multiple end points. 
 * <p/>
 * The servlet container is started in a free port.
 * <p/>
 */
public class ServletTestContext {
    private Server webServer;
    private String host = null;
    private int port = -1;
    private String contextPath;
    private Context context;
    
    /**
     * Creates a servlet test context.
     * 
     * @param contextPath  context path for the servlet, it must be prefixed with "/",
     *                     for the default context use "/"
     */
    public ServletTestContext(String contextPath) {
        if(!contextPath.startsWith("/")) {
            contextPath = "/" + contextPath;
        }

        this.contextPath = contextPath;        
        webServer = new org.mortbay.jetty.Server();
        
        // create context
        context = new Context(webServer, contextPath);
    }

    /**
     * Adds new servlet endpoint.
     * @param servletPath  servlet path for the servlet, it should be prefixed with '/", it may
     *                     contain a wild card at the end.
     * @param servletClass servlet class
     * @throws Exception
     */
    public void addServletEndpoint(String servletPath, Class servletClass) {
        context.addServlet(servletClass, servletPath);
    }
    
    /**
     * Start embedded server.
     * @throws Exception
     */
    public void startServer() throws Exception {
        SocketConnector connector = new SocketConnector();
        connector.setPort(0);
        connector.setHost("127.0.0.1");
        webServer.addConnector(connector);

        webServer.start();
        port = connector.getLocalPort();
        host = connector.getHost();
    }                  
    
    /**
     * Returns the hostname the servlet container is bound to.
     *
     * @return the hostname
     */
    public String getHost() {
        return host;
    }

    /**
     * Returns the port number the servlet container is bound to.
     *
     * @return the port number
     */
    public int getPort() {
        return port;
    }

    /**
     * Returns the full URL (including protocol, host, port, context path, servlet path) to the
     * servlet.
     *
     * @return URL to the servlet
     */
    public String getServletURL(String servletPath) {
        String path = servletPath;
        if (path.endsWith("*")) {
            path = path.substring(0, path.length() - 1);
        }
        return "http://" + host + ":" + port + contextPath + path;
    }

    /**
     * Invoke to stop the embedded servlet container.
     */
    public void stopServer() {
        try {
        webServer.stop();
        }
        catch (Exception e) {
            // ignore exception
        }

        try {
            webServer.destroy();
        }
        catch (Exception e) {
            // ignore exception
        }
        
        host = null;
        port = -1;
    }
}
