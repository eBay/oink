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
package org.pig.oink.rest;

import java.net.HttpURLConnection;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

/**
 * Handles uncaught IllegalArgumentExceptions. 
 * Such exception is thrown for instance when a needed file is not found.
 * @author Mihai Andronache (amihaiemil@gmail.com)
 *
 */
@Provider
public class IllegalArgumentMapper implements ExceptionMapper<IllegalArgumentException>{

	/**
	 * Log4j logger.
	 */
	private Logger logger= Logger.getLogger(IllegalArgumentMapper.class);

	/**
	 * HTTP request.
	 */
	@Context 
	private HttpServletRequest request;
	
	/**
	 * Maps IllegalArgumentException to a Response with HTTP 404 status and
	 * the body containing the exception's message.
	 */
	@Override
	public Response toResponse(IllegalArgumentException exception) {
        logger.error(
            "IllegalArgumentException when calling " +
            request.getRequestURL().toString() + 
            " ! Message: " + exception.getMessage()
        );
        return Response.status(HttpURLConnection.HTTP_NOT_FOUND).
            entity(exception.getMessage()).
            type(MediaType.TEXT_PLAIN).
            build();
	}

}
