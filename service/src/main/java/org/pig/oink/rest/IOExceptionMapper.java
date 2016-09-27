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

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.HttpURLConnection;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

import org.apache.log4j.Logger;

/**
 * Handles uncaught IOExceptions. 
 * @author Mihai Andronache (amihaiemil@gmail.com)
 *
 */
@Provider
public class IOExceptionMapper implements ExceptionMapper<IOException>{

	/**
	 * Log4j logger.
	 */
	private Logger logger= Logger.getLogger(IOExceptionMapper.class);

	/**
	 * HTTP request.
	 */
	@Context 
	private HttpServletRequest request;
	
	/**
	 * Maps an IOException to a Response with the body
	 * containing the exception's message. The status code
	 * is 404 NOT FOUND if the exception is FileNotFoundException or
	 * 500 SERVER ERROR otherwise.
	 */
	@Override
	public Response toResponse(IOException exception) {
		
		if (exception instanceof FileNotFoundException) {
			logger.error(
		        "FileNotFoundException when calling " +
		        request.getRequestURL().toString() + 
		        " ! Message: " + exception.getMessage()
		    );
	        return Response.status(HttpURLConnection.HTTP_NOT_FOUND).
	            entity(exception.getMessage()).
	            type(MediaType.TEXT_PLAIN).
	            build();
		}
        logger.error(
            "IOException when calling " +
            request.getRequestURL().toString() + 
            " ! Message: " + exception.getMessage()
        );
        return Response.serverError().
	        entity(exception.getMessage()).
            type(MediaType.TEXT_PLAIN).
            build();
	}
}
