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
 * Handles uncaught IllegalStateExceptions. 
 * Such exception is thrown for instance when a needed file is not found.
 * @author Mihai Andronache (amihaiemil@gmail.com)
 *
 */
@Provider
public class IllegalStateMapper implements ExceptionMapper<IllegalStateException>{

	/**
	 * Log4j logger.
	 */
	private Logger logger= Logger.getLogger(IllegalStateMapper.class);

	/**
	 * HTTP request.
	 */
	@Context 
	private HttpServletRequest request;

    /**
     * IllegalStateException might be thrown from PigResource.registerJar(...) or
     * PigResource.registerScript(...) if the files that are being registered already
     * exist on the server.
     * @return Response with 409 CONFLICT HTTP status and a Json body containing steps
     * to delete the registered file <b>OR</b><br> Response with 500 SERVER ERROR HTTP
     * status if the IllegalStateException occurred for other reasons than the 2 mentioned above.
     */
    @Override
    public Response toResponse(IllegalStateException exception) {
        String url = request.getRequestURL().toString();
    	logger.error(
	        "IllegalStateException when calling " + url + 
	        " ! Message: " + exception.getMessage()
	    );
        if(exception.getMessage().equals("File already exists")) { 
            if(url.contains("/jar/") || url.contains("/script/")) {
                return this.conflictResponse();
    	    }
        }
		return Response.serverError()
		    .entity(exception.getMessage())
		    .type(MediaType.TEXT_PLAIN)
		    .build();
	}

    public Response conflictResponse(){
        String jsonBody = "{"
            + "\"message\":\"File already exists. Try again after performing the fix.\",\n"
        	+ "\"fix\":\"DELETE %s\"\n"
        	+ "}";
        return Response
            .status(HttpURLConnection.HTTP_CONFLICT)
            .entity(String.format(jsonBody, request.getRequestURL().toString()))
            .type(MediaType.APPLICATION_JSON)
            .build();
    }
}
