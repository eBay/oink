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

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.Calendar;
import java.util.Date;
import java.util.UUID;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.pig.oink.bean.PigInputParameters;
import org.pig.oink.bean.PigRequestParameters;
import org.pig.oink.bean.PigRequestStats;
import org.pig.oink.common.config.PropertyLoader;
import org.pig.oink.commons.Constants;
import org.pig.oink.commons.PigScriptValidator;
import org.pig.oink.operation.impl.PigJobServerImpl;
import org.pig.oink.operation.impl.StreamingBinOutputImpl;
import org.pig.oink.operation.impl.StreamingPigOutputImpl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

@Path("/")
public class PigResource {
	private Logger logger= Logger.getLogger(PigResource.class);
	
	
	@Context 
	private HttpServletRequest requestInfo;

	@POST
	@Path("/jar/{jarName}")
	@Consumes ( {MediaType.APPLICATION_OCTET_STREAM} )
	@Produces ({MediaType.TEXT_PLAIN})
	public Response registerJar(@PathParam("jarName") String jarName, InputStream uploadedJar) throws IOException {
		logger.info("Request for registring jar with name " + jarName);
		try {
			if (uploadedJar == null || uploadedJar.available() == 0){
				logger.error("Empty input stream passed");
				return Response.status(400).entity("Bad request. No jar uploaded!").build();
			}
			String pathName= PropertyLoader.getInstance().getProperty(Constants.JARS_PATH) + jarName;
			PigJobServerImpl.getPigJobServer().registerFile(pathName, uploadedJar);
			return Response.ok().entity(pathName).build();
		} catch (IllegalArgumentException iae) {
			logger.error("Error while registering jar " + jarName, iae);
			throw new WebApplicationException(Response.status(409).entity(iae.getMessage()).build());
		} catch (IOException ie) {
			logger.error("Error while registering jar " + jarName, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		} catch (Exception e) {
			logger.error("Error while registering jar " + jarName, e);
			throw new WebApplicationException(Response.status(500).entity(e.getMessage()).build());
		}
	}
	   
	@DELETE
	@Path("/jar/{jarName}")
	@Produces ( {MediaType.TEXT_PLAIN} )
	public Response unregisterJar(@PathParam("jarName") String jarName) {
		logger.info("Request for deleting jar " + jarName);
		try {
			String pathName= PropertyLoader.getInstance().getProperty(Constants.JARS_PATH) + jarName;
			PigJobServerImpl.getPigJobServer().unregisterFile(pathName);
			return Response.ok().entity("Delete Successful").build();
		} catch (FileNotFoundException fnfe) {
			logger.error("Error while deleting file " + jarName, fnfe);
			throw new WebApplicationException(Response.status(404).entity(fnfe.getMessage()).build());
		} catch (IOException ie) {
			logger.error("Error while deleting file " + jarName, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		} catch (Exception e) {
			logger.error("Error while deleting file " + jarName, e);
			throw new WebApplicationException(Response.status(500).entity(e.getMessage()).build());
		}
	}
	
	@GET
	@Path("/jar/{jarName}")
	@Produces ( {MediaType.APPLICATION_OCTET_STREAM} )
	public Response getRegisteredJar(@PathParam("jarName") String jarName) {
		logger.info("REquest for getting jar " + jarName);
		try {
			String jarPath= PropertyLoader.getInstance().getProperty(Constants.JARS_PATH) + jarName;
			StreamingBinOutputImpl streamingOutputImpl = new StreamingBinOutputImpl(jarPath);	
			   
			if(streamingOutputImpl.isAvailable() == false) {
				logger.info("Requested jar " + jarName + " is not found");
				return Response.status(404).entity("Jar not found").build();				   
			} else {
				return Response.ok(streamingOutputImpl).build();
			}
		} catch (Exception ie){
			logger.error("Error while getting jar "+ jarName, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		}
	}
	
	@POST
	@Path("/script/{scriptName}")
	@Consumes ( {MediaType.APPLICATION_OCTET_STREAM} )
	@Produces ({MediaType.TEXT_PLAIN})
	public Response registerScript(@PathParam("scriptName") String scriptName, InputStream uploadedScript) throws IOException {
		logger.info("Request for registering script " + scriptName);
		try{
			if (uploadedScript == null || uploadedScript.available() == 0){
				logger.error("Empty input stream passed");
				return Response.status(400).entity("Bad request. No script uploaded!").build();
			}
			   
			StringWriter writer = new StringWriter();
			try {
				IOUtils.copy(uploadedScript, writer, "UTF-8");
			} catch (IOException e) {
				return Response.status(500).entity("Unable to read pig script").build();
			}
				
			String script= writer.toString();
			if (PigScriptValidator.validatePigScript(script)) {
				InputStream stream= new ByteArrayInputStream(script.getBytes());
				String pathName= PropertyLoader.getInstance().getProperty(Constants.SCRIPTS_PATH) + scriptName;
				PigJobServerImpl.getPigJobServer().registerFile(pathName, stream);
				return Response.ok().entity(pathName).build();
			}
			logger.info("Script " + scriptName + " is not valid");
			return Response.status(400).entity("Bad Request. Either DUMP command is used or STORE is used without '$output'").build();
		} catch (IllegalArgumentException iae) {
			logger.error("Error while registering pig script " + scriptName , iae);
			throw new WebApplicationException(Response.status(409).entity(iae.getMessage()).build());
		} catch (IOException ie) {
			logger.error("Error while registering pig script " + scriptName , ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		} catch (Exception e) {
			logger.error("Error while registering pig script " + scriptName , e);
			throw new WebApplicationException(Response.status(500).entity(e.getMessage()).build());
		}
	}
	   
	@DELETE
	@Path("/script/{scriptName}")
	@Produces ( {MediaType.TEXT_PLAIN} )
	public Response unregisterScript(@PathParam("scriptName") String scriptName) {
		logger.info("Request for deleting script " + scriptName);
		try {
			String pathName= PropertyLoader.getInstance().getProperty(Constants.SCRIPTS_PATH) + scriptName;
			PigJobServerImpl.getPigJobServer().unregisterFile(pathName);
			return Response.ok().entity("Script deleted Successfully").build();
		} catch (FileNotFoundException fnfe) {
			logger.error("Error while deleteing script " + scriptName, fnfe);
			throw new WebApplicationException(Response.status(404).entity(fnfe.getMessage()).build());
		} catch (IOException ie) {
			logger.error("Error while deleteing script " + scriptName, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		} catch (Exception e) {
			logger.error("Error while deleteing script " + scriptName, e);
			throw new WebApplicationException(Response.status(500).entity(e.getMessage()).build());
		}
	}
	   
	@GET
	@Path("/script/{scriptName}")
	@Produces ( {MediaType.APPLICATION_OCTET_STREAM} )
	public Response getRegisteredScript(@PathParam("scriptName") String scriptName) {
		logger.info("Request for getting script " + scriptName);
		try {
			String scriptPath= PropertyLoader.getInstance().getProperty(Constants.SCRIPTS_PATH) + scriptName;
			StreamingBinOutputImpl streamingOutputImpl = new StreamingBinOutputImpl(scriptPath);	
			
			if(streamingOutputImpl.isAvailable() == false) {
				logger.info("Requested script " + scriptName + " not found.");
				return Response.status(404).entity("Script not found").build();				   
			} else {
				return Response.ok(streamingOutputImpl).build();
			}
		} catch (Exception ie) {
			logger.error("Error while retrieving pig script " + scriptName, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		}
	}

	@POST
	@Path("/request/{scriptName}")
	@Consumes( {MediaType.APPLICATION_JSON} )
	@Produces( {MediaType.APPLICATION_JSON, MediaType.APPLICATION_XML, MediaType.TEXT_PLAIN} )
	public Response submitPigJob(@PathParam("scriptName") String scriptName, String data){
		logger.info("Request for running script " + scriptName);
		try {
			Gson gson = new GsonBuilder().setPrettyPrinting().setDateFormat("yyyy/MM/dd-HH:mm").create();
			PigInputParameters input = new PigInputParameters();
			if(data != null) {
				input= gson.fromJson(data, PigInputParameters.class);
			}
			PigRequestParameters request= new PigRequestParameters();
			request.setHttpCallback(input.getHttpCallback());
			request.setInputParameters(input.getInputParameters());
			request.setPigScript(scriptName);
			if (requestInfo != null){
				request.setRequestIp(requestInfo.getRemoteAddr());
			}
			Date date= Calendar.getInstance().getTime();
			request.setRequestStartTime(date);
			String scriptId= UUID.randomUUID().toString();
			logger.info("New request id generated " + scriptId + " for pig script " + scriptName);
			PigJobServerImpl.getPigJobServer().submitPigJob(scriptId, scriptName, request);
			return Response.ok().entity(scriptId).build();
		} catch(IllegalArgumentException e) {
			logger.error("Error while submitting request " , e);
			throw new WebApplicationException(Response.status(400).entity(e.getMessage()).build());
		} catch (IOException ie) {
			logger.error("Error while submitting script " + scriptName, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		}
	}
	   
	@GET 
	@Path("/request/{requestId}")
	@Produces( {MediaType.APPLICATION_JSON} )
	public Response getInput(@PathParam("requestId") String requestId) {
		logger.info("Request for getting request " + requestId);
		PigRequestParameters params= null;
		try {
			params = PigJobServerImpl.getPigJobServer().getInputRequest(requestId);
			Gson gson= new Gson();
			return Response.ok().entity(gson.toJson(params, PigRequestParameters.class)).build();	
		} catch (FileNotFoundException fnfe) {
			logger.error("Error while getting request " + requestId, fnfe);
			throw new WebApplicationException(Response.status(404).entity(fnfe.getMessage()).build());
		}catch (IllegalArgumentException ie) {
			logger.error("Error while getting request " + requestId, ie);
			throw new WebApplicationException(Response.status(404).entity(ie.getMessage()).build());
		}catch (IOException ie) {
			logger.error("Error while getting request " + requestId, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		}catch (Exception e) {
			logger.error("Error while getting request " + requestId, e);
			throw new WebApplicationException(Response.status(500).entity(e.getMessage()).build());
		}
	}

	@GET 
	@Path("/request/{requestId}/stats")
	@Produces( {MediaType.APPLICATION_JSON} )
	public Response getRequestStats(@PathParam("requestId") String requestId) {
		logger.info("Request for getting stats for request " + requestId);
		PigRequestStats stats= null;
		try {
			stats = PigJobServerImpl.getPigJobServer().getRequestStats(requestId);
			Gson gson= new Gson();
			return Response.ok().entity(gson.toJson(stats, PigRequestStats.class)).build();
		} catch (FileNotFoundException fnfe) {
			logger.error("Error while getting stats for request " + requestId, fnfe);
			throw new WebApplicationException(Response.status(404).entity(fnfe.getMessage()).build());
		} catch (IllegalArgumentException ie) {
			logger.error("Error while getting stats for request " + requestId, ie);
			throw new WebApplicationException(Response.status(404).entity(ie.getMessage()).build());
		} catch (IOException ie) {
			logger.error("Error while getting stats for request " + requestId, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		} catch (Exception e) {
			logger.error("Error while getting stats for request " + requestId, e);
			throw new WebApplicationException(Response.status(500).entity(e.getMessage()).build());
		}
	}
	   
	@GET 
	@Path("/request/{requestId}/output")
	@Produces( {MediaType.TEXT_PLAIN} )
	public Response getOutput(@PathParam("requestId") String requestId) {
		logger.info("Request for reading output for " + requestId);
		try {
			String outputPath= PropertyLoader.getInstance().getProperty(Constants.REQUEST_PATH) + requestId;
			StreamingPigOutputImpl streamingPigOutputImpl = new StreamingPigOutputImpl(outputPath);	
			   
			if(streamingPigOutputImpl.isAvailable() == false) {
				logger.info("Request " + requestId + " not available");
				return Response.status(404).entity("Invalid Request ID").build();				   
			} else {
				return Response.ok(streamingPigOutputImpl).build();
			}
		} catch (IOException ie) {
			logger.error("Error while reading output for " + requestId, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		}
	}
	   
	   
	@GET
	@Path("request/{requestId}/status")
	@Produces( {MediaType.TEXT_PLAIN} )
	public Response getRequestStatus(@PathParam("requestId") String requestId) {
		logger.info("Request for retrieving status for " + requestId);
		String status= null;
		try {
			status = PigJobServerImpl.getPigJobServer().getRequestStatus(requestId);
			return Response.ok().entity(status).build();
		} catch (IllegalArgumentException iae) {
			logger.error("Error while getting status for " + requestId, iae);
			throw new WebApplicationException(Response.status(404).entity(iae.getMessage()).build());
		} catch (IOException ie) {
			logger.error("Error while getting status for " + requestId, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		} catch (Exception e) {
			logger.error("Error while getting status for " + requestId, e);
			throw new WebApplicationException(Response.status(500).entity(e.getMessage()).build());
		}
	}
	   
	@GET
	@Path("request/{requestId}/cancel")
	@Produces( {MediaType.TEXT_PLAIN} )
	public Response cancelRequest(@PathParam("requestId") String requestId) {
		logger.info("Request for cancelling request " + requestId);
		boolean status= false;
		try {
			status = PigJobServerImpl.getPigJobServer().cancelRequest(requestId);
			
			if (status) {
				return Response.ok().entity("Jobs cancelled").build();
			} else {
				return Response.status(500).entity("Request doesnt have any running jobs").build();
			}
				
		} catch (IllegalArgumentException iae) {
			logger.error("Error while cancelling request " + requestId, iae);
			throw new WebApplicationException(Response.status(404).entity(iae.getMessage()).build());
		} catch (IOException ie) {
			logger.error("Error while cancelling request " + requestId, ie);
			throw new WebApplicationException(Response.status(500).entity(ie.getMessage()).build());
		} catch (Exception e) {
			logger.error("Error while cancelling request " + requestId, e);
			throw new WebApplicationException(Response.status(500).entity(e.getMessage()).build());
		}
	}
}
