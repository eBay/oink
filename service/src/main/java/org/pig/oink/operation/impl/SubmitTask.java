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

package org.pig.oink.operation.impl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.pig.PigRunner;
import org.apache.pig.tools.pigstats.PigStats;
import org.pig.oink.bean.PigRequestParameters;
import org.pig.oink.bean.PigRequestStats;
import org.pig.oink.common.config.PropertyLoader;
import org.pig.oink.common.service.impl.EndNotificationServiceImpl;
import org.pig.oink.commons.Constants;
import org.pig.oink.commons.PigUtils;
import org.pig.oink.commons.Status;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class SubmitTask implements Runnable {
	private final PigRequestParameters params;
	private final String requestId;
	private final String scriptName;
	private final Logger logger= Logger.getLogger(SubmitTask.class);
	
	public SubmitTask(String requestId, String scriptName, PigRequestParameters params) {
		this.requestId= requestId;
		this.scriptName= scriptName;
		this.params= params;
	}
	
	@Override
	public void run() {
		Gson gson= new GsonBuilder().setPrettyPrinting().setDateFormat("yyyy/MM/dd-HH:mm").create();
		
		List<String> parameters= new ArrayList<String>();
		Map<String,String> inputParams= params.getInputParameters();
		String httpCallBack= params.getHttpCallback();
		
		String defaultHdfsName= PropertyLoader.getInstance().getProperty(Constants.DEFAULT_HDFS_NAME);
		Configuration conf= new Configuration();
		conf.set(Constants.DEFAULT_HDFS_NAME, defaultHdfsName);
		FileSystem fileSystem= null;
		try {
			fileSystem = FileSystem.get(conf);
		} catch(Exception e) {
			logger.error("Unable to create filesystem object to write input file", e);
			return;
		}
		
		String requestPath= PropertyLoader.getInstance().getProperty(Constants.REQUEST_PATH);
		String scriptPath= PropertyLoader.getInstance().getProperty(Constants.SCRIPTS_PATH);
		
	        String log4jPath= getClass().getResource("/log4j.xml").getPath();	
		Path inputPath= new Path(requestPath + requestId + "/input");
		String jsonStr= gson.toJson(params);
		String encodedParams= new String(Base64.encodeBase64URLSafeString(jsonStr.getBytes()));
		try {
			if(!fileSystem.exists(inputPath)) {
				BufferedWriter writer= new BufferedWriter(new OutputStreamWriter(fileSystem.create(inputPath, false)));
				writer.write(encodedParams);
				writer.close();
			} else {
				logger.error("Input file already exists");
			}
		} catch (IOException e) {
			logger.error("Unable to write input file", e);
		}
		
		for (Map.Entry<String, String> entry : inputParams.entrySet()) {
			parameters.add("-p");
			parameters.add(entry.getKey() + "=" + entry.getValue());
		}
		
		String outputPath= requestPath + requestId + "/output";
		parameters.add("-p");
		parameters.add("output=" + outputPath);

		parameters.add("-4");
		parameters.add(log4jPath);
		
		parameters.add("-f");
		parameters.add(scriptPath + scriptName);
		
		String args[]= new String[parameters.size()];
		PigEventListener listener= new PigEventListener(httpCallBack, requestId);
		PigRequestStats requestStats= listener.getStats();
		Path statsPath= new Path(requestPath + requestId + "/stats");
		
		try {
			PigUtils.writeStatsFile(statsPath, requestStats);
		} catch (Exception e) {
			logger.error("Unable to write stats file", e);
		}
		
		PigStats pigStats= PigRunner.run(parameters.toArray(args), listener);
		
		requestStats= listener.getStats();
		requestStats.setBytesWritten(pigStats.getBytesWritten());
		requestStats.setDuration(pigStats.getDuration());
		Map<String, String> outputParams= new HashMap<String, String>();
		if(pigStats.isSuccessful()) {
			outputParams.put("$status", Status.SUCCEEDED.toString());
			requestStats.setStatus(Status.SUCCEEDED.toString());
		}
		else {
			outputParams.put("$status", Status.FAILED.toString());
			requestStats.setStatus(Status.FAILED.toString());
		}
		
		if(requestStats.getErrorMessage() == null) {
			requestStats.setErrorMessage(pigStats.getErrorMessage());
		}
		
		requestStats.setProgress(100);
		
		String encodedStats= new String(Base64.encodeBase64URLSafeString(gson.toJson(requestStats, PigRequestStats.class).getBytes()));

		try {
			PigUtils.writeStatsFile(statsPath, requestStats);
		} catch (Exception e) {
			logger.error("Unable to write stats file", e);
		}
		
		outputParams.put("$stats", encodedStats);
		outputParams.put("$id", requestId);
		if(httpCallBack != null) {
			EndNotificationServiceImpl.getService().sendHttpNotification(httpCallBack, outputParams);
		}
	}
}
