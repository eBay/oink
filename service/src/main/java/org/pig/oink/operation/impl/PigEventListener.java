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

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.TypeConverter;
import org.apache.log4j.Logger;
import org.apache.pig.backend.hadoop.executionengine.mapReduceLayer.plans.MROperPlan;
import org.apache.pig.tools.pigstats.JobStats;
import org.apache.pig.tools.pigstats.OutputStats;
import org.apache.pig.tools.pigstats.PigProgressNotificationListener;
import org.pig.oink.bean.PigRequestStats;
import org.pig.oink.common.config.PropertyLoader;
import org.pig.oink.common.service.impl.EndNotificationServiceImpl;
import org.pig.oink.commons.Constants;
import org.pig.oink.commons.PigUtils;
import org.pig.oink.commons.Status;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class PigEventListener implements PigProgressNotificationListener {
	
	private String httpUrl;
	private String requestId;
	private Logger logger= Logger.getLogger(PigEventListener.class);
	private Gson gson= new GsonBuilder().setPrettyPrinting().setDateFormat("yyyy/MM/dd-HH:mm").create();
	private static final String JT_UI= PropertyLoader.getInstance().getProperty("resourcemanager.ui");
	private PigRequestStats requestStats;
	private String requestPath;
	
	public PigEventListener(String httpUrl, String scriptId){
		this.httpUrl= httpUrl;
		this.requestId= scriptId;
		this.requestStats= new PigRequestStats();
		requestStats.setStatus(Status.SUBMITTED.toString());
		this.requestPath= PropertyLoader.getInstance().getProperty(Constants.REQUEST_PATH) + scriptId;
		logger.info("Created listener for call back url " + httpUrl);	
	}
	
	/*
	 * If the job has failed send the request ID, job ID, status=FAILED as HTTP notification
	 */
	@Override
	public void jobFailedNotification(String scriptId, JobStats jobStats) {
		logger.info("Job " + jobStats.getJobId() + " completed with status " + jobStats.getState() + " for request "
				+ requestId + " with error " + jobStats.getErrorMessage());
		requestStats.setErrorMessage(jobStats.getErrorMessage());
		try {
			PigUtils.writeStatsFile(new Path(requestPath + "/stats"), requestStats);
		} catch (Exception e) {
			logger.error("Unable to write stats file for path: " + requestPath);
		}
	}
	
	/*
	 * If an individual job has succeeded send the job ID, request ID and status=SUCCEEDED as HTTP notification
	 */
	@Override
	public void jobFinishedNotification(String scriptId, JobStats jobStats) {
		logger.info("Job " + jobStats.getJobId() + " completed with status " + jobStats.getState() + " for request " + requestId);		
	}

	/*
	 * If the Pig job has started send the request ID, job ID and status=SUBMITTED as HTTP notification
	 */
	@Override
	public void jobStartedNotification(String scriptId, String jobId) {
		logger.info("Job started with ID" + jobId + " for request " + requestId);
		String jobUrl= JT_UI + TypeConverter.toYarn(JobID.forName(jobId)).getAppId();
		
		if (! requestStats.getJobs().contains(jobUrl)) {
			requestStats.addJob(jobUrl);
		}
		
		try {
			PigUtils.writeStatsFile(new Path(requestPath + "/stats"), requestStats);
		} catch (Exception e) {
			logger.error("Unable to write stats file for path: " + requestPath);
		}	
	}

	/*
	 * If the Pig job has been submitted send the request ID, number of jobs submitted and status=SUBMITTED as HTTP notification
	 */
	@Override
	public void jobsSubmittedNotification(String scriptId, int numJobsSubmitted) {
		logger.info(numJobsSubmitted + "jobs submitted for request " + requestId);
		requestStats.setNumberOfJobs(numJobsSubmitted);
		try {
			PigUtils.writeStatsFile(new Path(requestPath + "/stats"), requestStats);
		} catch (Exception e) {
			logger.error("Unable to write stats file for path: " + requestPath);
		}	
		
	}
	
	/*
	 * Send an integer denoting the percentage of the Pig job that is completed as HTTP notification
	 */
	@Override
	public void progressUpdatedNotification(String scriptId, int progress) {
		logger.info(progress + "% completed for request " + requestId);
		requestStats.setStatus(Status.SUBMITTED.toString());
		requestStats.setProgress(progress);
		
		try {
			PigUtils.writeStatsFile(new Path(requestPath + "/stats"), requestStats);
		} catch (Exception e) {
			logger.error("Unable to write stats file for path: " + requestPath);
		}
		
		if (httpUrl != null){
			String encodedStats= new String(Base64.encodeBase64URLSafeString(gson.toJson(requestStats).getBytes()));
			Map<String, String> parameters= new HashMap<String, String>();
			parameters.put("$stats", encodedStats);
			parameters.put("$id", requestId);
			parameters.put("$status", Status.SUBMITTED.toString());
			EndNotificationServiceImpl.getService().sendHttpNotification(httpUrl, parameters);
		}
		
	}
	
	public PigRequestStats getStats(){
		return requestStats;
	}

	@Override
	public void initialPlanNotification(String scriptId, MROperPlan plan) {
	}

	@Override
	public void launchCompletedNotification(String scriptId, int numJobs) {
		
	}

	@Override
	public void launchStartedNotification(String scriptId, int numJobs) {
		
	}

	@Override
	public void outputCompletedNotification(String scriptId, OutputStats outputStats) {
		
	}

}
