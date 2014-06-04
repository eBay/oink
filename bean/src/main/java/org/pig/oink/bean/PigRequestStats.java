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

package org.pig.oink.bean;

import java.util.ArrayList;
import java.util.List;

public class PigRequestStats {
	private long bytesWritten= 0;
	private long duration= 0;
	private String errorMessage;
	private long numberOfJobs= 0;
	private String status;
	private int progress;
	private List<String> jobs;
	
	public PigRequestStats() {
		jobs= new ArrayList<String>();
	}
	
	public PigRequestStats(long bytesWritten, long duration, String errorMessage, long numberOfJobs){
		this.setBytesWritten(bytesWritten);
		this.setDuration(duration);
		this.setErrorMessage(errorMessage);
		this.setNumberOfJobs(numberOfJobs);
		jobs= new ArrayList<String>();
	}

	public void setBytesWritten(long bytesWritten) {
		this.bytesWritten = bytesWritten;
	}

	public long getBytesWritten() {
		return bytesWritten;
	}

	public void setDuration(long duration) {
		this.duration = duration;
	}

	public long getDuration() {
		return duration;
	}

	public void setErrorMessage(String errorMessage) {
		this.errorMessage = errorMessage;
	}

	public String getErrorMessage() {
		return errorMessage;
	}

	public void setNumberOfJobs(long numberOfJobs) {
		this.numberOfJobs = numberOfJobs;
	}

	public long getNumberOfJobs() {
		return numberOfJobs;
	}

	public void setStatus(String status) {
		this.status = status;
	}

	public String getStatus() {
		return status;
	}
	
	public void setProgress(int progress) {
		this.progress = progress;
	}
	
	public int getProgress() {
		return this.progress;
	}

	public List<String> getJobs() {
		return jobs;
	}

	public void setJobs(List<String> jobs) {
		this.jobs = jobs;
	}
	
	public void addJob(String job){
		this.jobs.add(job);
	}
}
