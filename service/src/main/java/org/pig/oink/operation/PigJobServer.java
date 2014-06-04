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

package org.pig.oink.operation;

import java.io.IOException;
import java.io.InputStream;

import org.pig.oink.bean.PigRequestParameters;
import org.pig.oink.bean.PigRequestStats;

public interface PigJobServer {
	/**
	 * This method executes the Pig script on Hadoop
	 * @param requestId - ID that was generated when the request was submitted
	 * @param scriptName - Name of the Pig script
	 * @param params - PigRequestParameters object that contains all the required parameters for execution of the script
	 * @throws IllegalArgumentException - if script is not registered or any other error in input parameter
	 * @throws IOException - if any error occurred while executing it
	 */
	public void submitPigJob(String requestId, String scriptName, PigRequestParameters params) throws 
		IllegalArgumentException, IOException;

	/**
	 * This method registers(writes) a file to HDFS
	 * @param filePath - Path from which to write the file to
	 * @param fileInputStream - InputStream object containing the bytes to write which are uploaded by the client
	 * @throws IOException 
	 */

	public void registerFile(String filePath, InputStream fileInputStream) throws IOException;

	/**
	 * This method unregisters(deletes) a file to HDFS
	 * @param filePath - Path from which to delete the file from
	 * @throws IOException 
	 */

	public void unregisterFile(String filePath) throws IOException;

	/**
	 * This method returns the request parameters submitted for a given requestId
	 * @param request - ID that was generated when the request was submitted
	 * @return PigRequestParameters object
	 * @throws IOException 
	 * @throws Exception 
	 */
	public PigRequestParameters getInputRequest(String requestId) throws IOException, Exception;

	/**
	 * This method returns the request execution statistics for a given requestId
	 * @param requestId - ID that was generated when the request was submitted
	 * @return PigRequestStats object
	 * @throws IOException 
	 * @throws Exception 
	 */
	public PigRequestStats getRequestStats(String requestId) throws IOException, Exception;

	/**
	 * This method returns the status of the submitted request. This can be used for polling
	 * @param requestId - ID that was generated when the request was submitted
	 * @return String object
	 * @throws IOException 
	 * @throws Exception 
	 */
	public String getRequestStatus(String requestId) throws IOException, Exception;
	
	/**
	 * This method cancels the MapReduce jobs associated with the requestId
	 * @param requestId - ID that was generated when the request was submitted
	 * @return boolean object
	 * @throws IOException 
	 * @throws Exception 
	 */
	public boolean cancelRequest(String requestId) throws IOException, Exception;
}
	