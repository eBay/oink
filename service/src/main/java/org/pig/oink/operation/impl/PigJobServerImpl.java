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

import java.io.BufferedInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobID;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.pig.oink.bean.PigRequestParameters;
import org.pig.oink.bean.PigRequestStats;
import org.pig.oink.common.config.PropertyLoader;
import org.pig.oink.commons.Constants;
import org.pig.oink.commons.PigUtils;
import org.pig.oink.commons.Status;
import org.pig.oink.operation.PigJobServer;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class PigJobServerImpl implements PigJobServer {
	private ExecutorService executors = Executors.newFixedThreadPool(Integer.parseInt(PropertyLoader.getInstance().getProperty("max.threads")));
	private static PigJobServer pigServer= new PigJobServerImpl();
	private static final String JT_UI= PropertyLoader.getInstance().getProperty("jobtracker.ui");
	private final Logger logger= Logger.getLogger(PigJobServerImpl.class);
	
	/*
	 * Return a PigJobServerImpl object
	 */
	public static PigJobServer getPigJobServer() {
		return pigServer;
	}
	
	private FileSystem getFileSystem() throws IOException {
		String defaultHdfsName= PropertyLoader.getInstance().getProperty(Constants.DEFAULT_HDFS_NAME);
		Configuration conf= new Configuration();
		conf.set(Constants.DEFAULT_HDFS_NAME, defaultHdfsName);
		FileSystem fileSystem= null;
		try {
			fileSystem = FileSystem.get(conf);
		} catch(IOException e) {
			logger.error(e.getMessage(), e);
			throw new IOException("Unable to connect to Hadoop FileSytem. Please try again later");
		}
		return fileSystem;
	}
	
	/* 
	 * Check if the pig script exists. If not then throw 404
	 * Since submitting a Pig job is a blocking call we allocate a thread from the thread pool and run the job through that thread.
	 */
	@Override
	public void submitPigJob(String requestId, String scriptName, PigRequestParameters params) throws IOException {
		FileSystem fileSystem= getFileSystem();
		String scriptPath= PropertyLoader.getInstance().getProperty(Constants.SCRIPTS_PATH);

		try{
			if (! fileSystem.exists(new Path(scriptPath  + scriptName))){
				throw new IllegalArgumentException("Pig script " + scriptName + " is not registered.");
			}
		}catch(IOException e){
			logger.log(Level.ERROR, "Unable to read script file");
			throw new IOException("Unable to read request directory");
		}
		executors.submit(new SubmitTask(requestId, scriptName, params));
	}

	public void setExecutors(ExecutorService executors) {
		this.executors = executors;
	}

	public ExecutorService getExecutors() {
		return executors;
	}

	/*
	 * To register a file connect to the HDFS by creating a FileSystem object. 
	 * Throw 500 if unable to create a FileSystem object
	 * Check if the file exists already. 
	 * If yes, then throw 409 and exit
	 * If the file path can not be opened then throw 500 and exit.
	 * If the path does not exist then create the file and write the bytes from the InputStream into the file location in the HDFS
	 * Close both the input and output stream after writing operation is complete.
	 */
	@Override
	public void registerFile(String filePath, InputStream fileInputStream) throws FileNotFoundException, IOException {
        FileSystem fs = getFileSystem();
        Path pathName= new Path(filePath);
		if(fs.exists(pathName)) {
            throw new IllegalStateException("File already exists");
        }
		
		FSDataOutputStream out= null;
		try {
			out= fs.create(pathName);
			byte[] b= new byte[1024];
			int numBytes= 0;
			while((numBytes= fileInputStream.read(b)) > 0) {
				out.write(b, 0, numBytes);
			}
		} catch(IOException e) {
			logger.log(Level.ERROR, e.getMessage(), e);
			throw new IOException("Unable to upload file to Hadoop FileSytem");
		} finally {
			if(fileInputStream != null) {
				try {
					fileInputStream.close();
				} catch(Exception e) {
					logger.error("Error while closing file ", e);
				}
			}
			if (out != null){
				try {
					out.close();
				} catch(Exception e) {
			        logger.error("Error while closing file ", e);
				}
			}
		}
	}

	/*
	 * Connect to the HDFS by creating a FileSystem object. Throw 500 if a FileSystem object can not be created.
	 * Check if the file exists in the HDFS. 
	 * If no then throw 404
	 * If yes then delete the file
	 * Throw 500 if there is any IOException while deleting the file
	 */
	@Override
	public void unregisterFile(String filePath) throws FileNotFoundException, IOException {
		FileSystem fs= getFileSystem();
		Path pathName= new Path(filePath);
		if(! fs.exists(pathName)) {
			throw new FileNotFoundException("No such file exists");
		}
		if(! fs.delete(pathName, false)) {
			throw new IOException("Unable to delete file");
		}			
	}

	/*
	 * Create a FileSystem object to connect to the HDFS. If unable to connect to the HDFS then throw 500
	 * Check if the path to the request ID/input exists in the HDFS. If no then throw 500
	 * Try to open the file. If unable then throw 500
	 * Read the file which contains a Base64Encoded string.
	 * Decode the string and get the JSON string.
	 * Convert the JSON string to PigRequestParameters object
	 * Return the object
	 */
	@Override
	public PigRequestParameters getInputRequest(String scriptId) throws IOException {
		FileSystem fileSystem= getFileSystem();
		String requestPath= PropertyLoader.getInstance().getProperty(Constants.REQUEST_PATH) + scriptId;

		if(!fileSystem.exists(new Path(requestPath))) {
			throw new IllegalArgumentException("Request id not found");
		}
		
		Path filePath= new Path(requestPath + "/input");
		
		if(!fileSystem.exists(filePath)) {
			throw new FileNotFoundException("Input details not available yet");
		}
		
		PigRequestParameters params= null;
		LineReader reader= null;
		Gson gson= new GsonBuilder().setPrettyPrinting().setDateFormat("yyyy/MM/dd-HH:mm").create();
		try {
			reader= new LineReader(new BufferedInputStream(fileSystem.open(filePath)));
			Text line= new Text();
			if(reader.readLine(line) > 0) {
				String encodedParams= line.toString();
				params= gson.fromJson(new String(Base64.decodeBase64(encodedParams.getBytes())), PigRequestParameters.class);
			}
		} catch(Exception e) {
			logger.error("Error while reading file " + filePath.getName(), e);
			throw new IOException("Unable to retrieve input details. Please try again later", e);
		} finally {
			if(reader != null) {
				try {
					reader.close();
					reader = null;
				} catch(Exception e) {
					logger.error("Error while closing file ", e);
				}
			}
		}
		return params;
	}

	/*
	 * Create a FileSystem object to connect to the HDFS. If unable to connect to the HDFS then throw 500
	 * Check if the path to the request ID/input exists in the HDFS. If no then throw 500
	 * Try to open the file. If unable then throw 500
	 * Read the file which contains a Base64Encoded string.
	 * Decode the string and get the JSON string.
	 * Convert the JSON string to PigRequestStats object
	 * Return the object
	 */
	@Override
	public PigRequestStats getRequestStats(String scriptId) throws IOException {
		FileSystem fileSystem= getFileSystem();
		String requestPath= PropertyLoader.getInstance().getProperty(Constants.REQUEST_PATH) + scriptId;
		if(! fileSystem.exists(new Path(requestPath))) {
			logger.error("Invalid request ID");
			throw new IllegalArgumentException("Invalid request ID");
		}
		
		Path filePath= new Path(requestPath + "/stats");
		
		if(! fileSystem.exists(filePath)) {
			throw new FileNotFoundException("Stats not available yet");
		}
		
		PigRequestStats stats= null;
		LineReader reader= null;
		Gson gson= new GsonBuilder().setPrettyPrinting().setDateFormat("yyyy/MM/dd-HH:mm").create();
		try {
			reader= new LineReader(new BufferedInputStream(fileSystem.open(filePath)));
			Text line= new Text();
			if(reader.readLine(line) > 0) {
				String encodedStats= line.toString();
				stats= gson.fromJson(new String(Base64.decodeBase64(encodedStats.getBytes())), PigRequestStats.class);
			}
		} catch(Exception e) {
			logger.error("Error while reading file " + filePath.getName(), e);
			throw new IOException("Unable to retrieve input details. Please try again later", e);
		} finally {
			if(reader != null) {
				try {
					reader.close();
					reader = null;
				} catch(Exception e) {
					logger.log(Level.ERROR, "Error while closing file ", e);
				}
			}
		}	
		return stats;
	}

	@Override
	public String getRequestStatus(String scriptId) throws IOException {
		FileSystem fileSystem= getFileSystem();
		String requestPath= PropertyLoader.getInstance().getProperty(Constants.REQUEST_PATH) + scriptId;
		if(!fileSystem.exists(new Path(requestPath))) {
			throw new IllegalArgumentException("Request id not found");
		}
		Path statsFilePath= new Path(requestPath + "/stats");
		try {
			if(!fileSystem.exists(statsFilePath)) {
				return Status.SUBMITTED.toString();
			}
		} catch (IOException ioe) {
			logger.error("Error while reading path " + requestPath, ioe);
			throw new IOException("Unable to retrieve input details. Please try again later");
		}
		PigRequestStats stats= getRequestStats(scriptId);	
		return stats.getStatus();
	}

	@Override
	public boolean cancelRequest(String requestId) throws IOException {
		PigRequestStats stats = this.getRequestStats(requestId);
		
		if (stats.getStatus().equals(Status.SUBMITTED.toString())) {
			List<String> jobs= stats.getJobs();
			for (String job : jobs) {
				job= job.substring(JT_UI.length());
				JobConf jobConf = new JobConf();
				jobConf.set("fs.default.name", PropertyLoader.getInstance().getProperty("fs.default.name"));
				jobConf.set("mapred.job.tracker", PropertyLoader.getInstance().getProperty("jobtracker"));
				try {
				   JobClient jobClient = new JobClient(jobConf);
				   RunningJob rJob = jobClient.getJob(JobID.forName(job));
				   
				   if (! rJob.isComplete()) {
					   rJob.killJob();
				   }
				} catch (Exception e) {
				   throw new IOException ("Unable to kill job " + job);
				}
			}
			PigRequestStats requestStats= new PigRequestStats(0, 0, null, jobs.size());
			requestStats.setJobs(jobs);
			requestStats.setStatus(Status.KILLED.toString());
			Path statsPath= new Path(PropertyLoader.getInstance().getProperty(Constants.REQUEST_PATH) + requestId + "/stats");
			PigUtils.writeStatsFile(statsPath, requestStats);
			return true;
		} else {
			return false;
		}
	}
}
