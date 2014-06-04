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
import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.StreamingOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.LineReader;
import org.apache.log4j.Logger;
import org.pig.oink.common.config.PropertyLoader;
import org.pig.oink.commons.Constants;

public class StreamingPigOutputImpl implements StreamingOutput {
	
	private String filePathStr;
	private Configuration conf;
	
	private final Logger logger= Logger.getLogger(StreamingPigOutputImpl.class);

	/*
	 * Check if the file name starts with part-r- and return true if it matches
	 */
	private PathFilter partFileFilter = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return path.getName().startsWith("part-")? true : false;
		}
	};

	public StreamingPigOutputImpl(String filePathStr) {
		this.filePathStr= filePathStr + "/output/";
		conf= new Configuration();
		conf.set(Constants.DEFAULT_HDFS_NAME, PropertyLoader.getInstance().getProperty(Constants.DEFAULT_HDFS_NAME));
	}

	/* Create a FileSystem object to connect to the HDFS. 
	 * If the FileSystem object can not be created return 500. 
	 * Check if the output path exists. 
	 * If yes, then get the list of all files that start with part-r-.
	 * Iterate through each file. Create a LineReader object for the given output file and stream each line in the file to the client
	 * Close the input and output stream objects
	 */
	@Override
	public void write(OutputStream output) throws IOException,
			WebApplicationException {
		FileSystem fileSystem= null;
		try {
			fileSystem= FileSystem.get(conf);
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new IOException("Unable to connect to Hadoop FileSytem. Please try again later");
		}
		Path filePath= new Path(filePathStr);
		
		if (isAvailable()) {
			FileStatus[] fileStatus = fileSystem.listStatus(filePath, partFileFilter);
			
			
			for (FileStatus status : fileStatus){
				LineReader reader= null;
				try {
					reader= new LineReader(new BufferedInputStream(fileSystem.open(status.getPath())));
					Text line= new Text();
					while (reader.readLine(line) > 0) {
						output.write(line.toString().getBytes());
						output.write("\n".getBytes());
					}
				} catch(Exception e) {
					logger.error("Error while reading file " + status.getPath().getName(), e);
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
			}	
		}
	}

	/*
	 * Create a FileSystem object to connect to the HDFS. Throw 500 if unable to connect
	 * Check if the file exists. If yes then return true else return false
	 */
	public boolean isAvailable() throws IOException {
		FileSystem fileSystem = null;
		try{
			fileSystem= FileSystem.get(conf);
		} catch(IOException e){
		 	logger.error(e.getMessage(), e);
			throw new IOException("Unable to connect to Hadoop FileSytem. Please try again later");
		}
		Path path = new Path(filePathStr);
		
		try {
			if(fileSystem.exists(path)) {
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			throw new IOException("Unable to open file");
		}
	}

}

