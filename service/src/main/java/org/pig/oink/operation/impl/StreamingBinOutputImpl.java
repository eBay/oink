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

import java.io.IOException;
import java.io.OutputStream;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.pig.oink.common.config.PropertyLoader;
import org.pig.oink.commons.Constants;

public class StreamingBinOutputImpl implements StreamingOutput {
	
	private String filePathStr;
	private Configuration conf;
	private final Logger logger= Logger.getLogger(StreamingBinOutputImpl.class);

	public StreamingBinOutputImpl(String filePathStr){
		this.filePathStr= filePathStr;
		conf= new Configuration();
		conf.set(Constants.DEFAULT_HDFS_NAME, PropertyLoader.getInstance().getProperty(Constants.DEFAULT_HDFS_NAME));
	}

	@Override
	public void write(OutputStream output) throws IOException,
			WebApplicationException {
		FileSystem fileSystem= null;
		try {
			fileSystem= FileSystem.get(conf);
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new WebApplicationException(Response.status(500).entity("Unable to connect to Hadoop FileSytem. Please try again later").build());
		}
		Path filePath= new Path(filePathStr);
		
		if (isAvailable()) {
			FSDataInputStream in= null;
			try {
				in= fileSystem.open(filePath);
				byte[] b= new byte[1024];
				int numBytes= 0;
				while((numBytes= in.read(b)) > 0){
					output.write(b, 0, numBytes);
				}
				in.close();
				output.close();
			} catch(Exception e) {
				logger.error(e.getMessage(), e);
				throw new WebApplicationException(Response.status(500).entity("Unable to retrieve file from Hadoop FileSystem").build());
			}
		}
	}

	public boolean isAvailable() {
		FileSystem fileSystem= null;
		try {
			fileSystem= FileSystem.get(conf);
		} catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new WebApplicationException(Response.status(500).entity("Unable to connect to Hadoop FileSytem. Please try again later").build());
		}
		Path path = new Path(filePathStr);
		
		try {
			if (fileSystem.exists(path)) {
				return true;
			} else {
				return false;
			}
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
			throw new WebApplicationException(Response.status(500).entity("Unable to open file").build());
		}
	}

}
