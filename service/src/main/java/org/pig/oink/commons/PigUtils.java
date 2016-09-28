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

package org.pig.oink.commons;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.pig.oink.bean.PigRequestStats;
import org.pig.oink.common.config.PropertyLoader;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class PigUtils {
	private static final Logger logger= Logger.getLogger(PigUtils.class);

	
	public static void writeStatsFile (Path filePath, PigRequestStats stats) throws IOException {
		Gson gson= new GsonBuilder().setPrettyPrinting().setDateFormat("yyyy/MM/dd-HH:mm").create();

		String encodedStats= new String(Base64.encodeBase64URLSafeString(gson.toJson(stats, PigRequestStats.class).getBytes()));

		try {
			Configuration conf= new Configuration();
			conf= new Configuration();
			conf.set(Constants.DEFAULT_HDFS_NAME, PropertyLoader.getInstance().getProperty(Constants.DEFAULT_HDFS_NAME));
			FileSystem fileSystem= FileSystem.get(conf);
			BufferedWriter writer= new BufferedWriter(new OutputStreamWriter(fileSystem.create(filePath, true)));
			writer.write(encodedStats);
			writer.close();
		} catch (Exception e) {
			logger.log(Level.ERROR, "Unable to write input file", e);
			throw new IOException("Unable to write input file", e);
		}
	}
}
