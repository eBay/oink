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

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.util.Date;
import java.util.UUID;

import javax.ws.rs.core.Response;

import junit.framework.Assert;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.pig.oink.bean.PigRequestParameters;
import org.pig.oink.bean.PigRequestStats;
import org.pig.oink.common.config.PropertyLoader;
import org.pig.oink.commons.PigUtils;
import org.pig.oink.operation.impl.StreamingBinOutputImpl;
import org.pig.oink.operation.impl.StreamingPigOutputImpl;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

public class TestPigResource {
	private static final String HADOOP_TMP_PATH = "target/hadoop";
	private Configuration conf;
	private FileSystem fileSystem;

    static {
        System.setProperty("java.tmp.dir", HADOOP_TMP_PATH);
    }
    
    @BeforeClass
    protected void setUp() throws Exception {
    	PropertyLoader.getInstance().init("/test.properties");
        PropertyLoader.getInstance().setProperty("scripts.basepath", HADOOP_TMP_PATH + "/pig/scripts/");
        PropertyLoader.getInstance().setProperty("jars.basepath",  HADOOP_TMP_PATH + "/pig/jars/");
        PropertyLoader.getInstance().setProperty("requests.basepath",  HADOOP_TMP_PATH + "/pig/requests/");
    	PropertyLoader.getInstance().setProperty("fs.defaultFS", "file:///");
    	conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        conf.set("mapred.job.tracker", "local");
        conf.setInt("dfs.replication", 1);
        conf.setInt("io.sort.mb", 10);
        conf.set("hadoop.tmp.dir", HADOOP_TMP_PATH);
        fileSystem= FileSystem.get(conf);
        new File(HADOOP_TMP_PATH).mkdirs();
    }
    
    @Test
    public void testRegisterScript() throws Exception {
    	PigResource resource= new PigResource();
    	String str= "STORE E into '$output' using PigStorage(',');";
    	InputStream input= new ByteArrayInputStream(str.getBytes("UTF-8"));
    	Response resp= resource.registerScript("script.pig", input);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	input= new ByteArrayInputStream(str.getBytes("UTF-8"));
    	try {
    		resp= resource.registerScript("script.pig", input);
    	} catch (Exception e) {
    		Assert.assertNotNull(e);
    	}
    	
    	
    	str= "DUMP E";
    	input= new ByteArrayInputStream(str.getBytes("UTF-8"));
    	
    	resp= resource.registerScript("script1.pig", input);
    	Assert.assertEquals(resp.getStatus(), 400);
    	
    }
    
    @Test
    public void testGetScript() throws Exception {
    	PigResource resource= new PigResource();
    	String str= "STORE E into '$output' using PigStorage(',');";
    	InputStream input= new ByteArrayInputStream(str.getBytes("UTF-8"));
    	Response resp= resource.registerScript("getscript.pig", input);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	resp= resource.getRegisteredScript("getscript.pig");
    	Assert.assertEquals(resp.getStatus(), 200);
    	StreamingBinOutputImpl out= (StreamingBinOutputImpl) resp.getEntity();
    	ByteArrayOutputStream output= new ByteArrayOutputStream();
    	out.write(output);
    	
    	String outStr= output.toString();
    	Assert.assertEquals(str, outStr);
    	
    	resp= resource.getRegisteredScript("notscript.pig");
    	Assert.assertEquals(resp.getStatus(), 404);
    	
    	
    }
    
    @Test
    public void testUnRegisterScript() throws Exception {
    	PigResource resource= new PigResource();
    	String str= "STORE E into '$output' using PigStorage(',');";
    	InputStream input= new ByteArrayInputStream(str.getBytes("UTF-8"));
    	Response resp= resource.registerScript("delete.pig", input);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	resource.unregisterScript("delete.pig");
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	try {
    		resource.unregisterScript("delete1.pig");
    	} catch (Exception e) {
    		Assert.assertNotNull(e);
    	}
    }
    
    @Test
    public void testRegisterJar() throws Exception {
    	PigResource resource= new PigResource();
    	String str= "abcde";
    	InputStream input= new ByteArrayInputStream(str.getBytes("UTF-8"));
    	Response resp= resource.registerJar("a.jar", input);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	input= new ByteArrayInputStream(str.getBytes("UTF-8"));
    	try {
    		resp= resource.registerJar("a.jar", input);
    	} catch (Exception e) {
    		Assert.assertNotNull(e);
    	}
    	
    }

    @Test
    public void testUnRegisterJar() throws Exception {
    	PigResource resource= new PigResource();
    	String str= "abcde";
    	InputStream input= new ByteArrayInputStream(str.getBytes("UTF-8"));
    	Response resp= resource.registerJar("delete.jar", input);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	resource.unregisterJar("delete.jar");
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	try {
    		resource.unregisterJar("delete1.jar");
    	} catch (Exception e) {
    		Assert.assertNotNull(e);
    	}
    }
    
    @Test
    public void testGetJar() throws Exception {
    	PigResource resource= new PigResource();
    	String str= "abcde";
    	InputStream input= new ByteArrayInputStream(str.getBytes("UTF-8"));
    	Response resp= resource.registerJar("get.jar", input);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	resp= resource.getRegisteredJar("get.jar");
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	StreamingBinOutputImpl out= (StreamingBinOutputImpl) resp.getEntity();
    	ByteArrayOutputStream output= new ByteArrayOutputStream();
    	out.write(output);
    	
    	String outStr= output.toString();
    	Assert.assertEquals(str, outStr);
    	
		resp= resource.getRegisteredJar("notget.jar");
		Assert.assertEquals(resp.getStatus(), 404);
    	
    }
    
    @Test 
    public void testGetRequest() throws Exception {
    	PigResource resource= new PigResource();
    	PigRequestParameters input= new PigRequestParameters();
    	input.setHttpCallback("123");
    	input.setPigScript("abc.pig");
    	input.setRequestStartTime(new Date());
    	
    	String requestId= UUID.randomUUID().toString();
    	Gson gson= new GsonBuilder().setPrettyPrinting().setDateFormat("yyyy/MM/dd-HH:mm").create();
    	String inputString= Base64.encodeBase64URLSafeString(gson.toJson(input).getBytes());
    	String path= PropertyLoader.getInstance().getProperty("requests.basepath") + requestId 
    			+ "/input";
    	
    	BufferedWriter writer= new BufferedWriter(
    			new OutputStreamWriter(fileSystem.create(new Path(path), false)));
		writer.write(inputString);
		writer.close();
		
    	Response resp=  resource.getInput(requestId);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	requestId= "abc";
    	try {
    		resp=  resource.getInput(requestId);
    	} catch (Exception e) {
    		Assert.assertNotNull(e);
    	}
    	
    	requestId= UUID.randomUUID().toString();
    	fileSystem.mkdirs(new Path(PropertyLoader.getInstance().getProperty("requests.basepath") + requestId));
    	try {
    		resp=  resource.getInput(requestId);
    	} catch (Exception e) {
    		Assert.assertNotNull(e);
    	}
    }
    
    @Test 
    public void testGetStats() throws Exception {
    	PigResource resource= new PigResource();
    	PigRequestStats stats= new PigRequestStats(15, 14, null, 5);
    	stats.setStatus("STATUS");
    	
    	String requestId= UUID.randomUUID().toString();
    	String path= PropertyLoader.getInstance().getProperty("requests.basepath") + requestId 
    			+ "/stats";
    	
    	PigUtils.writeStatsFile(new Path(path), stats);
		
    	Response resp=  resource.getRequestStats(requestId);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	requestId= "abc";
    	try {
    		resp=  resource.getRequestStats(requestId);
    	} catch (Exception e) {
    		Assert.assertNotNull(resp);
    	}
    	
    	requestId= UUID.randomUUID().toString();
    	fileSystem.mkdirs(new Path(PropertyLoader.getInstance().getProperty("requests.basepath") + requestId));
    	try {
    		resp=  resource.getRequestStats(requestId);
    	} catch (Exception e) {
    		Assert.assertNotNull(e);
    	}
    }
    
    @Test 
    public void testGetOutpu() throws Exception {
    	PigResource resource= new PigResource();

    	String requestId= UUID.randomUUID().toString();
    	String inputString= "1,2,3,4,5";
    	String path= PropertyLoader.getInstance().getProperty("requests.basepath") + requestId 
    			+ "/output/part-00000";
    	
    	BufferedWriter writer= new BufferedWriter(
    			new OutputStreamWriter(fileSystem.create(new Path(path), false)));
		writer.write(inputString);
		writer.close();
    	Response resp=  resource.getOutput(requestId);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	StreamingPigOutputImpl out= (StreamingPigOutputImpl) resp.getEntity();
    	ByteArrayOutputStream output= new ByteArrayOutputStream();
    	out.write(output);
    	
    	String outStr= output.toString();
    	Assert.assertEquals(outStr.contains("1,2,3,4"), true);
    	
    	requestId= "abc";
    	try {
    		resp=  resource.getOutput(requestId);
    	} catch (Exception e) {
    		Assert.assertNotNull(resp);
    	}
    }
    
    @Test 
    public void testGetStatus() throws Exception {
    	PigResource resource= new PigResource();
    	PigRequestStats stats= new PigRequestStats(15, 14, null, 5);
    	stats.setStatus("STATUS");
    	
    	String requestId= UUID.randomUUID().toString();
    	String path= PropertyLoader.getInstance().getProperty("requests.basepath") + requestId 
    			+ "/stats";
    	
    	PigUtils.writeStatsFile(new Path(path), stats);
    	Response resp=  resource.getRequestStatus(requestId);
    	Assert.assertEquals(resp.getStatus(), 200);
    	
    	requestId= "abc";
    	try {
    		resp=  resource.getRequestStatus(requestId);
    	} catch (Exception e) {
    		Assert.assertNotNull(resp);
    	}
    	
    	requestId= UUID.randomUUID().toString();
    	fileSystem.mkdirs(new Path(PropertyLoader.getInstance().getProperty("requests.basepath") + requestId));
    	try {
    		resp=  resource.getRequestStatus(requestId);
    	} catch (Exception e) {
    		Assert.assertNotNull(e);
    	}
    }
    
    @BeforeClass
    protected void tearDown() throws Exception {

    }
}
