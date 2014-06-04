#Oink

The aim of this project is to provide a REST based interface for PIG execution. Oink is Pig on Servlet which provides the following functionalities:

* Register/unregister/view a Pig script
* Register/unregister/view a jar file (for customer defined UDF functions)
* Execute a Pig job
* View the status/stats of a Pig job
* Cancel a Pig job

For more information on the targetted use-case and REST documentation please refer to our [wiki](https://github.scm.corp.ebay.com/vija/Oink-OSS/wiki).

## Usage

### Requisites
 * Tomcat with JDK 7
 * Maven 

### Build
The following steps need to be followed in order to build the war file of the REST service:
 * Clone the project on GitHub
 * Do a maven build at the top level of the project using `mvn clean install`
 * The war file will be generated in service/target/pig.war

### Running test cases
 * In order to run test cases use `mvn clean test`

### Deployment
 * Shut down the tomcat if it is running using `bin/shutdown.sh`
 * Kill the process manually if the tomcat hasn't shutdown gracefully
 * Place the pig.war in the webapps directory of the Tomcat instance and remove the exploded directory of pig if it exists
 * Startup the tomcat using `bin/startup.sh`

### Default Configuration
Default configuration is to use localhost as the Hadoop cluster. Following are configuration parameters available (under service/src/main/resources/default.properties file) which can be changed :

```
jobtracker=localhost:8021 (Job tracker address)
jobtracker.ui=http://localhost:50030/jobdetails.jsp?jobid= (Job tracker UI URL used for providing stats information)
fs.default.name=hdfs://localhost:8020 (Hadoop Namenode address)
scripts.basepath=hdfs://localhost:8020/tmp/pig/scripts/ (Location in DFS where PIG scripts will be stored)
jars.basepath=hdfs://localhost:8020/tmp/pig/jars/ (Location in DFS where jar files will be stored)
requests.basepath=hdfs://localhost:8020/tmp/pig/requests/ (Location where request specific information will be stored)
max.threads=20 (size of thread pool for running PIG requests in parallel)
```

The core-site.xml and mapred-site.xml files are bundled along with the property files to provide cluster specific parameters like 
namenode address, jobtracker address in order to enable connecting to remote clusters. If the cluster is a secure cluster 
and uses authentication mechanisms like Kerberos then parameters like kerberos principal need to be provided in the 
mapred-site.xml and core-site.xml. Also, the queue to which jobs must be submitted also needs to provided in the 
mapred-site.xml

Different kinds of property files can be created based on the environment in which the service is going to be hosted. 
For example a prod.properties file can be created to house the settings for a production instance. To pick up the property
file the following steps should be followed during startup of the Tomcat instance. The environment variable `env` should 
be set as the desired environment as follows:
```
export env=prod
```

This would make sure that prod.properties is picked up by the ConfigurationLoader at startup.

### Project Structure
The directory structure of our project is explained in the [Project Structure](https://github.scm.corp.ebay.com/vija/Oink-OSS/wiki/Project-Structure) wiki

## Known Issues and Feature Requests
Please use our [Issues](https://github.scm.corp.ebay.com/vija/Oink-OSS/issues) page to view the existing issues, to raise bugs and request for new features.

## Contribution Guidelines
Eager to contribute? Steps to contribute to our project is available in our [Contribution Guidelines](https://github.scm.corp.ebay.com/vija/Oink-OSS/wiki/Contribution-Guidelines) wiki


