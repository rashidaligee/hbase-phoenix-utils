#Introduction
This project helps analyzing phoenix query. Application prints out query PLAN in console. It also gathers query metrics and prints it. To understand query PLAN and metrics, visit [Query Plan](https://phoenix.apache.org/explainplan.html) and  [Phoenix Query Metrics](https://phoenix.apache.org/metrics.html)

## Build
Project is maven based and dependent on a ***phoenix-core*** artifact. To build the project run following command 
> mvn clean package

To build the project against a particular phoenix version, update ***phoenix-core*** version in ***pom.xml***

## Application Configuration
To run application, it requires a properties which must contain following three properties:

1. jdbc_url
2. con_props
3. query 

A sample properties file is provided in the project at [query.properties](https://github.com/rashidaligee/hbase-phoenix-utils/blob/master/phoenix-tester/query.properties).

## Application Execution
To run application, make sure HBASE class path and phoenix client jar file is added to the application class path.

> HBASE_CLASSPATH = `hbase classpath`
> 
> java -cp "<phoenix_client_jar_file>:<applicationJar File>:${HBASE_CLASSPATH}" com.srh.phoenixtester.PhoenixQueryTester <path_to_application_properties>

## Enabling Phoenix Client Debug Log
Phoenix uses log4j to control/configure log statements. To provide a custom log4j.properties file, add a jvm parameter. i.e. -Dlog4j.configuration=file:///<location_to_custom_log4_properties>