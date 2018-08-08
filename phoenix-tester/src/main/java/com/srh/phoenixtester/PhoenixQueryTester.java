package com.srh.phoenixtester;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collection;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.phoenix.coprocessor.MetaDataProtocol;
import org.apache.phoenix.monitoring.GlobalMetric;
import org.apache.phoenix.query.QueryServices;
import org.apache.phoenix.util.PhoenixRuntime;

/**
 * Hello world!
 *
 */
public class PhoenixQueryTester 
{
	private static String KEY_JDBC_URL = "jdbc_url";
	private static String KEY_CONN_PROPS = "conn_props";
	private static String KEY_PRINT_DATA = "FALSE";
	private static String KEY_QUERY = "query";
	private static Logger LOGGER = Logger.getLogger(PhoenixQueryTester.class.getName());
	
    public static void main( String[] args ) throws Exception
    {
    	
        if (args.length == 0){
        	usage();
        	return;
        }
//        Class.forName("");
        String fileLocation = args[0];
        PhoenixQueryTester pqt = new PhoenixQueryTester();
        Properties props = pqt.loadPropertiesFile(fileLocation);
	LOGGER.info("Phoenix version:"+MetaDataProtocol.PHOENIX_MAJOR_VERSION+"."+MetaDataProtocol.PHOENIX_MINOR_VERSION+"."+MetaDataProtocol.PHOENIX_PATCH_NUMBER);        
//        pqt.executeExplainQuery(props);
        for(int i =1; i< 6; i++){
		LOGGER.info("Query Execution Starts #"+i);
        	pqt.executeQueryToFindMetrics(props);
		LOGGER.info("Query Execution Ends #"+i);
	}
        		
    }
    
    private void executeQueryToFindMetrics(Properties props) throws Exception {
    	Map<String, Long> overAllQueryMetrics = null;
        Map<String, Map<String, Long>> requestReadMetrics = null;
        long queryStartTime = System.currentTimeMillis(); 
    	Connection con = getConnection(props);
        LOGGER.info("Time To Get Connection:"+(System.currentTimeMillis()-queryStartTime));
        String query = props.getProperty(KEY_QUERY);
        
	long queryExecutionStartTime = System.currentTimeMillis();
        PreparedStatement statment = con.prepareStatement(query);

        ResultSet rs = statment.executeQuery();
        LOGGER.info("QueryExecutionTime = "+(System.currentTimeMillis()-queryExecutionStartTime));
        int numberOfrecords = 0;
        boolean firstRecordRetrieved = false;
	ResultSetMetaData rsMetadata = rs.getMetaData();
	int colCount = rsMetadata.getColumnCount();
	
	long rsProcessingStartTime = System.currentTimeMillis();
	boolean printData = (props.getProperty(KEY_PRINT_DATA) != null && props.getProperty(KEY_PRINT_DATA).equalsIgnoreCase("TRUE"));
	for(int i = 1; i<= colCount; i++){
		if(i>1){
			System.out.print(",");
		}
		System.out.print(rsMetadata.getColumnName(i));
	}

	System.out.println("");

        while(rs.next()){
        	numberOfrecords++;
		if(printData){ 
			for(int i=1; i<= colCount; i++){
				if(i >1) {
					System.out.print(",");
					
				}
				System.out.print(rs.getString(i));
			}
			System.out.println("");
		}
        }
	LOGGER.info("Resultset Processing Time:"+(System.currentTimeMillis()-rsProcessingStartTime));
        LOGGER.info("Total Query Time ="+(System.currentTimeMillis()-queryStartTime));        
        LOGGER.info("Query: "+ query);
    	LOGGER.info("Number of Records:"+numberOfrecords);

    	// read metrics
//         overAllQueryMetrics = PhoenixRuntime.getOverAllReadRequestMetrics(rs);
//         requestReadMetrics = PhoenixRuntime.getRequestReadMetrics(rs);
        
//        Set<String> requestReadMetricskeys = requestReadMetrics.keySet();
//        LOGGER.info("Printing Read Request Metrics....");
/*        for (String key : requestReadMetricskeys) {
        	printMap(requestReadMetrics.get(key));
		}
        
        LOGGER.info("Printing overall Query Metrics....");
        printMap(overAllQueryMetrics);
        
        
        // Mutation Metrics
        
        Map<String, Map<String, Long>> readMutationMetrics = PhoenixRuntime.getReadMetricsForMutationsSinceLastReset(con);
        Map<String, Map<String, Long>> writeMutationmetrics = PhoenixRuntime.getWriteMetricsForMutationsSinceLastReset(con);
        
        LOGGER.info("Printing Read Mutation Metrics.....");
        Set<String> readMutationMetricsKeys = readMutationMetrics.keySet();
        for (String key : readMutationMetricsKeys) {
			printMap(readMutationMetrics.get(key));
		}
        
        LOGGER.info("Printing Write Mutation Metrics....");
        Set<String> writeMutationMetricskeys = writeMutationmetrics.keySet();
        for (String key : writeMutationMetricskeys) {
        	printMap(writeMutationmetrics.get(key));
		}
        
        // global metrics
        Collection<GlobalMetric> globalmetrics = PhoenixRuntime.getGlobalPhoenixClientMetrics();
        LOGGER.info("Printing Global metrics....");
        StringBuilder sb = new StringBuilder();
        for (GlobalMetric globalMetric : globalmetrics) {
			sb.append("Metric:{Name="+globalMetric.getName()+",Value="+globalMetric.getValue()+",totalValue="+globalMetric.getTotalSum()+",currentState="+globalMetric.getCurrentMetricState()+"}");
			sb.append("\n");
		}
        LOGGER.info(sb.toString());
*/
    	rs.close();
    	statment.close();
    	con.close();
        LOGGER.info("Time closing connection:"+(System.currentTimeMillis()-queryStartTime)); 
	}

    private void printMap(Map<String, Long> mapToPrint){
    	
    	if(mapToPrint == null){
    		LOGGER.info("Nothing to print... input is null");
    		return;
    	}
    	LOGGER.info(mapToPrint.toString());
//    	Set<String> keys = mapToPrint.keySet();
//    	for (String key : keys) {
//			LOGGER.info(key +"="+mapToPrint.get(key));
//		}
    	
    }
	public static void usage(){
    	LOGGER.severe("java -cp <classpath> com.srh.phoenixtester.PhoenixQueryTester <propertiesLocation>");
    }
    
    private void executeExplainQuery(Properties props) throws Exception{
    	Connection con = getConnection(props);
        String query = props.getProperty(KEY_QUERY);
        PreparedStatement statment = con.prepareStatement("EXPLAIN "+query);
        ResultSet rs = statment.executeQuery();
        ResultSetMetaData rsMetadata = rs.getMetaData();
        int colCount = rsMetadata.getColumnCount();
        LOGGER.info("Number of Columns = "+colCount);
        String columnNames = "";
        for (int i = 1; i <= colCount; i++) {
			columnNames += ( rsMetadata.getColumnName(i)+"|");
		}
        LOGGER.info(columnNames);
        LOGGER.info("Query: "+ query);
	StringBuffer sb = new StringBuffer();
        while(rs.next()){
        	for (int i = 1; i <= colCount; i++) {
			sb.append(""+rs.getObject(i));
			}
	sb.append('\n');
        }
	LOGGER.info(sb.toString());
    	rs.close();

    	statment.close();
    	con.close();
 	
    }
    
    private Properties getConnectionProperties(Properties props) {
    	
		Properties connProps = new Properties();
		connProps.setProperty(QueryServices.COLLECT_REQUEST_LEVEL_METRICS, "true");
		String strConnProps = props.getProperty(KEY_CONN_PROPS);
		if (strConnProps != null && !strConnProps.trim().equals("")){
			String[] tmp_props = strConnProps.split(",");
			
			for (String prop : tmp_props) {
				if (prop.contains("=")){
					String[] keyValue = prop.split("=");
					connProps.setProperty(keyValue[0].trim(), keyValue[1].trim());
				}
			}
		}
		
		
		return connProps;
	}

	private Connection getConnection(Properties props) throws Exception{
    	Properties connProps = getConnectionProperties(props);
    	String url = props.getProperty(KEY_JDBC_URL);
    	return DriverManager.getConnection(url, connProps);
    }
    
    private Properties loadPropertiesFile(String fileLocation) {
    	Properties pr = new Properties();
    	FileInputStream is = null;
    	try{
    	is = new FileInputStream(fileLocation);
    	pr.load(is);
    	}catch(Exception ex ){
    		ex.printStackTrace();
    	}finally{
    		if(is != null)
    			try{is.close();
    			}catch(Exception ex){
    				ex.printStackTrace();
    			}
    	}
    	return pr;
    }
}
