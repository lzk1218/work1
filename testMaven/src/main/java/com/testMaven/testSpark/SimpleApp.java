package com.testMaven.testSpark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class SimpleApp {
	public static void main(String[] args) {
	    String logFile = "D:/spark-2.2.0-bin-hadoop2.7/README.md"; // Should be some file on your system
	    SparkSession spark = SparkSession.builder().appName("Simple Application").getOrCreate();
	    Dataset<String> logData = spark.read().textFile(logFile).cache();
	    
	    logData.show();
	    
	    long numAs = logData.filter(s -> s.contains("a")).count();
	    long numBs = logData.filter(s -> s.contains("b")).count();
	
	    System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
	
	    spark.stop();
	  }
}
