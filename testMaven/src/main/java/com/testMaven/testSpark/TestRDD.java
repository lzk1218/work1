package com.testMaven.testSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class TestRDD {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf().setAppName("TestRDD");
		JavaSparkContext sc = new JavaSparkContext(conf);
		JavaRDD<String> lines = sc.textFile("D:\\apache-tomcat-8.0.8\\LICENSE");
		JavaRDD<Integer> lineLengths = lines.map(s -> s.length());
		int totalLength = lineLengths.reduce((a, b) -> a + b);
		
		System.out.println(totalLength);
	}

}
