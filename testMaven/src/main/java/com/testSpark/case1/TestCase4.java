package com.testSpark.case1;

import org.apache.spark.sql.SparkSession;

public class TestCase4 {

	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}
	
	private static SparkSession createSession() {
		SparkSession spark = SparkSession
			      .builder()
			      .master("local[2]")
			      .appName("test case 1 session")
			      .config("spark.some.config.option", "some-value")
			      .getOrCreate();
		
		return spark;
	}
}
