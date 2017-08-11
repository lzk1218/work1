package com.testSpark.case1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class TestCase3 {

	/**
	 * use spark sql only, no streamming
	 * @param args
	 */
	public static void main(String[] args) {
		SparkSession spark = createSession();
		
		StructType userSchema = new StructType().add("userId", "string")
												.add("depId", "string")
												.add("addressId", "string")
												.add("insurId", "string");
		
		StructType insuranceSchema = new StructType().add("insurId", "string")
				.add("userId", "string");
		
		Dataset<Row> ds = spark.read().schema(userSchema).csv("D:/spark-test/case1/user.csv");
		
		Dataset<Row> ds2 = spark.read().schema(insuranceSchema).csv("D:/spark-test/case1/insurance.csv");
		
		ds.show();
		
		ds.createOrReplaceTempView("user");
		
		ds2.createOrReplaceTempView("insurance");
		
		Dataset<Row> rtn = spark.sql("select u.* from user u where u.userId = 'u10'");
		
		Dataset<Row> results = spark.sql("select u.* from user u inner join insurance i on u.insurId = i.insurId");
		
		results.show();
		
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
