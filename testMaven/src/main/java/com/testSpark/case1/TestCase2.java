package com.testSpark.case1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

public class TestCase2 {

	public static void main(String[] args) throws Exception {
		SparkSession spark = createSession();
		
		StructType userSchema = new StructType().add("userId", "string")
				.add("depId", "string")
				.add("addressId", "string")
				.add("insurId", "string");

		Dataset<Row> ds = spark.readStream()
		.format("csv")
		.schema(userSchema)
		.load("D:/spark-test/case1/user");
		
		ds.printSchema();
		
		//method 1
//		ds = ds.filter(row -> row.get(0).equals("u10"));
//		
//		StreamingQuery query = ds.writeStream()
//				.format("console")
//				.start();
		
		//method 2
		StreamingQuery query = ds.groupBy(ds.col("depId")).count().writeStream()
		.outputMode("complete")
		.format("console")
		.start();
		
		
		
		//
		//rtn.show();
		//
		
		//method 3
//		ds.createOrReplaceTempView("user");
//		StreamingQuery query = spark.sql("select u.* from user u where u.userId = 'u10'")
//		.writeStream()
//		.format("console")
//		.start();
		
		query.awaitTermination();
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
