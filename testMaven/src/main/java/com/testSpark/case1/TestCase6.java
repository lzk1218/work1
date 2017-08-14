package com.testSpark.case1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

/**
 * structured data stream join with other streams
 * @author lzk48681
 *
 */
public class TestCase6 {

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
		
		StructType insSchema = new StructType().add("insurId", "string")
				.add("userId", "string");
		
		Dataset<Row> ds2 = spark.readStream()
				.format("csv")
				.schema(insSchema)
				.load("D:/spark-test/case1/insurance");
				
		ds2.printSchema();
		
		//method 1 inner join between two stream DataFrames
		//raise an error, because Inner join between two streaming DataFrames/Datasets is not supported;
//		ds = ds.join(ds2, ds.col("userId").equalTo(ds2.col("userId")));
//		StreamingQuery query = ds.writeStream()
//		.format("console")
//		.start();
		
		
		//method 2 inner join with static DataFrame
		//it is running good
		Dataset<Row> ds3 = spark.read()
							.format("csv")
							.schema(insSchema)
							.load("D:/spark-test/case1/insurance");
		ds = ds.join(ds3, ds.col("userId").equalTo(ds3.col("userId")));
		StreamingQuery query = ds.writeStream()
		.format("console")
		.start();
		
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
