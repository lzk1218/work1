package com.testSpark.case1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

/**
 * put two stream together via structured streaming
 * @author lzk48681
 *
 */
public class TestCase8 {
	
	public static void main(String[] args) throws Exception {
		SparkSession spark = createSession();
		
		StructType contentSchema = new StructType().add("v1", "string")
				.add("v2", "string")
				.add("v3", "string")
				.add("v4", "string");
		
		Dataset<Row> ds = spark.readStream()
				.schema(contentSchema)
				.csv("D:/spark-test/case1/together");
		
		ds.filter("v3 is not null").createOrReplaceTempView("user");
		
		ds.filter("v3 is null").createOrReplaceTempView("insurance");
		
		// inner join dose not work
		ds = spark.sql("select u.* from user u inner join insurance i on u.v4 = i.v1");
		
		// simple select works
		ds = spark.sql("select i.* from insurance i where i.v1 = 'i3'");
		
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
