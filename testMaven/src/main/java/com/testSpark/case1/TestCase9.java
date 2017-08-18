package com.testSpark.case1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

/**
 * same with TestCase8 but use Dataset's own join method
 * @author lzk48681
 *
 */
public class TestCase9 {

	public static void main(String[] args) throws Exception {
		SparkSession spark = createSession();
		
		StructType contentSchema = new StructType().add("v1", "string")
				.add("v2", "string")
				.add("v3", "string")
				.add("v4", "string");
		
		Dataset<Row> ds = spark.readStream()
				.schema(contentSchema)
				.csv("D:/spark-test/case1/together");
		
		Dataset<Row> ds1 = ds.filter("v3 is not null").alias("a");
		
		Dataset<Row> ds2 = ds.filter("v3 is null").alias("b");
		
		//this seems not working also.
		ds1 = ds1.join(ds2, ds1.col("v1").equalTo(ds2.col("v1")));
		
		StreamingQuery query = ds1.writeStream()
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
