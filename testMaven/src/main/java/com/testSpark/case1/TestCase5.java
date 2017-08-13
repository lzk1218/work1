package com.testSpark.case1;

import java.util.Arrays;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;

public class TestCase5 {

	public static void main(String[] args) throws Exception {
		SparkSession spark = createSession();
		// Create DataFrame representing the stream of input lines from connection to localhost:9999
		Dataset<Row> lines = spark
		  .readStream()
		  .format("socket")
		  .option("host", "localhost")//106.15.202.49
		  .option("port", 9999)
		  .load();

		// Split the lines into words
		Dataset<String> words = lines
		  .as(Encoders.STRING())
		  .flatMap((FlatMapFunction<String, String>) x -> Arrays.asList(x.split(" ")).iterator(), Encoders.STRING());

		// Generate running word count
		Dataset<Row> wordCounts = words.groupBy("value").count();
		
		// Start running the query that prints the running counts to the console
		StreamingQuery query = wordCounts.writeStream()
		  .outputMode("complete")
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
