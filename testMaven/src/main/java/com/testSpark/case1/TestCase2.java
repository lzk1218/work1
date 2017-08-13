package com.testSpark.case1;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.StructType;

public class TestCase2 {
	
	
	/**
	 * using structured streaming
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		SparkSession spark = createSession();
		
		StructType userSchema = new StructType().add("userId", "string")
												.add("depId", "string")
												.add("addressId", "string")
												.add("insurId", "string");
		
		Dataset<Row> ds = spark.readStream()
				.format("csv")
				.schema(userSchema)
				.load("D:/spark-test/case1/user.csv");
		
		ds.printSchema();
		
//		ds.createOrReplaceTempView("user");
//		
//		Dataset<Row> rtn = spark.sql("select u.* from user u where u.userId = 'u10'");
		
		ds.filter(row -> row.get(0).equals("u10"));
		
//		
//		rtn.show();
//		
		StreamingQuery query = ds.writeStream()
//				  .outputMode("complete")
				  .option("basePath", "file:////D:/spark-test/case1")
				  .format("console")
				  .start();
//		spark.sql("select u.* from user u where u.userId = 'u10'")
//			.writeStream()
//			.format("console")
//			.start()
//			.awaitTermination();
		
//		
		
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
