package com.testSpark.case1;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

/**
 * union and split
 * @author lzk48681
 *
 */
public class TestCase7 {

	public static void main(String[] args) throws Exception {
		JavaStreamingContext jssc = createContext();
		
		JavaDStream<String> ds1 = jssc.textFileStream("D:/spark-test/case1/user");
		
		JavaDStream<String> ds2 = jssc.textFileStream("D:/spark-test/case1/insurance");
		
		JavaDStream<String> ds = ds1.union(ds2);
		
		ds.foreachRDD(rdd -> {
			
			SparkSession spark = SparkSession.builder().config(rdd.context().getConf()).getOrCreate();
			JavaRDD<User> rowRDD = rdd.filter(line -> {
				return line.split(",").length == 4;
			}).map(record -> {
				String[] strs = record.split(",");
				User user = new User();
				user.setUserId(strs[0]);
				user.setDepId(strs[1]);
				user.setAddressId(strs[2]);
				user.setInsurId(strs[3]);
				return user;
			});
			Dataset<Row> dsr = spark.createDataFrame(rowRDD, User.class);
			dsr.createOrReplaceTempView("user");
			
			
//			JavaRDD<Insurance> rowRDD2 = rdd.filter(line -> {
//				return line.split(",").length == 2;
//			}).map(record -> {
//				String[] strs = record.split(",");
//				Insurance ins = new Insurance();
//				ins.setInsurId(strs[0]);
//				ins.setUserId(strs[1]);
//				return ins;
//			});
//			Dataset<Row> dsr2 = spark.createDataFrame(rowRDD2, Insurance.class);
//			dsr2.createOrReplaceTempView("insurance");
//			
//			Dataset<Row> results = spark.sql("select u.* from user u inner join insurance i on u.insurId = i.insurId");
//			
//			results.show();
		});
		
		jssc.start();
		jssc.awaitTermination();
	}
	
	private static JavaStreamingContext createContext() {
		SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TestCase7");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
		return jssc;
	}
}
