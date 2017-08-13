package com.testSpark.case1;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

public class TestCase1 {
	
	/**
	 * This case uses JavaStreamingContext to get stream source, then using Spark sql to get statistics.
	 * @param args
	 * @throws Exception
	 */
	
	public static void main(String[] args) throws Exception {
		
		SparkConf sparkConf = new SparkConf().setAppName("test case 1").setMaster("local[2]");
	    JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.milliseconds(300));

//	    JavaDStream<String> line = ssc.textFileStream("file://D:/spark-test/case1/user.csv");
	    
	    JavaDStream<String> line = ssc.receiverStream(new MyReceiver("D:/spark-test/case1/user.csv"));
	    
	    JavaDStream<String> line2 = ssc.receiverStream(new MyReceiver("D:/spark-test/case1/insurance.csv"));
	    
	    JavaDStream<User> users = line.map(l -> {
	    	String[] parts = l.split(",");
	    	User user = new User();
	    	user.setUserId(parts[0]);
	    	user.setDepId(parts[1]);
	    	user.setAddressId(parts[2]);
	    	user.setInsurId(parts[3]);
	    	return user;
	    });
	    
	    JavaDStream<Insurance> insurance = line2.map(i -> {
	    	String[] parts = i.split(",");
	    	Insurance ins = new Insurance();
	    	ins.setInsurId(parts[0]);
	    	ins.setUserId(parts[1]);
	    	return ins;
	    });
	    
	    List<JavaDStream> mqReceiverDStreamList = new ArrayList<>();
	    mqReceiverDStreamList.add(insurance);
	    //ssc.union(users, mqReceiverDStreamList);
	    
	    users.foreachRDD(rdd->{
	    	SparkSession spark = createSession();
	    	Dataset<Row> ds = spark.createDataFrame(rdd, User.class);
	    	ds.show();
	    	ds.createOrReplaceTempView("user");
	    });
	    
	    insurance.foreachRDD(rdd->{
	    	SparkSession spark = createSession();
	    	Dataset<Row> ds = spark.createDataFrame(rdd, Insurance.class);
	    	ds.show();
	    	ds.createOrReplaceTempView("insurance");
	    	
	    	Dataset<Row> results = spark.sql("select u.* from user u inner join insurance i on u.insurId = i.insurId");
	 	    
	 	    results.map(row -> row.getString(0), Encoders.STRING());
	 	    
	 	    results.show();
	    });
	    
	    SparkSession spark = createSession();
//	    Dataset<Row> results = spark.sql("select u.* from user u inner join insurance i on u.insurId = i.insurId");
//	    
//	    results.map(row -> row.getString(0), Encoders.STRING());
//	    
//	    results.show();
	    
	    ssc.start();
	    ssc.awaitTermination();
	}
	
	private static SparkSession createSession() {
		SparkSession spark = SparkSession
			      .builder()
			      .appName("test case 1 session")
			      .config("spark.some.config.option", "some-value")
			      .getOrCreate();
		
		return spark;
	}
	
	private static List<String> readFromFile(String path) throws Exception{
		return Files.readAllLines(Paths.get(path));
	}
}
