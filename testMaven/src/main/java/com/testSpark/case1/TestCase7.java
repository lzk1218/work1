package com.testSpark.case1;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

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
		
//		JavaReceiverInputDStream<String> ds1 = jssc.socketTextStream("localhost", 9999);
		
//		JavaDStream<String> ds1 = jssc.textFileStream("D:/spark-test/case1/user");
		
//		JavaDStream<String> ds2 = jssc.textFileStream("D:/spark-test/case1/insurance");
		
		// Create and push some RDDs into the queue
		
		List<String> list1 = new ArrayList<>();
	    list1.add("u1,d1,a1,i1");
	    list1.add("u2,d1,a1,i2");
	    list1.add("u3,d1,a2,i3");

	    Queue<JavaRDD<String>> rddQueue1 = new LinkedList<>();
	    for (int i = 0; i < 30; i++) {
	      rddQueue1.add(jssc.sparkContext().parallelize(list1));
	    }
		
	    JavaDStream<String> ds1 = jssc.queueStream(rddQueue1);
		
	    List<String> list = new ArrayList<>();
	    list.add("i2,u2");
	    list.add("i3,u3");

	    Queue<JavaRDD<String>> rddQueue = new LinkedList<>();
	    for (int i = 0; i < 30; i++) {
	      rddQueue.add(jssc.sparkContext().parallelize(list));
	    }
		
	    JavaDStream<String> ds2 = jssc.queueStream(rddQueue);
	    
		JavaDStream<String> ds = ds1.union(ds2);
		ds.print();
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
			
			
			JavaRDD<Insurance> rowRDD2 = rdd.filter(line -> {
				return line.split(",").length == 2;
			}).map(record -> {
				String[] strs = record.split(",");
				Insurance ins = new Insurance();
				ins.setInsurId(strs[0]);
				ins.setUserId(strs[1]);
				return ins;
			});
			Dataset<Row> dsr2 = spark.createDataFrame(rowRDD2, Insurance.class);
			dsr2.createOrReplaceTempView("insurance");
			
			Dataset<Row> results = spark.sql("select u.* from user u inner join insurance i on u.insurId = i.insurId");

			results.show();
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
