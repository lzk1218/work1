package com.testMaven.testSpark;

import java.util.Arrays;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class TestText {
	
	private static final Pattern SPACE = Pattern.compile(" ");
	
	public static void main(String[] args) throws Exception {
		SparkConf sparkConf = new SparkConf().setAppName("TextStream");
		
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));
		
		JavaDStream<String> stream = ssc.textFileStream("D:\\apache-tomcat-8.0.8\\LICENSE");
		
		JavaDStream<String> words = stream.flatMap(x -> Arrays.asList(SPACE.split(x)).iterator());
		JavaPairDStream<String, Integer> wordCounts = words.mapToPair(s -> new Tuple2<>(s, 1))
		        .reduceByKey((i1, i2) -> i1 + i2);

	    wordCounts.print();
	    ssc.start();
	    ssc.awaitTermination();
	}

}
