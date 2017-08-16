package com.testFlink.case1;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestCase1 {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		DataStream<String> ds1 = env.readTextFile("file:///D:/spark-test/case1/user/");
		DataStream<String> ds2 = env.readTextFile("file:///D:/spark-test/case1/insurance/");
		DataStream<Tuple4<String,String,String,String>> dds1 = ds1.map(str->{
			String[] strs = str.split(",");
			return new Tuple4<String,String,String,String>(strs[0],strs[1],strs[2],strs[3]);
		});
//		dds1.print();
		DataStream<Tuple2<String,String>> dds2 = ds2.map(str->{
			String[] strs = str.split(",");
			return new Tuple2<String,String>(strs[0],strs[1]);
		});
//		dds2.print();
		dds1.join(dds2)
		.where(tuple4 -> tuple4.f0)
		.equalTo(tuple2 -> tuple2.f1)
		.window(TumblingProcessingTimeWindows.of(Time.seconds(1))).apply((left,right) -> left)
		.print();

		env.execute("Test Joined Window");
	}

}
