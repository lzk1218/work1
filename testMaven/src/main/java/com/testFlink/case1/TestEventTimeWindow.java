package com.testFlink.case1;

import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestEventTimeWindow {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		DataStream<Tuple3<Integer,Long, Integer>> ds1 = env.fromCollection(new MyIte(2,200),
				TypeInformation.of(new TypeHint<Tuple3<Integer,Long, Integer>>(){}));
		
		ds1 = ds1.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<Integer,Long, Integer>>(){

			private static final long serialVersionUID = 424221564193141062L;

			@Override
			public long extractTimestamp(Tuple3<Integer,Long, Integer> tuple, long arg1) {
				return tuple.f1;
			}

			@Override
			public Watermark getCurrentWatermark() {
				return new Watermark(System.currentTimeMillis());
			}
			
		});
		
		ds1.keyBy(tuple -> tuple.f0)
		.timeWindow(Time.seconds(2))
		.sum(2)
		.print()
		.setParallelism(1);

		
		env.execute("Test Event Time Window");
		
	}
	
	
	public static class MyIte implements Iterator<Tuple3<Integer,Long, Integer>>, Serializable {
		
		private static final long serialVersionUID = -8609578154383761338L;
		
		private int windowSeconds;
		
		private int maxCount = 0;
		
		private long startTime = 0;
		
		private int currentCount = 0;
		
		private Random random = new Random();
		
		public MyIte(int windowSeconds, int maxCount) {
			this.windowSeconds = windowSeconds;
			this.maxCount = maxCount;
		}
		
		int i = 0;
		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public Tuple3<Integer,Long, Integer> next() {
			long currentTime = System.currentTimeMillis();
			if (startTime == 0) {
				startTime = currentTime;
			}
			if (currentCount > maxCount) {
				while (currentTime - startTime < windowSeconds * 1000) {
					try {
						Thread.sleep(50);
					} catch (Exception e) {
						e.printStackTrace();
						Thread.interrupted();
					}
				}
				currentCount = 0;
				startTime = currentTime;
			}
			currentCount ++;
			return new Tuple3<Integer,Long, Integer>(random.nextInt(this.maxCount), currentTime, 1);
		}
	}
}
