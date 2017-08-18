package com.testFlink.case1;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class TestCase2 {
	
	/**
	 * seems that the test data should be a stream that continuous coming in
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		final List<String> list1 = new ArrayList<String>();
		list1.add("u1,d1,a1,i1");
		list1.add("u2,d1,a2,i2");
		list1.add("u3,d1,a3,i3");
		list1.add("u4,d1,a4,i4");
		list1.add("u5,d1,a5,i5");
		
		final List<String> list2 = new ArrayList<String>();
		list2.add("i2,u2");
		list2.add("i3,u3");
		list2.add("i4,u4");
//		
//		DataStream<String> ds1 = env.fromCollection(list1,
//				TypeInformation.of(new TypeHint<String>(){}));
//		DataStream<String> ds2 = env.fromCollection(list2,
//				TypeInformation.of(new TypeHint<String>(){}));
		
		
		DataStream<String> ds1 = env.fromCollection(new MyIte(list1),
				TypeInformation.of(new TypeHint<String>(){}));
		DataStream<String> ds2 = env.fromCollection(new MyIte(list2),
				TypeInformation.of(new TypeHint<String>(){}));
		
		
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
		.print().setParallelism(1);

		env.execute("Test Joined Window");
	}
	
	public static class MyIte implements Iterator<String>, Serializable {
		
		private static final long serialVersionUID = -8609578154383761338L;
		
		private List<String> list = null;
				
		public MyIte(List<String> list) {
			this.list = list;
		}
		
		int i = 0;
		@Override
		public boolean hasNext() {
			return true;
		}

		@Override
		public String next() {
			return list.get(i++%list.size());
		}
	}
}
