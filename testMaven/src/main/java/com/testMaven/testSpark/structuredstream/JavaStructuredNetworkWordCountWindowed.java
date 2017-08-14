package com.testMaven.testSpark.structuredstream;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.StreamingQuery;

import scala.Tuple2;

public final class JavaStructuredNetworkWordCountWindowed {

	  public static void main(String[] args) throws Exception {
	    if (args.length < 3) {
	      System.err.println("Usage: JavaStructuredNetworkWordCountWindowed <hostname> <port>" +
	        " <window duration in seconds> [<slide duration in seconds>]");
	      System.exit(1);
	    }

	    String host = "localhost";
	    int port = 9999;
	    int windowSize = Integer.parseInt(args[2]);
	    int slideSize = (args.length == 3) ? windowSize : Integer.parseInt(args[3]);
	    if (slideSize > windowSize) {
	      System.err.println("<slide duration> must be less than or equal to <window duration>");
	    }
	    String windowDuration = windowSize + " seconds";
	    String slideDuration = slideSize + " seconds";

	    SparkSession spark = SparkSession
	      .builder()
	      .appName("JavaStructuredNetworkWordCountWindowed")
	      .getOrCreate();

	    // Create DataFrame representing the stream of input lines from connection to host:port
	    Dataset<Row> lines = spark
	      .readStream()
	      .format("socket")
	      .option("host", host)
	      .option("port", port)
	      .option("includeTimestamp", true)
	      .load();

	    // Split the lines into words, retaining timestamps
	    Dataset<Row> words = lines
	      .as(Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP()))
	      .flatMap((FlatMapFunction<Tuple2<String, Timestamp>, Tuple2<String, Timestamp>>) t -> {
	          List<Tuple2<String, Timestamp>> result = new ArrayList<>();
	          for (String word : t._1.split(" ")) {
	            result.add(new Tuple2<>(word, t._2));
	          }
	          return result.iterator();
	        },
	        Encoders.tuple(Encoders.STRING(), Encoders.TIMESTAMP())
	      ).toDF("word", "timestamp");

	    // Group the data by window and word and compute the count of each group
	    Dataset<Row> windowedCounts = words.groupBy(
	      functions.window(words.col("timestamp"), windowDuration, slideDuration),
	      words.col("word")
	    ).count().orderBy("window");

	    // Start running the query that prints the windowed word counts to the console
	    StreamingQuery query = windowedCounts.writeStream()
	      .outputMode("complete")
	      .format("console")
	      .option("truncate", "false")
	      .start();

	    query.awaitTermination();
	  }
	}
