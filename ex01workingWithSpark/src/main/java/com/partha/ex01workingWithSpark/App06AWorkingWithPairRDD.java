package com.partha.ex01workingWithSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/**
 * this program show how to build pairRDD
 * @author partha
 *
 */
public class App06AWorkingWithPairRDD {

	public static void main(String[] args) {
		List<String> inputData = new ArrayList<>();
		inputData.add("WARN: Tuesday 4 September 0405");
		inputData.add("ERROR: Tuesday 4 September 0408");
		inputData.add("FATAL: Wednesday 5 September 1632");
		inputData.add("ERROR: Friday 7 September 1854");
		inputData.add("WARN: Saturday 8 September 1942");


		SparkConf conf = new SparkConf()
				.setAppName("startingSpark")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> originalMessages = sc.parallelize(inputData); 
	
		JavaPairRDD<String, String> pairRDD = originalMessages.mapToPair(  rawData -> {
			String[] splittedInput = rawData.split(":");
			String level = splittedInput[0];
			String date = splittedInput[1];
			return new Tuple2<String, String>(level,date);			
		});
		
		sc.close();
	}

}
