package com.partha.ex01workingWithSpark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App09WorkingWithFilters {

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
		JavaRDD<String> words = originalMessages.flatMap(item -> Arrays.asList(item.split(" ")).iterator());
		
		//this will keep the previous unaltered
		//JavaRDD<String> filteredWords = words.filter(word -> true);
		
		//this will reject all words and leave each list empty
		//JavaRDD<String> filteredWords = words.filter(word -> false);
		
		
		JavaRDD<String> filteredWords = words.filter(word -> word.length() < 6);
		
		
		//this should be fine. 
		//words.foreach(System.out::println);
		
		//however if there is any serialization exception then we can print as below
		filteredWords.collect().forEach(System.out::println);
		
		sc.close();
	}	

}
