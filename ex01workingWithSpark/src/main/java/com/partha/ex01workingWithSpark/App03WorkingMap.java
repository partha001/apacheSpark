package com.partha.ex01workingWithSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App03WorkingMap {

	public static void main(String[] args) {

		List<Integer> inputData = new ArrayList<>();
		inputData.add(35);
		inputData.add(12);
		inputData.add(90);
		inputData.add(20);

		SparkConf conf = new SparkConf()
				.setAppName("startingSpark")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);



		JavaRDD<Integer> inputRDD = sc.parallelize(inputData); 
		JavaRDD<Long> transformRDD = inputRDD.map(input -> 1L);
		Long count = transformRDD.reduce((val1 , val2)-> val1 + val2);
		System.out.println("itemCount:"+ count);

		sc.close();

	}

}
