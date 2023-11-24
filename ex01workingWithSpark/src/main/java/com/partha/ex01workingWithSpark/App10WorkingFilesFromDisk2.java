package com.partha.ex01workingWithSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App10WorkingFilesFromDisk2 {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

		SparkConf conf = new SparkConf()
				.setAppName("startingSpark")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);
		
		JavaRDD<String> originalMessages = sc.textFile("src/main/resources/subtitles/input.txt");
		originalMessages.collect().forEach(System.out::println);
		
		sc.close();
	}	

}
