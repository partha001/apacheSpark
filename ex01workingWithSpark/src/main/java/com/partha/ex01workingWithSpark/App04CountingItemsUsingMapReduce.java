package com.partha.ex01workingWithSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App04CountingItemsUsingMapReduce {

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
        JavaRDD<Double> sqrtRDD = inputRDD.map(input -> Math.sqrt(input));
        
//        //this might through java.io.NotSerializableException and might work in some cases
//        sqrtRDD.foreach(System.out::print);
        
        //to avoid serializable exception first collecting the values from RDD in a list
        sqrtRDD.collect().forEach(System.out:: println);
        
        sc.close();
    
	}

}
