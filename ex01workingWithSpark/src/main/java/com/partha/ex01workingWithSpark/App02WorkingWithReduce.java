package com.partha.ex01workingWithSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class App02WorkingWithReduce {

	public static void main(String[] args) {

        List<Double> inputData = new ArrayList<>();
        inputData.add(35.5);
        inputData.add(12.499);
        inputData.add(90.32);
        inputData.add(20.32);

        SparkConf conf = new SparkConf()
        		.setAppName("startingSpark")
        		.setMaster("local[*]"); //this is the configuration to run spark locally
        //if we mention setMaster("local") then spark will run on a single thread. however if we mention 
        //as above then spark will execute it parallely on multiple cores of the local machine       
        JavaSparkContext sc = new JavaSparkContext(conf);
        

        
        JavaRDD<Double> inputRDD = sc.parallelize(inputData); //here we are loading the data on which we want to work . i.e. list in this case
        Double result = inputRDD.reduce((value1, value2) -> value1 + value2);
        System.out.println(result);
        //in theory we are suppossed to close the sparkContext in a finally block however its not required since spark takes care of it
        // finally { sc.close();}
        sc.close();
    
	}

}
