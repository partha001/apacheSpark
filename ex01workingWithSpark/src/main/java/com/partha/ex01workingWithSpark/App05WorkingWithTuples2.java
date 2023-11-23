package com.partha.ex01workingWithSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

//import com.partha.ex01workingWithSpark.dto.IntegerWithSqrtDto;

import scala.Tuple2;

public class App05WorkingWithTuples2 {

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
        //JavaRDD<IntegerWithSqrtDto> sqrtRDD = inputRDD.map(input -> new IntegerWithSqrtDto(input, Math.sqrt(input)));
        //sqrtRDD.collect().forEach(item -> System.out.println("number:"+item.getNumber() + "   sqrt:"+item.getSquareRoot()));
        
        JavaRDD<Tuple2<Integer, Double>> map = inputRDD.map(item -> new Tuple2<Integer,Double>(item,Math.sqrt(item)));
        map.collect().forEach(tuple -> System.out.println("number:"+tuple._1() + "   sqrt:"+tuple._2()));
        
        sc.close();
	}

}
