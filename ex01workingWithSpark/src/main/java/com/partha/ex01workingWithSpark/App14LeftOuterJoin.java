package com.partha.ex01workingWithSpark;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

/**
 * this program show how left outer join works in spark
 * @author biswa
 *
 */
public class App14LeftOuterJoin {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

		SparkConf conf = new SparkConf()
				.setAppName("startingSpark")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		List<Tuple2<Integer,Integer>> visitsRaw = new ArrayList<>();
		visitsRaw.add(new Tuple2<Integer,Integer>(4,18));
		visitsRaw.add(new Tuple2<Integer,Integer>(6,4));
		visitsRaw.add(new Tuple2<Integer,Integer>(10,9));


		List<Tuple2<Integer,String>> usersRaw = new ArrayList<>();
		usersRaw.add(new Tuple2<Integer,String>(1,"John"));
		usersRaw.add(new Tuple2<Integer,String>(2,"Bob"));
		usersRaw.add(new Tuple2<Integer,String>(3,"Alan"));
		usersRaw.add(new Tuple2<Integer,String>(4,"Doris"));
		usersRaw.add(new Tuple2<Integer,String>(5,"Marybelle"));
		usersRaw.add(new Tuple2<Integer,String>(6,"Raquel"));

		JavaPairRDD<Integer, Integer> visits = sc.parallelizePairs(visitsRaw);
		JavaPairRDD<Integer, String> users = sc.parallelizePairs(usersRaw);


		JavaPairRDD<Integer, Tuple2<Integer, Optional<String>>> joinedRDD = visits.leftOuterJoin(users);
		joinedRDD.foreach(tuple -> System.out.println(tuple));


		sc.close();
	}

}
