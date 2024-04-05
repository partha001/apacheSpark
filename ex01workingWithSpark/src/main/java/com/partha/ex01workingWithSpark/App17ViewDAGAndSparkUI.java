package com.partha.ex01workingWithSpark;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import scala.Tuple2;

/**
 * run the program and visit the link http://localhost:4040/jobs/ 
 * while the program waits for input
 * @author biswa
 *
 */
public class App17ViewDAGAndSparkUI {

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

		/** both full outer gets all the results from both sides **/
		JavaPairRDD<Integer, Tuple2<Optional<Integer>, Optional<String>>> joinedRDD = visits.fullOuterJoin(users);
		joinedRDD.foreach(tuple -> System.out.println(tuple));
//		//output:
//		(5,(Optional.empty,Optional[Marybelle]))
//		(6,(Optional[4],Optional[Raquel]))
//		(1,(Optional.empty,Optional[John]))
//		(4,(Optional[18],Optional[Doris]))
//		(3,(Optional.empty,Optional[Alan]))
//		(2,(Optional.empty,Optional[Bob]))
//		(10,(Optional[9],Optional.empty))

		
		Scanner scanner = new Scanner(System.in);
		scanner.nextLine();
		sc.close();
	}

}
