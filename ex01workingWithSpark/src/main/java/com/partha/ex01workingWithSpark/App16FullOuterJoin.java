package com.partha.ex01workingWithSpark;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;

import com.esotericsoftware.kryo.io.Input;

import scala.Tuple2;

/**
 * this program show how full outer join works in spark
 * @author biswa
 *
 */
public class App16FullOuterJoin {

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

		/** cartesian join however gets the cross product **/
//		JavaPairRDD<Tuple2<Integer, Integer>, Tuple2<Integer, String>> joinedRDD = visits.cartesian(users);
//		joinedRDD.foreach(tuple -> System.out.println(tuple));
//		//output
//		((4,18),(1,John))
//		((4,18),(2,Bob))
//		((4,18),(3,Alan))
//		((4,18),(4,Doris))
//		((4,18),(5,Marybelle))
//		((4,18),(6,Raquel))
//		((6,4),(1,John))
//		((6,4),(2,Bob))
//		((6,4),(3,Alan))
//		((6,4),(4,Doris))
//		((6,4),(5,Marybelle))
//		((6,4),(6,Raquel))
//		((10,9),(1,John))
//		((10,9),(2,Bob))
//		((10,9),(3,Alan))
//		((10,9),(4,Doris))
//		((10,9),(5,Marybelle))
//		((10,9),(6,Raquel))
		
		sc.close();
	}

}
