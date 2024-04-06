package com.partha.ex01workingWithSpark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.partha.ex01workingWithSpark.util.Util;

import scala.Tuple2;

public class App11KeywordRanking {

	public static void main(String[] args) {
		System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

		SparkConf conf = new SparkConf()
				.setAppName("startingSpark")
				.setMaster("local[*]");
		JavaSparkContext sc = new JavaSparkContext(conf);

		JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");

		//getting rid of special characters and numbers and then converting everything to lowercase
		JavaRDD<String> lettersOnlyRDD = initialRDD.map(sentence -> sentence.replaceAll("[^a-zA-Z\\s]","").toLowerCase());


		//getting rid of empty lines
		JavaRDD<String> removedBlankLinesRDD = lettersOnlyRDD.filter(sentence -> sentence.trim().length()>0);

		//just words
		JavaRDD<String> justWords = removedBlankLinesRDD.flatMap(sentence -> Arrays.asList(sentence.split(" ")).iterator());

		//removing blank words
		JavaRDD<String> blankWordsRemoved = justWords.filter(word -> word.trim().length()>0);

		//filtering out boring words
		JavaRDD<String> justInterestingWords = blankWordsRemoved.filter( word -> Util.isNotBoring(word));


		//now getting the frequency of the words
		JavaPairRDD<String, Long> pairRDD = justInterestingWords.mapToPair(word -> new Tuple2<String,Long>(word,1L));

		//getting word frequency
		JavaPairRDD<String, Long> wordFrequence = pairRDD.reduceByKey((val1, val2) ->  val1 + val2);


		/** now if we sort at this point we will get words sorted by ascending order since the keys are the words.
		 * however we want to sort by frequence so we have to switch the key and the value **/
		JavaPairRDD<Long, String> switched = wordFrequence.mapToPair( tuple -> new Tuple2<Long,String>(tuple._2, tuple._1));

		JavaPairRDD<Long, String> sorted = switched.sortByKey(false); //passing false to sort by descending order



		/** the take() reads the first n elements from the RDD
		 * thus here we are reading a small dataset  to verify the transformations that 
		 * we have applied [like getting rid of numbers, special characters] 
		 * are working or not.
		 *
		 * also since take() returns a small number of elements so it doesnt return a RDD rather a list
		 *
		 * thus we can simply replace <objectToReplace>.take() to see the output any intermediate processing
		 *
		 * this is really useful seeing intermediate results and debugging
		**/
		//List<String> results = justInterestingWords.take(100);
		//results.forEach(System.out::println);

		sorted.take(10).forEach(System.out::println);
		
			
		sc.close();
	}

}
