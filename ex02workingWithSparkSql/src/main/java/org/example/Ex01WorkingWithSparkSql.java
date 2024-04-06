package org.example;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Ex01WorkingWithSparkSql {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

//		SparkConf conf = new SparkConf()
//				.setAppName("startingSpark")
//				.setMaster("local[*]");
//		JavaSparkContext sc = new JavaSparkContext(conf);
//
//		JavaRDD<String> initialRDD = sc.textFile("src/main/resources/subtitles/input.txt");
//
//        sc.close();

        SparkSession spark = SparkSession.builder()
                .appName("testingSql") //this is not mandatory. not mentioning this java will give it a default appname
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/") //this line required only for windows
                .getOrCreate();


		Dataset<Row> dataset = spark.read()
                .option("header", true) //this is to say that the first line is a header
                .csv("src/main/resources/students.csv");
        //the above dataset object underlyingly uses RDD

        dataset.show(); //this is to print the data of the dataset


        long count = dataset.count();//getting dataset count
        System.out.println("there are "+ count + " records");
        spark.close();
    }

}
