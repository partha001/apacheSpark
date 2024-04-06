package org.example;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * this approach of doing filter is little complex and is a more programmatic approach
 */
public class Ex05DatasetFiltersUsingColumns {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

        SparkSession spark = SparkSession.builder()
                .appName("testingSql") //this is not mandatory. not mentioning this java will give it a default appname
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/") //this line required only for windows
                .getOrCreate();


        Dataset<Row> dataset = spark.read()
                .option("header", true) //this is to say that the first line is a header
                .csv("src/main/resources/students.csv");



//        Dataset<Row> modernArtResultForStudentsPost2007 = dataset.filter((FilterFunction<Row>) row ->
//                (row.getAs("subject").equals("Modern Art")
//                        && Integer.parseInt(row.getAs("year"))>=2007)
//        );

        Column subjectColumn = dataset.col("subject");
        Dataset<Row> modernArtResults = dataset.filter(subjectColumn.equalTo("Modern Art"));
        modernArtResults.show();


        //for chaning multiple filters using this approach. it will look like this
        Column yearColumn = dataset.col("year");
        Dataset<Row> modernArtResultForStudentsPost2007 = dataset.filter(subjectColumn.equalTo("Modern Art")
                .and(yearColumn.geq(2007)));
        modernArtResultForStudentsPost2007.show();

        spark.close();
    }
}
