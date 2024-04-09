package org.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;

import java.util.Arrays;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.date_format;

/**
 * working with datetime formatting. here we will understand this with a usecase
 * where the requirement is to get the count of records group by log_level and month
 */
public class Ex12WorkingWithPivotTableInDataSets {

    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "c:\\UserProgams\\hadoop");

        SparkSession spark = SparkSession.builder()
                .appName("testingSql") //this is not mandatory. not mentioning this java will give it a default appname
                .master("local[*]")
                .config("spark.sql.warehouse.dir", "file:///c:/temp/") //this line required only for windows
                .getOrCreate();


        Dataset<Row> dataset = spark.read()
                .option("header", true) //this is to say that the first line is a header
                .csv("src/main/resources/biglog.csv");


        Dataset<Row> dataset2 = dataset.select(col("level").as("level")
                , date_format(col("datetime"), "MMMM").as("month")
                , date_format(col("datetime"), "M").as("monthnum").cast(DataTypes.IntegerType));

        //now building the pivot table.
        Dataset<Row> dataset3 = dataset2.groupBy("level") //first do group by on the attribute that will appear on the left hand side of the pivot table
                .pivot("month") //here we have use the attribute that will expanded as columns on top
                .count(); //and finally any aggregation that will form the fact in the pivot table

        dataset3.show();
        /**
         +-----+-----+------+--------+--------+-------+-----+-----+-----+-----+--------+-------+---------+
         |level|April|August|December|February|January| July| June|March|  May|November|October|September|
         +-----+-----+------+--------+--------+-------+-----+-----+-----+-----+--------+-------+---------+
         | INFO|29302| 28993|   28874|   28983|  29119|29300|29143|29095|28900|   23301|  29018|    29038|
         |ERROR| 4107|  3987|    4106|    4013|   4054| 3976| 4059| 4122| 4086|    3389|   4040|     4161|
         | WARN| 8277|  8381|    8328|    8266|   8217| 8222| 8191| 8165| 8403|    6616|   8226|     8352|
         |FATAL|   83|    80|      94|      72|     94|   98|   78|   70|   60|   16797|     92|       81|
         |DEBUG|41869| 42147|   41749|   41734|  41961|42085|41774|41652|41785|   33366|  41936|    41433|
         +-----+-----+------+--------+--------+-------+-----+-----+-----+-----+--------+-------+---------+
         */


        //we can see that the months are not ordered properly. though there is no easy way work around this
        //but using monthnum gives a better result since we have mentioned the datatype so the months are sorted by the month number
        Dataset<Row> dataset4 = dataset2.groupBy("level")
                .pivot("monthnum")
                .count();
        dataset4.show();
        /**
         +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
         |level|    1|    2|    3|    4|    5|    6|    7|    8|    9|   10|   11|   12|
         +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
         | INFO|29119|28983|29095|29302|28900|29143|29300|28993|29038|29018|23301|28874|
         |ERROR| 4054| 4013| 4122| 4107| 4086| 4059| 3976| 3987| 4161| 4040| 3389| 4106|
         | WARN| 8217| 8266| 8165| 8277| 8403| 8191| 8222| 8381| 8352| 8226| 6616| 8328|
         |FATAL|   94|   72|   70|   83|   60|   78|   98|   80|   81|   92|16797|   94|
         |DEBUG|41961|41734|41652|41869|41785|41774|42085|42147|41433|41936|33366|41749|
         +-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
         */


        //1.now if we are in a scenario where we know the values that will be there in the pivot()
        //then we can order them as using the below technique.
        //2. this technique works only if we know the values for the column names ahead of time as here.
        //3. it is also to be noted that using this technique if we dont mention January in our object array  then the data for January will not be captured in our pivot table
        //      thus this technique only captures those values in the pivot table where a column name match is found.
        Object[] months = new Object[]{ "January","February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};
        List<Object> columns = Arrays.asList(months);
        Dataset<Row> dataset5 = dataset2.groupBy("level").pivot("month",columns).count();
        dataset5.show();
        /**
         *
         * +-----+-------+--------+-----+-----+-----+-----+-----+------+---------+-------+--------+--------+
         * |level|January|February|March|April|  May| June| July|August|September|October|November|December|
         * +-----+-------+--------+-----+-----+-----+-----+-----+------+---------+-------+--------+--------+
         * | INFO|  29119|   28983|29095|29302|28900|29143|29300| 28993|    29038|  29018|   23301|   28874|
         * |ERROR|   4054|    4013| 4122| 4107| 4086| 4059| 3976|  3987|     4161|   4040|    3389|    4106|
         * | WARN|   8217|    8266| 8165| 8277| 8403| 8191| 8222|  8381|     8352|   8226|    6616|    8328|
         * |FATAL|     94|      72|   70|   83|   60|   78|   98|    80|       81|     92|   16797|      94|
         * |DEBUG|  41961|   41734|41652|41869|41785|41774|42085| 42147|    41433|  41936|   33366|   41749|
         * +-----+-------+--------+-----+-----+-----+-----+-----+------+---------+-------+--------+--------+
         */

        //further to demonstrate point 2 and 3. note that January and some other months are which are not present in object array are not captured as part of pivot table
        //also since somemonth doesnt exist in our dataset to we get null for it.
        Object[] months2 = new Object[]{ "February", "March", "April", "May", "June", "July", "August","Somemonth"};
        List<Object> columns2 = Arrays.asList(months2);
        Dataset<Row> dataset6 = dataset2.groupBy("level").pivot("month",columns2).count();
        dataset6.show();
        /**
         +-----+--------+-----+-----+-----+-----+-----+------+---------+
         |level|February|March|April|  May| June| July|August|Somemonth|
         +-----+--------+-----+-----+-----+-----+-----+------+---------+
         | INFO|   28983|29095|29302|28900|29143|29300| 28993|     null|
         |ERROR|    4013| 4122| 4107| 4086| 4059| 3976|  3987|     null|
         | WARN|    8266| 8165| 8277| 8403| 8191| 8222|  8381|     null|
         |FATAL|      72|   70|   83|   60|   78|   98|    80|     null|
         |DEBUG|   41734|41652|41869|41785|41774|42085| 42147|     null|
         +-----+--------+-----+-----+-----+-----+-----+------+---------+
         */

        //futhermore for whereever there is no value in the pivot table [as for somemonth here] if want to show 0 instead of null
        //then that can be done as below
        Dataset<Row> dataset7 = dataset2.groupBy("level").pivot("month",columns2).count().na().fill(0);
        dataset7.show();
        /**
         * +-----+--------+-----+-----+-----+-----+-----+------+---------+
         * |level|February|March|April|  May| June| July|August|Somemonth|
         * +-----+--------+-----+-----+-----+-----+-----+------+---------+
         * | INFO|   28983|29095|29302|28900|29143|29300| 28993|        0|
         * |ERROR|    4013| 4122| 4107| 4086| 4059| 3976|  3987|        0|
         * | WARN|    8266| 8165| 8277| 8403| 8191| 8222|  8381|        0|
         * |FATAL|      72|   70|   83|   60|   78|   98|    80|        0|
         * |DEBUG|   41734|41652|41869|41785|41774|42085| 42147|        0|
         * +-----+--------+-----+-----+-----+-----+-----+------+---------+
         */

        spark.close();
    }
}


