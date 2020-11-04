import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.DataFrameReader;
import scala.Tuple2;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import static org.apache.spark.sql.functions.*;

public class Main {

    private static String fileName = "univ_rankings.txt";
//    private static String cars = "cars.json";
    public static void main(String[] args) throws NoSuchFieldException, IllegalAccessException {
        SparkConf conf = new SparkConf().setAppName("Spark").setMaster("local"); //local to run in local mode
        //JavaSparkContext sc = new JavaSparkContext(conf); //In Spark shell, special interpreter-aware SparkContext is already created for you, in the variable called sc
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();
//        JavaRDD<String> rowRdd = sc.textFile(fileName);
//        System.out.println(rowRdd.count());
//        JavaRDD<String> filteredRows = rowRdd.filter(s -> s.contains("Santa Barbara"));
//        System.out.println(filteredRows.count());
//        System.out.println("Dupa");
//        JavaRDD<String> rddX = sc.parallelize(Arrays.asList("big data","analytics","using java"));
//        System.out.println(rddX.collect());
//        JavaRDD<String> rddX2 = sc.parallelize(Arrays.asList("1","2","3"));
//        JavaRDD<String> rddX3 = sc.parallelize(Arrays.asList("element-1","element-2","element-3"));
//        rddX3.foreach(f -> System.out.println(f));
        Dataset<Row> carsBaseDF = spark.read().json("/home/ewa/IdeaProjects/New/cars.json");
            carsBaseDF.show();
            carsBaseDF.createOrReplaceTempView("cars");
        Dataset<Row> netDF = spark.sql("select * from cars");
            netDF.show();
        Dataset<Row> singleColDF = spark.sql("select make_country,make_display from cars");
        singleColDF.show();
        System.out.println("Total Rows in Data --" + netDF.count()) ; //it means objects - 8 rows
        Dataset<Row> germanyCarsDF = spark.sql("select * from cars where make_country = 'Germany'").orderBy(org.apache.spark.sql.functions.col("make_id").asc());
        germanyCarsDF.show();
    }

}
