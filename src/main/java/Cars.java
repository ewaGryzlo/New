import org.apache.spark.SparkConf;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;


public class Cars{

    private static String carsName = "cars.json";

    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setAppName("Spark").setMaster("local");

        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        Dataset<Row> carsBaseDF = spark.read().json(carsName);
        carsBaseDF.show();

        carsBaseDF.createOrReplaceTempView("cars");
        Dataset<Row> netDF = spark.sql("select * from cars");
        netDF.show();

        Dataset<Row> singleColDF = spark.sql("select make_country,make_display from cars");
        singleColDF.show();

        System.out.println("Total Rows in Data => " + netDF.count()) ; //it means objects - 9 rows

        //select all cars from Germany
        Dataset<Row> germanyCarsDF = spark.sql("select * from cars where make_country = 'Germany'").orderBy(org.apache.spark.sql.functions.col("make_id").asc());
        germanyCarsDF.show();
        System.out.println("Number of cars from Germany: " + germanyCarsDF.count());
    }
}