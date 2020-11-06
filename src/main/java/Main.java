import java.util.Arrays;
import java.util.List;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.mllib.fpm.AssociationRules;
import org.apache.spark.sql.SparkSession;

public class Main {

    private static String fpgFile = "transactionsFP.txt";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Spark").setMaster("local"); //local to run in local mode
        JavaSparkContext sc = new JavaSparkContext(conf); //In Spark shell, special interpreter-aware SparkContext is already created for you, in the variable called sc
        SparkSession spark = SparkSession.builder()
                .config(conf)
                .getOrCreate();

        //loading dataset of transactions
       JavaRDD<String> data = sc.textFile(fpgFile);

       //break each row of transaction into individual items and store them as a list of strings per row in SparkRDD object
        // called transactions
       JavaRDD<List<String>> transactions = data.map(line -> Arrays.asList(line.split(" ")));

       //create an instance of FP-Growth algorithm
       FPGrowth fpg = new FPGrowth()
               .setMinSupport(0.2)
               .setNumPartitions(1);

       //run this on the transactions RDD object. It creates the FP-tree and association rules internally
        //and store results in a FPGrowthModel object
       FPGrowthModel<String> model = fpg.run(transactions);

       //get the list of frequent items and print them. We can get this list from FPGrowthModel
       for (FPGrowth.FreqItemset<String> itemset: model.freqItemsets().toJavaRDD().collect()) {
           System.out.println("[" + itemset.javaItems() + "], " + itemset.freq());
       }

       //define minimum confidence value and apply it to get the association rules from FPGrowthModel object
        double minConfidence = 0.0;
       for (AssociationRules.Rule<String> rule
       :model.generateAssociationRules(minConfidence).toJavaRDD().collect()){
           System.out.println(rule.javaAntecedent() + " => " + rule.javaConsequent() + ", " + rule.confidence());
       }
    }
}
