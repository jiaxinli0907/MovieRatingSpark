import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;

public class Test {
    public static void main(String[] args) {
        System.setProperty("hadoop.home.dir", "E:\\hadoop\\hadoop-2.7.1\\");

        SparkConf sparkConf = new SparkConf().setAppName("spark.TestSpark").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);
        String inputFileName = "D:\\myProject\\MovieRating\\data\\movies.dat" ;
        JavaRDD<String> rdd = jsc.textFile(inputFileName);
//        rdd.foreach();
        System.out.println(rdd.count());

        System.out.println(rdd);
        jsc.stop();
        jsc.close();
//        SparkSession spark = SparkSession.builder().appName("SparkApp").config("spark.master", "local").getOrCreate();
//        Dataset<String> textFile = spark.read().textFile("D:\\myProject\\MovieRating\\data\\movies.dat");
//        System.out.println("Number of lines " + textFile.count());
    }
}

