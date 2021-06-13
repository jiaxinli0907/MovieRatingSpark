import org.apache.spark
import org.apache.spark.sql.SparkSession

object HelloWorld {
  def main(args: Array[String]): Unit = {
    println("Hello scala world!")
//    val spark = SparkSession
//      .builder()
//      .appName("Spark SQL basic example")
//      .config("spark.master", "Local")
//      .getOrCreate()
//    val textFile = spark.read.text("D:\\myProject\\MovieRating\\data\\movies.dat");
//    println("Number of lines " + textFile.count());

  }
}