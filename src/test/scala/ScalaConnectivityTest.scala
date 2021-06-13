import java.nio.file.Paths

import junit.framework.TestCase
import org.apache.spark.sql.SparkSession
import org.junit.Assert.assertEquals
import org.junit.Test

class ScalaConnectivityTest extends TestCase {

  lazy val spark: SparkSession = {
    SparkSession.builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[*]")
      .getOrCreate()
  }

  @Test def testScalaConnectivity(): Unit ={
    val path = Paths.get(".").normalize.toAbsolutePath + "/data/movies.dat"
    val data = spark.read.option("delimiter", "::").format("csv").load(path)
    assertEquals(data.count(), 3883)
  }
}
