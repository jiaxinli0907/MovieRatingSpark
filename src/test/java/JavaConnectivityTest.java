import config.Schema;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;

import static config.Configuration.loadData;
import static org.junit.Assert.assertEquals;

public class JavaConnectivityTest<ls> {

    private static SparkSession spark;

    @Before
    public void setUp(){
        spark = SparkSession.builder().appName("SparkApp").config("spark.master", "local").getOrCreate();
    }

    @Test
    public void testJavaConnection(){
        String path = new java.io.File("").getAbsolutePath() + "/data/movies.dat";
        Schema schema = new Schema();
        Dataset<Row> movie = loadData(spark, schema.getMovieSchema(),path );
//        Dataset<Row> data = spark.read().option("delimiter", "::").format("csv").load(path);
        assertEquals(movie.count(), 3883);
    }

//    @Test
//    public void testLoad(){
//
//    }
}
