package config;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

public class Configuration {

    // read data into spark
    public static Dataset<Row> loadData(SparkSession sparksession, StructType schema, String path){
        return sparksession.read().option("delimiter", "::").schema(schema)
                .format("csv")
                .load(path);
    }

}
