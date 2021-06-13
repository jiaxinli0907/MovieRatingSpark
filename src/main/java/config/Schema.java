package config;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class Schema {

    // define schema

    public StructType getMovieSchema() {
        return movieSchema;
    }

    public StructType getRatingSchema(){
        return ratingSchema;
    }

    public StructType getUserSchema(){
        return userSchema;
    }

    StructType movieSchema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("movieId",  DataTypes.IntegerType, true),
            DataTypes.createStructField("name", DataTypes.StringType, true),
            DataTypes.createStructField("genre", DataTypes.StringType, true)
    });

    StructType ratingSchema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("userId",  DataTypes.IntegerType, true),
            DataTypes.createStructField("movieId",  DataTypes.IntegerType, true),
            DataTypes.createStructField("rating", DataTypes.IntegerType, true),
            DataTypes.createStructField("ts", DataTypes.StringType, true)
    });

    StructType userSchema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("userId",  DataTypes.IntegerType, true),
            DataTypes.createStructField("gender", DataTypes.StringType, true),
            DataTypes.createStructField("age", DataTypes.IntegerType, true),
            DataTypes.createStructField("Occupation", DataTypes.StringType, true),
            DataTypes.createStructField("zipCode", DataTypes.StringType, true)
    });

}
