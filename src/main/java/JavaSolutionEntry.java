import config.Configuration;
import config.Schema;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.*;

import static config.Configuration.loadData;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.regexp_extract;

public class JavaSolutionEntry {
    public static void main(String[] args) {
        // get the absolute path of current project
        String currentPath = new java.io.File("").getAbsolutePath();

//         create spark entry point
        SparkSession spark = SparkSession.builder().appName("SparkApp").config("spark.master", "local").getOrCreate();


        Schema schema = new Schema();
        StructType movieSchema = schema.getMovieSchema();
        StructType ratingSchema = schema.getRatingSchema();
        StructType userSchema = schema.getUserSchema();
        // read data
        Dataset<Row> movie = loadData(spark, movieSchema,currentPath + "/data/movies.dat");

        Dataset<Row> ratings = loadData(spark, ratingSchema,currentPath + "/data/ratings.dat");

        Dataset<Row> users = loadData(spark, userSchema,currentPath + "/data/users.dat");

        // data cleaning
        // parse the column "name" to get the release year and a cleaned name, convert the "genre" column to array type
        Dataset<Row> mov = movie.withColumn("release", regexp_extract(col("name"), "\\(([0-9]{4})\\)", 1).cast(DataTypes.IntegerType))
                .withColumn("name", regexp_extract(col("name"), "(.*?)\\(", 1).cast(DataTypes.StringType))
                .withColumn("genre", split(col("genre"), "\\|"));

        // convert the "TimeStamp" column  from string to TimestampType
        Dataset<Row> rat = ratings.withColumn("date", col("ts").cast(DataTypes.DoubleType).cast(DataTypes.TimestampType))
                .drop(col("ts"));

        // show the data
        ratings.show(10);

        // join table rating, movie and user, and drop the duplicate columns
        Dataset<Row> movieRating = rat.join(mov, mov.col("movieId").equalTo(rat.col("movieId")) ,"left")
                .drop(mov.col("movieId"))
                .join(users, users.col("userId").equalTo(rat.col("userId")) ,"left")
                .drop(users.col("userId"));

        // according to the Email from Rajini, only consider the movie released after 1989, and persons aged 18-49
        Dataset<Row> movRat = movieRating.filter(col("release" ).$greater(1989))
                .filter(col("age").between(18,45));

        // write the result to csv to facilitate us to check it without running repeatedly
        writeToCSV(movRat.withColumn("genre", stringify(col("genre"))), currentPath + "/javaResult/movieRatingJoinedInfo");

//        Dataset<Row> avgRatGenre = movRat.groupBy(col("genre")).agg(avg(col("rating")));
//        avgRatGenre.show(10);

        // calculate the average ratings, per genre
        // first unwind/flat/unfold the column "genre", because each item is an array. eg Action|Adventure|Mystery now is unfolded into three rows
        Dataset<Row>  flatMovRat = movRat.select(col("rating"), col("release"),explode(col("genre")).alias("singleGenre"));

        // calculate the average rating of each genre
        Dataset<Row>  avgRatSigGenre = flatMovRat.groupBy(col("singleGenre")).agg(avg(col("rating")));
        avgRatSigGenre.show();

        // write the result to csv
        writeToCSV(avgRatSigGenre, currentPath + "/javaResult/avgRatSigGenre");

        // calculate the average ratings, per year
        Dataset<Row>  avgRatYear = movRat.groupBy(col("release")).agg(avg(col("rating"))).orderBy(col("release"));
        avgRatYear.show();

        // save the result as csv
        writeToCSV(avgRatYear, currentPath + "/javaResult/avgRatYear");

        // calculate the average rating of each genre and each year
        Dataset<Row> avgRatGenreYear = flatMovRat.groupBy("singleGenre", "release")
                .agg(avg(col("rating")))
                .orderBy("singleGenre", "release");
        avgRatGenreYear.show();
        writeToCSV(avgRatGenreYear, currentPath + "/javaResult/avgRatGenreAndYear" );
    }

    // this method is used to write an array to csv. array -> concatenated to string
    private static Column stringify(Column genre) {
        return functions.concat(lit("["), concat_ws(",", genre), lit("]"));
    }

//    // read data into spark
//    public static Dataset<Row> loadData(SparkSession sparksession, StructType schema, String path){
//        return sparksession.read().option("delimiter", "::").schema(schema)
//                .format("csv")
//                .load(path);
//    }

    // write the result int csv
    public static void writeToCSV(Dataset<Row> data, String path){
        data.coalesce(1)
                .write()
                .mode(SaveMode.Append)
                .option("header", "true")
                .csv(path);
    }
}
