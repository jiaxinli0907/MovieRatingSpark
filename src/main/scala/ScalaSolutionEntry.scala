import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession, functions}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructType, TimestampType}
import java.nio.file.Paths

import org.apache.spark.sql.functions.{avg, col, concat_ws, explode, lit, regexp_extract, split}

object ScalaSolutionEntry {
  def main(args: Array[String]): Unit = {

    // create spark entry point
    val spark = SparkSession
      .builder()
      .appName("Spark SQL basic example")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // define schema
    val movieSchema = new StructType()
      .add("movieId", IntegerType, true)
      .add("name", StringType, true)
      .add("genre", StringType, true)

    val ratingSchema = new StructType()
      .add("userId", IntegerType, true)
      .add("movieId", IntegerType, true)
      .add("rating", IntegerType, true)
      .add("ts", StringType, true)

    val userSchema = new StructType()
      .add("userId", IntegerType, true)
      .add("gender", StringType, true)
      .add("age", IntegerType, true)
      .add("Occupation", StringType, true)
      .add("zipCode", StringType, true)

    // get the absolute path of current project
    val root = Paths.get(".").normalize.toAbsolutePath

    // read data
    val movies = loadData(spark, movieSchema,root + "/data/movies.dat")
    val ratings = loadData(spark, ratingSchema, root + "/data/ratings.dat")
    val users = loadData(spark, userSchema, root + "/data/users.dat")

    // clean the data
    // parse the column "name" to get the release year and a cleaned name, convert the "genre" column to array type
    val mov = movies.withColumn("release" , regexp_extract(col("name"), "\\((.*?)\\)", 1).cast(IntegerType))
                    .withColumn("name",  regexp_extract(col("name"), "(.*?)\\(", 1).cast(StringType))
                    .withColumn("genre", split(col("genre"), "\\|"))
    mov.show(10)

    // convert the "TimeStamp" column  from string to TimestampType
    val rat = ratings.withColumn("date", col("ts").cast(DoubleType).cast(TimestampType))
      .drop(col("ts"))

    // join table rating, movie and user, and drop the duplicate columns
    val movieRating = rat.join(mov, mov("movieId") === rat("movieId") ,"left")
                            .drop(mov("movieId"))
                            .join(users, users("userId") === rat("userId"),"left")
                            .drop(users("userId"))

    // according to the Email from Rajini, only consider the movie released after 1989, and persons aged 18-49
    val movRat = movieRating.filter(col("release") > 1989 )
      .filter(col("age").between(18,45))

    // this method is used to write an array to csv. array -> concatenated to string
    def stringify(c: Column) = functions.concat(lit("["), concat_ws(",", c), lit("]"))

    // save the result as csv
    writeOutput(movRat.withColumn("genre", stringify(col("genre"))), root + "/scalaResult/movieRatingJoinedInfo")

//    val avgRatGenre = movRat.groupBy(col("genre")).agg(avg(col("rating")))
//    avgRatGenre.show()

    // calculate the average ratings, per genre
    // first unwind/flat/unfold the column "genre", because each item is an array. eg Action|Adventure|Mystery now is unfolded into three rows
    val flatMovRat = movRat.select(col("rating"),col("release"), explode(col("genre")).alias("singleGenre"))

    // calculate the average rating of each genre
    val avgRatSigGenre = flatMovRat.groupBy(col("singleGenre")).agg(avg(col("rating")))
    avgRatSigGenre.show()

    // write the result to csv
    writeOutput(avgRatSigGenre, root + "/scalaResult/avgRatSigGenre" )

    // calculate the average ratings, per year
    val avgRatYear = movRat.groupBy(col("release")).agg(avg(col("rating"))).orderBy(col("release"))
    avgRatYear.show()

    // save the result as csv
    writeOutput(avgRatYear, root + "/scalaResult/avgRatYear" )

    // calculate the average rating of each genre and each year
    val avgRatGenreYear = flatMovRat.groupBy("singleGenre", "release")
      .agg(avg(col("rating")))
      .orderBy("singleGenre", "release")
    avgRatGenreYear.show()
    writeOutput(avgRatGenreYear, root + "/scalaResult/avgRatGenreAndYear" )

  }

  // read data into spark
  def loadData(spark:SparkSession, schema:StructType, path:String): DataFrame = {

    spark.read
      .option("delimiter", "::")
      .schema(schema)
      .format("csv")
      .load(path)
  }

  // write dataframe to csv
  def writeOutput(dataFrame: DataFrame, path:String): Unit ={

    dataFrame.coalesce(1)
      .write
      .mode(SaveMode.Append)
      .option("header", "true")
      .csv(path)
  }

}
