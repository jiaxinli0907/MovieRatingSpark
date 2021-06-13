# MovieRatingSpark


## Introduction

The project is managed by maven, the solution is written in both scala and Java with spark framework, ref: src/main/java/JavaSolutionEntry and src/main/scala/ScalaSolutionEntry. (The solutions are same, just used two kinds of language.)

In this project, I use scala 2.12.10 and Spark 3.0.8 with JRE 1.8. If you want to run it, please refer to these information. 



## Process

1. initiate sparksession, define the schema of three dataset and then read the data into spark;

2. clean the data, convert the data into the format required in the next step;

3. join three tables, filter the movies released after 1989, and persons aged 18-49

4. I'm not sure about "calculate the average ratings, per genre and year". It means that group by genre and year seperately or group by genre first then within genre group by year, so I did them both. 
To facilitate us check the result, the results are stored into csv. ref: folder javaResult and scalaResult.
