// Databricks notebook source
// Q2 [25 pts]: Analyzing a Large Graph with Spark/Scala on Databricks

// STARTER CODE - DO NOT EDIT THIS CELL
import org.apache.spark.sql.functions.desc
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

// STARTER CODE - DO NOT EDIT THIS CELL
// Definfing the data schema
val customSchema = StructType(Array(StructField("answerer", IntegerType, true), StructField("questioner", IntegerType, true),
    StructField("timestamp", LongType, true)))

// COMMAND ----------

// STARTER CODE - YOU CAN LOAD ANY FILE WITH A SIMILAR SYNTAX.
// MAKE SURE THAT YOU REPLACE THE examplegraph.csv WITH THE mathoverflow.csv FILE BEFORE SUBMISSION.
val df = spark.read
   .format("com.databricks.spark.csv")
   .option("header", "false") // Use first line of all files as header
   .option("nullValue", "null")
   .schema(customSchema)
   .load("/FileStore/tables/mathoverflow.csv")
   .withColumn("date", from_unixtime($"timestamp"))
   .drop($"timestamp")

// COMMAND ----------

//display(df)
df.show()

// COMMAND ----------

// PART 1: Remove the pairs where the questioner and the answerer are the same person.
// ALL THE SUBSEQUENT OPERATIONS MUST BE PERFORMED ON THIS FILTERED DATA

// ENTER THE CODE BELOW
val filteredDF = df.filter($"answerer" !== $"questioner")
filteredDF.show()

// COMMAND ----------

// PART 2: The top-3 individuals who answered the most number of questions - sorted in descending order - if tie, the one with lower node-id gets listed first : the nodes with the highest out-degrees.

// ENTER THE CODE BELOW
val top3answerDF = filteredDF.groupBy($"answerer")
  .count()
  .withColumnRenamed("count", "questions_answered")
  .orderBy(desc("questions_answered"), asc("answerer"))
  .limit(3)
top3answerDF.show()

// COMMAND ----------

// PART 3: The top-3 individuals who asked the most number of questions - sorted in descending order - if tie, the one with lower node-id gets listed first : the nodes with the highest in-degree.

// ENTER THE CODE BELOW
val top3questionDF = filteredDF.groupBy($"questioner")
  .count()
  .withColumnRenamed("count", "questions_asked")
  .orderBy(desc("questions_asked"), asc("questioner"))
  .limit(3)
top3questionDF.show()

// COMMAND ----------

// PART 4: The top-5 most common asker-answerer pairs - sorted in descending order - if tie, the one with lower value node-id in the first column (u->v edge, u value) gets listed first.

// ENTER THE CODE BELOW
val top5pairDF = filteredDF.groupBy($"answerer", $"questioner")
  .count()
  .orderBy(desc("count"), asc("answerer"), asc("questioner"))
  .limit(5)
top5pairDF.show()

// COMMAND ----------

// PART 5: Number of interactions (questions asked/answered) over the months of September-2010 to December-2010 (i.e. from September 1, 2010 to December 31, 2010). List the entries by month from September to December.

// Reference: https://www.obstkel.com/blog/spark-sql-date-functions
// Read in the data and extract the month and year from the date column.
// Hint: Check how we extracted the date from the timestamp.

// ENTER THE CODE BELOW

/*
val interactionfilteredDF = filteredDF.filter(to_date($"date") >= lit("2010-09-01") && to_date($"date") <= lit("2010-12-31"))
  .withColumn("month", month(to_date($"date")))
  .groupBy("month")
  .count()
  .withColumnRenamed("count", "total_interactions")
  .select($"month", $"total_interactions")
*/

val interactionfilteredDF = filteredDF.filter(datediff(to_date($"date"), lit("2010-09-01")) >= 0 && datediff(to_date($"date"), lit("2010-12-31")) <= 0)
  .withColumn("month", month(to_date($"date")))
  .groupBy("month")
  .count()
  .withColumnRenamed("count", "total_interactions")
  .select($"month", $"total_interactions")
  .orderBy(asc("month"))
interactionfilteredDF.show()

// COMMAND ----------

// PART 6: List the top-3 individuals with the maximum overall activity, i.e. total questions asked and questions answered.

// ENTER THE CODE BELOW
val questionDF = filteredDF.groupBy($"questioner")
  .count()
  .withColumnRenamed("count", "questions_asked")
  .withColumnRenamed("questioner", "userID")
val answerDF = filteredDF.groupBy($"answerer")
  .count()
  .withColumnRenamed("count", "questions_answered")
  .withColumnRenamed("answerer", "userID")
val activityDF = questionDF.join(answerDF, Seq("userID"), "outer")
  .na.fill(0, Seq("questions_asked"))
  .na.fill(0, Seq("questions_answered"))
  .select($"userID", ($"questions_asked" + $"questions_answered").as("total_activity"))
  .orderBy(desc("total_activity"), asc("userID"))
  .limit(3)
activityDF.show()
