package com.sample

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

/**
  * Created by sinchan on 31/07/18.
  */
object Driver {

  case class Weather(slno: String ,air_pressure_9am: String , air_temp_9am: String ,avg_wind_direction_9am: String ,avg_wind_speed_9am: String , max_wind_direction_9am: String, max_wind_speed_9am: String ,rain_accumulation_9am: String ,rain_duration_9am: String ,relative_humidity_9am: String, relative_humidity_3pm: String)


  def main(args: Array[String]): Unit = {
     val spark = SparkSession.builder().appName("DataframeTests").getOrCreate()


    val df = spark.read.format("csv").option("header","true").load("/Users/sinchan/Documents/Coursera/BigData & MachineLearning/big-data-4/daily_weather.csv")

    df.printSchema() //untypedSchema _c0,_c1

    println("Total Rows: - "+df.count())

    df.describe().show()


    import  spark.implicits._
    val ds = spark.read.format("csv").option("header","true").load("/Users/sinchan/Documents/Coursera/BigData & MachineLearning/big-data-4/daily_weather.csv").as[Weather] //

    ds.printSchema()

    case class SomeSchema(name:String,value:Int)
    val rdd = spark.sparkContext.parallelize(Seq(Row.fromTuple(("A",1)),Row.fromTuple(("B",1))))

    val ds2 = spark.createDataset(rdd)

    val myschema = StructType(
     List(new StructField("name",StringType,true),
      new StructField("value",IntegerType,true))
    )

    spark.createDataFrame(rdd,myschema)

  }
}
