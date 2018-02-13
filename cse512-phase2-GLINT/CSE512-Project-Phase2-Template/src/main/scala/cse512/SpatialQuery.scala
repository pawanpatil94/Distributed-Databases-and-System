package cse512

import org.apache.spark.sql.SparkSession
import scala.math._

object SpatialQuery extends App{
  def ST_Contains(pointString : String, rectString : String)  : Boolean = {
    var pointArray : Array[String] = pointString.split(",")
    var rectArray : Array[String] = rectString.split(",")

    var x : Double = pointArray(0).toDouble
    var y : Double = pointArray(1).toDouble

    var x1 : Double = rectArray(0).toDouble
    var y1 : Double = rectArray(1).toDouble
    var x2 : Double = rectArray(2).toDouble
    var y2 : Double = rectArray(3).toDouble

    var boolean1 : Boolean = false;
    var boolean2 : Boolean = false;

    if(math.min(x1, x2) <= x && x <= math.max(x1, x2)){
      boolean1 = true;
    }
    if(math.min(y1, y2) <= y && y<= math.max(y1, y2)){
      boolean2 = true;
    }


    return (boolean1 && boolean2)
  }

  def area(x : Double, y : Double, x1 : Double, y1 : Double, x2 : Double, y2 : Double) : Double = {
    return math.abs(0.5 * (x*(y1-y2) + x1*(y2-y) + x2*(y-y1)))
  }

  def ST_Within(pointString1 : String, pointString2 : String, distance : Double) : Boolean = {
    var pointArray1 : Array[String] = pointString1.split(",")
    var pointArray2 : Array[String] = pointString2.split(",")

    var x1 : Double = pointArray1(0).toDouble
    var y1 : Double = pointArray1(1).toDouble

    var x2 : Double = pointArray2(0).toDouble
    var y2 : Double = pointArray2(1).toDouble

    var l2Dist : Double = math.sqrt(math.pow((x1-x2), 2) + math.pow((y1-y2), 2))
    return (l2Dist <= distance)
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(pointString, queryRectangle))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((ST_Contains(pointString, queryRectangle))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((ST_Within(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
