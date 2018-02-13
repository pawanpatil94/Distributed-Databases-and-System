package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer


object HotcellAnalysis {

  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)


  def isValidPoint(minX: Double, maxX: Double, minY: Double, maxY: Double, minZ: Double, maxZ: Double, x: Double, y: Double, z: Double): Boolean = {

    return (minX <= x && x <= maxX && minY <= y && y <= maxY && minZ <= z && z <= maxZ)

  }


  def isValidPointInList(x: Double, y: Double, z: Double): Boolean = {

    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)


    if (x < minX || x > maxX) return false
    if (y < minY || y > maxY) return false
    if (z < minZ || z > maxZ) return false
    true

  }

  def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame = {
    // Load the original data from a data source
    var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter", ";").option("header", "false").load(pointPath);
    pickupInfo.createOrReplaceTempView("nyctaxitrips")

    // Assign cell coordinates based on pickup points
    spark.udf.register("CalculateX", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 0)
      )))
    spark.udf.register("CalculateY", (pickupPoint: String) => ((
      HotcellUtils.CalculateCoordinate(pickupPoint, 1)
      )))
    spark.udf.register("CalculateZ", (pickupTime: String) => ((
      HotcellUtils.CalculateCoordinate(pickupTime, 2)
      )))
    pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
    var newCoordinateName = Seq("x", "y", "z")
    pickupInfo = pickupInfo.toDF(newCoordinateName: _*)
    pickupInfo.createOrReplaceTempView("allpoints")


    var sc = spark.sparkContext
    // Define the min and max of x, y, z
    val minX = -74.50 / HotcellUtils.coordinateStep
    val maxX = -73.70 / HotcellUtils.coordinateStep
    val minY = 40.50 / HotcellUtils.coordinateStep
    val maxY = 40.90 / HotcellUtils.coordinateStep
    val minZ = 1
    val maxZ = 31
    val numCells = (maxX - minX + 1) * (maxY - minY + 1) * (maxZ - minZ + 1)

    spark.udf.register("isValidPoint", (minX: Double, maxX: Double, minY: Double, maxY: Double, minZ: Double, maxZ: Double, x: Double, y: Double, z: Double)
    => (HotcellUtils.isValidPoint(minX, maxX, minY, maxY, minZ, maxZ, x, y, z)))
    val validInfo = spark.sql("select x,y,z,count(*) as count from allpoints where isValidPoint(" + minX + "," + maxX + "," + minY + "," + maxY + "," + minZ + "," + maxZ + ", x,y,z) group by x,y,z").persist()
    validInfo.createOrReplaceTempView("validpoints")
    validInfo.show()

    val countMap = validInfo.collect().map(x => (x.getInt(0).toString + "," + x.getInt(1).toString + "," + x.getInt(2).toString, x.getLong(3))).toMap

    val sumpoints = spark.sql("select sum(count) as sigma, sum(count*count) as xsqr from validpoints")
    sumpoints.createOrReplaceTempView("sumOfPoints")

    val sigma = sumpoints.first().getLong(0)
    val sqrtCount = sumpoints.first().getLong(1)


    val mean = sigma / numCells
    val sqrMean = mean * mean
    val average = sqrtCount / numCells
    val sd = Math.sqrt(average - sqrMean)

    val height: Int = 31
    val length: Int = (maxY - minY + 1).toInt
    val width: Int = (maxX - minX + 1).toInt
    var spaceTimeCube = Array.ofDim[Int](height, width, length)
    val totalEligiblePoints = sigma

    var coOrdinateZScoreMap = mutable.Map[String, Double]()
    populateZScoreMap(coOrdinateZScoreMap, countMap, validInfo, sigma, mean, sd, numCells)

    import scala.collection.immutable.ListMap
    import spark.implicits._

    val finalDF = coOrdinateZScoreMap.toList.toDF().sort($"_2".desc).drop($"_2")
    val finalDF2 = finalDF.withColumnRenamed("_1", "cell")
    val finalDF3 = finalDF2.selectExpr("split(cell, ',')[0]", "split(cell, ',')[1]", "split(cell, ',')[2]")

    return finalDF3 // YOU NEED TO CHANGE THIS PART
  }


  def populateZScoreMap(coOrdinateZScoreMap: mutable.Map[String, Double], countMap: Map[String, Long], validInfo: DataFrame, totalEligiblePoints: Long, mean: Double, standardDeviation: Double, numCells: Double): Unit = {

    validInfo.collect().foreach { t =>
      val getisOrdStatistic1 = getGetisOrdStatisticValue(t.getInt(0), t.getInt(1), t.getInt(2), countMap, mean, standardDeviation, totalEligiblePoints, numCells)
      var spaceTime: String = ""
      val latitude = (t.getInt(0))
      val longitude = t.getInt(1)
      spaceTime = latitude.toString + "," + longitude.toString + "," + t.getInt(2).toString
      coOrdinateZScoreMap += (spaceTime.toString -> getisOrdStatistic1)


    }

  }

  private def getDenominatorValue(xIndex: Int, yIndex: Int, zIndex: Int, standardDeviation: Double, totalEligiblePoints: Long, countMap: Map[String, Long], numCells: Double): Double = {
    val neighbors = getWeightedSumOfNeighbors(xIndex, yIndex, zIndex, countMap)
    val squareRootValue = Math.sqrt(((neighbors.size * numCells.*(1.0)) - (neighbors.size * neighbors.size.*(1.0))) / numCells - 1)
    return (standardDeviation * squareRootValue)
  }

  private def getWeightedSumOfNeighbors(i: Int, j: Int, k: Int, countMap: Map[String, Long]): List[Long] = {

    var sumList = new ListBuffer[Long]()

    for (x1 <- i - 1 to i + 1) {
      for (x2 <- j - 1 to j + 1) {
        for (x3 <- k - 1 to k + 1) {

          if (isValidPointInList(x1, x2, x3)) {
            val toSearch = x1.toString + "," + x2.toString + "," + x3.toString
            if (countMap.contains(toSearch)) {
              sumList += countMap(toSearch)
            }
            else {
              sumList += 0
            }
          }

        }
      }
    }
    return sumList.toList
  }

  def getGetisOrdStatisticValue(xIndex: Int, yIndex: Int, zIndex: Int, countMap: Map[String, Long], mean: Double, standardDeviation: Double, totalEligiblePoints: Long, numCells: Double): Double = {
    val numerator = getNumeratorValue(xIndex, yIndex, zIndex, countMap, mean)
    val denominator = getDenominatorValue(xIndex, yIndex, zIndex, standardDeviation, totalEligiblePoints, countMap, numCells)
    return (numerator / denominator)
  }

  def getNumeratorValue(xIndex: Int, yIndex: Int, zIndex: Int, countMap: Map[String, Long], mean: Double): Double = {
    val nList = getWeightedSumOfNeighbors(xIndex, yIndex, zIndex, countMap)
    val operandTwo = mean * nList.size.*(1.0)
    var operandOne = 0.0
    nList.foreach(operandOne += _)
    return (operandOne - operandTwo)
  }

}
