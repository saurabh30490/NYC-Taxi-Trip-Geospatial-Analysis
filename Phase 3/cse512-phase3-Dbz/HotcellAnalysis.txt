package cse512

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

object HotcellAnalysis {
  Logger.getLogger("org.spark_project").setLevel(Level.WARN)
  Logger.getLogger("org.apache").setLevel(Level.WARN)
  Logger.getLogger("akka").setLevel(Level.WARN)
  Logger.getLogger("com").setLevel(Level.WARN)

def runHotcellAnalysis(spark: SparkSession, pointPath: String): DataFrame =
{
  // Load the original data from a data source
  var pickupInfo = spark.read.format("com.databricks.spark.csv").option("delimiter",";").option("header","false").load(pointPath);
  pickupInfo.createOrReplaceTempView("nyctaxitrips")
  //pickupInfo.show()

  // Assign cell coordinates based on pickup points
  spark.udf.register("CalculateX",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 0)
    )))
  spark.udf.register("CalculateY",(pickupPoint: String)=>((
    HotcellUtils.CalculateCoordinate(pickupPoint, 1)
    )))
  spark.udf.register("CalculateZ",(pickupTime: String)=>((
    HotcellUtils.CalculateCoordinate(pickupTime, 2)
    )))
  pickupInfo = spark.sql("select CalculateX(nyctaxitrips._c5),CalculateY(nyctaxitrips._c5), CalculateZ(nyctaxitrips._c1) from nyctaxitrips")
  var newCoordinateName = Seq("x", "y", "z")
  pickupInfo = pickupInfo.toDF(newCoordinateName:_*)
  //pickupInfo.show()

  // Define the min and max of x, y, z
  val minX = -74.50/HotcellUtils.coordinateStep
  val maxX = -73.70/HotcellUtils.coordinateStep
  val minY = 40.50/HotcellUtils.coordinateStep
  val maxY = 40.90/HotcellUtils.coordinateStep
  val minZ = 1
  val maxZ = 31
  val numCells = (maxX - minX + 1)*(maxY - minY + 1)*(maxZ - minZ + 1)

  var filteredData = pickupInfo.filter(col("x") >= minX && col("x") <= maxX).toDF()
  filteredData = filteredData.filter(col("y") >= minY && col("y") <= maxY).toDF()
  filteredData = filteredData.filter(col("z") >= minZ && col("z") <= maxZ).toDF()

  filteredData.createOrReplaceTempView("filteredData")
  // to test if our minmax are working
  //spark.sql("select * from filteredData where z > 31").show()

  // now to find count we create new DataFrame groupby the actual coordinates
  var columnName = Seq("coordinate")
  var CoordinateDF = filteredData.select((concat(col("x"), lit(","), col("y"), lit(","), col("z")))).toDF(columnName:_*)

  var coordinateCountDF = CoordinateDF.groupBy("coordinate").count().orderBy(asc("coordinate")).toDF()

  // cache this. Can also be cache persist in file system . .persist(level:StorageLevel)
  coordinateCountDF.cache()

  // to view this DF
  coordinateCountDF.createOrReplaceTempView("CoordinateCount")

  // to calculate mean and standard deviation we need all count and sum(count*count)
  val countsForFormula = spark.sql("select sum(count), sum(count * count) from CoordinateCount").first()

  // x bar
  val mean = countsForFormula.getLong(0) / numCells

  // for S
  val diff = (countsForFormula.getLong(1) / numCells) - Math.pow(mean,2)
  val sd = Math.sqrt(diff)

  // create df of contants for join
  val constantList = List(numCells,mean,sd)
  val constantTempRow = Row.fromSeq(constantList)
  val rddConstants = spark.sparkContext.makeRDD(List(constantTempRow))
  val rddFields = List(StructField("CellNum", DoubleType, nullable = false),StructField("Mean", DoubleType, nullable = false),StructField("SD", DoubleType, nullable = false))

  var constantDF = spark.createDataFrame(rddConstants, StructType(rddFields))
  constantDF.createOrReplaceTempView("Constants")

  // to get cell count we can self join and check for neighbour property diff -1,0,+1

  spark.udf.register("JoinCheck",(coordinate1:String, coordinate2:String)=>((
    HotcellUtils.JoinCheck(coordinate1, coordinate2)
    )))

  var neighborJoinDF = spark.sql("select CoordinateCount1.coordinate AS origin, CoordinateCount2.coordinate AS neighbor, CoordinateCount2.count AS pickupCount FROM CoordinateCount CoordinateCount1 INNER JOIN CoordinateCount CoordinateCount2 ON JoinCheck(CoordinateCount1.coordinate,CoordinateCount2.coordinate)").toDF()

  neighborJoinDF.createOrReplaceTempView("Neighbors")

  // to get a cell and total trips at that cell i.e. considering all neighbours as single cell
  var columnNames = Seq("origin","total")
  var originWithPickupCount = spark.sql("select origin, sum(pickupCount) AS total FROM Neighbors GROUP BY origin").toDF(columnNames:_*)
  originWithPickupCount.createOrReplaceTempView("PickupCount")


  // to check for corner case in 3d space since all cells wont have equal number of neighbours
  spark.udf.register("CalculateNeighbors",(coordinate:String)=>((
    HotcellUtils.CalculateNeighbors(coordinate)
    )))

  var neighborColNames = Seq("origin","neighborcount","total")

  var neighborCount = spark.sql("select origin, CalculateNeighbors(origin) AS neighborcount, first(total) FROM PickupCount GROUP BY origin").toDF(neighborColNames:_*)

  // to get origin coordinates, neighbour count and total trips ??? why needed ANS : to find actual neighbours and filter column values.
  neighborCount.createOrReplaceTempView("NeighborCount")

  spark.udf.register("CalculateZScore",(neighborcount: Int, total: Int, cellNum: Double, Mean: Double, SD: Double)=>((
    HotcellUtils.CalculateZScore(neighborcount, total, cellNum, Mean, SD)
    )))

  var zScoreColNames = Seq("origin","zscore")
  var zScoreDF = spark.sql("select NeighborCount.origin, CalculateZScore(first(NeighborCount.neighborcount),first(NeighborCount.total),first(Constants.CellNum),first(Constants.Mean),first(Constants.SD)) as zscore FROM NeighborCount CROSS JOIN Constants GROUP BY NeighborCount.origin ORDER BY NeighborCount.origin").toDF(zScoreColNames:_*)
  zScoreDF.createOrReplaceTempView("zScores")

  // finally to get top 50 origins in given column format
  var finalColName = Seq("origin")
  var resultDF = spark.sql("select first(origin) from zScores GROUP BY zscore ORDER BY zscore DESC limit 50").toDF(finalColName:_*)
  resultDF.createOrReplaceTempView("zScoreResult")
  var result = resultDF.withColumn("_tmp", split(col("origin"), ",")).select(col("_tmp").getItem(0).as("x"), col("_tmp").getItem(1).as("y"),col("_tmp").getItem(2).as("z")).drop("_tmp").toDF()

  return result

}
}

