package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var result = 0
    coordinateOffset match
    {
      case 0 => result = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => result = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 inclusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        result = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return result
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }

  def JoinCheck(coordinate1: String, coordinate2: String) : Boolean =
  {
    val start = coordinate1.split(',')
    val neighbor = coordinate2.split(',')
    val xDiff = start(0).toInt - neighbor(0).toInt
    val yDiff = start(1).toInt - neighbor(1).toInt
    val zDiff = start(2).toInt - neighbor(2).toInt
    val xCheck = (xDiff == -1) || (xDiff == 0) || (xDiff == 1)
    val yCheck = (yDiff == -1) || (yDiff == 0) || (yDiff == 1)
    val zCheck = (zDiff == -1) || (zDiff == 0) || (zDiff == 1)
    val result = xCheck && yCheck && zCheck
    return result
  }

  def CalculateNeighbors(coordinate: String): Int =
  {
    val minX = -74.50/coordinateStep
    val maxX = -73.70/coordinateStep
    val minY = 40.50/coordinateStep
    val maxY = 40.90/coordinateStep
    val minZ = 1
    val maxZ = 31
    val coordValues = coordinate.split(',')
    val currX = coordValues(0).toInt
    val currY = coordValues(1).toInt
    val currZ = coordValues(2).toInt

    var vertexCount = 0
    if(currX == minX || currX == maxX) {
      vertexCount = vertexCount + 1
    }
    if(currY == minY || currY == maxY) {
      vertexCount = vertexCount + 1
    }
    if(currZ == minZ || currZ == maxZ) {
      vertexCount = vertexCount + 1
    }
    if (vertexCount == 1)
      return 18
    if (vertexCount == 2)
      return 12
    if (vertexCount == 3)
      return 8
    else
      return 27
  }

  def CalculateZScore(neighborNum: Int, total: Int, cellCount: Double, Mean: Double, SD: Double) : Double =
  {
    val numerator = total - (Mean * (neighborNum))
    val x = cellCount * (neighborNum)
    val y  = (neighborNum) * (neighborNum)
    val diff = x - y
    val div = diff/(cellCount-1)
    val denominator = SD * Math.sqrt(div)
    val zscore = numerator / denominator
    return zscore
  }



}

