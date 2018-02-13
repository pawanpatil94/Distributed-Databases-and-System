package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String): Boolean = {
    // YOU NEED TO CHANGE THIS PART

    var pointArray: Array[String] = pointString.split(",")
    var rectArray: Array[String] = queryRectangle.split(",")

    var x: Double = pointArray(0).toDouble
    var y: Double = pointArray(1).toDouble

    var x1: Double = rectArray(0).toDouble
    var y1: Double = rectArray(1).toDouble
    var x2: Double = rectArray(2).toDouble
    var y2: Double = rectArray(3).toDouble

    var boolean1: Boolean = false;
    var boolean2: Boolean = false;

    if (math.min(x1, x2) <= x && x <= math.max(x1, x2)) {
      boolean1 = true;
    }
    if (math.min(y1, y2) <= y && y <= math.max(y1, y2)) {
      boolean2 = true;
    }
    return (boolean1 && boolean2)


    return true // YOU NEED TO CHANGE THIS PART
  }

  // YOU NEED TO CHANGE THIS PART

}
