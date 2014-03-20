package water.api.dsl

/** Row accessor. */
trait Row {
  def d(ncol:Int):scala.Double
  def l(ncol:Int):scala.Long
  def ncols() : Int
}