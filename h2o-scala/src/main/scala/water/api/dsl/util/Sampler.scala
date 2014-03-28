package water.api.dsl.util

import water.api.dsl.{Row, T_T_Collect}

/**
 * Resevoir Sampler to extract a column from a DataFrame and bring it to the local context.
 *
 *
 *    Some Scala REPL foo:
 *
 *    val g = parse ("/Users/jerdavis/temp/export1.gz")
 *    import water.api.dsl.util._
 *    val smallData = g(100) collect ( new Reservoir(1000), new Sampler() )
 *
 *
 *    import scalax.chart._
 *    import scalax.chart.Charting._
 *
 *    val data = (1 to smallData.getNumValues) zip smallData.getValues
 *    val dataset = data.toXYSeriesCollection("some points")
 *    val chart = XYLineChart(dataset)
 *    chart.show
 *
 *    Not entirely sure about threading / synchronization in this model
 */
class Sampler extends T_T_Collect[Reservoir] {

  override def apply(acc:Reservoir, rhs:Row):Reservoir = {
      for( col <- 0 until rhs.ncols() ) {
        acc.add(rhs.d(col))
      }
      acc
  }

  override def reduce(lhs:Reservoir,rhs:Reservoir) = {
    lhs.merge(rhs)
  }
}