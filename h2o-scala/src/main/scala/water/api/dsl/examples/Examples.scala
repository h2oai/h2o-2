package water.api.dsl.examples

import water.Iced
import water.api.dsl.T_T_Collect

/**
 * Ideas:
 * GroupBy for H2O.
 * Histogram.
 * Matrix product.
 */
object Examples {
  
  /** Compute average for given column. */
  def example1() = {
    import water.api.dsl.H2ODsl._
    
    /** Mutable class */
    class Avg(var sum:scala.Double, var cnt:Int) extends Iced;
    
    val f = parse("../private/cars.csv")
    val r = f collect ( new Avg(0,0), 
      new T_T_Collect[Avg,scala.Double] {
	      override def apply(acc:Avg, rhs:Array[scala.Double]):Avg = {
	        acc.sum += rhs(2)
	        acc.cnt += 1
	        return acc
	      }
	      override def reduce(l:Avg,r:Avg) = new Avg(l.sum+r.sum, l.cnt+r.cnt)
    	} ) 
    
    println("Average of 2. column is: " + r.sum / r.cnt);
    shutdown()
  }
 
}

