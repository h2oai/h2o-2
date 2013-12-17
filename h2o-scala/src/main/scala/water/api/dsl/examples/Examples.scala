package water.api.dsl.examples

import water.Iced
import water.api.dsl.T_T_Collect
import water.api.dsl.DFrame
import water.api.dsl.DFrame

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
  
  /** Call DRF, make a model, predict on a train data, compute MSE. */ 
  def example2() = {
    
    import water.api.dsl.H2ODsl._
    val f = parse("../private/cars.csv")
    val source = f(1) ++ f(3 to 7)
    val response = f(2)
    
    // build a model
    val model = drf(source, response, 10, false)  // doing regression
    println("The DRF model is: \n" + model)
    // make a prediction
    val predict:DFrame = model.score(source.frame())
    
    println("Prediction on train data: \n" + predict)
    
    // compute squared errors
    val serr = (response - predict)^2
    println("Errors per row: " + serr)
    // make a sum
    val rss = serr collect (0.0, new CDOp() {
      def apply(acc:scala.Double, rhs:Array[scala.Double]) = acc + rhs(0)
      def reduce(acc1:scala.Double, acc2:scala.Double) = acc1+acc2
    })

    println("RSS: " + rss)
    
    shutdown()
  }
 
}

