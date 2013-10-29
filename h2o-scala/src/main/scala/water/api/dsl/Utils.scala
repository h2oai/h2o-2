package water.api.dsl

import water.fvec.Frame
import water.fvec.Vec
import water.Iced
import water.MRTask2
import water.fvec.Chunk

/** Frame utils. In most of cases, they do not modify
 *  original frame but create a new H2O frame. 
 */
object Utils {
  def a() = {
    ffilterByName(null, "A"::Nil)
  }
  
  def colIdx(f:Frame, cols: Seq[String]) = for ((n,idx) <- f.names().view.zipWithIndex; if cols.contains(n)) yield idx
  // Nasty hack -  How to beat type-erasure by implicit types: http://michid.wordpress.com/2010/06/14/working-around-type-erasure-ambiguities-scala/
  def ffilterByName(f:Frame, cols: Seq[String]):Frame = ffilterByIdx(f, colIdx(f, cols))
  def ffilterByIdx(f:Frame, cols: Seq[Int]):Frame = {
    val names = for ( idx <- cols) yield f.names()(idx)
    val vecs  = for ( idx <- cols) yield f.vecs ()(idx)
    
    return frame(names.toArray, vecs.toArray)
  }
  
  // Just inline call to create a new frame
  private def frame(ns:Array[String], vs:Array[Vec]): Frame = new Frame(ns,vs)
}

object TT {
  trait FM[-T,+R] extends (T=>R) { def d() = {println("dummY") } }
  abstract class Map2Value[R] extends FM[DFrame,R]
  abstract class Map2Frame extends Map2Value[DFrame]
  
  class CodeBlock extends Iced with ( () => Unit) {
    def apply(): Unit = { 
      println("A")
    }
  }
  
  class X(t: CodeBlock) extends MRTask2 {
    override def map(cs: Array[Chunk]):Unit = {
        println(cs)
        t()
      }
  }
  
  class TestMap(t: => CodeBlock) extends Map2Value[scala.Double] {
    final val x = new X(t)
    def apply(f:DFrame):scala.Double = { x.doAll(f.frame())
    return 0.0
    }
  }

  //def map( m: Map2Value[T]): 
  
  def test = {
    import H2ODsl._
    val f = parse("../smalldata/cars.csv") 
    new TestMap(new CodeBlock())(f)
  }
}
