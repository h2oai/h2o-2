package water.api.dsl

import water.fvec.Frame
import water.fvec.Vec
import water.Iced
import water.MRTask2
import water.fvec.Chunk
import water.fvec.NewChunk
import water.fvec.Chunk
import water.Freezable

/** Frame utils. In most of cases, they do not modify
 *  original frame but create a new H2O frame. 
 */
object Utils {
  
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
  
  def cbind(lhs:Frame, rhs:Frame):Frame = new Frame(lhs.names()++rhs.names(), lhs.vecs()++rhs.vecs()) 
}


// Sandbox
object TT {
  trait FM[-T,+R] extends (T=>R) { def d() = {println("dummY") } }
  abstract class Map2Value[R] extends FM[DFrame,R]
  abstract class Map2Frame extends Map2Value[DFrame]
  
  class CodeBlock extends Iced with ( () => Unit) {
    def apply(): Unit = { 
      println("Code Block")
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
  
  trait ParamsExtractor[T]
  trait ResultExtractor[T]
 
  trait Selector[P,R] {
  }
  
  class Select2tD extends Selector[(Symbol, Symbol), Double.type] {
    def apply(d: Double, b:Double) : Double = 0
  }
   
  /* What i need to produce
  class MappCaller extends P {
    val n = 2
    // vectory - numbers
    new MRTask2() {
      override def map(Chunk[] cs) = {
        // tady vim velikost cs
        F(cs[1], cs[2])
      }
    }.doAll("//vyselectovane vectory z frame")
    
  }*/
  implicit object ParamsExtractor extends ParamsExtractor[(Symbol, Symbol)] {
  }
  implicit object ResultExtractor extends ResultExtractor[Double.type] {
  }

  trait Mapper[FCTOR] {
    def apply(fn : FCTOR)
  }
  
  class Mapper2D2D extends Mapper[ (Double,Double) => Double ] {
    def apply(fn: (Double,Double) => Double) = println("Mapper")
  }
  def map[P : ParamsExtractor, R : ResultExtractor](t:(P,R)):Mapper[(Double,Double) => Double] = new Mapper2D2D() 

  // ================== Ad-hoc example of Scala code for map on 1 vec producing one vector
}

object XT {
  /* -- SHOULD BE GENERATED -- */
  abstract class IcedFunctor1[T1] extends Iced with (T1=>T1)
  abstract class IcedFunctor1to1[T1,R1] extends Iced with (T1=>R1)
  abstract class IcedFunctor2to1[T1,T2,R1] extends Iced with ((T1,T2)=>R1)
  abstract class IcedFunctor2to2[T1,T2,R1,R2] extends Iced with ((T1,T2)=>(R1,R2))
  
  def xmap (f:DFrame, t:(Symbol, Symbol))(fc: IcedFunctor1[Double]):DFrame = {
    val inVectorName = t._1.name
    val outVectorName = t._2.name
    
    val inVector = f.frame.vecs()(f.frame().find(inVectorName))
    
    val mrTask = new MRTask2() {
      override def map(in:Chunk, out:NewChunk) = {
        for (r <- 0 until in._len) {
          out.addNum(fc.apply(in.at0(r)))
        } 
      }
    }
    // invoke task
    mrTask.doAll(1, inVector)
    val result = mrTask.outputFrame(Array(outVectorName), Array(inVector._domain))
    
    new DFrame(result)
  }
  
  def xmap (f:DFrame, t:((Symbol, Symbol), (Symbol,Symbol)))(fc: IcedFunctor2to2[Double,Double,Double,Double]):DFrame = {
    val inSchema = t._1;
    val outSchema = t._2;
    
    // result
    new DFrame(new Frame())
  }

  // Another way of thinking about view bounds and context bounds is that the first transfers implicit conversions from the caller's scope. 
  // The second transfers implicit objects from the caller's scope
  def test = {
    import H2ODsl._
    implicit val f = parse("../smalldata/cars.csv") 
    println(f)
    //val x = xmap (f, ('cylinders -> 'moreThan4)) { (x:Double) => ( if (x%2==0) 0.0 else 1.0 ) }
    val y = xmap (f, ('name, 'cylinders) -> ('name, 'moreThan4) ) { // Explicit use of iced functor since i have no way how to make Function(s) to extend Iced 
      new IcedFunctor2to2[Double,Double,Double,Double] { def apply(x:Double, y:Double) = (if (x>4) 1 else 0, 0) } 
    }
    println(y)
    // intention is to have:
    //       |----------------MAP SELECTOR -------| |-------------MAP Implementation ---------------------|
    // f map ('cylinders, 'year) -> ('col1, 'col2) { (cyl:Double, year:Double) => (cyl+3*year, year+1900) }
    //x
  }
  def test2 = {
    import H2ODsl._
    implicit val f = parse("../smalldata/cars.csv")
    println(f)
  }
}

