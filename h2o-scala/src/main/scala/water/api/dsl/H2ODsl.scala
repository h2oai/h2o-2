package water.api.dsl

import water.fvec.Frame
import scala.collection.immutable.Range
import water.fvec.Frame
import water.UKV
import water.Key
import water.H2O
import water.TestUtil
import java.io.File
import water.fvec.NFSFileVec
import water.fvec.ParseDataset2
import java.util.UUID

/** The object carry global environment and provides basic global methods such as head, tail, nrows, ... */
object H2ODsl extends H2ODslImplicitConv with T_R_Env[DFrame] with T_H2O_Env[HexKey, DFrame] {
  // Binary operator type alias
  type BOp = T_NV_Transf[scala.Double]
  // Filter operator type alias
  type FOp = T_NF_Transf[scala.Double]
 
  // Dummy tester and H2O launcher - should launch H2O with REPL
  def main(args: Array[String]): Unit = {
    println("Launching H2O...")
    water.Boot.main(Array("-mainClass", "water.api.dsl.ScAlH2ORepl"));
  }
  
  // An object to represent an empty range selector 
  case object * extends Range(0, -1, 1);
  
  def frame(kname:String):DFrame = new DFrame(get(kname))
  def echo = println
  
  def example():DFrame = {
    println("""
== Parsing smalldata/cars.csv""")
    val f = parse("smalldata/cars.csv")
    println("""
== Number of cylinders - f("cylinders")""")
    println(f("cylinders"))
    println("""
== Selecting a set of columns and storing a frame reference - val f2 = f(2::3::7::Nil)""")
    val f2 = f(2::3::7::Nil)
    println(f2)
    println("""
== Using boolean transformation on last column - f("year") > 80 """)
    val f3 = f("year") > 80
    println(f3)
    println("""
== Launching map function on a select column:
    f("cylinders") map (new BOp { 
      var sum:scala.Double = 0
      def apply(rhs:scala.Double) = { sum += rhs; rhs*rhs / sum; } 
    })""")
    val f4 = f("cylinders") map (new BOp { 
      var sum:scala.Double = 0
      def apply(rhs:scala.Double) = { sum += rhs; rhs*rhs / sum; } 
    })  
    println(f4)
    f
  }

  def test():DFrame = {
    val f = parse("../smalldata/cars.csv")
    f("cylinders") map (new BOp { 
      var sum:scala.Double = 0
      def apply(rhs:scala.Double) = { sum += rhs; rhs*rhs / sum; } 
    })
  }
}

/** A wrapper for H2O Frame proving basic operations */
class DFrame(private val _frame:Frame = new Frame) extends T_Frame with T_MR[DFrame] {
  import water.api.dsl.Utils._;
  
  def frame() = _frame
  
  def ncol() = frame().numCols()
  def nrow() = frame().numRows()
  def names() = frame().names()
  
  def apply(cols: Seq[Int]) = new DFrame(ffilterByIdx(frame(), cols))
  def apply(cols: String) = new DFrame(ffilterByName(frame, cols::Nil))
  def apply(rows: Seq[Int], cols: Seq[Int]) = apply(cols)
  def apply(rows: Seq[Int], cols: String) = apply(cols)
  def apply(f: Frame) = new DFrame(f)
  
  def +(rhs: Number) = map(Add(rhs.doubleValue()))
  def -(rhs: Number) = map(Sub(rhs.doubleValue()))
  def *(rhs: Number) = map(Mul(rhs.doubleValue()))
  def /(rhs: Number) = apply(MRUtils.div(frame(), rhs.doubleValue()))

  def <(rhs: Number)  = map(Greater(rhs.doubleValue()))
  def >=(rhs: Number) = map(LessOrEqual(rhs.doubleValue()))
  def >(rhs: Number)  = map(Less(rhs.doubleValue()))
  def <=(rhs: Number) = map(GreaterOrEqual(rhs.doubleValue()))
  def ==(rhs: Number) = map(Equal(rhs.doubleValue()))
  def !=(rhs: Number) = map(NEqual(rhs.doubleValue()))
  
  // Append
  def ++(rhs: T_Frame) = new DFrame(cbind(frame(), rhs.frame()))
  override def toString() = frame().toStringHead(NHEAD)
  
}

sealed case class HexKey(key: Key) extends TRef {
  def name = key.toString()
}

// ++ implicit conversions


trait H2ODslImplicitConv {
  // Implicit conversion from Int to Range
  implicit def Int2Range(n: Int): Range = Range(n, n + 1, 1);
  implicit def String2HexKey(s: String): HexKey = HexKey(Key.make(s));
  //  implicit def String2DFrame(s: String): DFrame = DFrame(s);
} 

