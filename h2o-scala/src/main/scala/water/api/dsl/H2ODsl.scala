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

object A {
  def main(args: Array[String]) = {
    H2O.main(args);
    ScAlH2ORepl.launchRepl()
    
    try {
      import H2ODsl._
      // Run test code - Whoooo! 
      H2ODsl.test()
      shutdown()
    } catch {
    	case e: Throwable => e.printStackTrace(); 
    } finally {
      // And exit
      H2O.exit(0);
    }
  }
}

/** The object carry global environment and provides basic global methods such as head, tail, nrows, ... */
object H2ODsl extends H2ODslImplicitConv with T_R_Env[DFrame] with T_H2O_Env[HexKey, DFrame] {

  // Dummy tester and H2O launcher - should launch H2O with REPL
  def main(args: Array[String]): Unit = {
    println("Launching H2O...muheheh from Scala")
    //H2O.main(Array.empty[String]);
    water.Boot.main(Array("-mainClass", "water.api.dsl.A"));
    //water.Boot.main(Array.empty[String]);
  }
  
  // An object to represent an empty range selector 
  case object * extends Range(0, -1, 1);
  
  def frame(kname:String):DFrame = new DFrame(get(kname))

  def test() = {
    helpme
    keys
    val f = parse("/Users/michal/Devel/projects/h2o/repos/NEW.h2o.github/smalldata/iris/iris_wheader.csv")
    // one colum
    println(f(*, 3))
    // more columns
    println(f(*, 3 to 4))
    // out of bound column
    // FIXME:println(f(*, 6::5::4::Nil))
    
    // scalar operation (hidden M/R)
    println(f(*, 3 to 4)+1)
    
    println(f(*,3)-1)
    println(f(*,3)*100)
    println(f(*,3)/100)
    println(f(*,3)/0)
    // List of all keys in KV store
    val nf = f(*,1)
    keys
    put("aajjaja.hex", nf)
    println(nf)
    println(frame("aajjaja.hex"))
    keys
    //println(f \ (3 to 5));
    //println(f \ "col1"::"col2"::"col3"::Nil);
    //println(f \ "col1"::"col2"::"col3"::Nil);
    //val x = 1 :: 2 :: Nil
    //println(f ## "sepal_len")
  }
}

class DFrame(private val _frame:Frame = new Frame) extends TFrame with T_MR[DFrame] {
  import water.api.dsl.Utils._;
  
  def frame() = _frame
  
  def ncol() = frame().numCols()
  def nrow() = frame().numRows()
  def apply(cols: Seq[Int]) = new DFrame(ffilterByIdx(frame(), cols))
  def apply(rows: Seq[Int], cols: Seq[Int]) = apply(cols)
  def apply(f: Frame) = new DFrame(f)
  
  def +(rhs: Number) = apply(MRUtils.add(frame(), rhs.doubleValue()))
  def -(rhs: Number) = apply(MRUtils.sub(frame(), rhs.doubleValue()))
  def *(rhs: Number) = apply(MRUtils.mul(frame(), rhs.doubleValue()))
  def /(rhs: Number) = apply(MRUtils.div(frame(), rhs.doubleValue()))
  
  override def toString() = frame().toStringHead(10)
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

