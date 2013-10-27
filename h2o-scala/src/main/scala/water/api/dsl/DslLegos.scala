package water.api.dsl

import water.fvec.Frame
import water.UKV
import water.H2O
import water.Key
import water.MRTask2
import water.fvec.Chunk
import water.fvec.NewChunk
import java.lang.Double
import water.Iced
import water.fvec.NFSFileVec
import java.io.File
import water.fvec.ParseDataset2

trait TRef {}

/** Trait holding basic Frame' operations. */
trait T_Frame {
  // Selector for columns
  def apply(cols: Seq[Int]):T_Frame
  // Selector for view defined by rows X cols
  //def apply[TR, TC](rows: Selector[TR], cols: Selector[TC]):TFrame
  def apply(rows: Seq[Int], cols: Seq[Int]):T_Frame
  /** Returns a new frame containing specified vectors. */
  //def \[T](ve: CSelect): TFrame = null; // Can be seq of Strings or Ints 
  //def ##[T](ve: CSelect) = \(ve::Nil)
  
  /** Basic arithmetic ops with scalar. */
  def +(rhs: Number): T_Frame;
  def -(rhs: Number): T_Frame;
  def *(rhs: Number): T_Frame;
  def /(rhs: Number): T_Frame;
  
  /** Basic arithmetic ops with another frame */
//  def +(rhs: TFrame): TFrame;
//  def -(rhs: TFrame): TFrame;
//  def *(rhs: TFrame): TFrame;
//  def /(rhs: TFrame): TFrame;
//  
//  def %*%(rhs: TFrame): TFrame;
  
  def ncol():Int;
  def nrow():Long;
}

/** Generic Frame transformer */
//trait T_F_Transf[T <: DFrame] extends (T => T) // This is Function[+T,-T]

/** M/R based transformer */
//trait T_F_MR_Transf[T <: DFrame ] extends T_F_Transf[T] {
//}

/** Numeric value transformer. */
trait T_NV_Transf[T] extends (T => T)

case class Add(lhs:Double) extends Iced with T_NV_Transf[Double] {
  def apply(rhs:Double):Double = lhs+rhs
}

/** Support for M/R operation for frame - expect that frame contains all vector which we are operating on. */
// f[,2-3]+1 => f[,2-3].map( { x => x+1 }) => map(Chunks[] ch, NewChunk[] ncs) { }  
trait T_MR[T <: DFrame] {
  //self:T => def frame():Frame // target type should contain method frame()
  // use all columns in frame and apply a transformation on all of them
  //
  def frame():Frame
  def apply(f:Frame):T
  
  def map(vt: T_NV_Transf[Double]):T = {
    val f = frame()
    val mrt = new MRTask2() {
      override def map(in:Array[Chunk], out:Array[NewChunk]) = {
        var cnt = 0
        for (oc:NewChunk <- out) {
          val ic = in(cnt)
          val rlen = ic._len
          for( row:Int <- 0 until rlen )
        	  //oc.addNum(vt(ic.at0(row))) // append a new number into output chunk
              oc.addNum(0f);
        }
      }   
    }
    mrt.doAll(f.numCols(), f)

    apply(mrt._outputFrame)
  }
}

/** Trait representing H2O published environment. 
 *  Provides transformation between DSL types and H2O types.
 */
trait T_H2O_Env[K<:HexKey, VT <: DFrame] { // Operating with only given representation of key
  //self => def apply(v:Frame):VT
  // Parse a dataset
  def parse(s:String):DFrame = {
    val dest: Key = Key.make(s+".hex")
    val fkey = NFSFileVec.make(new File(s))
    val f = ParseDataset2.parse(dest, Array(fkey))
    UKV.remove(fkey)
    // Wrap the frame
    new DFrame(f)
  }
  // Simply print a list of keys in KV store
  def keys() = {
    import scala.collection.JavaConversions._ // import implicit inversion for Java collections
    println("*** Available keys *** ")
    if (H2O.keySet().isEmpty()) println("<None>")
    else H2O.keySet().foreach((k:Key) => if (k.user_allowed()) println(k))
    println("-----------------------")
  }
  // Access to DKV store is defined by reference
  // expressed by key and value
  def get(k: K): Frame      = UKV.get(k.key)
  def put[V<:VT](k: K, v:V) = UKV.put(k.key, v.frame())
  // We need shutdown for sure ! :-)
  def shutdown() = H2O.CLOUD.shutdown()

}

/** Trait representing provided global environment in R-like style.
 *  Working with first level entities: Frame
 */
trait T_R_Env[T<:T_Frame] {
   // Global methods to support R-style of programming 
  def ncol(d: T): Int  = d.ncol()
  def nrow(d: T): Long = d.nrow()
  def vecs (d: T): Int = ncol(d)
  def head (d: T, rows:Int = NHEAD) = println(d)
  def tail (d: T, rows:Int = NTAIL) = println(d)
  def length(d: T) = if (ncol(d) > 1) ncol(d) else nrow(d)
  def helpme = help
  def help = println("""
*** Welcome into world of SkAlH2O ***
      
Available R commands:
      help
      ncol <frame>
      nrow <frame>
      head <frame>
      tail <frame>
      f(2)           - returns 2. column
      f(*,2)         - returns 2. column
      f(*, 2 to 5)   - returns 2., 3., 4., 5. columns
      f(*,2)+2       - scalar operation - 2.column + 2
      f(2)*3         - scalar operation - 2.column * 3
      f-1            - scalar operation - all columns - 1

Available H2O commands:
      keys              - shows all available keys i KV store
      parse("iris.csv") - parse given file and return a frame
      put("a.hex", f)   - put a frame into KV store
      get("b.hex")      - return a frame from KV store
      shutdown          - shutdown H2O cloud

M/R commands
      NA
      
Example:
      val f = parse("iris.csv")
      println(ncol f)
      val v2 = f(*, 2 to (ncol f))
""") 
  
  // Predefined constants
  def NHEAD = 10
  def NTAIL = 10
}
