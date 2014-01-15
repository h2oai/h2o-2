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
import water.Job
import hex.drf.DRF
import water.fvec.NewChunk

trait TRef {}

/** Trait holding basic Frame' operations. 
 *  
 *  It supports basic algebraic operations and selectors similar to R-like syntax. 
 */
trait T_Frame extends T_H20_Frame {
  def NHEAD:Int = 10;
  
  // Selector for columns
  def apply(cols: Seq[Int]):T_Frame
  def apply(cols: String): T_Frame
  def apply(rows: Seq[Int], cols: Seq[Int]):T_Frame
  def apply(rows: Seq[Int], cols: String):T_Frame
  /** Returns a new frame containing specified vectors. */
  //def \[T](ve: CSelect): TFrame = null; // Can be seq of Strings or Ints 
  //def ##[T](ve: CSelect) = \(ve::Nil)
  
  /** Basic arithmetic ops with scalar. */
  def +(rhs: Number): T_Frame;
  def -(rhs: Number): T_Frame;
  def *(rhs: Number): T_Frame;
  def /(rhs: Number): T_Frame;
  def ^(rhs: Number): T_Frame
  
  def <(rhs: Number): T_Frame;
  def <=(rhs: Number): T_Frame;
  def >(rhs: Number): T_Frame;
  def >=(rhs: Number): T_Frame;
  def ==(rhs: Number): T_Frame;
  def !=(rhs: Number): T_Frame;
  
  /** Basic arithmetic ops with another frame - R-like semantics. */ 
  def +(rhs: T_Frame): T_Frame;
  def -(rhs: T_Frame): T_Frame;
//  def *(rhs: TFrame): TFrame;
//  def /(rhs: TFrame): TFrame;
//  
//  def %*%(rhs: TFrame): TFrame;
  /** Append given frames */
  def ++(rhs: T_Frame): T_Frame;
  
  def ncol():Int;
  def nrow():Long;

}

trait T_H20_Frame {
  def frame() : Frame;
  def names():Array[String];
}

/** Generic Frame transformer */
//trait T_F_Transf[T <: DFrame] extends (T => T) // This is Function[+T,-T]

/** M/R based transformer */
//trait T_F_MR_Transf[T <: DFrame ] extends T_F_Transf[T] {
//}

/** Numeric value transformer. */
abstract class T_NV_Transf[T] extends Iced with (T => T)
/** A row transformer */
abstract class T_A2A_Transf[T,R] extends Iced with (Array[T] => Array[R])
abstract class T_A2B_Transf[T] extends Iced with (Array[T] => Boolean)
abstract class T_T_Collect[@specialized(scala.Double) ACCU,T] extends Iced with ((ACCU,Array[T]) => ACCU) {
  def reduce(accu1:ACCU, accu2:ACCU):ACCU
}

case class Add(lhs:scala.Double) extends T_NV_Transf[scala.Double] {
  def apply(rhs:scala.Double):scala.Double = lhs+rhs
}
case class Sub(lhs:scala.Double) extends T_NV_Transf[scala.Double] {
  def apply(rhs:scala.Double):scala.Double = lhs-rhs
}
case class Mul(lhs:scala.Double) extends T_NV_Transf[scala.Double] {
  def apply(rhs:scala.Double):scala.Double = lhs*rhs
}
case class Div(lhs:scala.Double) extends T_NV_Transf[scala.Double] {
  def apply(rhs:scala.Double):scala.Double = if (rhs!=0) lhs-rhs else scala.Double.NaN;
}

/** Partial specialization for types returning boolean. */
abstract class T_NF_Transf[T] extends Iced with (T => Boolean)
case class Less(lhs:scala.Double) extends T_NF_Transf[scala.Double] {
  def apply(rhs:scala.Double):Boolean = lhs < rhs
}
case class LessOrEqual(lhs:scala.Double) extends T_NF_Transf[scala.Double] {
  def apply(rhs:scala.Double):Boolean = lhs <= rhs
}
case class Greater(lhs:scala.Double) extends T_NF_Transf[scala.Double] {
  def apply(rhs:scala.Double):Boolean = lhs > rhs
}
case class GreaterOrEqual(lhs:scala.Double) extends T_NF_Transf[scala.Double] {
  def apply(rhs:scala.Double):Boolean = lhs >= rhs
}
case class Equal(lhs:scala.Double) extends T_NF_Transf[scala.Double] {
  def apply(rhs:scala.Double):Boolean = lhs == rhs
}
case class NEqual(lhs:scala.Double) extends T_NF_Transf[scala.Double] {
  def apply(rhs:scala.Double):Boolean = lhs != rhs
}

/** Chunk combinator for 2 frames 
 *  
 *  TODO: use a monad to express internal operation between chunks (+,-)
 */
abstract class T_Chunk_Combinator extends Iced with ( (Chunk, Chunk, NewChunk) => Unit );
case class CAdd() extends T_Chunk_Combinator {
  def apply(ic1:Chunk, ic2:Chunk, oc:NewChunk) = {
    for (row <- 0 until ic1._len) 
		        if (ic1.isNA0(row) || ic2.isNA0(row)) oc.addNA() else oc.addNum(ic1.at0(row)+ic2.at0(row))
  }
}
case class CSub() extends T_Chunk_Combinator {
  def apply(ic1:Chunk, ic2:Chunk, oc:NewChunk) = {
    for (row <- 0 until ic1._len) 
		        if (ic1.isNA0(row) || ic2.isNA0(row)) oc.addNA() else oc.addNum(ic1.at0(row)-ic2.at0(row))
  }
}

/** Support for M/R operation for frame - expect that frame contains all vector which we are operating on. */
// f[,2-3]+1 => f[,2-3].map( { x => x+1 }) => map(Chunks[] ch, NewChunk[] ncs) { }  
abstract trait T_MR[T <: DFrame] {
  
  import water.api.dsl.MRUtils._
  //self:T => def frame():Frame // target type should contain method frame()
  // use all columns in frame and apply a transformation on all of them
  //
  def frame():Frame
  def apply(f:Frame):T

  def map(vt: T_NV_Transf[scala.Double]):T = {
    val f = frame()
    val mrt = new MRTask2() {
      override def map(in:Array[Chunk], out:Array[NewChunk]) = {
        var cnt = 0
        for (oc:NewChunk <- out) {
          val ic = in(cnt)
          val rlen = ic._len
          for( row:Int <- 0 until rlen )
        	  oc.addNum(vt(ic.at0(row))) // append a new number into output chunk
              //oc.addNum(0f);
        }
      }   
    }
    mrt.doAll(f.numCols(), f)
    val result = mrt.outputFrame(f.names(), f.domains())
    apply(result) // return the DFrame
  }
  
  def map(vf: T_NF_Transf[scala.Double]):T = {
    val f = frame()
    
    val mrt = new MRTask2() {
      override def map(in:Array[Chunk], out:Array[NewChunk]) = {
        var cnt = 0
        for (oc:NewChunk <- out) {
          val ic = in(cnt)
          val rlen = ic._len
          for( row:Int <- 0 until rlen ) {
        	  val v = ic.at0(row)
        	  oc.addNum(if (vf(v)) 1 else 0) 
          }
        }
      }   
    }
    mrt.doAll(f.numCols(), f)
    val result = mrt.outputFrame(f.names(), f.domains())
    apply(result) // return the DFrame
  }
  
  // Apply filter over rows and produce a binary vector
  def map(af: T_A2B_Transf[scala.Double]):T = {
    val f = frame()
    
    val mrt = new MRTask2() {
      override def map(in:Array[Chunk], out:NewChunk) = {
        val rlen = in(0)._len
        val tmprow = new Array[scala.Double](in.length)
        for (row:Int <- 0 until rlen ) {
          out.addNum(if (af(Utils.readRow(in,row,tmprow))) 1 else 0)
        }  
      }   
    }
    mrt.doAll(1, f)
    val result = mrt.outputFrame(Array("result"), Array(null))
    apply(result) // return the DFrame
  }
  
  def filter(af: T_A2B_Transf[scala.Double]):T = {
    val f = frame()
    
    val mrt = new MRTask2() {
      override def map(in:Array[Chunk], out:Array[NewChunk]) = {
        val rlen = in(0)._len
        val tmprow = new Array[scala.Double](in.length)
        for (row:Int <- 0 until rlen ) {
          if (af(Utils.readRow(in,row,tmprow))) {
            for (i:Int <- 0 until in.length) out(i).addNum(tmprow(i))
          }
        }  
      }   
    }
    mrt.doAll(f.numCols(), f)
    val result = mrt.outputFrame(f.names(), f.domains())
    apply(result) // return the DFrame
  }
  
  def collect[X<:Iced](acc:X, cf: T_T_Collect[X, scala.Double]):X = {
    val f = frame()
    
    val mrt = new MRICollector(acc, cf)
    mrt.doAll(f)
    mrt.mracc
  }
  def collect(acc:scala.Double, cf: T_T_Collect[scala.Double, scala.Double]):scala.Double = {
    val f = frame()
    
    val mrt = new MRCollector(acc, cf)
    mrt.doAll(f)
    mrt.mracc
  }
}

/** Trait representing H2O published environment. 
 *  Provides transformation between DSL types and H2O types.
 */
trait T_H2O_Env[K<:HexKey, VT <: DFrame] { // Operating with only given representation of key

  // Parse a dataset
  def parse(s:String):DFrame = parse(s, s+".hex")
  def parse(s:String, destKey:String):DFrame = {
    val dest: Key = Key.make(destKey)
    val fkey = NFSFileVec.make(new File(s))
    val f = ParseDataset2.parse(dest, Array(fkey))
    UKV.remove(fkey)
    // Wrap the frame
    new DFrame(f)
  }
  def keys:Unit = keys(false)
  // Simply print a list of keys in KV store
  def keys(verbose:Boolean = false) = {
    import scala.collection.JavaConversions._ // import implicit inversion for Java collections
    println("*** Available keys *** ")
    if (H2O.keySet().isEmpty()) println("<None>")
    else H2O.keySet().foreach((k:Key) => if (k.user_allowed() || verbose) println(k))
    println("-----------------------")
  }
  // Access to DKV store is defined by reference
  // expressed by key and value
  def get(k: K): Frame      = UKV.get(k.key)
  def put[V<:VT](k: K, v:V) = UKV.put(k.key, v.frame())
  
  // Shows a list of jobs
  def jobs() = { 
    val aj = Job.all()
    aj foreach { j:Job =>
      val cancelled = if (j.end_time == 0) j.cancelled() else j.end_time == Job.CANCELLED_END_TIME
      val progress = if (j.end_time == 0) (if (cancelled) "DONE" else j.progress()) else "DONE" 
      println(j.description + " | " + (if (cancelled) "CANCELLED" else progress))
      }
  }
  // We need shutdown for sure ! :-)
  def shutdown() = H2O.CLOUD.shutdown()
  
  // DRF API call
  def drf(f: VT, r: VT, ntrees: Int = 50, classification:Boolean = false): DRF.DRFModel = {
    val drf:DRF = new DRF()
    val response = r.frame().vecs()(0)
    response.rollupStats()
    drf.source = new Frame(f.frame().names() ++ Array("response"), f.frame.vecs()++Array(response))
    drf.response = response
    drf.classification = classification
    drf.ntrees = ntrees;
    drf.invoke()
    return UKV.get(drf.dest())
  }
}

/** Trait representing provided global environment in R-like style.
 *  Working with first level entities: DFrame
 */
trait T_R_Env[T<:T_Frame] {
   // Global methods to support R-style of programming 
  def ncol(d: T): Int  = d.ncol()
  def nrow(d: T): Long = d.nrow()
  def vecs (d: T): Int = ncol(d)
  def head (d: T, rows:Int = NHEAD) = println(d)
  def tail (d: T, rows:Int = NTAIL) = println(d)
  def length(d: T) = if (ncol(d) > 1) ncol(d) else nrow(d)
  def names(d: T): Array[String] = d.names
  def helpme = help
  def help = println("""
*** Welcome into world of Shalala ***
      
Available R commands:
      help
      ncol <frame>
      nrow <frame>
      head <frame>
      tail <frame>
      f(2)           - returns 2. column
      f("year")		 - returns column "year"
      f(*,2)         - returns 2. column
      f(*, 2 to 5)   - returns 2., 3., 4., 5. columns
      f(*,2)+2       - scalar operation - 2.column + 2
      f(2)*3         - scalar operation - 2.column * 3
      f-1            - scalar operation - all columns - 1
      f < 10         - transform the frame into boolean frame respecting the condition
      f ++ f         - create a new frame by appending frames together

Available H2O commands:
      keys              - shows all available keys i KV store
      parse("iris.csv") - parse given file and return a frame
      put("a.hex", f)   - put a frame into KV store
      get("b.hex")      - return a frame from KV store
      jobs              - shows a list of executed jobs
      shutdown          - shutdown H2O cloud

M/R commands
      f map (Add(3))   - call of map function of all columns in frame
                          - function is (Double=>Double) and has to extend Iced
      f map (Less(10)) - call of map function on all columns
		  				  - function is (Double=>Boolean) 
       
Example:
      val f = parse("iris.csv")
      println(ncol f)
      val v2 = f(*, 2 to (ncol f))
""") 
  
  // Predefined constants
  def NHEAD = 10
  def NTAIL = 10
}
