package water.api.dsl

import water.fvec.Frame
import water.fvec.Vec
import water.Iced
import water.MRTask2
import water.fvec.Chunk
import water.fvec.NewChunk
import water.fvec.Chunk
import water.Freezable
import water.fvec.Frame
import java.lang.StringBuilder
import java.lang.Long

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
  
  def readRow(chunks: Array[Chunk], rowIdx:Int, row:Array[Double]): Array[Double] = {
    for(i <- 0 until chunks.length) row(i) = chunks(i).at0(rowIdx)
    row 
  }
  
  def combine[T<:T_Frame](f1:T, f2:T, t:T_Chunk_Combinator): DFrame = {
		assert(f1.ncol == f2.ncol && f1.nrow == f2.nrow)
		val f = f1 ++ f2
		val r = new MRTask2() {
		  override def map(ic: Array[Chunk], oc: Array[NewChunk]) = {
		    val half = ic.length / 2
		    for (i <- 0 until half) {
		      val ic1 = ic(i)
		      val ic2 = ic(i+half)
		      val o   = oc(i)
		      t.apply(ic1, ic2, o)
		    }
		  }
		}
		r.doAll(f1.ncol, f.frame)
		new DFrame(r.outputFrame(f1.frame.names(), f1.frame.domains()))
	}
  
  def head(f: Frame, nrows: Long ):String = {
    val r : StringBuilder = new StringBuilder
    if (f!=null) { 
      val fs:Array[String] = f.toStringHdr(r);
      for (i <- 0L until Math.min(f.numRows(), nrows) ) f.toString(r, fs, i)
    }
    return r.toString
  }
  
  def tail(f: Frame, nrows: Long ):String = {
    val r : StringBuilder = new StringBuilder
    if (f!=null) { 
      val fs:Array[String] = f.toStringHdr(r);
      for (i <- Math.max(0, f.numRows()-nrows) until f.numRows() ) f.toString(r, fs, i)
    }
    return r.toString
  }
}

class MRICollector[X<:Iced](acc:X, cf: T_T_Collect[X]) extends SMRTask[MRICollector[X]] {
  var mracc: X = acc
  
  override def map(in:Array[Chunk]) = {
    mracc = acc
    val it = iterator(in)
    while (it.hasNext) {
      mracc = cf(mracc, it.next())
    }  
  }
  override def reduce(o:MRICollector[X]) = {
    mracc = cf.reduce(mracc, o.mracc)
  }
  
}
class MRCollector(acc:scala.Double, cf: T_T_Collect[scala.Double]) extends SMRTask[MRCollector] {
  
  var mracc: scala.Double = acc
  
  override def map(in:Array[Chunk]) = {
    mracc = acc
    val it = iterator(in)
    while (it.hasNext) {
      mracc = cf(mracc, it.next())
    }  
  }
  override def reduce(o:MRCollector) = {
    mracc = cf.reduce(mracc, o.mracc)
  }
} 
