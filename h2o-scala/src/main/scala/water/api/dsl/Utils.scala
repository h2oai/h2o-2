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
  
  def readRow(chunks: Array[Chunk], rowIdx:Int, row:Array[Double]): Array[Double] = {
    for(i <- 0 until chunks.length) row(i) = chunks(i).at0(rowIdx)
    row 
  }
}

class MRICollector[X<:Iced](acc:X, cf: T_T_Collect[X, scala.Double]) extends MRTask2[MRICollector[X]] {
  var mracc: X = acc
  
  override def map(in:Array[Chunk]) = {
    mracc = acc
    val rlen = in(0)._len
    val tmprow = new Array[scala.Double](in.length)
    for (row:Int <- 0 until rlen ) {
      val rowdata = Utils.readRow(in,row,tmprow)
      mracc = cf(mracc, rowdata)
    }  
  }
  override def reduce(o:MRICollector[X]) = {
    mracc = cf.reduce(mracc, o.mracc)
  }
  
}
class MRCollector(acc:scala.Double, cf: T_T_Collect[scala.Double, scala.Double]) extends MRTask2[MRCollector] {
  
  var mracc: scala.Double = acc
  
  override def map(in:Array[Chunk]) = {
    mracc = acc
    val rlen = in(0)._len
    val tmprow = new Array[scala.Double](in.length)
    for (row:Int <- 0 until rlen ) {
      val rowdata = Utils.readRow(in,row,tmprow)
      mracc = cf(mracc, rowdata)
    }  
  }
  override def reduce(o:MRCollector) = {
    mracc = cf.reduce(mracc, o.mracc)
  }
} 
