package water.api.dsl

import water.fvec.Chunk
import water.MRTask2

abstract class SMRTask[T <: SMRTask[T]] extends MRTask2[T] {
  def iterator(chunks:Array[Chunk]) : Iterator[Row] = new RowIterator(chunks)

  private class RowIterator (private val chunks:Array[Chunk]) extends Iterator[Row] {
    private var rowNum: Int = -1
    private val row = new CRow
    def next()  : Row = {
      rowNum += 1
      return if (rowNum < chunks(0)._len) row else null
    }
    def hasNext() : Boolean = rowNum+1 < chunks(0)._len

    /** Array of chunks encapsulation */
    class CRow extends Row {
      override def d(ncol: Int): scala.Double = chunks(ncol).at0 (rowNum)
      override def l(ncol: Int): scala.Long   = chunks(ncol).at80(rowNum)
      override def ncols(): Int = chunks.length
    }
  }
}



