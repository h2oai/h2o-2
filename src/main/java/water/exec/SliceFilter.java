package water.exec;

import water.*;

/** Slice filter!
 *
 * This filter is invoked on DEST, not on source argument!!!
 *
 * @author peta
 */
public class SliceFilter extends MRTask {
  final Key _source;
  final long _start;
  final long _length;
  final int _rowSize;
  long _filteredRows;

  public SliceFilter(Key source, long start, long length) {
    _source = source;
    _start = start;
    _length = length;
    ValueArray ary = ValueArray.value(source);
    assert (start + length <= ary.numRows());
    _rowSize = ary._rowsize;
  }


  @Override public void map(Key key) {
    ValueArray ary = ValueArray.value(_source);
    long cidx = ValueArray.getChunkIndex(key);
    long startRow = ary.startRow(cidx);
    int rowsInChunk = chunkSize(key, _length*_rowSize, _rowSize) / _rowSize;
    VAIterator iter = new VAIterator(_source,0,_start+startRow);
    AutoBuffer bits = new AutoBuffer(rowsInChunk*_rowSize);
    for (int offset = 0; offset < bits.limit(); offset += _rowSize) {
      iter.next();
      iter.copyCurrentRow(bits,offset);
      ++_filteredRows;
    }
    DKV.put(key, new Value(key,bits.buf()), getFutures());
  }

  @Override public void reduce(DRemoteTask drt) {
    SliceFilter other = (SliceFilter) drt;
    _filteredRows += other._filteredRows;
  }

  public static int chunkSize(Key k, long aryLength, int rowSize) {
    int result = (int) ValueArray.CHUNK_SZ;
    result = (result / rowSize) * rowSize; //- (result % rowSize);
    long offset = ValueArray.getChunkIndex(k) * result;
    if (offset + result + result >= aryLength)
      return (int) (aryLength - offset);
    else
      return result;
  }
}
