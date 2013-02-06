
package water.exec;

import java.util.Iterator;

import water.*;
import water.ValueArray.Column;

/**
 *
 * @author peta
 */
public final class VAIterator implements Iterator<VAIterator> {

  public final ValueArray _ary;

  public final long _rows;

  public final int _rowSize;

  private Column _defaultCol;

  private int _rowInChunk;
  private int _rowsInChunk;
  private AutoBuffer _chunkBits;
  private long _chunkOffset;
  private long _currentRow;
  private int _chunkIdx;

  public VAIterator(Key k, int defaultColumn, long startRow) {
    _ary = ValueArray.value(k);
    assert (_ary != null) : "VA for key "+k.toString()+" not found.";
    _rows = _ary.numRows();
    _rowSize = _ary.rowSize();
    setDefaultColumn(defaultColumn);
    _rowInChunk = -1;
    _rowsInChunk = 0;
    _currentRow = -1;
    _chunkIdx = -1;
    if (startRow!=0)
      skipRows((startRow % _rows));
  }

  public VAIterator(Key key, int defaultColumn) {
    this(key,defaultColumn,0);
  }

  public void setDefaultColumn(int colIdx) {
    assert (colIdx>=0) && (colIdx<_ary.numCols());
    _defaultCol = _ary._cols[colIdx];
  }

  public Column defaultColumn() {
    return _defaultCol;
  }

  private void skipRows(long rows) {
    assert (_currentRow + rows < _rows);
    next();
    --rows; // on the next() call we will be at the desired row
    while (true) {
      if (rows < _rowsInChunk) {
        _rowInChunk += rows;
        _currentRow += rows;
        break;
      }
      _rowInChunk = _rowsInChunk-1;
      _currentRow += _rowsInChunk-1;
      rows -= _rowsInChunk;
      next(); // move to next chunk
    }
  }

  public long row() {
    return _currentRow;
  }

  @Override public boolean hasNext() {
    return (_currentRow < _rows);
  }

  @Override public VAIterator next() {
    ++_currentRow;
    ++_rowInChunk;
    if (_rowInChunk == _rowsInChunk) {
      if (_currentRow == _rows) { // wrap after end has been reached
        _currentRow = 0;
        _rowInChunk = 0;
        _chunkOffset = 0;
        _chunkIdx = 0;
      } else {
        // load new chunk
        _chunkOffset = _chunkOffset + _rowsInChunk * _rowSize;
        _chunkIdx += 1;
      }
      _chunkBits = _ary.getChunk(_chunkIdx);
      _rowsInChunk = _chunkBits.remaining() / _rowSize;
      _rowInChunk = 0;
    }
    return this;
  }

  @Override public void remove() {
    throw new UnsupportedOperationException("Not supported yet.");
  }

  public long    data () { return _ary.data (_chunkBits, _rowInChunk, _defaultCol); }
  public double  datad() { return _ary.datad(_chunkBits, _rowInChunk, _defaultCol); }
  public boolean isNA () { return _ary.isNA (_chunkBits, _rowInChunk, _defaultCol); }
  public long    data (int column) { return _ary.data (_chunkBits,_rowInChunk,column); }
  public double  datad(int column) { return _ary.datad(_chunkBits,_rowInChunk,column); }
  public boolean isNA (int column) { return _ary.isNA (_chunkBits,_rowInChunk,column); }

  public int copyCurrentRow(AutoBuffer bits, int offset) {
    bits.copyArrayFrom(offset, _chunkBits, _rowInChunk*_rowSize, _rowSize);
    return offset + _rowSize;
  }

  public int copyCurrentRowPart(AutoBuffer dest, int offset, int rowStart, int rowEnd) {
    dest.copyArrayFrom(offset, _chunkBits, _rowInChunk*_rowSize+rowStart, rowEnd-rowStart);
    return rowEnd - rowStart + offset;
  }
}
