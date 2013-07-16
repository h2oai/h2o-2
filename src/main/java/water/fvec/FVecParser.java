package water.fvec;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import water.parser.*;
import water.parser.Enum;

/**
 * CSVParser specialization which outputs result into NewChunk.
 *
 * @author tomasnykodym
 *
 */
public abstract class FVecParser extends CsvParser {
  final NewChunk [] _nvs;
  final Enum [] _enums;
  long _nLines;
  final int _nCols;
  int _col = 0;

  public FVecParser(NewChunk [] nvs, CsvParser.Setup setup, Enum [] enums, boolean skip){
    super(setup, skip);
    _nvs = nvs;
    _enums = enums;
    _nCols = nvs.length;
  }

  @Override public final void newLine() {
    if(_col > 0){
      ++_nLines;
      for(int i = _col+1; i < _nCols; ++i)
        addInvalidCol(i);
    }
  }
  protected long linenum(){return _nLines;}
  @Override public final void addNumCol(int colIdx, long number, int exp) {
    if(colIdx >= _nvs.length)
      System.err.println("additional column (" + colIdx + ":" + number + "," + exp + ") on line " + linenum());
    else
      _nvs[_col = colIdx].addNum(number,exp);
  }

  @Override public final void addInvalidCol(int colIdx) {
    _nvs[_col = colIdx].addNA();
  }
  @Override public final boolean isString(int colIdx) { return false; }

  @Override public final void addStrCol(int colIdx, ValueString str) {
    if(colIdx >= _nvs.length){
      System.err.println("additional column (" + colIdx + ":" + str + ") on line " + linenum());
      return;
    }
    if(!_enums[_col = colIdx].isKilled()) {
      // store enum id into exponent, so that it will be interpreted as NA if compressing as numcol.
      int id = _enums[colIdx].addKey(str);
      _nvs[colIdx].addEnum(id);
      assert _enums[colIdx].getTokenId(str) == id;
      assert id <= _enums[colIdx].maxId();
    } else // turn the column into NAs by adding value overflowing Enum.MAX_SIZE
      _nvs[colIdx].addEnum(Integer.MAX_VALUE);
  }
  @Override public final void rollbackLine() {}

  /**
   * FVecCsvParser which takes input from a single chunk (+ the first line from the next one).
   * @author tomasnykodym
   *
   */
  public static class ChunkParser extends FVecParser {
    Chunk _chk;
    final long _firstLine;
    public ChunkParser(Chunk chk, NewChunk [] nvs, CsvParser.Setup setup, Enum [] enums){
      super(nvs,setup, enums, chk._start>0);
      _chk = chk;
      _firstLine = _chk._start;
    }
    protected long linenum(){return _nLines + _firstLine;}
    @Override public byte[] getChunkData(int cidx) {
      assert 0==cidx || cidx==1;// we expect to process only 1 chunk and the first line from the next one
      if(cidx == 0) return _chk._mem;
      Chunk nextChk = _chk._vec.nextBV(_chk);
      assert nextChk != _chk && (nextChk == null || nextChk._mem != _chk._mem);
      return (nextChk == null)?null:nextChk._mem;
    }
  }

  public static class StreamParser extends FVecParser {
    final InputStream _is;
    private byte[] _bits0 = new byte[32*1024];
    private byte[] _bits1 = new byte[32*1024];
    private int _cidx0=-1, _cidx1=-1; // Chunk #s

    public StreamParser(InputStream is, NewChunk [] nvs, CsvParser.Setup setup, Enum [] enums){
      super(nvs,setup,enums,false);
      _is = is;
    }
    @Override public byte[] getChunkData(int cidx) {
      if(cidx == _cidx0)return _bits0;
      if(cidx == _cidx1)return _bits1;
      assert cidx==_cidx0+1 || cidx==_cidx1+1;
      byte[] bits = _cidx0<_cidx1 ? _bits0 : _bits1;
      if( _cidx0<_cidx1 ) _cidx0 = cidx;
      else                _cidx1 = cidx;
      // Read as much as the buffer will hold
      int off=0;
      try {
        while( off < bits.length ) {
          int len = _is.read(bits,off,bits.length-off);
          if( len == -1 ) break;
          off += len;
        }
      } catch( IOException ioe ) {
        //_parserr = ioe.toString(); }
      }
      if( off == bits.length ) return bits;
      // Final read is short; cache the short-read
      byte[] bits2 = (off == 0) ? null : Arrays.copyOf(bits,off);
      if( _cidx0==cidx ) _bits0 = bits2;
      else               _bits1 = bits2;
      return bits2;
    }
  }
}
