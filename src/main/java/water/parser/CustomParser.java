package water.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;

import water.*;


public abstract class CustomParser extends Iced {
  public static final byte CHAR_TAB = '\t';
  public static final byte CHAR_LF = 10;
  public static final byte CHAR_SPACE = ' ';
  public static final byte CHAR_CR = 13;
  public static final byte CHAR_VT = 11;
  public static final byte CHAR_FF = 12;
  public static final byte CHAR_DOUBLE_QUOTE = '"';
  public static final byte CHAR_SINGLE_QUOTE = '\'';
  public static final byte CHAR_NULL = 0;
  public static final byte CHAR_COMMA = ',';

  public final ParserSetup _setup;

  public CustomParser(ParserSetup setup){_setup = setup;}

  public enum ParserType {AUTO,XLS,XLSX,CSV, SVMLight}
  public static class ParserSetup extends Iced implements Cloneable{
    public final ParserType _pType;
    public final byte _separator;
    public boolean _header;
    public String [] _columnNames;
    public final transient String [][] _data;
    public final transient byte[] _bits;
    public final int _ncols;

    public ParserSetup() {
      _pType = ParserType.AUTO;
      _separator = CsvParser.NO_SEPARATOR;
      _header = false;
      _data = null;
      _bits = null;
      _ncols = 0;
      _columnNames = null;
    }
    protected ParserSetup(ParserType t, byte sep, int ncols, boolean header, String [][] data, byte [] bits) {
      _pType = t;
      _separator = sep;
      _header = header;
      _columnNames = _header?data[0]:null;
      _data = data;
      _bits = bits;
      _ncols = ncols;
    }
    private ParserSetup(ParserType t, byte sep, int ncols, boolean header, String [] columnNames, String [][] data, byte [] bits) {
      _pType = t;
      _separator = sep;
      _ncols = ncols;
      _header = header;
      _columnNames = columnNames;
      _data = data;
      _bits = bits;
    }
    public void setHeader(boolean val){
      if(!val){
        _header = false;
        _columnNames = null;
      } else if(_data != null){
        _header = true;
        _columnNames = _data[0];
      } else
        assert false;
    }
    public ParserSetup clone(){
      return new ParserSetup(_pType, _separator, _ncols, _header, _columnNames, _data,_bits);
    }
    public boolean isCompatible(ParserSetup other){
      if(other == null || _pType != other._pType)return false;
      if(_pType == ParserType.CSV && (_separator != other._separator || _ncols != other._ncols))
        return false;
      return true;
    }

    public boolean isCompatible(Key k){
      ParserSetup s = ParseDataset.guessSetup(DKV.get(k), _pType, _separator);
      return isCompatible(s);
    }
    public static ParserSetup makeSetup(){return new ParserSetup(ParserType.AUTO,CsvParser.NO_SEPARATOR, 0, false, null,null);}
    public static ParserSetup makeSVMLightSetup(int ncols, String [][] data, byte [] bits){return new ParserSetup(ParserType.SVMLight,(byte)' ', ncols, false, data,bits);}
    public static ParserSetup makeCSVSetup(byte sep, boolean hdr, String [][] data, byte [] bits, int ncols){return new ParserSetup(ParserType.CSV,sep, ncols,hdr, data,bits);}
  }
  public boolean isCompatible(CustomParser p){return _setup == p._setup || (_setup != null && _setup.isCompatible(p._setup));}
  public void parallelParse(int cidx, final DataIn din, final DataOut dout) {throw new UnsupportedOperationException();}
  public boolean parallelParseSupported(){return false;}
  // ------------------------------------------------------------------------
  // Zipped file; no parallel decompression; decompress into local chunks,
  // parse local chunks; distribute chunks later.
  public void streamParse( final InputStream is, final DataOut dout) throws Exception {
    // All output into a fresh pile of NewChunks, one per column
    StreamData din = new StreamData(is);
    int cidx=0;
    while( is.available() > 0 )
      parallelParse(cidx++,din,dout);
    parallelParse(cidx++,din,dout);     // Parse the remaining partial 32K buffer
  }
  protected static final boolean isWhitespace(byte c) {
    return (c == CHAR_SPACE) || (c == CHAR_TAB);
  }

  protected static final boolean isEOL(byte c) {
    return (c >= CHAR_LF) && ( c<= CHAR_CR);
  }
  public interface DataIn {
    // Get another chunk of byte data
    public abstract byte[] getChunkData( int cidx );
  }
  public interface DataOut extends Freezable {
    public void setColumnNames(String [] names);
    // Register a newLine from the parser
    public void newLine();
    // True if already forced into a string column (skip number parsing)
    public boolean isString(int colIdx);
    // Add a number column with given digits & exp
    public void addNumCol(int colIdx, long number, int exp);
 // Add a number column with given digits & exp
    public void addNumCol(int colIdx, double d);
    // An an invalid / missing entry
    public void addInvalidCol(int colIdx);
    // Add a String column
    public void addStrCol( int colIdx, ValueString str );
    // Final rolling back of partial line
    public void rollbackLine();
    public void invalidLine(int lineNum);
    public void invalidValue(int line, int col);
  }

  public static class StreamData implements CustomParser.DataIn {
    final transient InputStream _is;
    private byte[] _bits0 = new byte[32*1024];
    private byte[] _bits1 = new byte[32*1024];
    private int _cidx0=-1, _cidx1=-1; // Chunk #s
    public StreamData(InputStream is){_is = is;}
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
  public abstract CustomParser clone();
  public String [] headers(){return null;}
}



