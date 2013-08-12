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

  public enum ParserType {
    AUTO(false),XLS(false),XLSX(false),CSV(true), SVMLight(true);
    public final boolean parallelParseSupported;
    ParserType(boolean par){parallelParseSupported = par;}
  }
  public static class ParserSetup extends Iced implements Cloneable{
    public final ParserType _pType;
    public final byte _separator;
    public boolean _header;
    public String [] _columnNames;
    public final transient String [][] _data;
    public final int _ncols;

    public ParserSetup() {
      _pType = ParserType.AUTO;
      _separator = CsvParser.AUTO_SEP;
      _header = false;
      _data = null;
      _ncols = 0;
      _columnNames = null;
    }
    protected ParserSetup(ParserType t) {
      this(t,CsvParser.AUTO_SEP,false,null);
    }
    public ParserSetup(ParserType t, byte sep, boolean header) {
      _pType = t;
      _separator = sep;
      _header = header;
      _columnNames = null;
      _data = null;
      _ncols = 0;
    }
    public ParserSetup(ParserType t, byte sep, boolean header, String [][] data) {
      _pType = t;
      _separator = sep;
      _header = header;
      _columnNames = _header?data[0]:null;
      _data = data;
      _ncols = data != null?data[0].length:0;
    }
    public void setHeader(boolean val){
      if(!(_header = val))
        _columnNames = null;
      else if(_data != null)
        _columnNames = _data[0];
      else
        assert false;
    }
    public ParserSetup clone(){
      return new ParserSetup(_pType, _separator, _header);
    }
    public boolean isCompatible(ParserSetup other){
      if(other == null || _pType == ParserType.AUTO || _pType != other._pType)return false;
      if(_pType == ParserType.CSV && (_separator != other._separator || _ncols != other._ncols))
        return false;
      return true;
    }
    public CustomParser parser(){
      switch(this._pType){
        case CSV:
          return new CsvParser(this,false);
        case SVMLight:
          return new SVMLightParser(this);
        case XLS:
          return new XlsParser(this);
        default:
          throw H2O.unimpl();
      }
    }
    public String toString(){
      StringBuilder sb = new StringBuilder(_pType.name());
      switch(_pType){
        case SVMLight:
          sb.append(" data with (estimated) " + _ncols + " columns.");
          break;
        case CSV:
          sb.append(" data with " + _ncols + " columns using '" + (char)_separator + "' (\\" + _separator + "04d) as separator.");
          break;
        case XLS:
          sb.append(" data with " + _ncols + " columns.");
          break;
        case AUTO:
          sb.append("");
          break;
        default:
          throw H2O.unimpl();
      }
      return sb.toString();
    }
  }
  public boolean isCompatible(CustomParser p){return _setup == p._setup || (_setup != null && _setup.isCompatible(p._setup));}
  public void parallelParse(int cidx, final DataIn din, final DataOut dout) {throw new UnsupportedOperationException();}
  public boolean parallelParseSupported(){return false;}
  // ------------------------------------------------------------------------
  // Zipped file; no parallel decompression; decompress into local chunks,
  // parse local chunks; distribute chunks later.
  public void streamParse( final InputStream is, final DataOut dout) throws Exception {
    // All output into a fresh pile of NewChunks, one per column
    if(_setup._pType.parallelParseSupported){
      StreamData din = new StreamData(is);
      int cidx=0;
      while( is.available() > 0 )
        parallelParse(cidx++,din,dout);
      parallelParse(cidx++,din,dout);     // Parse the remaining partial 32K buffer
    } else {
      throw H2O.unimpl();
    }
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


  protected static class CustomInspectDataOut extends Iced implements DataOut {
    public int _nlines;
    public int _ncols;
    public int _invalidLines;
    public boolean _header;
    public final static int MAX_COLS = 100;
    public final static int MAX_LINES = 50;
    private String []   _colNames;
    private String [][] _data = new String[MAX_LINES][MAX_COLS];
    public CustomInspectDataOut() {
     for(int i = 0; i < MAX_LINES;++i)
       Arrays.fill(_data[i],"NA");
    }
    public String [][] data(){
      String [][] res = Arrays.copyOf(_data, Math.min(MAX_LINES, _nlines));
      for(int i = 0; i < res.length; ++i)
        res[i] = Arrays.copyOf(_data[i], Math.min(MAX_COLS,_ncols));
      return (_data = res);
    }
    @Override public void setColumnNames(String[] names) {
      _colNames = names;
      _data[0] = names;
      ++_nlines;
      _ncols = names.length;
      _header = true;
    }
    @Override public void newLine() {
      ++_nlines;
    }
    @Override public boolean isString(int colIdx) {return false;}
    @Override public void addNumCol(int colIdx, long number, int exp) {
      if(colIdx < _ncols && _nlines < MAX_LINES)
        _data[_nlines][colIdx] = Double.toString(number*DParseTask.pow10(exp));
    }
    @Override public void addNumCol(int colIdx, double d) {
      _ncols = Math.max(_ncols,colIdx);
      if(_nlines < MAX_LINES && colIdx < MAX_COLS)
        _data[_nlines][colIdx] = Double.toString(d);
    }
    @Override public void addInvalidCol(int colIdx) {
      if(colIdx < _ncols && _nlines < MAX_LINES)
        _data[_nlines][colIdx] = "NA";
    }
    @Override public void addStrCol(int colIdx, ValueString str) {
      if(colIdx < _ncols && _nlines < MAX_LINES)
        _data[_nlines][colIdx] = str.toString();
    }
    @Override public void rollbackLine() {--_nlines;}
    @Override public void invalidLine(int linenum) {++_invalidLines;}
    @Override public void invalidValue(int linenum, int colnum) {}
  }

}



