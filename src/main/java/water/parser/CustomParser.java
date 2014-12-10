package water.parser;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;


import water.*;
import water.fvec.ParseDataset2.ParseProgressMonitor;


public abstract class CustomParser extends Iced {
  public static final byte CHAR_TAB = '\t';
  public static final byte CHAR_LF = 10;
  public static final byte CHAR_SPACE = ' ';
  public static final byte CHAR_CR = 13;
  public static final byte CHAR_DOUBLE_QUOTE = '"';
  public static final byte CHAR_SINGLE_QUOTE = '\'';

  public final static int MAX_PREVIEW_COLS  = 100;
  public final static int MAX_PREVIEW_LINES = 50;
  public final static int STRING_DOMINANCE_RATIO = 4;

  public final ParserSetup _setup;

  public CustomParser(ParserSetup setup){_setup = setup;}

  public static class PSetupGuess extends Iced {
    public final ParserSetup _setup;
    public final int _invalidLines;
    public final int _validLines;
    public final String []   _errors;
    public Key _setupFromFile;
    public Key _hdrFromFile;
    public String [][] _data;
    public final boolean _isValid;
    public PSetupGuess(ParserSetup ps, int vlines, int ilines, String [][] data, boolean isValid, String [] errors){
      _setup = ps;
      _invalidLines = ilines;
      _validLines = vlines;
      _errors = errors;
      _data = data;
      _isValid = isValid;
    }

    public Set<String> checkDupColumnNames(){
      return _setup.checkDupColumnNames();
    }

    public final boolean hasErrors(){
      return _errors != null && _errors.length > 0;
    }

    @Override public String toString(){
      if(!_isValid)
        return "Parser setup appears to be broken, got " + _setup.toString();
      else if(hasErrors())
        return "Parser setup appears to work with some errors, got " + _setup.toString();
      else
        return "Parser setup working fine, got " + _setup.toString();
    }
  }
  public enum ParserType {
    AUTO(false),XLS(false),XLSX(false),CSV(true), SVMLight(true);
    public final boolean parallelParseSupported;
    ParserType(boolean par){parallelParseSupported = par;}
  }
  public static class ParserSetup extends Iced implements Cloneable{
    public final ParserType _pType;
    public final byte _separator;
    public boolean _header;
    public boolean _hashHeader;
    public boolean _singleQuotes;
    public String [] _columnNames;
    public final int _ncols;


    public enum Coltype {
      NUM,ZERO,STR,AUTO,INVALID;
    }

    public static class TypeInfo extends Iced{
      Coltype _type;
      ValueString _naStr = new ValueString("");
      boolean _strongGuess;

      public void merge(TypeInfo tinfo){
        if(_type == Coltype.AUTO || !_strongGuess && tinfo._strongGuess){ // copy over stuff from the other
          _type = tinfo._type;
          _naStr = tinfo._naStr;
          _strongGuess = tinfo._strongGuess;
        } else if(tinfo._type != Coltype.AUTO && !_strongGuess){
          tinfo._type = Coltype.INVALID;
        } // else just keep mine
      }
    }
    public String [][] domains;
    public double [] _min;
    public double [] _max;
    public int _nnums;
    public int _nstr;
    public int _missing;
    public int _nzeros;

    TypeInfo [] _types;

    public ParserSetup() {
      _pType = ParserType.AUTO;
      _separator = CsvParser.AUTO_SEP;
      _header = false;
      _hashHeader = false;
      _ncols = 0;
      _columnNames = null;
    }
    protected ParserSetup(ParserType t) {
      this(t,CsvParser.AUTO_SEP,0,false,null,false);
    }
    public ParserSetup(ParserType t, byte sep, boolean header) {
      _pType = t;
      _separator = sep;
      _header = header;
      _hashHeader = false;
      _columnNames = null;
      _ncols = 0;
    }
    public ParserSetup(ParserType t, byte sep, boolean header, boolean hashHeader, boolean singleQuotes) {
      _pType = t;
      _separator = sep;
      _header = header || hashHeader;
      _hashHeader = hashHeader;
      _columnNames = null;
      _ncols = 0;
      _singleQuotes = singleQuotes;
    }
    public ParserSetup(ParserType t, byte sep, int ncolumns, boolean header, String [] columnNames, boolean singleQuotes) {
      _pType = t;
      _separator = sep;
      _ncols = ncolumns;
      _header = header;
      _hashHeader = false;
      _columnNames = columnNames;
      _singleQuotes = singleQuotes;
    }
    public boolean isSpecified(){
      return _pType != ParserType.AUTO && _separator != CsvParser.AUTO_SEP && (_header || _ncols > 0);
    }
    public Set<String> checkDupColumnNames(){
      HashSet<String> uniqueNames = new HashSet<String>();
      HashSet<String> conflictingNames = new HashSet<String>();
      if(_header){
        for(String n:_columnNames){
          if(!uniqueNames.contains(n)){
            uniqueNames.add(n);
          } else {
            conflictingNames.add(n);
          }
        }
      }
      return conflictingNames;
    }
    @Override public ParserSetup clone(){
      return new ParserSetup(_pType, _separator, _ncols,_header,null,_singleQuotes);
    }
    public boolean isCompatible(ParserSetup other){
      if(other == null || _pType != other._pType)return false;
      if(_pType == ParserType.CSV && (_separator != other._separator || _ncols != other._ncols))
        return false;
      if(_types == null) _types = other._types;
      else if(other._types != null){
        for(int i = 0; i < _types.length; ++i)
          _types[i].merge(other._types[i]);
      }
      return true;
    }
    public CustomParser parser(){
      switch(this._pType){
        case CSV:
          return new CsvParser(this);
        case SVMLight:
          return new SVMLightParser(this);
        case XLS:
          return new XlsParser(this);
        default:
          throw H2O.unimpl();
      }
    }
    @Override public String toString(){
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
  public DataOut parallelParse(int cidx, final DataIn din, final DataOut dout) {throw new UnsupportedOperationException();}
  public boolean parallelParseSupported(){return false;}

  public DataOut streamParse( final InputStream is, final DataOut dout) throws Exception {
    if(_setup._pType.parallelParseSupported){
      StreamData din = new StreamData(is);
      int cidx=0;
      while( is.available() > 0 )
        parallelParse(cidx++,din,dout);
      parallelParse(cidx++,din,dout);     // Parse the remaining partial 32K buffer
    } else {
      throw H2O.unimpl();
    }
    return dout;
  }
  // ------------------------------------------------------------------------
  // Zipped file; no parallel decompression; decompress into local chunks,
  // parse local chunks; distribute chunks later.
  public DataOut streamParse( final InputStream is, final StreamDataOut dout, ParseProgressMonitor pmon) throws IOException {
    // All output into a fresh pile of NewChunks, one per column
    if(_setup._pType.parallelParseSupported){
      StreamData din = new StreamData(is);
      int cidx=0;
      StreamDataOut nextChunk = dout;
      long lastProgress = pmon.progress();
      while( is.available() > 0 ){
        if (pmon.progress() > lastProgress) {
          lastProgress = pmon.progress();
          nextChunk.close();
          if(dout != nextChunk) dout.reduce(nextChunk);
          nextChunk = nextChunk.nextChunk();
        }
        parallelParse(cidx++,din,nextChunk);
      }
      parallelParse(cidx++,din,nextChunk);     // Parse the remaining partial 32K buffer
      nextChunk.close();
      if(dout != nextChunk)dout.reduce(nextChunk);
    } else {
      throw H2O.unimpl();
    }
    return dout;
  }
  protected static final boolean isWhitespace(byte c) {
    return (c == CHAR_SPACE) || (c == CHAR_TAB);
  }

  protected static final boolean isEOL(byte c) {
    return ((c == CHAR_LF) || (c == CHAR_CR));
  }
  public interface DataIn {
    // Get another chunk of byte data
    public abstract byte[] getChunkData( int cidx );
    public abstract int  getChunkDataStart( int cidx );
    public abstract void setChunkDataStart( int cidx, int offset );
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
    public void invalidLine(String err);
    public void invalidValue(int line, int col);
  }

  public interface StreamDataOut extends DataOut {
    StreamDataOut nextChunk();
    StreamDataOut reduce(StreamDataOut dout);
    StreamDataOut close();
    StreamDataOut close(Futures fs);
    int nChunks();
  }

  public static class StreamData implements CustomParser.DataIn {
    final transient InputStream _is;
    private byte[] _bits0 = new byte[2*1024*1024]; //allows for row lengths up to 2M
    private byte[] _bits1 = new byte[2*1024*1024];
    private int _cidx0=-1, _cidx1=-1; // Chunk #s
    private int _coff0=-1, _coff1=-1; // Last used byte in a chunk
    public StreamData(InputStream is){_is = is;}
    @Override public byte[] getChunkData(int cidx) {
      if(cidx == _cidx0)return _bits0;
      if(cidx == _cidx1)return _bits1;
      assert cidx==_cidx0+1 || cidx==_cidx1+1;
      byte[] bits = _cidx0<_cidx1 ? _bits0 : _bits1;
      if( _cidx0<_cidx1 ) { _cidx0 = cidx; _coff0 = -1; }
      else                { _cidx1 = cidx; _coff1 = -1; }
      // Read as much as the buffer will hold
      int off=0;
      try {
        while( off < bits.length ) {
          int len = _is.read(bits,off,bits.length-off);
          if( len == -1 ) break;
          off += len;
        }
        assert off == bits.length || _is.available() <= 0;
      } catch( IOException ioe ) {
        throw new RuntimeException(ioe);
      }
      if( off == bits.length ) return bits;
      // Final read is short; cache the short-read
      byte[] bits2 = (off == 0) ? null : Arrays.copyOf(bits,off);
      if( _cidx0==cidx ) _bits0 = bits2;
      else               _bits1 = bits2;
      return bits2;
    }
    @Override public int  getChunkDataStart(int cidx) {
      if( _cidx0 == cidx ) return _coff0;
      if( _cidx1 == cidx ) return _coff1;
      return 0;
    }
    @Override public void setChunkDataStart(int cidx, int offset) {
      if( _cidx0 == cidx ) _coff0 = offset;
      if( _cidx1 == cidx ) _coff1 = offset;
    }
  }
  public abstract CustomParser clone();
  public String [] headers(){return null;}


  protected static class TypeGuesserDataOut extends Iced implements DataOut {

    transient private HashSet<String> [] _domains;
    int [] _nnums;
    int [] _nstrings;
    int [] _nzeros;
    int _nlines = 0;
    final int _ncols;

    public TypeGuesserDataOut(int ncols){
      _ncols = ncols;
      _domains = new HashSet[ncols];
      _nzeros = new int[ncols];
      _nstrings = new int[ncols];
      _nnums = new int[ncols];
      for(int i = 0; i < ncols; ++i)
        _domains[i] = new HashSet<String>();
    }
    // TODO: ugly quick hack, needs revisit
    public ParserSetup.TypeInfo[] guessTypes() {
      ParserSetup.TypeInfo [] res = new ParserSetup.TypeInfo[_ncols];
      for(int i = 0; i < res.length; ++i)
        res[i] = new ParserSetup.TypeInfo();
      for(int i = 0; i < _ncols; ++i){
        if(_domains[i].size() <= 1) // only consider enums with multiple strings (otherwise it's probably garbage on NA)
          res[i]._type = ParserSetup.Coltype.NUM;
        else if(_nzeros[i] > 0 && (Math.abs(_nzeros[i] + _nstrings[i] - _nlines) <= 1)) { // enum with 0s for NAs
          res[i]._naStr = new ValueString("0");
          res[i]._type = ParserSetup.Coltype.STR;
          res[i]._strongGuess = true;
        } else if(_nstrings[i] >= STRING_DOMINANCE_RATIO*(_nnums[i]+_nzeros[i])) { // probably generic enum
          res[i]._type = ParserSetup.Coltype.STR;
        }
      }
      return res;
    }

    @Override
    public void setColumnNames(String[] names) {}

    @Override
    public void newLine() {
      ++_nlines;
    }

    @Override
    public boolean isString(int colIdx) {
      return false;
    }

    @Override
    public void addNumCol(int colIdx, long number, int exp) {
      if(colIdx < _nnums.length)
        if (number == 0)
          ++_nzeros[colIdx];
        else
          ++_nnums[colIdx];
    }

    @Override
    public void addNumCol(int colIdx, double d) {
      if(colIdx < _nnums.length)
        if (d == 0)
          ++_nzeros[colIdx];
        else
          ++_nnums[colIdx];
    }

    @Override
    public void addInvalidCol(int colIdx) {

    }

    @Override
    public void addStrCol(int colIdx, ValueString str) {
      if(colIdx < _nstrings.length) {
        ++_nstrings[colIdx];
        _domains[colIdx].add(str.toString());
      }
    }

    @Override
    public void rollbackLine() {--_nlines;}

    @Override
    public void invalidLine(String err) {}

    @Override
    public void invalidValue(int line, int col) {}
  }
  protected static class CustomInspectDataOut extends Iced implements DataOut {
    public int _nlines;
    public int _ncols;
    public int _invalidLines;
    public boolean _header;

    private String []   _colNames;
    private String [][] _data = new String[MAX_PREVIEW_LINES][MAX_PREVIEW_COLS];
    transient ArrayList<String> _errors;
    public CustomInspectDataOut() {
     for(int i = 0; i < MAX_PREVIEW_LINES;++i)
       Arrays.fill(_data[i],"NA");
    }
    public String [][] data(){
      String [][] res = Arrays.copyOf(_data, Math.min(MAX_PREVIEW_LINES, _nlines));
      for(int i = 0; i < res.length; ++i)
        res[i] = Arrays.copyOf(_data[i], Math.min(MAX_PREVIEW_COLS,_ncols));
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
      if(colIdx < _ncols && _nlines < MAX_PREVIEW_LINES)
        _data[_nlines][colIdx] = Double.toString(number*PrettyPrint.pow10(exp));
    }
    @Override public void addNumCol(int colIdx, double d) {
      if(colIdx < _ncols) {
        _ncols = Math.max(_ncols, colIdx);
        if (_nlines < MAX_PREVIEW_LINES && colIdx < MAX_PREVIEW_COLS)
          _data[_nlines][colIdx] = Double.toString(d);
      }
    }
    @Override public void addInvalidCol(int colIdx) {
      if(colIdx < _ncols && _nlines < MAX_PREVIEW_LINES)
        _data[_nlines][colIdx] = "NA";
    }
    @Override public void addStrCol(int colIdx, ValueString str) {
      if(colIdx < _ncols && _nlines < MAX_PREVIEW_LINES)
        _data[_nlines][colIdx] = str.toString();
    }
    @Override public void rollbackLine() {--_nlines;}
    @Override public void invalidLine(String err) {
      ++_invalidLines;
      _errors.add("Error at line: " + _nlines + ", reason: " + err);
    }
    @Override public void invalidValue(int linenum, int colnum) {}
  }

}



