package water.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import water.Iced;
import water.PrettyPrint;

/**
 * Parser for SVM light format.
 * @author tomasnykodym
 *
 */
public class SVMLightParser extends CustomParser{
  private static final byte SKIP_LINE = 0;
  private static final byte EXPECT_COND_LF = 1;
  private static final byte EOL = 2;
  private static final byte TOKEN = 3;
  private static final byte SKIP_TOKEN = 4;
  private static final byte NUMBER = 5;
  private static final byte NUMBER_FRACTION = 6;
  private static final byte NUMBER_EXP = 7;
  private static final byte INVALID_NUMBER = 8;
  private static final byte NUMBER_EXP_START = 9;
  private static final byte NUMBER_END = 10;
  private static final byte WHITESPACE_BEFORE_TOKEN = 11;
  private static final byte POSSIBLE_EMPTY_LINE = 12;
  private static final byte QID0 = 13;
  private static final byte QID1 = 14;

  // line global states
  private static final int TGT = 1;
  private static final int COL = 2;
  private static final int VAL = 3;

  private static final long LARGEST_DIGIT_NUMBER = 1000000000000000000L;

  final static char DECIMAL_SEP = '.';

  public SVMLightParser(ParserSetup setup) {super(setup);}
  @Override
  public SVMLightParser clone(){return new SVMLightParser(_setup);}
  @Override
  public boolean parallelParseSupported(){return true;}

  /**
   * Try to parse the bytes as svm light format, return SVMParser instance if the input is in svm light format, null otherwise.
   * @param bytes
   * @return SVMLightPArser instance or null
   */
  public static PSetupGuess guessSetup(byte [] bytes){
    // find the last eof
    int i = bytes.length-1;
    while(i > 0 && bytes[i] != '\n')--i;
    assert i >= 0;
    InputStream is = new ByteArrayInputStream(Arrays.copyOf(bytes,i));
    SVMLightParser p = new SVMLightParser(new ParserSetup(ParserType.SVMLight, CsvParser.AUTO_SEP, false));
    InspectDataOut dout = new InspectDataOut();
    try{p.streamParse(is, dout);}catch(Exception e){throw new RuntimeException(e);}
    return new PSetupGuess(new ParserSetup(ParserType.SVMLight, CsvParser.AUTO_SEP, dout._ncols,false,null,false),dout._nlines,dout._invalidLines,dout.data(),dout._ncols > 0 && dout._nlines > 0 && dout._nlines > dout._invalidLines,dout.errors());
  }
  @Override
  public boolean isCompatible(CustomParser p){return p instanceof SVMLightParser;}
  @SuppressWarnings("fallthrough")
  @Override public final DataOut parallelParse(int cidx, final CustomParser.DataIn din, final CustomParser.DataOut dout) {
      ValueString _str = new ValueString();
      byte[] bits = din.getChunkData(cidx);
      if( bits == null ) return dout;
      final byte[] bits0 = bits;  // Bits for chunk0
      boolean firstChunk = true;  // Have not rolled into the 2nd chunk
      byte[] bits1 = null;        // Bits for chunk1, loaded lazily.
      int offset = 0;             // General cursor into the giant array of bytes
      // Starting state.  Are we skipping the first (partial) line, or not?  Skip
      // a header line, or a partial line if we're in the 2nd and later chunks.
      int lstate = (cidx > 0)? SKIP_LINE : WHITESPACE_BEFORE_TOKEN;
      int gstate = TGT;
      long number = 0;
      int zeros = 0;
      int exp = 0;
      int sgn_exp = 1;
      boolean decimal = false;
      int fractionDigits = 0;
      int colIdx = 0;
      byte c = bits[offset];
      // skip comments for the first chunk (or if not a chunk)
      if( cidx == 0 ) {
        while (c == '#') {
          while ((offset   < bits.length) && (bits[offset] != CHAR_CR) && (bits[offset  ] != CHAR_LF)) ++offset;
          if    ((offset+1 < bits.length) && (bits[offset] == CHAR_CR) && (bits[offset+1] == CHAR_LF)) ++offset;
          ++offset;
          if (offset >= bits.length)
            return dout;
          c = bits[offset];
        }
      }
      //dout.newLine();
      int linestart = 0;
//      String linePrefix = "";
  MAIN_LOOP:
      while (true) {
  NEXT_CHAR:
        switch (lstate) {
          // ---------------------------------------------------------------------
          case SKIP_LINE:
            if (!isEOL(c))
              break NEXT_CHAR;
            // fall through
          case EOL:
            if (colIdx != 0) {
              colIdx = 0;
              linestart = offset+1;
              if(lstate != SKIP_LINE)
                dout.newLine();
            }
            if( !firstChunk )
              break MAIN_LOOP; // second chunk only does the first row
            lstate = (c == CHAR_CR) ? EXPECT_COND_LF : POSSIBLE_EMPTY_LINE;
            gstate = TGT;
            linestart = offset;
            break NEXT_CHAR;
          // ---------------------------------------------------------------------
          case EXPECT_COND_LF:
            lstate = POSSIBLE_EMPTY_LINE;
            if (c == CHAR_LF)
              break NEXT_CHAR;
            continue MAIN_LOOP;
          // ---------------------------------------------------------------------

          // ---------------------------------------------------------------------

          // ---------------------------------------------------------------------
          case POSSIBLE_EMPTY_LINE:
            if (isEOL(c)) {
              if (c == CHAR_CR)
                lstate = EXPECT_COND_LF;
              break NEXT_CHAR;
            }
            lstate = WHITESPACE_BEFORE_TOKEN;
            // fallthrough to WHITESPACE_BEFORE_TOKEN
          // ---------------------------------------------------------------------
          case WHITESPACE_BEFORE_TOKEN:
            if (isWhitespace(c))
                break NEXT_CHAR;
            if (isEOL(c)){
              lstate = EOL;
              continue MAIN_LOOP;
            }
          // fallthrough to TOKEN
          case TOKEN:
            if (((c >= '0') && (c <= '9')) || (c == '-') || (c == DECIMAL_SEP) || (c == '+')) {
              lstate = NUMBER;
              number = 0;
              fractionDigits = 0;
              decimal = false;

              if (c == '-') {
                exp = -1;
                break NEXT_CHAR;
              } else if(c == '+'){
                exp = 1;
                break NEXT_CHAR;
              } else {
                exp = 1;
              }
              // fallthrough
            } else if(c == 'q'){
              lstate = QID0;
            } else { // failed, skip the line
              // TODO
              dout.invalidLine("Unexpected character, expected number or qid, got '" + new String(Arrays.copyOfRange(bits, offset,Math.min(bits.length,offset+5))) + "...'");
              lstate = SKIP_LINE;
              continue MAIN_LOOP;
            }
            // fallthrough to NUMBER
          // ---------------------------------------------------------------------
          case NUMBER:
            if ((c >= '0') && (c <= '9')) {
              number = (number*10)+(c-'0');
              if (number >= LARGEST_DIGIT_NUMBER)
                lstate = INVALID_NUMBER;
              break NEXT_CHAR;
            } else if (c == DECIMAL_SEP) {
              lstate = NUMBER_FRACTION;
              fractionDigits = offset;
              decimal = true;
              break NEXT_CHAR;
            } else if ((c == 'e') || (c == 'E')) {
              lstate = NUMBER_EXP_START;
              sgn_exp = 1;
              break NEXT_CHAR;
            }
            if (exp == -1) {
              number = -number;
            }
            exp = 0;
            // fallthrough NUMBER_END
          case NUMBER_END:
            exp = exp - fractionDigits;
            switch(gstate){
              case COL:
                if(c == ':'){
                  if(exp == 0 && number >= colIdx && (int)number == number){
                    colIdx = (int)number;
                    gstate = VAL;
                    lstate = WHITESPACE_BEFORE_TOKEN;
                  } else {
                    // wrong col Idx, just skip the token and try to continue
                    // col idx is either too small (according to spec, cols must come in strictly increasing order)
                    // or too small (col ids currently must fit into int)
                    String err = "";
                    if(number <= colIdx)
                      err = "Columns come in non-increasing sequence. Got " + number + " after " + colIdx + ".";
                    else if(exp != 0)
                      err = "Got non-integer as column id: " + number*PrettyPrint.pow10(exp);
                    else
                      err = "column index out of range, " + number + " does not fit into integer.";
                    dout.invalidLine("invalid column id:" + err);
                    lstate = SKIP_LINE;
                  }
                } else { // we're probably out of sync, skip the rest of the line
                  dout.invalidLine("unexpected character after column id: " + c);
                  lstate = SKIP_LINE;
                  // TODO output error
                }
                break NEXT_CHAR;
              case TGT:
              case VAL:
                dout.addNumCol(colIdx++,number,exp);
                lstate = WHITESPACE_BEFORE_TOKEN;
                gstate = COL;
                continue MAIN_LOOP;
            }
          // ---------------------------------------------------------------------
          case NUMBER_FRACTION:
            if(c == '0'){
              ++zeros;
              break NEXT_CHAR;
            }
            if ((c > '0') && (c <= '9')) {
              if (number < LARGEST_DIGIT_NUMBER) {
                number = (number*PrettyPrint.pow10i(zeros+1))+(c-'0');
              } else {
                dout.invalidLine("number " + number + " is out of bounds.");
                lstate = SKIP_LINE;
              }
              zeros = 0;
              break NEXT_CHAR;
            } else if ((c == 'e') || (c == 'E')) {
              if (decimal)
                fractionDigits = offset - zeros - 1 - fractionDigits;
              lstate = NUMBER_EXP_START;
              sgn_exp = 1;
              zeros = 0;
              break NEXT_CHAR;
            }
            lstate = NUMBER_END;
            if (decimal)
              fractionDigits = offset - zeros - fractionDigits-1;
            if (exp == -1) {
              number = -number;
            }
            exp = 0;
            zeros = 0;
            continue MAIN_LOOP;
          // ---------------------------------------------------------------------
          case NUMBER_EXP_START:
            if (exp == -1) {
              number = -number;
            }
            exp = 0;
            if (c == '-') {
              sgn_exp *= -1;
              break NEXT_CHAR;
            } else if (c == '+'){
              break NEXT_CHAR;
            }
            if ((c < '0') || (c > '9')){
              lstate = INVALID_NUMBER;
              continue MAIN_LOOP;
            }
            lstate = NUMBER_EXP;  // fall through to NUMBER_EXP
          // ---------------------------------------------------------------------
          case NUMBER_EXP:
            if ((c >= '0') && (c <= '9')) {
              exp = (exp*10)+(c-'0');
              break NEXT_CHAR;
            }
            exp *= sgn_exp;
            lstate = NUMBER_END;
            continue MAIN_LOOP;
          // ---------------------------------------------------------------------
          case INVALID_NUMBER:
            if(gstate == TGT) { // invalid tgt -> skip the whole row
              lstate = SKIP_LINE;
              dout.invalidLine("invalid number (expecting target)");
              continue MAIN_LOOP;
            }
            if(gstate == VAL){ // add invalid value and skip until whitespace or eol
              dout.addInvalidCol(colIdx++);
              gstate = COL;
            }
          case QID0:
            if(c == 'i'){
              lstate = QID1;
              break NEXT_CHAR;
            } else {
              lstate = SKIP_TOKEN;
              break NEXT_CHAR;
            }
          case QID1:
            if(c == 'd'){
              lstate = SKIP_TOKEN; // skip qid for now
              break NEXT_CHAR;
            } else {
              // TODO report an error
              lstate = SKIP_TOKEN;;
              break NEXT_CHAR;
            }
            // fall through
          case SKIP_TOKEN:
            if(isEOL(c))
              lstate = EOL;
            else if(isWhitespace(c))
              lstate = WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          default:
            assert (false) : " We have wrong state "+lstate;
        } // end NEXT_CHAR
        ++offset; // do not need to adjust for offset increase here - the offset is set to tokenStart-1!
        if (offset < 0) {         // Offset is negative?
          assert !firstChunk;     // Caused by backing up from 2nd chunk into 1st chunk
          firstChunk = true;
          bits = bits0;
          offset += bits.length;
          _str.set(bits,offset,0);
        } else if (offset >= bits.length) { // Off end of 1st chunk?  Parse into 2nd chunk
          // Attempt to get more data.
          if( firstChunk && bits1 == null ){
            bits1 = din.getChunkData(cidx+1);
//            linePrefix = new String(Arrays.copyOfRange(bits, linestart, bits.length));
            linestart = 0;
          }
          // if we can't get further we might have been the last one and we must
          // commit the latest guy if we had one.
          if( !firstChunk || bits1 == null ) { // No more data available or allowed
            // If we are mid-parse of something, act like we saw a LF to end the
            // current token.
            if ((lstate != EXPECT_COND_LF) && (lstate != POSSIBLE_EMPTY_LINE)) {
              c = CHAR_LF;  continue MAIN_LOOP;
            }
            break MAIN_LOOP;      // Else we are just done
          }
          // Now parsing in the 2nd chunk.  All offsets relative to the 2nd chunk start.
          firstChunk = false;
          if (lstate == NUMBER_FRACTION)
            fractionDigits -= bits.length;
          offset -= bits.length;
          bits = bits1;           // Set main parsing loop bits
          if( bits[0] == CHAR_LF && lstate == EXPECT_COND_LF )
            break MAIN_LOOP; // when the first character we see is a line end
        }
        c = bits[offset];
      } // end MAIN_LOOP
      return dout;
  }
  private static class InspectDataOut extends Iced implements DataOut {
    public int _nlines;
    public int _ncols;
    public int _invalidLines;
    public final static int MAX_COLS = 100;
    public final static int MAX_LINES = 10;
    private String [][] _data = new String[MAX_LINES][MAX_COLS];
    transient ArrayList<String> _errors = new ArrayList<String>();
    public InspectDataOut() {
     for(int i = 0; i < MAX_LINES;++i)
       Arrays.fill(_data[i],"0");
    }
    public String [][] data(){
      if(_data.length <= _nlines && _data[0].length <= _ncols)
        return _data;
      String [][] res = Arrays.copyOf(_data, Math.min(MAX_LINES, _nlines));
      for(int i = 0; i < res.length; ++i)
        res[i] = Arrays.copyOf(_data[i], Math.min(MAX_COLS,_ncols));
      return (_data = res);
    }
    @Override public void setColumnNames(String[] names) {}
    @Override public void newLine() {
      ++_nlines;
    }
    @Override public boolean isString(int colIdx) {return false;}
    @Override public void addNumCol(int colIdx, long number, int exp) {
      _ncols = Math.max(_ncols,colIdx);
      if(colIdx < MAX_COLS && _nlines < MAX_LINES)
        _data[_nlines][colIdx] = Double.toString(number*PrettyPrint.pow10(exp));
    }
    @Override public void addNumCol(int colIdx, double d) {
      _ncols = Math.max(_ncols,colIdx);
      if(colIdx < MAX_COLS)
        _data[_nlines][colIdx] = Double.toString(d);
    }
    @Override public void addInvalidCol(int colIdx) {}
    @Override public void addStrCol(int colIdx, ValueString str) {}
    @Override public void rollbackLine() {--_nlines;}
    @Override public void invalidLine(String error) {
      ++_invalidLines;
      if(_errors.size() < 10)
        _errors.add("error at line " + (_nlines +_invalidLines) + ", cause: " + error);
    }
    @Override public void invalidValue(int linenum, int colnum) {}
    public String [] errors(){
      String [] res = new String[_errors.size()];
      return _errors.toArray(res);
    }

  }
}
