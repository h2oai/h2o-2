package water.parser;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.*;
import water.fvec.ParseTime;
import water.util.Log;

public class CsvParser extends CustomParser {

  /* Constant to specify that separator is not specified. */
  public static final byte AUTO_SEP = -1;

  public final byte CHAR_DECIMAL_SEPARATOR = '.';
  public final byte CHAR_SEPARATOR;
  public static final byte HIVE_SEP = 1;

  private static final byte SKIP_LINE = 0;
  private static final byte EXPECT_COND_LF = 1;
  private static final byte EOL = 2;
  private static final byte TOKEN = 3;
  private static final byte COND_QUOTED_TOKEN = 4;
  private static final byte NUMBER = 5;
  private static final byte NUMBER_SKIP = 6;
  private static final byte NUMBER_SKIP_NO_DOT = 7;
  private static final byte NUMBER_FRACTION = 8;
  private static final byte NUMBER_EXP = 9;
  private static final byte NUMBER_EXP_NEGATIVE = 10;
  private static final byte NUMBER_EXP_START = 11;
  private static final byte NUMBER_END = 12;
  private static final byte STRING = 13;
  private static final byte COND_QUOTE = 14;
  private static final byte SEPARATOR_OR_EOL = 15;
  private static final byte WHITESPACE_BEFORE_TOKEN = 16;
  private static final byte STRING_END = 17;
  private static final byte COND_QUOTED_NUMBER_END = 18;
  private static final byte POSSIBLE_EMPTY_LINE = 19;
  private static final byte POSSIBLE_CURRENCY = 20;

  private static final long LARGEST_DIGIT_NUMBER = Long.MAX_VALUE/10;

  public CsvParser(ParserSetup setup) {
    super(setup);
    CHAR_SEPARATOR = setup._separator;
  }

  public CsvParser clone(){
    return new CsvParser(_setup == null?null:_setup.clone());
  }

  @Override public boolean parallelParseSupported(){return true;}

  @SuppressWarnings("fallthrough")
  @Override public final DataOut parallelParse(int cidx, final CustomParser.DataIn din, final CustomParser.DataOut dout) {
    ValueString _str = new ValueString();
    byte[] bits = din.getChunkData(cidx);
    if( bits == null ) return dout;
    int offset  = din.getChunkDataStart(cidx); // General cursor into the giant array of bytes
    final byte[] bits0 = bits;  // Bits for chunk0
    boolean firstChunk = true;  // Have not rolled into the 2nd chunk
    byte[] bits1 = null;        // Bits for chunk1, loaded lazily.
    // Starting state.  Are we skipping the first (partial) line, or not?  Skip
    // a header line, or a partial line if we're in the 2nd and later chunks.
    int state = (_setup._header || cidx > 0) ? SKIP_LINE : WHITESPACE_BEFORE_TOKEN;
    // If handed a skipping offset, then it points just past the prior partial line.
    if( offset >= 0 ) state = WHITESPACE_BEFORE_TOKEN;
    else offset = 0; // Else start skipping at the start
    int quotes = 0;
    long number = 0;
    int exp = 0;
    int sgn_exp = 1;
    boolean decimal = false;
    int fractionDigits = 0;
    int tokenStart = 0; // used for numeric token to backtrace if not successful
    int colIdx = 0;
    byte c = bits[offset];
    // skip comments for the first chunk (or if not a chunk)
    if( cidx == 0 ) {
      while (c == '#' || c == '@'/*also treat as comments leading '@' from ARFF format*/) {
        while ((offset   < bits.length) && (bits[offset] != CHAR_CR) && (bits[offset  ] != CHAR_LF)) ++offset;
        if    ((offset+1 < bits.length) && (bits[offset] == CHAR_CR) && (bits[offset+1] == CHAR_LF)) ++offset;
        ++offset;
        if (offset >= bits.length)
          return dout;
        c = bits[offset];
      }
    }
    dout.newLine();

MAIN_LOOP:
    while (true) {
NEXT_CHAR:
      switch (state) {
        // ---------------------------------------------------------------------
        case SKIP_LINE:
          if (isEOL(c)) {
            state = EOL;
          } else {
            break NEXT_CHAR;
          }
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case EXPECT_COND_LF:
          state = POSSIBLE_EMPTY_LINE;
          if (c == CHAR_LF)
            break NEXT_CHAR;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case STRING:
          if (c == quotes) {
            state = COND_QUOTE;
            break NEXT_CHAR;
          }
          if (!isEOL(c) && ((quotes != 0) || (c != CHAR_SEPARATOR))) {
            _str.addChar();
            break NEXT_CHAR;
          }
          // fallthrough to STRING_END
        // ---------------------------------------------------------------------
        case STRING_END:
          if ((c != CHAR_SEPARATOR) && (c == CHAR_SPACE))
            break NEXT_CHAR;
          // we have parsed the string enum correctly
          if((_str.get_off() + _str.get_length()) > _str.get_buf().length){ // crossing chunk boundary
            assert _str.get_buf() != bits;
            _str.addBuff(bits);
          }
          if(_setup._types != null && colIdx < _setup._types.length && _str.equals(_setup._types[colIdx]._naStr))
            dout.addInvalidCol(colIdx);
          else
            dout.addStrCol(colIdx, _str);
          _str.set(null, 0, 0);
          ++colIdx;
          state = SEPARATOR_OR_EOL;
          // fallthrough to SEPARATOR_OR_EOL
        // ---------------------------------------------------------------------
        case SEPARATOR_OR_EOL:
          if (c == CHAR_SEPARATOR) {
            state = WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          }
          if (c==CHAR_SPACE)
            break NEXT_CHAR;
          // fallthrough to EOL
        // ---------------------------------------------------------------------
        case EOL:
          if(quotes != 0){
            System.err.println("Unmatched quote char " + ((char)quotes) + " " + (((_str.get_length()+1) < offset && _str.get_off() > 0)?new String(Arrays.copyOfRange(bits,_str.get_off()-1,offset)):""));
            dout.invalidLine("Unmatched quote char " + ((char)quotes));
            colIdx = 0;
            quotes = 0;
          }else if (colIdx != 0) {
            dout.newLine();
            colIdx = 0;
          }
          state = (c == CHAR_CR) ? EXPECT_COND_LF : POSSIBLE_EMPTY_LINE;
          if( !firstChunk )
            break MAIN_LOOP; // second chunk only does the first row
          break NEXT_CHAR;
        // ---------------------------------------------------------------------
        case POSSIBLE_CURRENCY:
          if (((c >= '0') && (c <= '9')) || (c == '-') || (c == CHAR_DECIMAL_SEPARATOR) || (c == '+')) {
            state = TOKEN;
          } else {
            _str.set(bits,offset-1,0);
            _str.addChar();
            if (c == quotes) {
              state = COND_QUOTE;
              break NEXT_CHAR;
            }
            if ((quotes != 0) || ((!isEOL(c) && (c != CHAR_SEPARATOR)))) {
              state = STRING;
            } else {
              state = STRING_END;
            }
          }
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case POSSIBLE_EMPTY_LINE:
          if (isEOL(c)) {
            if (c == CHAR_CR)
              state = EXPECT_COND_LF;
            break NEXT_CHAR;
          }
          state = WHITESPACE_BEFORE_TOKEN;
          // fallthrough to WHITESPACE_BEFORE_TOKEN
        // ---------------------------------------------------------------------
        case WHITESPACE_BEFORE_TOKEN:
          if (c == CHAR_SPACE || (c == CHAR_TAB && CHAR_TAB!=CHAR_SEPARATOR)) {
              break NEXT_CHAR;
          } else if (c == CHAR_SEPARATOR) {
            // we have empty token, store as NaN
            dout.addInvalidCol(colIdx++);
            break NEXT_CHAR;
          } else if (isEOL(c)) {
            dout.addInvalidCol(colIdx++);
            state = EOL;
            continue MAIN_LOOP;
          }
          // fallthrough to COND_QUOTED_TOKEN
        // ---------------------------------------------------------------------
        case COND_QUOTED_TOKEN:
          state = TOKEN;
          if( CHAR_SEPARATOR!=HIVE_SEP && // Only allow quoting in CSV not Hive files
              ((_setup._singleQuotes && c == CHAR_SINGLE_QUOTE) || (c == CHAR_DOUBLE_QUOTE))) {
            assert (quotes == 0);
            quotes = c;
            break NEXT_CHAR;
          }
          // fallthrough to TOKEN
        // ---------------------------------------------------------------------
        case TOKEN:
          if(_setup._types != null && colIdx < _setup._types.length && _setup._types[colIdx]._type == ParserSetup.Coltype.STR){
            state = STRING; // Do not attempt a number parse, just do a string parse
            _str.set(bits, offset, 0);
            continue MAIN_LOOP;
          } else if (((c >= '0') && (c <= '9')) || (c == '-') || (c == CHAR_DECIMAL_SEPARATOR) || (c == '+')) {
            state = NUMBER;
            number = 0;
            fractionDigits = 0;
            decimal = false;
            tokenStart = offset;
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
          } else if (c == '$') {
            state = POSSIBLE_CURRENCY;
            break NEXT_CHAR;
          } else {
            state = STRING;
            _str.set(bits, offset, 0);
            continue MAIN_LOOP;
          }
          // fallthrough to NUMBER
        // ---------------------------------------------------------------------
        case NUMBER:
          if ((c >= '0') && (c <= '9')) {
            if (number >= LARGEST_DIGIT_NUMBER)  state = NUMBER_SKIP;
            else  number = (number*10)+(c-'0');
            break NEXT_CHAR;
          } else if (c == CHAR_DECIMAL_SEPARATOR) {
            state = NUMBER_FRACTION;
            fractionDigits = offset;
            decimal = true;
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            state = NUMBER_EXP_START;
            sgn_exp = 1;
            break NEXT_CHAR;
          }
          if (exp == -1) {
            number = -number;
          }
          exp = 0;
          // fallthrough to COND_QUOTED_NUMBER_END
        // ---------------------------------------------------------------------
        case COND_QUOTED_NUMBER_END:
          if ( c == quotes) {
            state = NUMBER_END;
            quotes = 0;
            break NEXT_CHAR;
          }
          // fallthrough NUMBER_END
        case NUMBER_END:
          if (c == CHAR_SEPARATOR && quotes == 0) {
            exp = exp - fractionDigits;
            dout.addNumCol(colIdx,number,exp);
            ++colIdx;
            // do separator state here too
            state = WHITESPACE_BEFORE_TOKEN;
            break NEXT_CHAR;
          } else if (isEOL(c)) {
            exp = exp - fractionDigits;
            dout.addNumCol(colIdx,number,exp);
            // do EOL here for speedup reasons
            colIdx = 0;
            dout.newLine();
            state = (c == CHAR_CR) ? EXPECT_COND_LF : POSSIBLE_EMPTY_LINE;
            if( !firstChunk )
              break MAIN_LOOP; // second chunk only does the first row
            break NEXT_CHAR;
          } else if ((c == '%')) {
            state = NUMBER_END;
            exp -= 2;
            break NEXT_CHAR;
          } else if ((c != CHAR_SEPARATOR) && ((c == CHAR_SPACE) || (c == CHAR_TAB))) {
            state = NUMBER_END;
            break NEXT_CHAR;
          } else {
            state = STRING;
            offset = tokenStart-1;
            _str.set(bits,tokenStart,0);
            break NEXT_CHAR; // parse as String token now
          }
        // ---------------------------------------------------------------------
        case NUMBER_SKIP:
          if ((c >= '0') && (c <= '9')) {
            exp++;
            break NEXT_CHAR;
          } else if (c == CHAR_DECIMAL_SEPARATOR) {
            state = NUMBER_SKIP_NO_DOT;
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            state = NUMBER_EXP_START;
            sgn_exp = 1;
            break NEXT_CHAR;
          }
          state = COND_QUOTED_NUMBER_END;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_SKIP_NO_DOT:
          if ((c >= '0') && (c <= '9')) {
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            state = NUMBER_EXP_START;
            sgn_exp = 1;
            break NEXT_CHAR;
          }
          state = COND_QUOTED_NUMBER_END;
          continue MAIN_LOOP;
        // ---------------------------------------------------------------------
        case NUMBER_FRACTION:
          if ((c >= '0') && (c <= '9')) {
            if (number >= LARGEST_DIGIT_NUMBER) {
              if (decimal)
                fractionDigits = offset - 1 - fractionDigits;
              if (exp == -1) {
                number = -number;
              }
              exp = 0;
              state = NUMBER_SKIP_NO_DOT;
            } else {
              number = (number*10)+(c-'0');
            }
            break NEXT_CHAR;
          } else if ((c == 'e') || (c == 'E')) {
            if (decimal)
              fractionDigits = offset - 1 - fractionDigits;
            state = NUMBER_EXP_START;
            sgn_exp = 1;
            break NEXT_CHAR;
          }
          state = COND_QUOTED_NUMBER_END;
          if (decimal)
            fractionDigits = offset - fractionDigits-1;
          if (exp == -1) {
            number = -number;
          }
          exp = 0;
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
            state = STRING;
            offset = tokenStart-1;
            _str.set(bits,tokenStart,0);
            break NEXT_CHAR; // parse as String token now
          }
          state = NUMBER_EXP;  // fall through to NUMBER_EXP
        // ---------------------------------------------------------------------
        case NUMBER_EXP:
          if ((c >= '0') && (c <= '9')) {
            exp = (exp*10)+(c-'0');
            break NEXT_CHAR;
          }
          exp *= sgn_exp;
          state = COND_QUOTED_NUMBER_END;
          continue MAIN_LOOP;

        // ---------------------------------------------------------------------
        case COND_QUOTE:
          if (c == quotes) {
            _str.addChar();
//            _str.skipChar();
            state = STRING;
            break NEXT_CHAR;
          } else {
            quotes = 0;
            state = STRING_END;
            continue MAIN_LOOP;
          }
        // ---------------------------------------------------------------------
        default:
          assert (false) : " We have wrong state "+state;
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
        if( firstChunk && bits1 == null )
          bits1 = din.getChunkData(cidx+1);
        // if we can't get further we might have been the last one and we must
        // commit the latest guy if we had one.
        if( !firstChunk || bits1 == null ) { // No more data available or allowed
          // If we are mid-parse of something, act like we saw a LF to end the
          // current token.
          if ((state != EXPECT_COND_LF) && (state != POSSIBLE_EMPTY_LINE)) {
            c = CHAR_LF;
            if (!firstChunk) Log.warn("Row entry exceeded " + bits.length + " bytes in size, exceeded current parse limit.");
            continue MAIN_LOOP;
          }
          break MAIN_LOOP;      // Else we are just done
        }

        // Now parsing in the 2nd chunk.  All offsets relative to the 2nd chunk start.
        firstChunk = false;
        if (state == NUMBER_FRACTION)
          fractionDigits -= bits.length;
        offset -= bits.length;
        tokenStart -= bits.length;
        bits = bits1;           // Set main parsing loop bits
        if( bits[0] == CHAR_LF && state == EXPECT_COND_LF )
          break MAIN_LOOP; // when the first character we see is a line end
      }
      c = bits[offset];
      if(isEOL(c) && state != COND_QUOTE && quotes != 0) // quoted string having newline character => fail the line!
        state = EOL;

    } // end MAIN_LOOP
    if (colIdx == 0)
      dout.rollbackLine();
    // If offset is still validly within the buffer, save it so the next pass
    // can start from there.
    if( offset+1 < bits.length ) {
      if( state == EXPECT_COND_LF && bits[offset+1] == CHAR_LF ) offset++;
      if( offset+1 < bits.length ) din.setChunkDataStart(cidx+1, offset+1 );
    }
    return dout;
  }

  // ==========================================================================
//  /** Setup of the parser.
//   *
//   * Simply holds the column names, their length also determines the number of
//   * columns, the separator used and whether the CSV file had a header or not.
//   */
//  public static class Setup extends Iced {
//    public final byte _separator;
//    public final boolean _header;
//    // Row zero is column names.
//    // Remaining rows are parsed from the given data, until we run out
//    // of data or hit some arbitrary display limit.
//    public final String[][] _data;
//    public final int _numlines;        // Number of lines parsed
//    public final byte[] _bits;  // The original bits
//
//    public Setup(byte separator, boolean header, String[][] data, int numlines, byte[] bits) {
//      _separator = separator;
//      _header = header;
//      _data = data;
//      _numlines = numlines;
//      _bits = bits;
//    }
//    public Setup(Setup S, boolean header) {
//      _separator = S._separator;
//      _header = header;
//      _data = S._data;
//      _numlines = S._numlines;
//      _bits = S._bits;
//    }
//
//    public int numCols(){return _data == null?-1:_data[0].length;}
//
//    @Override public boolean equals( Object o ) {
//      if( o == null || !(o instanceof Setup) ) return false;
//      if( o == this ) return true;
//      Setup s = (Setup)o;
//      // "Compatible" setups means same columns and same separators
//      return _separator == s._separator &&
//        ((_data==null && s._data==null) ||
//         (_data[0].length == s._data[0].length));
//      }
//    @Override public String toString() {
//      return "'"+(char)_separator+"' head="+_header+" cols="+(_data==null?-2:(_data[0]==null?-1:_data[0].length));
//    }
//  }

  /** Separators recognized by the parser.  You can add new separators to this
   *  list and the parser will automatically attempt to recognize them.  In
   *  case of doubt the separators are listed in descending order of
   *  probability, with space being the last one - space must always be the
   *  last one as it is used if all other fails because multiple spaces can be
   *  used as a single separator.
   */
  private static byte[] separators = new byte[] { HIVE_SEP/* '^A',  Hive table column separator */, ',', ';', '|', '\t',  ' '/*space is last in this list, because we allow multiple spaces*/ };

  /** Dermines the number of separators in given line. Correctly handles quoted
   * tokens.
   */
  private static int[] determineSeparatorCounts(String from, int single_quote) {
    int[] result = new int[separators.length];
    byte[] bits = from.getBytes();
    boolean in_quote = false;
    for( int j=0; j< bits.length; j++ ) {
      byte c = bits[j];
      if( (c == single_quote) || (c == CHAR_DOUBLE_QUOTE) )
        in_quote ^= true;
      if( !in_quote || c == HIVE_SEP )
        for( int i = 0; i < separators.length; ++i)
          if (c == separators[i])
            ++result[i];
    }
    return result;
  }

  /** Determines the tokens that are inside a line and returns them as strings
   *  in an array.  Assumes the given separator.
   */
  private static String[] determineTokens(String from, byte separator, int single_quote) {
    ArrayList<String> tokens = new ArrayList();
    byte[] bits = from.getBytes();
    int offset = 0;
    int quotes = 0;
    while (offset < bits.length) {
      while ((offset < bits.length) && (bits[offset] == CHAR_SPACE)) ++offset; // skip first whitespace
      if(offset == bits.length)break;
      StringBuilder t = new StringBuilder();
      byte c = bits[offset];
      if ((c == CHAR_DOUBLE_QUOTE) || (c == single_quote)) {
        quotes = c;
        ++offset;
      }
      while (offset < bits.length) {
        c = bits[offset];
        if ((c == quotes)) {
          ++offset;
          if ((offset < bits.length) && (bits[offset] == c)) {
            t.append((char)c);
            ++offset;
            continue;
          }
          quotes = 0;
        } else if ((quotes == 0) && ((c == separator) || (c == CHAR_CR) || (c == CHAR_LF))) {
          break;
        } else {
          t.append((char)c);
          ++offset;
        }
      }
      c = (offset == bits.length) ? CHAR_LF : bits[offset];
      tokens.add(t.toString());
      if ((c == CHAR_CR) || (c == CHAR_LF) || (offset == bits.length))
        break;
      if (c != separator)
        return new String[0]; // an error
      ++offset;               // Skip separator
    }
    // If we have trailing empty columns (split by seperators) such as ",,\n"
    // then we did not add the final (empty) column, so the column count will
    // be down by 1.  Add an extra empty column here
    if( bits[bits.length-1] == separator  && bits[bits.length-1] != CHAR_SPACE)
      tokens.add("");
    return tokens.toArray(new String[tokens.size()]);
  }

  private static boolean allStrings(String [] line){
    ValueString str = new ValueString();
    for( String s : line ) {
      try {
        Double.parseDouble(s);
        return false;       // Number in 1st row guesses: No Column Header
      } catch (NumberFormatException e) { /*Pass - determining if number is possible*/ }
      if( ParseTime.attemptTimeParse(str.setTo(s)) != Long.MIN_VALUE ) return false;
      ParseTime.attemptUUIDParse0(str.setTo(s));
      ParseTime.attemptUUIDParse1(str);
      if( str.get_off() != -1 ) return false; // Valid UUID parse
    }
    return true;
  }
  // simple heuristic to determine if we have headers:
  // return true iff the first line is all strings and second line has at least one number
  private static boolean hasHeader(String[] l1, String[] l2) {
    return allStrings(l1) && !allStrings(l2);
  }

  private static byte guessSeparator(String l1, String l2, int single_quote){
    int[] s1 = determineSeparatorCounts(l1, single_quote);
    int[] s2 = determineSeparatorCounts(l2, single_quote);
    // Now we have the counts - if both lines have the same number of separators
    // the we assume it is the separator.  Separators are ordered by their
    // likelyhoods.  
    int max = 0;
    for( int i = 0; i < s1.length; ++i ) {
      if( s1[i] == 0 ) continue;   // Separator does not appear; ignore it
      if( s1[max] < s1[i] ) max=i; // Largest count sep on 1st line
      if( s1[i] == s2[i] ) {       // Sep counts are equal?
        try {
          String[] t1 = determineTokens(l1, separators[i], single_quote);
          String[] t2 = determineTokens(l2, separators[i], single_quote);
          if( t1.length != s1[i]+1 || t2.length != s2[i]+1 )
            continue;           // Token parsing fails
          return separators[i];
        } catch (Exception e) { /*pass; try another parse attempt*/ }
      }
    }
    // No sep's appeared, or no sep's had equal counts on lines 1 & 2.  If no
    // separators have same counts, the largest one will be used as the default
    // one.  If there's no largest one, space will be used.
    if( s1[max]==0 ) max=separators.length-1; // Try last separator (space)
    if( s1[max]!=0 ) {
      String[] t1 = determineTokens(l1, separators[max], single_quote);
      String[] t2 = determineTokens(l2, separators[max], single_quote);
      if( t1.length == s1[max]+1 && t2.length == s2[max]+1 )
        return separators[max];
    }

    return AUTO_SEP;
  }


  private static int guessNcols(ParserSetup setup,String [][] data){
    int res = data[0].length;
    if(setup._header)return res;
    boolean samelen = true;     // True if all are same length
    boolean longest0 = true;    // True if no line is longer than 1st line
    for(String [] s:data) {
      samelen  &= (s.length == res);
      if( s.length > res ) longest0=false;
    }
    if(samelen)return res;      // All same length, take it
    if( longest0 ) return res;  // 1st line is longer than all the rest; take it

    // we don't have lines of same length, pick the most common length
    HashMap<Integer, Integer> lengths = new HashMap<Integer, Integer>();
    for(String [] s:data){
      if(!lengths.containsKey(s.length))lengths.put(s.length, 1);
      else
        lengths.put(s.length, lengths.get(s.length)+1);
    }
    int maxCnt = 0;
    for(Map.Entry<Integer, Integer> e:lengths.entrySet())
      if(e.getValue() > maxCnt){
        maxCnt = e.getValue();
        res = e.getKey();
      }
    return res;
  }

  /** Determines the CSV parser setup from the first two lines.  Also parses
   *  the next few lines, tossing out comments and blank lines.
   *
   *  A separator is given or it is selected if both two lines have the same ammount of them
   *  and the tokenization then returns same number of columns.
   */
  public static CustomParser.PSetupGuess guessSetup(byte[] bits) { return guessSetup(bits, new ParserSetup(ParserType.CSV),true); }
  public static CustomParser.PSetupGuess guessSetup(byte[] bits, ParserSetup setup){return guessSetup(bits,setup,false);}
  public static CustomParser.PSetupGuess guessSetup(byte[] bits, ParserSetup setup, boolean checkHeader) {
    ArrayList<String> lines = new ArrayList();
    int offset = 0;
    while (offset < bits.length && lines.size() < 10) {
      int lineStart = offset;
      while ((offset < bits.length) && (bits[offset] != CHAR_CR) && (bits[offset] != CHAR_LF)) ++offset;
      int lineEnd = offset;
      ++offset;
      if ((offset < bits.length) && (bits[offset] == CHAR_LF)) ++offset;
      if (bits[lineStart] == '#' && !setup._hashHeader) continue; // Ignore comment lines unless header set to start with a hash character
      if (bits[lineStart] == '@') continue; // Ignore ARFF comment lines
      if (lineEnd>lineStart){
        String str = new String(bits, lineStart,lineEnd-lineStart).trim();
        if(!str.isEmpty())lines.add(str);
      }
    }
    if(lines.isEmpty())
      return new PSetupGuess(new ParserSetup(ParserType.AUTO,CsvParser.AUTO_SEP,0,false,null,setup._singleQuotes),0,0,null,false,new String[]{"No data!"});
    boolean hasHeader = false;
    final int single_quote = setup._singleQuotes ? CHAR_SINGLE_QUOTE : -1;
    byte sep = setup._separator;
    final String [][] data = new String[lines.size()][];
    int ncols;
    if( lines.size() < 2 ) {
      if(sep == AUTO_SEP){
        if(lines.get(0).split(",").length > 2)
          sep = (byte)',';
        else if(lines.get(0).split(" ").length > 2)
          sep = ' ';
        else {
          data[0] = new String[]{lines.get(0)};
          return new PSetupGuess(new ParserSetup(ParserType.CSV,CsvParser.AUTO_SEP,1,false,null,setup._singleQuotes),lines.size(),0,data,false,new String[]{"Failed to guess separator."});
        }
      }
      if(lines.size() == 1)
        data[0] = determineTokens(lines.get(0), sep, single_quote);
      ncols = (setup._ncols > 0)?setup._ncols:data[0].length;
      hasHeader = (checkHeader && allStrings(data[0])) || setup._header;
    } else {
      if(setup._separator == AUTO_SEP){ // first guess the separator
        sep = guessSeparator(lines.get(0), lines.get(1), single_quote);
        if(sep == AUTO_SEP && lines.size() > 2){
          if(sep == AUTO_SEP)sep = guessSeparator(lines.get(1), lines.get(2), single_quote);
          if(sep == AUTO_SEP)sep = guessSeparator(lines.get(0), lines.get(2), single_quote);
        }
        if(sep == AUTO_SEP)sep = (byte)' ';
      }
      for(int i = 0; i < lines.size(); ++i)
        data[i] = determineTokens(lines.get(i), sep, single_quote);
      // we do not have enough lines to decide
      ncols = (setup._ncols > 0)?setup._ncols:guessNcols(setup,data);
      if(checkHeader){
        assert !setup._header;
        assert setup._columnNames == null;
        hasHeader = hasHeader(data[0],data[1]) && (data[0].length == ncols);
      } else if(setup._header){
        if(setup._columnNames != null){ // we know what the header looks like, check if the current file has matching header
          hasHeader = data[0].length == setup._columnNames.length;
          for(int i = 0; hasHeader && i < data[0].length; ++i)
            hasHeader = data[0][i].equalsIgnoreCase(setup._columnNames[i]);
        } else // otherwise we're told to take the first line as header whatever it might be
          hasHeader = true;
      }
    }
    ParserSetup resSetup = new ParserSetup(ParserType.CSV, sep, ncols,hasHeader, hasHeader?data[0]:null,setup._singleQuotes);
    ArrayList<String> errors = new ArrayList<String>();
    int ilines = 0;
    for(int i = 0; i < data.length; ++i){
      if(data[i].length != resSetup._ncols){
        errors.add("error at line " + i + " : incompatible line length. Got " + data[i].length + " columns.");
        ++ilines;
      }
    }
    String [] err = null;
    if(!errors.isEmpty()){
      err = new String[errors.size()];
      errors.toArray(err);
    }
    PSetupGuess res = new PSetupGuess(resSetup,lines.size()-ilines,ilines,data,setup.isSpecified() || lines.size() > ilines, err);
    if(res._isValid){ // now guess the types
      InputStream is = new ByteArrayInputStream(bits);
      CsvParser p = new CsvParser(res._setup);
      TypeGuesserDataOut dout = new TypeGuesserDataOut(res._setup._ncols);
      try{
        p.streamParse(is, dout);
        res._setup._types = dout.guessTypes();
      }catch(Throwable e){}
    }
    return res;
  }

  @Override public boolean isCompatible(CustomParser p) {
    return (p instanceof CsvParser) && p._setup._separator == _setup._separator && p._setup._ncols == _setup._ncols;
  }
}
