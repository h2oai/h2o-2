package water.parser;

public class TextParser extends CustomParser {

  /* Constant to specify that separator is not specified. */
  public static final byte AUTO_SEP = -1;

  public final byte CHAR_DECIMAL_SEPARATOR = '.';
  public final byte CHAR_SEPARATOR;
  public static final byte HIVE_SEP = 1;

  public TextParser() {
    super(new ParserSetup(ParserType.TEXT));
    CHAR_SEPARATOR = ' '; //default to break on WS
  }

  public TextParser clone(){ return new TextParser(); }

  @Override public boolean parallelParseSupported(){return true;}

  // a bunch of characters to skip/strip out of the text stream
  boolean isSpecial(byte c) {
    return  c == '\"'   || c == '\''|| c == '\\' || c == '.'  || c == ' '  || c == '@'
            || c == '{' || c == '}' || c == '['  || c == ']'  || c == '('  || c == ';'
            || c == ')' || c == '_' || c == '&'  || c == '*'  || c == ','  || c == '^'
            || c == '|' || c == '!' || c == '?'  || c == '<'  || c == '>'  || c == '/'
            || c == '`' || c == '~' || c == '\t' || c == '+'  || c == '\n' || c == '\r'
            || c == '=' || c == '$' || c == '%'  || c == ':'  || c == '#';
  }

  @Override public final DataOut parallelParse(int cidx, final CustomParser.DataIn din, final CustomParser.DataOut dout) {
    ValueString _str = new ValueString();
    byte[] bits = din.getChunkData(cidx);
    byte[] bits0;
    if( bits == null ) return dout;
    int offset  = Math.max(0, din.getChunkDataStart(cidx)); // General cursor into the giant array of bytes
    // Starting state.  Are we skipping the first (partial) line, or not?  Skip
    // a header line, or a partial line if we're in the 2nd and later chunks.
    // If handed a skipping offset, then it points just past the prior partial line.
    int colIdx = -1; // must pass a colIdx to addStrCol unfortunately, so just pass -1
    byte c;
    while(offset < bits.length) {
      c = bits[offset++];
      // was parsing along and we hit some special char or end of bits, addString, restart.
      if ((isSpecial(c) || ( (offset -1) >= bits.length) )) {
        // only do this if we have some chars already!
        if (_str.get_length() > 0) {
          if (_str.get_length() > 1) {
            bits0 = new byte[_str.get_length()];
            for (int i = 0; i < _str.get_length(); ++i) bits0[i] = (byte)Character.toLowerCase(bits[ ((offset - 1) - _str.get_length()) + i]);
            _str.set(bits0, 0, _str.get_length());
            dout.addStrCol(colIdx, _str);    // add to the DataOut
          }
          _str = new ValueString();
        }
      // otherwise keep adding chars and continue
      } else _str.addChar();
    }
    return dout;
  }
}
