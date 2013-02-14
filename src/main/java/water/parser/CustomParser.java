package water.parser;

import water.Key;


public abstract class CustomParser {
  public enum Type { CSV, XLS, XLSX }
  
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

  public abstract void parse(Key key) throws Exception;

}
