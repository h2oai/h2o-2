package water.api;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Pattern;

import water.*;
import water.fvec.*;
import water.parser.*;

public class Parse2 extends Request {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Parses a key to H2O's Frame format";

  // HTTP request parameters

  @API(help="Field separator, typically commas ',' or TABs.")
  final Separator separator = new Separator("separator");

  @API(help="Key (or regex of keys) to ignore.")
  final Str exclude = new Str("exclude", "");

  @API(help="An existing H2O CSV text key (or regex of keys).")
  final ExistingCSVKey source_key = new ExistingCSVKey("source_key");

  @API(help="Destination key.")
  final NewH2OHexKey dst_key = new NewH2OHexKey("dst");

  @API(help="If checked, first data row is assumed to be a header.  If unchecked, first data row is assumed to be data.")
  final Header header = new Header("header");

  // JSON output fields
  @API(help="Destination key.")
  String destination_key;

  @API(help="Job key, useful to query for progress.")
  String job;

  @API(help="Web page to redirect to, once the job is done")
  final String redirect="Inspect2";

  //@Override public String[] DocExampleSucc() { return new String[]{ "source_key","./smalldata/logreg/prostate.cvs" }; }
  @Override public String[] DocExampleFail() { return new String[]{ "source_key","./aMispeltFile" }; }

  public Parse2() {
    exclude.setRefreshOnChange();
  }

  private static class PSetup {
    final ArrayList<Key> _keys;
    final CustomParser.ParserSetup _setup;
    PSetup( ArrayList<Key> keys, CustomParser.ParserSetup setup) { _keys=keys; _setup=setup; }
    PSetup( Key key, CustomParser.ParserSetup setup) {
      _keys = new ArrayList();
      _keys.add(key);
      _setup=setup;
    }
  };

  // An H2O Key Query, which runs the basic CSV parsing heuristics.  Accepts
  // Key wildcards, and gathers all matching Keys for simultaneous parsing.
  // Multi-key parses are only allowed on compatible CSV files, and only 1 is
  // allowed to have headers.
  public class ExistingCSVKey extends TypeaheadInputText<PSetup> {
    public ExistingCSVKey(String name) { super(TypeaheadKeysRequest.class, name, true); }

    @Override protected PSetup parse(String input) throws IllegalArgumentException {
      Key k1 = Key.make(input);
      Value v1 = DKV.get(k1);
      Pattern incl = makePattern(input);
      Pattern excl = null;
      if(exclude.specified())
        excl = makePattern(exclude.value());

      ArrayList<Key> keys = new ArrayList();
      for( Key key : H2O.keySet() ) { // For all keys
        if( !key.user_allowed() ) continue;
        String ks = key.toString();
        if( !incl.matcher(ks).matches() ) // Ignore non-matching keys
          continue;
        if(excl != null && excl.matcher(ks).matches())
          continue;
        Value v2 = DKV.get(key);  // Look at it
        if(v2.isHex())// filter common mistake such as *filename* with filename.hex already present
          continue;
        Object o = v2.type() != TypeMap.PRIM_B ? v2.get() : null;
        if(o instanceof Frame && ((Frame) o)._vecs[0] instanceof ByteVec)
          keys.add(key);
      }

      if(keys.size() == 0 )
        throw new IllegalArgumentException(errors()[0]);
      Collections.sort(keys);   // Sort all the keys, except the 1 header guy
      // now we assume the first key has the header
      Key hKey = keys.get(0);
      Value v = DKV.get(hKey);
      v = ((Frame) v.get())._vecs[0].chunkIdx(0);
      byte sep = separator.specified() ? separator.value() : CsvParser.NO_SEPARATOR;
      CustomParser.ParserSetup setup = ParseDataset.guessSetup(v, CustomParser.ParserType.AUTO, sep);
      if( setup._data == null || setup._data[0].length == 0 )
        throw new IllegalArgumentException(errors()[1]+hKey);
      return new PSetup(keys,setup);
    }

    private Pattern makePattern(String input) {
      // Reg-Ex pattern match all keys, like file-globbing.
      // File-globbing: '?' allows an optional single character, regex needs '.?'
      // File-globbing: '*' allows any characters, regex needs '*?'
      // File-globbing: '\' is normal character in windows, regex needs '\\'
      String patternStr = input.replace("?",".?").replace("*",".*?").replace("\\","\\\\").replace("(","\\(").replace(")","\\)");
      Pattern p = Pattern.compile(patternStr);
      return p;
    }

    @Override protected PSetup defaultValue() { return null; }
    @Override protected String queryDescription() { return "Source CSV key"; }
    @Override protected String[] errors() { return new String[] {
        "I did not find any keys matching this pattern!",
        "I cannot figure out this file; I only handle common CSV formats: "
      }; }
  }

  // A Query String, which defaults to the source Key with a '.hex' suffix
  private class NewH2OHexKey extends Str {
    NewH2OHexKey(String name) {
      super(name,null/*not required flag*/);
      addPrerequisite(source_key);
    }
    @Override protected String defaultValue() {
      PSetup setup = source_key.value();
      if( setup == null ) return null;
      String n = setup._keys.get(0).toString();
      int dot = n.lastIndexOf('.'); // Peel off common .csv or .csv.gz suffix
      if( dot > 0 && n.lastIndexOf(File.separator) < dot )
        n = n.substring(0, dot);
      dot = n.lastIndexOf('.'); // Peel off common .csv.gz suffix
      if( dot > 0 && n.lastIndexOf(File.separator) < dot )
        n = n.substring(0, dot);
      int i = 0;
      String res = n + Extensions.HEX;
      Key k = Key.make(res);
      while(DKV.get(k) != null)
        k = Key.make(res = n + ++i + Extensions.HEX);
      return res;
    }
    @Override protected String queryDescription() { return "Destination hex key"; }
  }

  // A Query Bool, which includes a pretty HTML-ized version of the first few
  // parsed data rows.  If the value() is TRUE, we display as-if the first row
  // is a label/header column, and if FALSE not.
  public class Header extends Bool {
    Header(String name) {
      super(name, false, "First row is column headers?");
      addPrerequisite(source_key);
      setRefreshOnChange();
    }
    @Override protected String queryElement() {
      // first determine the value to put in the field
      Record record = record();
      String value = record._originalValue;
      // if no original value was supplied, use the provided one
      PSetup psetup = source_key.value();
      if (value == null)
        value = psetup._setup._header ? "1" : "";
      StringBuilder sb = new StringBuilder();
      sb.append("<input value='1' class='span5' type='checkbox' ");
      sb.append("name='").append(_name).append("' ");
      sb.append("id='").append(_name).append("' ");
      if( value.equals("1") ) sb.append("checked");
      sb.append("/>&nbsp;&nbsp;").append(queryDescription()).append("<p>");
      String[][] data = psetup._setup._data;
      if( data != null ) {
        sb.append("<div class='alert'><b>");
        sb.append(String.format("Detected %s ",psetup._setup.toString()));
        sb.append("</b></div>");
        sb.append("<table class='table table-striped table-bordered'>");
        int j=psetup._setup._header?0:1; // Skip auto-gen header in data[0]
        if( value.equals("1") ) { // Obvious header display, if asked for
          sb.append("<tr><th>Row#</th>");
          for( String s : data[j++] ) sb.append("<th>").append(s).append("</th>");
          sb.append("</tr>");
        }
        for( int i=j; i<data.length; i++ ) { // The first few rows
          sb.append("<tr><td>Row ").append(i-j).append("</td>");
          for( String s : data[i] ) sb.append("<td>").append(s).append("</td>");
          sb.append("</tr>");
        }
        sb.append("</table>");
      }
      return sb.toString();
    }
  }

  //public static String link(Key k, String content) {
  //  return link(k.toString(),content);
  //}
  //public static String link(String k, String content) {
  //  RString rs = new RString("<a href='Parse2.query?%key_param=%$key'>%content</a>");
  //  rs.replace("key_param", "");
  //  rs.replace("key", k.toString());
  //  rs.replace("content", content);
  //  return rs.toString();
  //}

  @Override protected Response serve() {
    PSetup p = source_key.value();
    CustomParser.ParserSetup setup = p._setup;
    Key d = Key.make(dst_key.value());
    try {
      // Make a new Setup, with the 'header' flag set according to user wishes.
      if(header.originalValue() != null) // No user wish?
         setup._header = header.value();
      Key[] keys = p._keys.toArray(new Key[p._keys.size()]);
      Key jobkey = ParseDataset2.forkParseDataset(d, keys, setup).job_key;
      job = jobkey.toString();
      destination_key = d.toString();

      return Progress2.redirect(this,jobkey,d);
    } catch (IllegalArgumentException e) {
      return Response.error(e.getMessage());
    } catch (Error e) {
      return Response.error(e.getMessage());
    }
  }

  private class Separator extends InputSelect<Byte> {
    public Separator(String name) {
      super(name,false);
      setRefreshOnChange();
    }

    @Override protected String   queryDescription() { return "ASCII character"; }
    @Override protected String[] selectValues()     { return DEFAULT_IDX_DELIMS;   }
    @Override protected String[] selectNames()      { return DEFAULT_DELIMS; }
    @Override protected Byte     defaultValue()     { return -1;             }
    @Override protected String   selectedItemValue(){ return value() != null ? value().toString() : defaultValue().toString(); }
    @Override protected Byte parse(String input) { return Byte.valueOf(input); }
  }

  /** List of white space delimiters */
  static final String[] WHITE_DELIMS = { "NULL", "SOH (start of heading)", "STX (start of text)", "ETX (end of text)", "EOT (end of transmission)",
    "ENQ (enquiry)", "ACK (acknowledge)", "BEL '\\a' (bell)", "BS '\b' (backspace)", "HT  '\\t' (horizontal tab)", "LF  '\\n' (new line)", " VT '\\v' (vertical tab)",
    "FF '\\f' (form feed)", "CR '\\r' (carriage ret)", "SO  (shift out)", "SI  (shift in)", "DLE (data link escape)", "DC1 (device control 1) ", "DC2 (device control 2)",
    "DC3 (device control 3)", "DC4 (device control 4)", "NAK (negative ack.)", "SYN (synchronous idle)", "ETB (end of trans. blk)", "CAN (cancel)", "EM  (end of medium)",
    "SUB (substitute)", "ESC (escape)", "FS  (file separator)", "GS  (group separator)", "RS  (record separator)", "US  (unit separator)", "' ' SPACE" };
  /** List of all ASCII delimiters */
  static final String[] DEFAULT_DELIMS     = new String[127];
  static final String[] DEFAULT_IDX_DELIMS = new String[127];
  static {
    int i = 0;
    for (i = 0; i < WHITE_DELIMS.length; i++) DEFAULT_DELIMS[i] = String.format("%s: '%02d'", WHITE_DELIMS[i],i);
    for (;i < 126; i++) {
      String s = null; // Escape HTML entities manually or use StringEscapeUtils from Apache
      switch ((char)i) {
        case '&': s = "&amp;"; break;
        case '<': s = "&lt;";  break;
        case '>': s = "&gt;";  break;
        case '\"': s = "&quot;"; break;
        default : s = Character.toString((char)i);
      }
      DEFAULT_DELIMS[i] = String.format("%s: '%02d'", s, i);
    }
    for (i = 0; i < 126; i++) DEFAULT_IDX_DELIMS[i] = String.valueOf(i);
    DEFAULT_DELIMS[i]     = "Guess separator ...";
    DEFAULT_IDX_DELIMS[i] = String.valueOf(CsvParser.NO_SEPARATOR);
  };
}
