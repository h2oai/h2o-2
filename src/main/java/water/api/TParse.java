package water.api;

import water.DKV;
import water.H2O;
import water.Job;
import water.Key;
import water.parser.CsvParser;
import water.util.RString;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Pattern;

public class TParse extends Request {

  protected final Bool           _sQuotes   = new Bool("single_quotes",false,"Enable single quotes as field quotation character");
  protected final Str            _excludeExpression    = new Str("exclude","");
  protected final ExistingTXTKey _source    = new ExistingTXTKey(SOURCE_KEY);
  protected final NewH2OHexKey   _dest      = new NewH2OHexKey(DEST_KEY);
  protected final Bool           _blocking  = new Bool("blocking",false,"Synchronously wait until parse completes");

  public TParse() {
    _excludeExpression.setRefreshOnChange();
    _blocking._hideInQuery = true;
  }

  @Override protected Response serve() {
    TPSetup p = _source.value();
    Key destination_key = Key.make(_dest.value());
    try {
      // Make a new Setup, with the 'header' flag set according to user wishes.
      Key[] keys = p._keys.toArray(new Key[p._keys.size()]);
      Job parseJob = water.fvec.TParse.forkParseDataset(destination_key, keys, true);
//      job_key = parseJob.self();
      // Allow the user to specify whether to block synchronously for a response or not.
//      if (_blocking.value()) {
//        parseJob.get(); // block until the end of job
//        assert Job.isEnded(job_key) : "Job is still running but we already passed over its end. Job = " + job_key;
//      }
      return Response.done(this);
    } catch( Throwable e) {
      return Response.error(e);
    }
  }

  protected static class TPSetup {
    final transient ArrayList<Key> _keys;
    final transient Key [] _failedKeys;
    TPSetup(ArrayList<Key> keys, Key[] fkeys) { _keys=keys; _failedKeys = fkeys; }
  }

  // Query to parse text data. Still called ExistingCSVKey
  public class ExistingTXTKey extends TypeaheadInputText<TPSetup> {
    public ExistingTXTKey(String name) {
      super(TypeaheadKeysRequest.class, name, true);
    }

    @Override protected TPSetup parse(String input) throws IllegalArgumentException {
      final Pattern p = makePattern(input);
      final Pattern exclude;
      exclude = _excludeExpression.specified()?makePattern(_excludeExpression.value()):null;

      // boolean badkeys = false;
      final Key [] keyAry = H2O.KeySnapshot.globalSnapshot().filter(new H2O.KVFilter() {
        @Override
        public boolean filter(H2O.KeyInfo k) {
          if(k._rawData && k._nrows > 0) {
            String ks = k._key.toString();
            return (p.matcher(ks).matches() && (exclude == null || !exclude.matcher(ks).matches()));
          }
          return false;
        }
      }).keys();
      ArrayList<Key> keys = new ArrayList<Key>(keyAry.length);
      Collections.addAll(keys, keyAry);
      return new TPSetup(keys,null);  //TODO: Detect failed keys...
    }

    private String keyRow(Key k){ return "<tr><td>" + k + "</td></tr>\n"; }

    @Override
    public String queryComment(){
      if(!specified())return "";
      TPSetup p = value();
      StringBuilder sb = new StringBuilder();
      if(p._keys.size() <= 10){
        for(Key k:p._keys)
          sb.append(keyRow(k));
      } else {
        int n = p._keys.size();
        for(int i = 0; i < 5; ++i)
          sb.append(keyRow(p._keys.get(i)));
        sb.append("<tr><td>...</td></tr>\n");
        for(int i = 5; i > 0; --i)
          sb.append(keyRow(p._keys.get(n-i)));
      }
      return
              "<div class='alert'><b> Found " + p._keys.size() +  " files matching the expression.</b><br/>\n" +
                      "<table>\n" +
                      sb.toString() +
                      "</table></div>";
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

    @Override protected TPSetup defaultValue() { return null; }
    @Override protected String queryDescription() { return "An existing H2O key (or regex of keys) of CSV text"; }
  }


  // A Query String, which defaults to the source Key with a '.hex' suffix
  protected class NewH2OHexKey extends Str {
    NewH2OHexKey(String name) {
      super(name,null/*not required flag*/);
      addPrerequisite(_source);
    }
    @Override protected String defaultValue() {
      TPSetup setup = _source.value();
      if( setup == null ) return null;
      String n = setup._keys.get(0).toString();
      // blahblahblah/myName.ext ==> myName
      int sep = n.lastIndexOf(File.separatorChar);
      if( sep > 0 ) n = n.substring(sep+1);
      int dot = n.lastIndexOf('.');
      if( dot > 0 ) n = n.substring(0, dot);
      if( !Character.isJavaIdentifierStart(n.charAt(0)) ) n = "X"+n;
      char[] cs = n.toCharArray();
      for( int i=1; i<cs.length; i++ )
        if( !Character.isJavaIdentifierPart(cs[i]) )
          cs[i] = '_';
      n = new String(cs);
      int i = 0;
      String res = n + Extensions.HEX;
      Key k = Key.make(res);
      while(DKV.get(k) != null)
        k = Key.make(res = n + ++i + Extensions.HEX);
      return res;
    }
    @Override protected String queryDescription() { return "Destination hex key"; }
  }

  public static String link(Key k, String content) {
    return link(k.toString(),content);
  }
  public static String link(String k, String content) {
    RString rs = new RString("<a href='Parse.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k);
    rs.replace("content", content);
    return rs.toString();
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
    int i;
    for (i = 0; i < WHITE_DELIMS.length; i++) DEFAULT_DELIMS[i] = String.format("%s: '%02d'", WHITE_DELIMS[i],i);
    for (;i < 126; i++) {
      String s; // Escape HTML entities manually or use StringEscapeUtils from Apache
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
    DEFAULT_DELIMS[i]     = "AUTO";
    DEFAULT_IDX_DELIMS[i] = String.valueOf(CsvParser.AUTO_SEP);
  }
}
