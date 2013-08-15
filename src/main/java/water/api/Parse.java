package water.api;

import java.util.*;
import java.util.regex.Pattern;

import water.*;
import water.fvec.Frame;
import water.parser.*;
import water.util.RString;
import water.util.Utils;

import com.google.gson.JsonObject;

public class Parse extends Request {

  private   final ParserType     _parserType= new ParserType(PARSER_TYPE);
  private   final Separator      _separator = new Separator(SEPARATOR);
  private   final Bool           _header    = new Bool(HEADER,false,"Use first line as a header");
  protected final HeaderKey      _hdrFrom   = new HeaderKey("header_from_file",false);
  protected final Str            _excludeExpression    = new Str("exclude","");
  protected final ExistingCSVKey _source    = new ExistingCSVKey(SOURCE_KEY);
  protected final NewH2OHexKey   _dest      = new NewH2OHexKey(DEST_KEY);
  @SuppressWarnings("unused")
  private   final Preview        _preview   = new Preview(PREVIEW);

  public Parse() {
    _hdrFrom.addPrerequisite(_header);
    _excludeExpression.setRefreshOnChange();
    _excludeExpression._hideInQuery = true;
  }


  protected CustomParser.PSetupGuess guessSetup(ArrayList<Key> keys,Key headerKey, CustomParser.ParserSetup setup, boolean checkHeader){
    String [] colNames = null;
    CustomParser.PSetupGuess gSetup = null, hSetup = null;
    if(headerKey != null){ // handle the header file!
      Value v = DKV.get(headerKey);
      if(!v.isRawData()){ // either ValueArray or a Frame, just extract the headers
        if(v.isArray()){
          ValueArray ary = v.get();
          colNames = ary.colNames();
        } else if(v.isFrame()){
          Frame fr = v.get();
          colNames = fr._names;
        } else
          throw new IllegalArgumentException("Headers can only come from unparsed data, ValueArray or a frame. Got " + v.newInstance().getClass().getSimpleName());
      } else { // check the hdr setup by parsing first bytes
        hSetup = ParseDataset.guessSetup(Utils.getFirstUnzipedBytes(headerKey),setup,checkHeader);
        if(hSetup == null || hSetup._setup._ncols != setup._ncols) // no match with global setup, try once more with general setup (e.g. header file can have different separator than the rest)
          hSetup = ParseDataset.guessSetup(Utils.getFirstUnzipedBytes(headerKey),new CustomParser.ParserSetup(),checkHeader);
        else if(hSetup._setup._data != null && hSetup._setup._data.length > 1){ // the hdr file had both hdr and data, it better be part of the parse and represent the global parser setup
          if(keys.contains(headerKey))
            gSetup = hSetup; // else we got a global setup file all other files must comply with!
          else throw new IllegalArgumentException(headerKey + " can not be used as a header file. Please either parse it separately first or include the file in the parse. Raw (unparsed) files can only be used as headers if they are included in the parse or they contain ONLY the header and NO DATA.");
        }
        if(hSetup != null)
          colNames = hSetup._setup._columnNames;
      }
    }
    if(gSetup == null){ // we did not get global setup
      Key k = keys.get(0);
      if(headerKey != null && k.equals(headerKey) && keys.size() > 1)
        k = keys.get(1);
      gSetup = ParseDataset.guessSetup(Utils.getFirstUnzipedBytes(k), setup, checkHeader && headerKey == null);
      if(gSetup == null) throw new IllegalArgumentException(setup._pType == water.parser.CustomParser.ParserType.AUTO?"Does not recognize the type of " + k:"File " + k + " is not in " + setup._pType + " format.");
      if(colNames != null && colNames.length != gSetup._setup._ncols) throw new IllegalArgumentException("number of columns in the header file and the parsed files don't match!");
    }
    // now set the header info in the final setup
    if(colNames != null){
      gSetup._setup._header = true;
      gSetup._setup._columnNames = colNames;
    }
    return gSetup;
  }


  protected static class PSetup {
    final transient ArrayList<Key> _keys;
    final CustomParser.PSetupGuess _setup;
    PSetup( ArrayList<Key> keys, CustomParser.PSetupGuess pguess) { _keys=keys; _setup = pguess; }
  };
  // An H2O Key Query, which runs the basic CSV parsing heuristics.  Accepts
  // Key wildcards, and gathers all matching Keys for simultaneous parsing.
  // Multi-key parses are only allowed on compatible CSV files, and only 1 is
  // allowed to have headers.
  public class ExistingCSVKey extends TypeaheadInputText<PSetup> {
    public ExistingCSVKey(String name) {
      super(TypeaheadKeysRequest.class, name, true);
//      addPrerequisite(_parserType);
//      addPrerequisite(_separator);
    }


    @Override protected PSetup parse(String input) throws IllegalArgumentException {
      Pattern p = makePattern(input);
      Pattern exclude = null;
      if(_excludeExpression.specified())
        exclude = makePattern(_excludeExpression.value());
      ArrayList<Key> keys = new ArrayList();
     // boolean badkeys = false;
      for( Key key : H2O.keySet() ) { // For all keys
        if( !key.user_allowed() ) continue;
        String ks = key.toString();
        if( !p.matcher(ks).matches() ) // Ignore non-matching keys
          continue;
        if(exclude != null && exclude.matcher(ks).matches())
          continue;
        Value v2 = DKV.get(key);  // Look at it
        if(v2.isHex())// filter common mistake such as *filename* with filename.hex already present
          continue;
        keys.add(key);        // Add to list
      }
      if(keys.size() == 0 )
        throw new IllegalArgumentException("I did not find any keys matching this pattern!");
      Collections.sort(keys);   // Sort all the keys, except the 1 header guy
      // now we assume the first key has the header
      Key hKey = null;
      if(_hdrFrom.specified())
        hKey = _hdrFrom.value()._key;
      boolean checkHeader = !_header.specified();
      boolean hasHeader = _header.value();
      CustomParser.PSetupGuess setup = guessSetup(keys, hKey, new CustomParser.ParserSetup(_parserType.value(),_separator.value(),hasHeader),checkHeader);
      if(setup == null)
        throw new IllegalArgumentException("I do not recognize the file " + keys.get(0) + "; Please select the parse setup manually.");
      if(!_header.specified())
        _header.setValue(setup._setup._header);
      else
        setup._setup._header = _header.value();
      if(!_header.value())
        _hdrFrom.disable("Header is disabled.");
      PSetup res = new PSetup(keys,setup);
      _parserType.setValue(setup._setup._pType);
      _separator.setValue(setup._setup._separator);
      _hdrFrom._hideInQuery = _header._hideInQuery = _separator._hideInQuery = setup._setup._pType != CustomParser.ParserType.CSV;
      return res;
    }

    private final String keyRow(Key k){
      return "<tr><td>" + k + "</td></tr>\n";
    }

    @Override
    public String queryComment(){
      if(!specified())return "";
      PSetup p = value();
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

    @Override protected PSetup defaultValue() { return null; }
    @Override protected String queryDescription() { return "An existing H2O key (or regex of keys) of CSV text"; }
  }


  // A Query String, which defaults to the source Key with a '.hex' suffix
  protected class NewH2OHexKey extends Str {
    NewH2OHexKey(String name) {
      super(name,null/*not required flag*/);
      addPrerequisite(_source);
    }
    @Override protected String defaultValue() {
      PSetup setup = _source.value();
      if( setup == null ) return null;
      String n = setup._keys.get(0).toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 ) n = n.substring(0, dot);
      int i = 0;
      String res = n + Extensions.HEX;
      Key k = Key.make(res);
      while(DKV.get(k) != null)
        k = Key.make(res = n + ++i + Extensions.HEX);
      return res;
    }
    @Override protected String queryDescription() { return "Destination hex key"; }
  }
  public class HeaderKey extends H2OExistingKey {
    public HeaderKey(String name, boolean required) {
      super(name, required);
    }
    @Override protected String queryElement() {
      StringBuilder sb = new StringBuilder(super.queryElement() + "\n");
      try{
        String [] colnames = _source.value()._setup._setup._columnNames;
        if(colnames != null){
          sb.append("<table class='table table-striped table-bordered'>").append("<tr><th>Header:</th>");
          for( String s : colnames ) sb.append("<th>").append(s).append("</th>");
          sb.append("</tr></table>");
        }
      }catch(Exception e){}
      return sb.toString();
    }

  }
  // A Query Bool, which includes a pretty HTML-ized version of the first few
  // parsed data rows.  If the value() is TRUE, we display as-if the first row
  // is a label/header column, and if FALSE not.
  public class Preview extends Argument {
      Preview(String name) {
      super(name,false);
      addPrerequisite(_source);
      addPrerequisite(_separator);
      addPrerequisite(_parserType);
      addPrerequisite(_header);
      setRefreshOnChange();
    }
    @Override protected String queryElement() {
      // first determine the value to put in the field
      // if no original value was supplied, use the provided one
      String[][] data = null;
      PSetup psetup = _source.value();
      String [] err = psetup._setup._errors;
      boolean hasErrors = err != null && err.length > 0;
      boolean parsedOk = psetup._setup.valid();
      String parseMsgType = hasErrors?parsedOk?"warning":"error":"success";
      StringBuilder sb = new StringBuilder("<div class='alert alert-" + parseMsgType + "'><b>" + psetup._setup.toString() + "</b>");
      if(hasErrors)
        for(String s:err)sb.append("<div>" + s + "</div>");
      sb.append("</div>");
      if(psetup._setup != null)
        data = psetup._setup._setup._data;
      String [] header = psetup._setup._setup._columnNames;

      if( data != null ) {
        sb.append("<table class='table table-striped table-bordered'>");
        int j = 0;
        if( psetup._setup._setup._header && header != null) { // Obvious header display, if asked for
          sb.append("<tr><th>Row#</th>");
          for( String s : header ) sb.append("<th>").append(s).append("</th>");
          sb.append("</tr>");
          if(header == data[0]) ++j;
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
    @Override protected Object parse(String input) throws IllegalArgumentException {return null;}
    @Override protected Object defaultValue() {return null;}

    @Override protected String queryDescription() {
      return "Preview of the parsed data";
    }
    @Override protected String jsRefresh(String callbackName) {
      return "";
    }
    @Override protected String jsValue() {
      return "";
    }
  }

  public static String link(Key k, String content) {
    return link(k.toString(),content);
  }
  public static String link(String k, String content) {
    RString rs = new RString("<a href='Parse.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    PSetup p = _source.value();
    if(!p._setup.valid())
      return Response.error("Given parser setup is not valid, I can not parse this file.");
    System.out.println("setup = " + p._setup.toString());
    CustomParser.ParserSetup setup = p._setup._setup;
    Key dest = Key.make(_dest.value());
    try {
      // Make a new Setup, with the 'header' flag set according to user wishes.
      Key[] keys = p._keys.toArray(new Key[p._keys.size()]);
      Job job = ParseDataset.forkParseDataset(dest, keys,setup);
      JsonObject response = new JsonObject();
      response.addProperty(RequestStatics.JOB, job.self().toString());
      response.addProperty(RequestStatics.DEST_KEY,dest.toString());
      Response r = Progress.redirect(response, job.self(), dest);
      r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
      return r;
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
    @Override protected String   queryDescription() { return "Utilized separator"; }
    @Override protected String[] selectValues()     { return DEFAULT_IDX_DELIMS;   }
    @Override protected String[] selectNames()      { return DEFAULT_DELIMS; }
    @Override protected Byte     defaultValue()     {return CsvParser.AUTO_SEP;}
    public void setValue(Byte b){record()._value = b;}
    @Override protected String selectedItemValue(){ return value() != null ? value().toString() : defaultValue().toString(); }
    @Override protected Byte parse(String input) throws IllegalArgumentException {
      Byte result = Byte.valueOf(input);
      return result;
    }
  }

  private class ParserType extends InputSelect<CustomParser.ParserType> {
    public ParserType(String name) {
      super(name,false);
      setRefreshOnChange();
      _values = new String [CustomParser.ParserType.values().length];
      int i = 0;
      for(CustomParser.ParserType t:CustomParser.ParserType.values())
        _values[i++] = t.name();
    }
    private final String [] _values;
    @Override protected String   queryDescription() { return "File type"; }
    @Override protected String[] selectValues()     {
      return _values;
    }
    @Override protected String[] selectNames()      {
      return _values;
    }
    @Override protected CustomParser.ParserType defaultValue() {
      return CustomParser.ParserType.AUTO;
    }
    public void setValue(CustomParser.ParserType pt){record()._value = pt;}
    @Override protected String   selectedItemValue(){
      return value() != null ? value().toString() : defaultValue().toString(); }
    @Override protected CustomParser.ParserType parse(String input) throws IllegalArgumentException {
      return  CustomParser.ParserType.valueOf(input);
    }
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
    DEFAULT_DELIMS[i]     = "AUTO";
    DEFAULT_IDX_DELIMS[i] = String.valueOf(CsvParser.AUTO_SEP);
  };
}
