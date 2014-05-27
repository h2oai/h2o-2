package water.api.v2;

import java.io.File;
import java.util.*;
import java.util.regex.Pattern;

import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.DefaultHttpClient;

import water.*;
import water.api.DocGen.FieldDoc;
import water.api.*;
import water.api.Constants.Extensions;
import water.api.RequestArguments.*;
import water.api.RequestBuilders.Response;
import water.api.RequestServer.API_VERSION;
import water.api.v2.Parser.*;
import water.parser.*;

import com.google.gson.*;

public class PreviewData extends JSONOnlyRequest {

  private   final ParserType     _parserType= new ParserType(PARSER_TYPE);
  private   final Separator      _separator = new Separator("data_separator");
  private   final Separator      _headerSeparator = new Separator("header_separator");
  private   final Bool           _header    = new Bool("dont_skip_header",false,"Use first line as a header");
  protected final Bool           _sQuotes   = new Bool("single_quotes",false,"Enable single quotes as field quotation character");
  protected final Bool           _userSetColumnNames   = new Bool("user_set_column_names",false, "");
  //protected final HeaderKey      _hdrFrom   = new HeaderKey("header_from_file",false);
  protected final HeaderKey      _hdrFrom   = new HeaderKey("header_file_old",false);
  protected final Str            _excludeExpression    = new Str("exclude","");
  protected final ExistingCSVKey _source    = new ExistingCSVKey(URIS);//SOURCE_KEY
  protected final HeaderFileNew _headerFileNew    = new HeaderFileNew("header_file");//SOURCE_KEY
  protected final H2OKey         _key       = new H2OKey(URIS,true);//SOURCE_KEY
  protected final NewH2OHexKey   _dest      = new NewH2OHexKey("dst");
  protected final Columns        _columns   = new Columns("columns");
  @SuppressWarnings("unused")
  private   final Preview        _preview   = new Preview(PREVIEW);
  private   final PreviewLen     _previewLen = new PreviewLen("preview_len");



  @Override protected String href(API_VERSION v) {
    return v.prefix() + "parse_preview";
  }

  @Override protected Response serve() {
    JsonObject response = new JsonObject();
    JsonObject parserConfig = new JsonObject();
    PSetup psetup = _source.value();

    if (_userSetColumnNames.value() && _headerFileNew.value() == null)
      _columns.changeColumnNames(psetup);
    else if (_userSetColumnNames.value() && _headerFileNew.value() != null)
      _columns.changeColumnNames(_headerFileNew.value());

    JsonArray uris = new JsonArray();
    uris.add(new JsonPrimitive(_key.value().toString()));
    JsonElement parserType  = new JsonPrimitive(_parserType.value().toString());
    JsonElement headerSeparator  = new JsonPrimitive(_headerSeparator.getStringValue());
    JsonElement dataSeparator  = new JsonPrimitive(_separator.getStringValue());
    JsonElement skipHeader  = new JsonPrimitive(new Boolean(_header.value().toString()));
    JsonElement previewLen = new JsonPrimitive(psetup._setup._data.length);


    JsonArray jHRowArray = new JsonArray();
    JsonObject jHObject = new JsonObject();


    String [] colnames = psetup._setup._setup._columnNames;
    if (_headerFileNew != null && _headerFileNew.value() != null && colnames != null && colnames.length == _headerFileNew.value()._setup._setup._columnNames.length)
      colnames = _headerFileNew.value()._setup._setup._columnNames;
    if (colnames != null && _header.value()){
      for (int i=0;i<colnames.length;i++){
        jHObject = new JsonObject();
        jHObject.add("header", new JsonPrimitive(colnames[i]));
        jHObject.add("type", new JsonPrimitive("ENUM"));
        jHRowArray.add(jHObject);
      }
    }else{
      for (int i=0;i<psetup._setup._setup._ncols;i++){
        jHObject = new JsonObject();
        jHObject.add("header", new JsonPrimitive("c"+i));
        jHObject.add("type", new JsonPrimitive("ENUM"));
        jHRowArray.add(jHObject);
      }
    }

    response.add("uris", uris);
    response.add("columns", jHRowArray);
    response.add("parser_type", parserType);
    response.add("header_separator", headerSeparator);
    response.add("data_separator", dataSeparator);
    response.add("dont_skip_header", skipHeader);
    if(_headerFileNew != null && _headerFileNew.value() != null){
      response.add("header_file", new JsonPrimitive(_headerFileNew._headerFileName));
    }else{
      response.add("header_file", new JsonPrimitive(""));
    }

    String[][] data = null;
    data = psetup._setup._data;

    StringBuilder sb = new StringBuilder();
    JsonArray jRowArray = new JsonArray();
    JsonArray jArray = new JsonArray();

    if( data != null ) {
      int j = 0;
      if( psetup._setup._setup._header && colnames != null) { // Obvious header display, if asked for
        if(colnames == data[0]) ++j;
      }
      String s2 = "";
      for( int i=j; i<data.length; i++ ) {
        jArray = new JsonArray();
        s2 = "";
        for( String s : data[i] ){
          jArray.add(new JsonPrimitive(s));
          s2+=s;
        }
        jRowArray.add(jArray);
      }
    }

    parserConfig.add("parser_config", response);
    parserConfig.add("preview_len", previewLen);
    parserConfig.add("preview", jRowArray);

    return Response.custom(parserConfig);
  }

  private class ParserType extends InputSelect<CustomParser.ParserType> {
    public ParserType(String name) {
      super(name,false);
      setRefreshOnChange();
      _values = new String [CustomParser.ParserType.values().length-1];
      int i = 0;
      for(CustomParser.ParserType t:CustomParser.ParserType.values())
        if(t != CustomParser.ParserType.XLSX)
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

  private class Separator extends InputSelect<Byte> {
    public Separator(String name) {
      super(name,false);
      setRefreshOnChange();
    }
    @Override protected String   queryDescription() { return "Utilized separator"; }
    @Override protected String[] selectValues()     { System.out.println("DEFAULT_IDX_DELIMS: "+DEFAULT_IDX_DELIMS.toString()); return DEFAULT_IDX_DELIMS;   }
    @Override protected String[] selectNames()      { System.out.println("DEFAULT_DELIMS: "+DEFAULT_DELIMS.toString()); return DEFAULT_DELIMS; }
    @Override protected Byte     defaultValue()     {return CsvParser.AUTO_SEP;}
    public void setValue(Byte b){record()._value = b;}
    public String getStringValue(){ return record() != null && record()._value != CsvParser.AUTO_SEP ? String.valueOf((char)((record()._value&0x00FF))) : ","; }
    @Override protected String selectedItemValue(){ return value() != null ? value().toString() : defaultValue().toString(); }
    @Override protected Byte parse(String input) throws IllegalArgumentException {
      Byte result = input.getBytes()[0];//Byte.valueOf(input);
      return result;
    }
  }

  private class Columns extends InputSelect<String> {
    public Columns(String name) {
      super(name,false);
      setRefreshOnChange();
    }
    @Override protected String   queryDescription() { return "header column name"; }
    @Override protected String[] selectValues()     { return null;  }
    @Override protected String[] selectNames()      { return null; }
    @Override protected String     defaultValue()     {return "";}
    public void setValue(String s){record()._value = s;}
    @Override protected String selectedItemValue(){ return value() != null ? value().toString() : defaultValue().toString(); }
    @Override protected String parse(String input) throws IllegalArgumentException {
      return input;
    }
    protected void changeColumnNames(PSetup psetup){
    String [] colnames = psetup._setup._setup._columnNames;


    if (colnames != null && this.value() != null && this.value() != ""){
      for (int i=0;i<colnames.length;i++){
        colnames[i]=getHeaderValue();
      }
    }
    }

    private String getHeaderValue(){
      setValue(value().substring(value().indexOf("header=")+7));
      return value().substring(0, value().indexOf(","));
    }
  }

  private class PreviewLen extends InputSelect<String> {
    public PreviewLen(String name) {
      super(name,false);
      setRefreshOnChange();
    }
    @Override protected String   queryDescription() { return "previre length"; }
    @Override protected String[] selectValues()     { return null;  }
    @Override protected String[] selectNames()      { return null; }
    @Override protected String     defaultValue()     {return "";}
    public void setValue(String s){record()._value = s;}
    @Override protected String selectedItemValue(){ return value() != null ? value().toString() : defaultValue().toString(); }
    @Override protected String parse(String input) throws IllegalArgumentException {
      return input;
    }
  }



  public class HeaderKey extends H2OExistingKey {
    public HeaderKey(String name, boolean required) {
      super(name, required);
    }
    @Override protected String queryElement() {
      return "";
    }

  }

  public class HeaderFileNew extends TypeaheadInputText<PSetup> {
    public String _headerFileName = "";
    public HeaderFileNew(String name) {
      super(TypeaheadKeysRequest.class, name, false);
    }


    @Override protected PSetup parse(String input) throws IllegalArgumentException {
      _headerFileName = input;
      Pattern p = makePattern(input);
      Pattern exclude = null;
      if(_hdrFrom.specified())
        _header.setValue(true);
      if(_excludeExpression.specified())
        exclude = makePattern(_excludeExpression.value());
      ArrayList<Key> keys = new ArrayList();
      // boolean badkeys = false;
      for( Key key : H2O.globalKeySet(null) ) { // For all keys
        if( !key.user_allowed() ) continue;
        String ks = key.toString();
        if (p.toString().contains(", ") && p.toString().contains(ks)){
        }else{
          if( !p.matcher(ks).matches() ) // Ignore non-matching keys
            continue;
          if(exclude != null && exclude.matcher(ks).matches())
            continue;
          Value v2 = DKV.get(key);  // Look at it
          if( !v2.isRawData() ) // filter common mistake such as *filename* with filename.hex already present
            continue;
        }
        keys.add(key);        // Add to list
      }
      if(keys.size() == 0 )
        throw new IllegalArgumentException("I did not find any keys matching this pattern!");
      Collections.sort(keys);   // Sort all the keys, except the 1 header guy
      // now we assume the first key has the header
      Key hKey = null;
      if(_hdrFrom.specified()){
        hKey = _hdrFrom.value()._key;
        _header.setValue(true);
      }
      boolean checkHeader = !_header.specified();
      boolean hasHeader = _header.value();
      CustomParser.ParserSetup userSetup =  new CustomParser.ParserSetup(_parserType.value(),_headerSeparator.value(),hasHeader);
      CustomParser.PSetupGuess setup = null;
      try {
       setup = ParseDataset.guessSetup(keys, hKey, userSetup,checkHeader);
      }catch(ParseDataset.ParseSetupGuessException e){
        throw new IllegalArgumentException(e.getMessage());
      }
      if(setup._isValid){
        if(setup._hdrFromFile != null)
          _hdrFrom.setValue(DKV.get(setup._hdrFromFile));
        if(!_header.specified())
          _header.setValue(setup._setup._header);//header
        else
          setup._setup._header = _header.value();
        if(!_header.value())
          _hdrFrom.disable("Header is disabled.");
        PSetup res = new PSetup(keys,null,setup);
        _parserType.setValue(setup._setup._pType);
        _headerSeparator.setValue(setup._setup._separator);
        _hdrFrom._hideInQuery = _header._hideInQuery = _headerSeparator._hideInQuery = setup._setup._pType != CustomParser.ParserType.CSV;
        Set<String> dups = setup.checkDupColumnNames();
        if(!dups.isEmpty())
          throw new IllegalArgumentException("Column labels must be unique but these labels are repeated: "  + dups.toString());
        return res;
      } else
        throw new IllegalArgumentException("Invalid parser setup. " + setup.toString());
    }

    @Override
    public String queryComment(){
      return "";
    }

    private Pattern makePattern(String input) {
      String patternStr = input.replace("?",".?").replace("*",".*?").replace("\\","\\\\").replace("(","\\(").replace(")","\\)");
      Pattern p = Pattern.compile(patternStr);
      return p;
    }

    @Override protected PSetup defaultValue() { return null; }
    @Override protected String queryDescription() { return "An existing H2O key (or regex of keys) of CSV text"; }
  }

  public class ExistingCSVKey extends TypeaheadInputText<PSetup> {
    public ExistingCSVKey(String name) {
      super(TypeaheadKeysRequest.class, name, true);
    }
    public ExistingCSVKey(String name, boolean req) {
      super(TypeaheadKeysRequest.class, name, req);
    }


    @Override protected PSetup parse(String input) throws IllegalArgumentException {
      Pattern p = makePattern(input);
      Pattern exclude = null;
      if(_hdrFrom.specified())
        _header.setValue(true);
      if(_excludeExpression.specified())
        exclude = makePattern(_excludeExpression.value());
      ArrayList<Key> keys = new ArrayList();
      // boolean badkeys = false;
      for( Key key : H2O.globalKeySet(null) ) { // For all keys
        if( !key.user_allowed() ) continue;
        String ks = key.toString();
        if (p.toString().contains(", ") && p.toString().contains(ks)){
        }else{
          if( !p.matcher(ks).matches() ) // Ignore non-matching keys
            continue;
          if(exclude != null && exclude.matcher(ks).matches())
            continue;
          Value v2 = DKV.get(key);  // Look at it
          if( !v2.isRawData() ) // filter common mistake such as *filename* with filename.hex already present
            continue;
        }
        keys.add(key);        // Add to list
      }
      if(keys.size() == 0 )
        throw new IllegalArgumentException("I did not find any keys matching this pattern!");
      Collections.sort(keys);   // Sort all the keys, except the 1 header guy
      // now we assume the first key has the header
      Key hKey = null;
      if(_hdrFrom.specified()){
        hKey = _hdrFrom.value()._key;
        _header.setValue(true);
      }
      boolean checkHeader = !_header.specified();
      boolean hasHeader = _header.value();
      CustomParser.ParserSetup userSetup =  new CustomParser.ParserSetup(_parserType.value(),_separator.value(),hasHeader);
      CustomParser.PSetupGuess setup = null;
      try {
       setup = ParseDataset.guessSetup(keys, hKey, userSetup,checkHeader);
      }catch(ParseDataset.ParseSetupGuessException e){
        throw new IllegalArgumentException(e.getMessage());
      }
      if(setup._isValid){
        if(setup._hdrFromFile != null)
          _hdrFrom.setValue(DKV.get(setup._hdrFromFile));
        if(!_header.specified())
          _header.setValue(setup._setup._header);//header
        else
          setup._setup._header = _header.value();
        if(!_header.value())
          _hdrFrom.disable("Header is disabled.");
        PSetup res = new PSetup(keys,null,setup);
        _parserType.setValue(setup._setup._pType);
        _separator.setValue(setup._setup._separator);
        _hdrFrom._hideInQuery = _header._hideInQuery = _separator._hideInQuery = setup._setup._pType != CustomParser.ParserType.CSV;
        Set<String> dups = setup.checkDupColumnNames();
        if(!dups.isEmpty())
          throw new IllegalArgumentException("Column labels must be unique but these labels are repeated: "  + dups.toString());
        return res;
      } else
        throw new IllegalArgumentException("Invalid parser setup. " + setup.toString());
    }

    @Override
    public String queryComment(){
      return "";
    }

    private Pattern makePattern(String input) {
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

  // A Query Bool, which includes a pretty HTML-ized version of the first few
  // parsed data rows.  If the value() is TRUE, we display as-if the first row
  // is a label/header column, and if FALSE not.
  public class Preview extends Argument {
      Preview(String name) {
      super(name,false);
      setRefreshOnChange();
    }
    @Override protected String queryElement() {
      return "";
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
  }

  public static String quote(String string) {
    if (string == null || string.length() == 0) {
        return "\"\"";
    }

    char         c = 0;
    int          i;
    int          len = string.length();
    StringBuilder sb = new StringBuilder(len + 4);
    String       t;

    sb.append('"');
    for (i = 0; i < len; i += 1) {
        c = string.charAt(i);
        switch (c) {
        case '\\':
        case '"':
            sb.append('\\');
            sb.append(c);
            break;
        case '/':
//                if (b == '<') {
                sb.append('\\');
//                }
            sb.append(c);
            break;
        case '\b':
            sb.append("\\b");
            break;
        case '\t':
            sb.append("\\t");
            break;
        case '\n':
            sb.append("\\n");
            break;
        case '\f':
            sb.append("\\f");
            break;
        case '\r':
           sb.append("\\r");
           break;
        default:
            if (c < ' ') {
                t = "000" + Integer.toHexString(c);
                sb.append("\\u" + t.substring(t.length() - 4));
            } else {
                sb.append(c);
            }
        }
    }
    sb.append('"');
    return sb.toString();
}
}
