package water.api;

import water.*;
import water.parser.*;
import water.parser.CsvParser.Setup;
import water.util.RString;

import com.google.gson.JsonObject;

public class Parse extends Request {
  protected final ExistingCSVKey _source = new ExistingCSVKey(SOURCE_KEY);
  protected final NewH2OHexKey _dest = new NewH2OHexKey(DEST_KEY);
  private final Header _header = new Header(HEADER);

  private static class PSetup {
    final Key _key;
    final CsvParser.Setup _setup;
    final boolean _xls;
    PSetup( Key key, CsvParser.Setup setup ) { _key=key; _setup=setup; _xls = false;}
    PSetup( Key key, CsvParser.Setup setup, boolean xls) { _key=key; _setup=setup; _xls = xls;}
  };

  // An H2O Hex Query, which does runs the basic CSV parsing heuristics.
  public class ExistingCSVKey extends TypeaheadInputText<PSetup> {
    public ExistingCSVKey(String name) { super(TypeaheadKeysRequest.class, name, true); }
    @Override protected PSetup parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      if(input.endsWith(".xlsx") || input.endsWith(".xls"))return new PSetup(k, new Setup((byte) 0,false,null,0,null), true);;
      Value v = DKV.get(k);
      if (v == null) throw new IllegalArgumentException("Key "+input+" not found!");
      CsvParser.Setup setup = Inspect.csvGuessValue(v);
      if( setup._data == null || setup._data[0].length == 0 )
        throw new IllegalArgumentException("The dataset format is not recognized/supported");
      return new PSetup(k,setup);
    }
    @Override protected PSetup defaultValue() { return null; }
    @Override protected String queryDescription() { return "An existing H2O key of CSV text"; }
  }


  // A Query String, which defaults to the source Key with a '.hex' suffix
  private class NewH2OHexKey extends Str {
    NewH2OHexKey(String name) {
      super(name,null/*not required flag*/);
      addPrerequisite(_source);
    }
    @Override protected String defaultValue() {
      PSetup setup = _source.value();
      if( setup == null ) return null;
      String n = setup._key.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 ) n = n.substring(0, dot);
      int i = 0;
      String res = n + ".hex";
      Key k = Key.make(res);
      while(DKV.get(k) != null)
        k = Key.make(res = n + ++i + ".hex");
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
      addPrerequisite(_source);
      setRefreshOnChange();
    }
    @Override protected String queryElement() {
      // first determine the value to put in the field
      Record record = record();
      String value = record._originalValue;
      // if no original value was supplied, use the provided one
      PSetup psetup = _source.value();
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

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='Parse.query?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    PSetup p = _source.value();
    CsvParser.Setup q = p._setup;
    Key dest = Key.make(_dest.value());
    try {
      // Make a new Setup, with the 'header' flag set according to user wishes.
      CsvParser.Setup new_setup = new CsvParser.Setup(q._separator,_header.value(),q._data,q._numlines,q._bits);
      Job job = ParseDataset.forkParseDataset(dest, DKV.get(p._key),new_setup);
      JsonObject response = new JsonObject();
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
}
