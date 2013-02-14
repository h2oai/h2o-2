package water.api;

import java.util.Properties;
import water.Key;
import water.DKV;
import water.Value;
import water.parser.*;
import water.util.RString;

import com.google.gson.JsonObject;

public class Parse extends Request {
  protected final H2OExistingKey _source = new H2OExistingKey(SOURCE_KEY);
  protected final NewH2OHexKey _dest = new NewH2OHexKey(DEST_KEY);
  private final Header _header = new Header(HEADER);

  // An H2O Hex Query, which does runs the basic CSV parsing heuristics.
  public class ExistingCSVKey extends TypeaheadInputText<CsvParser.Setup> {
    public ExistingCSVKey(String name) { super(TypeaheadKeysRequest.class, name, true); }
    @Override protected CsvParser.Setup parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null) throw new IllegalArgumentException("Key "+input+" not found!");
      return Inspect.csvGuessValue(v);
    }
    @Override protected CsvParser.Setup defaultValue() { return null; }
    @Override protected String queryDescription() { return "An existing H2O key of CSV text"; }
  }


  // A Query String, which defaults to the source Key with a '.hex' suffix
  private class NewH2OHexKey extends Str {
    NewH2OHexKey(String name) {
      super(name);
      addPrerequisite(_source);
    }
    @Override protected String defaultValue() {
      Value src_v = _source.value();
      if( src_v == null ) return null;
      String n = src_v._key.toString();
      int dot = n.lastIndexOf('.');
      if( dot > 0 ) n = n.substring(0, dot);
      String dst = n+".hex";
      return dst;
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
    }
    @Override protected String queryElement() {
      // first determine the value to put in the field
      Record record = record();
      String value = record._originalValue;
      // if no original value was supplied, use the provided one
      if (value == null) {
        Boolean v = defaultValue();
        value = ((v == null) || (v == false)) ? "" : "1" ;
      }
      StringBuilder sb = new StringBuilder();
      sb.append("<input value='1' class='span5' type='checkbox' ");
      sb.append("name='").append(_name).append("' ");
      sb.append("id='").append(_name).append("' ");
      if( value.equals("1") ) sb.append("checked");
      sb.append("/>&nbsp;&nbsp;").append(queryDescription()).append("<p>");
      sb.append("<table class='table table-striped table-bordered'>");
      sb.append("<th>");
      sb.append("</th>");
      sb.append("</table>");
      return sb.toString();
    }
  }

  public static String link(Key k, String content) {
    RString rs = new RString("<a href='Parse.html?%key_param=%$key'>%content</a>");
    rs.replace("key_param", SOURCE_KEY);
    rs.replace("key", k.toString());
    rs.replace("content", content);
    return rs.toString();
  }

  @Override protected Response serve() {
    Value source = _source.value();
    Key dest = Key.make(_dest.value());
    try {
      ParseDataset.forkParseDataset(dest, source);
      JsonObject response = new JsonObject();
      response.addProperty(RequestStatics.DEST_KEY,dest.toString());

      Response r = ParseProgress.redirect(response, dest);
      r.setBuilder(RequestStatics.DEST_KEY, new KeyElementBuilder());
      return r;
    } catch (IllegalArgumentException e) {
      return Response.error(e.getMessage());
    } catch (Error e) {
      return Response.error(e.getMessage());
    }
  }
}
