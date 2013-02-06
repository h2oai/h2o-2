
package water.web;

import java.util.Properties;

import com.google.common.base.Objects;

import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;

/**
 *
 * @author peta
 */
public class RFBuildQuery1 extends H2OPage {
  @Override public String[] requiredArguments() {
    return new String[] { "dataKey" };
  }

  static String html =
            "<script type='text/javascript'>\n"
          + "  var classIdx = %classIdx\n"
          + "  function sendForm() {\n"
          + "    if (classIdx == -1)\n"
          + "      classIdx = document.getElementById('cco').value\n"
          + "    document.getElementById('class').value=classIdx\n"
          + "    document.forms['rfbuild'].submit()\n"
          + "  }\n"
          + "  function checkOther() {\n"
          + "    document.getElementById('cc0').checked = false\n"
          + "    document.getElementById('cc1').checked = false\n"
          + "    document.getElementById('cc2').checked = false\n"
          + "    document.getElementById('cc3').checked = true\n"
          + "    classIdx = -1\n"
          + "  }\n"
          + "</script>"
          + "<p>We will be building a random forest from <b>%key</b>"
          + "<form class='form-horizontal' action='RFBuildQuery2' id='rfbuild'>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='dataKey'>Data</label>"
          + "    <div class='controls'>"
          + "      <input disabled='disabled' class='uneditable-input span5' type='text' value='%dataKey'>"
          + "      <input type='hidden' id='dataKey' name='dataKey' value='%dataKey'>"
          + "      <input type='hidden' id='class' name='class'>"
          + "    </div>"
          + "  </div>"
          + "</form>"
          + "<div class='form-horizontal' action='RFBuildQuery2'>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='class'>Class column</label>"
          + "    <div class='controls'>"
          + "      <input onclick='classIdx=0; 'type='radio' id='cc0' name='cc' value='0'>&nbsp;%col0 <i>(first)</i><br />"
          + "      <input onclick='classIdx=1; '%check1 type='radio' id='cc1' name='cc' value='1'>&nbsp;%col1 <i>(second)</i><br />"
          + "      <input onclick='classIdx=%classIdx;' %checkLast type='radio' style='%styleLast' id='cc2' name='cc' value='%colLastIdx'>&nbsp;%colLast <i>(last)</i><br />"
          + "      <div style='%styleOther'>"
          + "        <input onclick='classIdx=-1;' type='radio' id='cc3' name='cc' value='-1'>&nbsp;other&nbsp;"
          + "        <select id='cco' onchange=\"checkOther()\">"
          + "          %colClass{<option value='%colIdx'>%colName</option>}"
          + "        </select>"
          + "      </div>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <div class='controls'>"
          + "      <button onclick='sendForm()' class='btn btn-primary'>Next</button>"
          + "    </div>"
          + "  </div>"
          + "</div>"
          ;

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    RString result = new RString(html);
    result.replace("dataKey",args.getProperty("dataKey"));
    Value v = DKV.get(Key.make(args.getProperty("dataKey")));
    if (v == null)
      throw new PageError("Key not found!");
    if( v._isArray == 0 )
      throw new PageError("Key is not a dataframe");
    ValueArray va = ValueArray.value(v);
    if (va._numrows <= 0)
      throw new PageError("Key is not a parsed dataframe");
    int numCols = va._cols.length;
    assert (numCols>=2);
    result.replace("col0", va._cols[0]._name);
    result.replace("col1", va._cols[1]._name);
    if (numCols>2) {
      result.replace("colLastIdx",numCols-1);
      result.replace("colLast", va._cols[numCols-1]._name);
      result.replace("checkLast","checked");
      result.replace("classIdx",numCols-1);
      if (numCols>3) {
        for (int i = 0; i < va._cols.length; ++i) {
          RString str = result.restartGroup("colClass");
          str.replace("colIdx",i);
          str.replace("colName", Objects.firstNonNull(va._cols[i]._name, i));
          str.append();
        }
      } else {
        result.replace("styleOther","display:none");
      }
    } else {
      result.replace("styleLast","display:none");
      result.replace("check1","checked");
      result.replace("classIdx",1);
    }
    return result.toString();
  }

}
