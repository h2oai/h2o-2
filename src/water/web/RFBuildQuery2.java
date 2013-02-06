
package water.web;

import java.util.Properties;

import com.google.common.base.Objects;

import water.*;
import water.ValueArray.Column;

/**
 *
 * @author peta
 */
public class RFBuildQuery2 extends H2OPage {

  @Override public String[] requiredArguments() {
    return new String[] { "dataKey", "class" };
  }

  static String html = "<p>We will be building a random forest from <b>%key</b>"
          + "<script type='text/javascript'>\n"
          + "function getWeightsString() {\n"
          + "    var str = \"\"\n"
          + "    for (var i = 0; i < %numClasses; ++i) {\n"
          + "      var name = document.getElementById('n'+i).value\n"
          + "      var weight = document.getElementById('w'+i).value\n"
          + "      if (weight == \"\")\n"
          + "        continue\n"
          + "      if (str == \"\")\n"
          + "        str = name+'='+weight\n"
          + "      else\n"
          + "        str = str+','+name+'='+weight\n"
          + "    }\n"
          + "    return str\n"
          + "}\n"
          + "function sendForm() {\n"
          + "  document.getElementById('classWt').value = getWeightsString()\n"
          + "  document.forms['rfbuild'].submit()\n"
          + "}\n"
          + "</script>"
          + "<form class='form-horizontal' action='RF' id='rfbuild'>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='Key'>Data</label>"
          + "    <div class='controls'>"
          + "      <input disabled='disabled' class='uneditable-input span5' type='text' value='%dataKey'>"
          + "      <input type='hidden' id='Key' name='Key' value='%dataKey'>"
          + "      <input style='display:none' type='text' id='classWt' name='classWt'>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for=''>Class column</label>"
          + "    <div class='controls'>"
          + "      <input disabled='disabled' class='uneditable-input span5' type='text'  value='%classColName'>"
          + "      <input style='display:none' type='text' id='class' name='class' value='%classColIdx'>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='ntree'>Number of trees</label>"
          + "    <div class='controls'>"
          + "      <input type='text span5' id='ntree' name='ntree' placeholder='50 (default)'>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for=''>Algorithm</label>"
          + "    <div class='controls'>"
          + "      <select id='gini' name='gini'>"
          + "        <option value='1'>Gini</option>"
          + "        <option value='0'>Entropy</option>"
          + "      </select>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for=''>Additional args</label>"
          + "    <div class='controls'>"
          + "      <input type='text' class='span2' name='depth' id='depth' placeholder='(depth, no limit)'>"
          + "      <input type='text' class='span2' name='binLimit' id='binLimit' placeholder='(bin limit, 1024)'>"
          + "      <input type='text' class='span2' name='sample' id='sample' placeholder='(sample, 67)'>"
          + "      <input type='text' class='span2' name='seed' id='seed' placeholder='(seed, 181247619891)'>"
          + "      <input type='text' class='span2' name='features' id='features' placeholder='(features, %defFeatures)'>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='OOBEE'>Error estimation</label>"
          + "    <div class='controls'>"
          + "      <input type='checkbox' id='OOBEE' name='OOBEE' value='true' checked>&nbsp;out-of-bag</input>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for=''>Ignore columns</label>"
          + "    <div class='controls'>"
          + "      %ignoreCol{<input type='checkbox' id='ignore' name='ignore' value='%colIdx'>&nbsp;%colName&nbsp;}"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='modelKey'>Model</label>"
          + "    <div class='controls'>"
          + "      <input class='span5' type='text' id='modelKey' name='modelKey' placeholder='model key (default model)'>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='stratify'></label>"
          + "    <div class='controls'>"
          + "      <input type='checkbox' id='stratify' name='stratify' value='1'>&nbsp;stratify</input>"
          + "    </div>"
          + "  </div>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for='strata'>Strata</label>"
          + "    <div class='controls'>"
          + "      <input class='span5' type='text' id='strata' name='strata' placeholder='strata ({class:value} csl)'>"
          + "    </div>"
          + "  </div>"
          + "</form>"
          + "<div class='form-horizontal'>"
          + "  <div class='control-group'>"
          + "    <label class='control-label' for=''>Class weights</label>"
          + "    <div class='controls'>"
          + "      %classWeight{"
          + "      <div class='input-append'>"
          + "        <input class='span1' id='w%wi' type='text' placeholder='(default 1)'><span class='add-on'>%className</span>"
          + "          <input style='display:none' id='n%wi' type='text' value='%className'>"
          + "      </div><br/><br/>}"
          + "    <button class='btn btn-primary' onclick='sendForm()' >Calculate Confusion Matrix</button>"
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
    int classCol = getAsNumber(args, "class", va._cols.length-1);
    result.replace("classColIdx", classCol);
    for (int i = 0; i < va._cols.length; ++i) {
      Column c = va._cols[i];
      if ( i == classCol) {
        result.replace("classColName", Objects.firstNonNull(c._name, i));
      } else {
        RString str = result.restartGroup("ignoreCol");
        str.replace("colIdx",i);
        str.replace("colName", Objects.firstNonNull(c._name, i));
        str.append();
      }
    }
    result.replace("defFeatures",(int)Math.ceil(Math.sqrt(va._cols.length)));
    String[] classes = RandomForestPage.determineColumnClassNames(va, classCol, RandomForestPage.MAX_CLASSES);
    result.replace("numClasses",classes.length);
    for (int i = 0; i < classes.length; ++i) {
      RString str = result.restartGroup("classWeight");
      str.replace("wi",i);
      str.replace("className",classes[i]);
      str.append();
    }

    return result.toString();
  }

}
