package water.api;

import hex.DGLM.CaseMode;
import hex.DGLM.Family;
import hex.DGLM.GLMModel;
import hex.DGLM.Link;
import hex.rf.Confusion;
import hex.rf.RFModel;

import java.io.File;
import java.util.*;

import water.*;
import water.ValueArray.Column;
import water.util.Check;
import water.util.RString;

import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.google.gson.JsonObject;

/** All arguments related classes are defined in this guy.
 *
 * Argument is the base class for all arguments, which then branches to
 * different still abstract subclasses that specify how are any given HTML input
 * elements being drawn.
 *
 * From these a proper Arguments that define particular value types are then
 * subclassed.
 *
 * When an argument is created, its pointer is stored in the _arguments array
 * list so that the request knows how many arguments and in which order it has.
 *
 * Because request objects and therefore also argument objects one per
 * application, while the codepath can be multithreaded (server decides this),
 * the argument state is not preserved in the argument itself, but in the
 * Record static object that is kept thread local and must be properly
 * initialized at each iteration by calling reset() method on the argument.
 *
 * See the respective classes for more details.
 *
 * NOTE add more arguments to this class as they are needed and keep them here.
 *
 * @author peta
 */
public class RequestArguments extends RequestStatics {

  // ===========================================================================
  // Helper functions
  // ===========================================================================

  /** Returns a json object containing all arguments specified to the page.
   *
   * Useful for redirects and polling.
   */
  protected JsonObject argumentsToJson() {
    JsonObject result = new JsonObject();
    for (Argument a : _arguments) {
      if (a.specified())
        result.addProperty(a._name,a.originalValue());
    }
    return result;
  }

  protected static int vaColumnNameToIndex(ValueArray va, String input) {
    // first check if we have string match
    for (int i = 0; i < va._cols.length; ++i) {
      String colName = va._cols[i]._name;
      if (colName == null)
        colName = String.valueOf(i);
      if (colName.equals(input))
        return i;
    }
    try {
      int i = Integer.parseInt(input);
      if ((i<0) || (i>=va._cols.length))
        return -1;
      return i;
    } catch (NumberFormatException e) {
      return -1;
    }
  }

  /** Compute union of categories in model column and data column.
   * The result is ordered and the values are unique. */
  protected static String[] vaCategoryNames(ValueArray.Column modelCol, ValueArray.Column dataCol, int maxClasses) throws IllegalArgumentException {
    String[] result = Confusion.domain(modelCol, dataCol);
    if (result.length > maxClasses)
      throw new IllegalArgumentException("The column has more than "+maxClasses+" values. Are you sure you have that many classes?");
    return result;
  }

  protected static String[] vaCategoryNames(ValueArray.Column col, int maxClasses) throws IllegalArgumentException {
    String[] domain = col._domain;
    if ((domain == null) || (domain.length == 0)) {
      int min = (int) col._min;
      if (col._min!= min)
        throw new IllegalArgumentException("Only integer or enum columns can be classes!");
      int max = (int) col._max;
      if (col._max != max)
        throw new IllegalArgumentException("Only integer or enum columns can be classes!");
      if (max - min > maxClasses) // arbitrary number
        throw new IllegalArgumentException("The column has more than "+maxClasses+" values. Are you sure you have that many classes?");
      String[] result = new String[max-min+1];
      for (int i = 0; i <= max - min; ++i)
        result[i] = String.valueOf(min+i);
      return result;
    } else {
      return domain;
    }
  }

  // ===========================================================================
  // Record
  // ===========================================================================

  /** List of arguments for the request. Automatically filled in by the argument
   * constructors.
   */
  protected ArrayList<Argument> _arguments = new ArrayList();

  // ---------------------------------------------------------------------------

  /** Argument state record.
   *
   * Contains all state required for the argument and a few functions to operate
   * on the state.
   */
  protected static class Record<T> {

    /** Determines the original input value of the argument. null if the value
     * was not supplied, or was empty. Retains the original value even if the
     * argument value is wrong.
     */
    public String _originalValue = null;

    /** Parsed value. If the parse was successful, or default value if the
     * checking failed, or the argument is not required and was missing. Note
     * that default value may very well be null and thus you cannot check this
     * for null of determine validity.
     */
    public T _value = null;

    /** Reason why the argument is disabled, or null if it is enabled. A
     * disabled argument cannot be edited by the user yet.
     */
    public String _disabledReason = null;

    /** True if the argument's value stored in _value is valid, that is either
     * correctly parsed, or not present and default value used. Note that if
     * checking fails, the defaultValue is stored in _value, but _valid is
     * false.
     */
    public boolean _valid = false;

    /** Returns true if the argument is disabled.
     */
    public boolean disabled() {
      return _disabledReason != null;
    }

    /** Returns true if the argument is valid.
     */
    public boolean valid() {
      return _valid;
    }

    /** Returns if the argument is specified by user. returns true only if it is
     * valid and parsing the argument was successful.
     * @return
     */
    public boolean specified() {
      return valid() && _originalValue != null;
    }
  }

  // A string used to display the query element part of the argument
  protected static final String _queryHtml =
            "\n<dl class='dl-horizontal'>"
          +     "<dt style='padding-top:3px'><span rel='tooltip' title='%TOOLTIP_DESCRIPTION' data-placement='left'>%ASTERISK %NAME</span></dt>"
          +     "<dd>%ELEMENT %COMMENT</dd>"
          +  "</dl>"
          ;

  // ===========================================================================
  // Argument
  // ===========================================================================

  public abstract class Argument<T> {

    /** As with request's _requestHelp, this provides the extended help that
     * will be displayed on the help and wiki pages. Specify this in the
     * particular request constructor.
     */
    public String _requestHelp;

    /** True if the argument should not appear in the automatically generated
     * query.
     */
    public boolean _hideInQuery = false;

    /**
     * True if the argument should be only read-only.
     */
    public boolean _readOnly = false;

    /** Override this method to provide parsing of the input string to the Java
     * expected value. The input is guaranteed to be non-empty when this method
     * is called and all prerequisities are guaranteed to be valid before this
     * method is called.
     */
    protected abstract T parse(String input) throws IllegalArgumentException;

    /** Returns the default value of the argument. Note that the method will be
     * called also on required arguments, in which case it is ok return null.
     *
     * It is kept abstract because defining a proper default value might be
     * tricky and in many case you do not want it to be null. Overriding it
     * always makes you think:)
     */
    protected abstract T defaultValue();

    /** Returns the javascript code that will be executed when the query is
     * loaded that associates the given callback JS function with the on change
     * event of the input. This method is only called if the element should
     * refresh the webpage upon its change.
     */
    protected abstract String jsRefresh(String callbackName);

    /** Returns the javascript code that will be executed when the value of
     * the argument is to be determined. It must contain a return statement,
     * that returns the string that should be sent back to the request for the
     * given arhument.
     */
    protected abstract String jsValue();

    /** If there is any additional javascript that should be dumped to the
     * query page, it should be defined here. Please follow chaining rules.
     */
    protected String jsAddons() {
      return "";
    }

    /** Returns the HTML elements of the argument query only. This should return
     * the elements in HTML that will be used to enter the value. For instance
     * the input text, selection, etc.
     */
    protected abstract String queryElement();

    /* A little bonus extra text out to the right */
    protected String queryComment() { return ""; }

    /** Returns the query description. This is a concise description of a
     * correct value for the argument. generally used as a placeholder in the
     * html query elements.
     */
    protected abstract String queryDescription();

    /** Any query addons can be specified here. These will be displayed with
     * the query html code and should be used for instance for default value
     * calculators, etc.
     */
    protected String queryAddons() {
      return "";
    }

    /** Returns the html query for the given argument, including the full
     * formatting. That means not only the queryElement, but also the argument
     * name in front of it, etc.
     *
     * You may want to override this if you wont different form layouts to be
     * present.
     */
    protected String query() {
      RString result = new RString(_queryHtml);
      result.replace("ID",_name);
      result.replace("NAME", JSON2HTML(_name));
      if (disabled())
        result.replace("ELEMENT","<div class='alert alert-info' style='padding-top:4px;padding-bottom:4px;margin-bottom:5px'>"+record()._disabledReason+"</div>");
      else
        result.replace("ELEMENT",queryElement());
      result.replace("TOOLTIP_DESCRIPTION", queryDescription());
      result.replace("COMMENT",queryComment());
      if (_required)
        result.replace("ASTERISK","<span style='color:#ff0000'>* </span>");
      return result.toString();
    }

    /** Creates the request help page part for the given argument. Displays its
     * JSON name, query name (the one in HTML), value type and the request help
     * provided by the argument.
     */
    public final JsonObject requestHelp() {
      JsonObject r = new JsonObject();
      r.addProperty(NAME, _name);
      r.addProperty(DESCRIPTION, queryDescription());
      r.addProperty(HELP, _requestHelp);
      return r;
    }

    /** Name of the argument. This must correspond to the name of the JSON
     * request argument.
     */
    public final String _name;

    /** True if the argument is required, false if it may be skipped.
     */
    public final boolean _required;

    /** True if change of the value in the query controls should trigger an
     * automatic refresh of the query form.
     *
     * This is set by the setrefreshOnChange() method. It is automatically set
     * for any controls that are prerequisites for other controls and can be
     * manually select for other controls by users (do it in the request
     * constructor).
     */
    private boolean _refreshOnChange;

    /** List of all prerequisite arguments for the current argument. All the
     * prerequisite arguments must be created before the current argument.
     */
    public ArrayList<Argument<T>> _prerequisites = null;

    /** The thread local argument state record. Must be initialized at the
     * beginning of each request before it can be used.
     */
    private ThreadLocal<Record> _argumentRecord = new ThreadLocal();

    /** Creates the argument of given name. Also specifies whether the argument
     * is required or not. This cannot be changed later.
     */
    protected Argument(String name, boolean required) {
      assert Check.paramName(name);
      _name = name;
      _required = required;
      _refreshOnChange = false;
      _arguments.add(this);
    }

    /** Adds the given argument as a prerequisite. This means that current
     * argument will not be checked and/or reported in queries as a control form
     * unless all its prerequisite arguments are in a valid state. (the argument
     * will be disabled if not all its prerequisites are satisfied).
     */
    protected final void addPrerequisite(Argument arg) {
      if (_prerequisites == null)
        _prerequisites = new ArrayList();
      _prerequisites.add(arg);
      arg.setRefreshOnChange();
    }

    /** Returns the thread local argument state record.
     */
    protected final Record<T> record() {
      return _argumentRecord.get();
    }

    /** Disables the argument with given reason. If the argument is already
     * disabled, its reason is overwritten by the new one.
     *
     * NOTE disable(null) effectively enables the argument, that is why the
     * assert!
     */
    public final void disable(String reason) {
      assert (reason != null);
      record()._disabledReason = reason;
    }

    /** Disables the argument and makes its input value empty. This is the
     * preferred way of disabling arguments.
     */
    public final void disable(String reason, Properties args) {
      assert (reason != null);
      disable(reason);
      args.remove(_name);
    }

    /** Returns whether the argument is disabled or not.
     */
    public final boolean disabled() {
      return record().disabled();
    }

    /** Makes the argument refresh the query page on its change automatically.
     * If you want this behavior to be disabled for the argument, overwrite this
     * method to error.
     */
    protected void setRefreshOnChange() {
      _refreshOnChange = true;
    }

    /** Returns true if the argument refreshes the query automatically on its
     * change.
     */
    public boolean refreshOnChange() {
      return _refreshOnChange;
    }

    /** Returns true if the argument is valid. Valid means specified by user
     * and parsed properly, or not required and not specified.
     */
    public final boolean valid() {
      return record().valid();
    }

    /** Returns true if the argument is specified by the user. That is if the
     * argument value was submitted by the user and parsed correctly.
     */
    public final boolean specified() {
      return record().specified();
    }

    /** Returns the value of the argument. This is either the value parsed, if
     * specified, or defaultValue. Note that default value is returned also for
     * invalid arguments.
     */
    public final T value() {
      return record()._value;
    }

    /** Returns the input value submitted by the user, if specified.
     */
    public final String originalValue() {
      return record()._originalValue;
    }

    /** Resets the argument by creating it a new thread local state. Everything
     * is null and the argument is not valid.
     */
    public final void reset() {
      _argumentRecord.set(new Record());
    }

    /** Checks that the argument supplied is correct. This method is called for
     * each argument and is given the HTTP supplied argument value. If the value
     * was not supplied, input contains an empty string.
     *
     * The argument must already be reseted before calling this method.
     *
     * If the argument is disabled, the function does not do anything except
     * setting the original value in the record.
     *
     * If the prerequisites of the argument are not all valid, then the argument
     * is disabled and function returns.
     *
     * Then the argument is parsed if provided, or an error thrown if the input
     * is empty and the argument is required.
     *
     * At the end of the function the value is either the result of a successful
     * parse() call or a defaultValue or null if the argument is disabled.
     * However if the argument is disabled a defaultValue should not be called.
     */
    public void check(String input) throws IllegalArgumentException {
      // get the record -- we assume we have been reset properly
      Record record = record();
      // check that the input is canonical == value or null and store it to the
      // record
      if (input.isEmpty())
        input = null;
      record._originalValue = input;
      // there is not much to do if we are disabled
      if (record.disabled()) {
        record._value = null;
        return;
      }
      // check that we have all prerequisities properly initialized
      if (_prerequisites != null) {
        for (Argument dep : _prerequisites)
          if (!dep.valid()) {
            record._disabledReason = "Not all prerequisite arguments have been supplied: "+dep._name;
            record._value = null;
            return;
          }
      }
      // if input is null, throw if required, otherwise use the default value
      if (input == null) {
        if (_required)
          throw new IllegalArgumentException("Argument "+_name+" is required, but not specified");
        record._value = defaultValue();
        record._valid = true;
      // parse the argument, if parse throws we will still be invalid correctly
      } else {
        try {
          record._value = parse(input);
          record._valid = true;
        } catch (IllegalArgumentException e) {
          record._value = defaultValue();
          Throwables.propagate(e);
        }
      }
    }
  }

  // ===========================================================================
  // InputText
  // ===========================================================================

  /** Argument that uses simple text input to define its value.
   *
   * This is the simplest argument. Uses the classic input element. All
   * functionality is supported.
   *
   * @param <T>
   */
  public abstract class InputText<T> extends Argument<T> {

    public InputText(String name, boolean required) {
      super(name, required);
    }

    /** A query element is the default HTML form input.
     *
     * The id of the element is the name of the argument. Placeholder is the
     * query description and the value is filled in either as the value
     * submitted, or as the toString() method on defaultValue.
     */
    @Override protected String queryElement() {
      // first determine the value to put in the field
      Record record = record();
      String value = record._originalValue;
      // if no original value was supplied, try the one provided by the
      // default value
      if (value == null) {
        T v = defaultValue();
        value = (v == null) ? "" : v.toString();
      }
      return "<input" + (_readOnly ? " disabled" : "")+ " class='span5' type='text' name='"+_name+"' id='"+_name+"' placeholder='"+queryDescription()+"' "+ (!value.isEmpty() ? (" value='"+value+"' />") : "/>");
    }

    /** JS refresh is a default jQuery hook to the change() method.
     */
    @Override protected String jsRefresh(String callbackName) {
      return "$('#"+_name+"').change('"+_name+"',"+callbackName+");";
    }

    /** JS value is the simple jQuery val() method.
     */
    @Override protected String jsValue() {
      return "return $('#"+_name+"').val();";
    }
  }

  // ===========================================================================
  // TypeaheadInputText
  // ===========================================================================

  /** Typeahead enabled text input.
   *
   * Typeahead is enabled using the jQuery typeahead plugin. You must specify
   * the JSON request which provides the typeahead, and the data name in the
   * response that contains the array of strings corresponding to the typeahead
   * options. Optionally you can specify the typeahead limit (how many options
   * will be displayed), which is 1024 by default.
   *
   * The typeahead json request must take Str argument filter and Int optional
   * argument limit.
   */
  public abstract class TypeaheadInputText<T> extends InputText<T> {

    /** href of the json request supplying the typeahead values.
     */
    protected final String _typeaheadHref;

    /** Typeahead limit. If more than this limit options will be available, the
     * typeahead will be disabled.
     */
    protected final int _typeaheadLimit;


    /** Creates the typeahead.
     */
    protected TypeaheadInputText(Class<? extends TypeaheadRequest> href,
        String name, boolean required) {
      super(name, required);
      _typeaheadHref = href.getSimpleName();
      _typeaheadLimit = 1024;
    }

    /** Adds the json to hook initialize the typeahead functionality. It is
     * jQuery typeahead plugin standard initialization with async filler.
     */
    @Override protected String jsAddons() {
      RString s = new RString("" +
          "$('#%ID').typeahead({\n" +
          "  source:\n" +
          "    function(query,process) {\n" +
          "      return $.get('%HREF', { filter: query, limit: %LIMIT }, function (data) {\n" +
          "        return process(data.%DATA_NAME);\n" +
          "      });\n" +
          "    },\n" +
          "});\n" +
          "\n");
      s.replace("ID", _name);
      s.replace("HREF", _typeaheadHref);
      s.replace("LIMIT", _typeaheadLimit);
      s.replace("DATA_NAME", ITEMS);
      return super.jsAddons()+s.toString();
    }
  }

  // ===========================================================================
  // InputCheckBox
  // ===========================================================================

  /** A boolean argument that is represented as the checkbox.
   *
   * The only allowed values for a boolean checkbox are "0" for false, "1" for
   * true. If the argument is not required, then default value will be used.
   *
   * Please note that due to the nature of a checkbox, the html query will
   * always specify this argument to its default value, or to false if the user
   * did not specify it explicitly.
   */
  public abstract class InputCheckBox extends Argument<Boolean> {

    /** Default value.
     */
    public final Boolean _defaultValue;

    /** Creates the argument with specified default value.
     */
    public InputCheckBox(String name, boolean defaultValue) {
      super(name, false); // checkbox is never required
      _defaultValue = defaultValue;
    }

    /** Creates the argument as required one. This has only effect on JSON, for
     * HTML it means the default value is false effectively.
     */
    public InputCheckBox(String name) {
      super(name, true);
      _defaultValue = null;
    }

    /** Parses the value. 1 to true and 0 to false. Anything else is an error.
     */
    @Override public Boolean parse(String input) {
      if (input.equals("1"))
        return true;
      if (input.equals("0"))
        return false;
      if (input.equals("true"))
        return true;
      if (input.equals("false"))
        return false;
      throw new IllegalArgumentException(input+" is not valid boolean value. Only 1 and 0 are allowed.");
    }

    /** Displays the query element. This is just the checkbox followed by the
     * description.
     */
    @Override protected String queryElement() {
      // first determine the value to put in the field
      Record record = record();
      String value = record._originalValue;
      // if no original value was supplied, use the provided one
      if (value == null) {
        Boolean v = defaultValue();
        value = ((v == null) || (v == false)) ? "" : "1" ;
      }
      return "<input value='1' class='span5' type='checkbox' name='"+_name+"' id='"+_name+"' "+ (value.equals("1") ? (" checked />") : "/>")+"&nbsp;&nbsp;"+queryDescription();
    }

    /** Refresh only taps to jQuery change event.
     */
    @Override protected String jsRefresh(String callbackName) {
      return "$('#"+_name+"').change('"+_name+"',"+callbackName+");";
    }

    /** Returns 1 if the checkbox is checked and 0 otherwise.
     */
    @Override protected String jsValue() {
      return "return $('#"+_name+"').is(':checked') ? '1' : '0';";
    }

    /** Returns the default value.
     */
    @Override protected Boolean defaultValue() {
      return _defaultValue;
    }
  }

  // ===========================================================================
  // InputSelect
  // ===========================================================================

  /** Select element from the list of options.
   *
   * Array of values and arrays of names can be specified together with the
   * selected element's value.
   */
  public abstract class InputSelect<T> extends Argument<T> {

    /** Override this method to provide the values for the options. These will
     * be the possible values returned by the form's input and should be the
     * possible values for the JSON argument.
     */
    protected abstract String[] selectValues();

    /** Returns which value should be selected. This is *not* the default value
     * itself, as the default values may be of any type, but the input value
     * that should be selected in the browser.
     */
    protected abstract String selectedItemValue();

    /** Override this method to determine the value names, that is the names
     * displayed in the browser. Return null, if the value strings should be
     * used (this is default behavior).
     */
    protected String[] selectNames() {
      return null;
    }

    /** Constructor just calls super.
     */
    public InputSelect(String name, boolean required) {
      super(name, required);
    }

    /** Displays the query element. It is a select tag with option tags inside.
     * If the argument is required then additional empty value is added with
     * name "Please select..." that ensures that the user selects actual value.
     */
    @Override protected String queryElement() {
      StringBuilder sb = new StringBuilder();
      sb.append("<select id='"+_name+"' name='"+_name+"'>");
      String selected = selectedItemValue();
      String[] values = selectValues();
      String[] names = selectNames();
      if (names == null)
        names = values;
      assert (values.length == names.length);
      if (_required)
          sb.append("<option value=''>Please select...</option>");
      for (int i = 0 ; i < values.length; ++i) {
        if (values[i].equals(selected))
          sb.append("<option value='"+values[i]+"' selected>"+names[i]+"</option>");
        else
          sb.append("<option value='"+values[i]+"'>"+names[i]+"</option>");
      }
      sb.append("</select>");
      return sb.toString();
    }

    /** Refresh is supported using standard jQuery change event.
     */
    @Override protected String jsRefresh(String callbackName) {
      return "$('#"+_name+"').change('"+_name+"',"+callbackName+");";
    }

    /** Get value is supported by the standard val() jQuery function.
     */
    @Override protected String jsValue() {
      return "return $('#"+_name+"').val();";
    }
  }

  // ===========================================================================
  // MultipleCheckbox
  // ===========================================================================

  /** Displays multiple checkboxes for different values. Returns a list of the
   * checked values separated by commas.
   */
  public abstract class MultipleSelect<T> extends Argument<T> {
    /** Override this method to provide the values for the options. These will
     * be the possible values returned by the form's input and should be the
     * possible values for the JSON argument.
     */
    protected abstract String[] selectValues();

    /** Returns true if the given option (by its value) is selected. False
     * otherwise.
     */
    protected abstract boolean isSelected(String value);

    /** Override this method to determine the value names, that is the names
     * displayed in the browser. Return null, if the value strings should be
     * used (this is default behavior).
     */
    protected String[] selectNames() {
      return null;
    }

    /** Constructor just calls super. Is never required, translates to the
     * default value.
     */
    public MultipleSelect(String name) {
      super(name, false);
    }

    /** Displays the query element. It is a tabled list of all possibilities
     * with an optional scrollbar on the right.
     */
    @Override protected String queryElement() {
      String[] values = selectValues();
      String[] names = selectNames();
      if (names == null) names = values;
      assert (values.length == names.length);
      if (values.length == 0)
        return "<div class='alert alert-error'>No editable controls under current setup</div>";

      StringBuilder sb = new StringBuilder();
      sb.append("<select multiple");
      sb.append(" size='").append(Math.min(10, values.length)).append("'");
      sb.append(" id='").append(_name).append("' >");
      for (int i = 0 ; i < values.length; ++i) {
        sb.append("<option value='").append(values[i]).append("' ");
        if( isSelected(values[i]) ) sb.append("selected='true' ");
        sb.append(">").append(names[i]).append("</option>");
      }
      sb.append("</select>");
      return sb.toString();
    }

    /** Refresh is supported using standard jQuery change event. Each
     * possibility's checkbox is instrumented.
     */
    @Override protected String jsRefresh(String callbackName) {
      return "$('#"+_name+"').change('"+_name+"',"+callbackName+");";
    }

    /** Get value is supported by a JS function that enumerates over the
     * possibilities. If checked, the value of the possibility is appended to
     * a comma separated list.
     */
    @Override protected String jsValue() {
      return "var tmp = $('#"+_name+"').val(); return tmp == null ? \"\" : tmp.join(',');";
    }
  }

  // ===========================================================================
  // MultipleText
  // ===========================================================================

  private static final String _multipleTextValueJS =
            "  var str = ''\n"
          + "  for (var i = 0; i < %NUMITEMS; ++i) {\n"
          + "    var element = $('#%NAME'+i);\n"
          + "    if (element.val() != '') {\n"
          + "      if (str == '')\n"
          + "        str = element.attr('name') + '=' +element.val();\n"
          + "      else\n"
          + "        str = str + ',' + element.attr('name') + '=' + element.val();\n"
          + "    }\n"
          + "  }\n"
          + "  return str;\n"
          ;


  public abstract class MultipleText<T> extends Argument<T> {
    protected abstract String[] textValues();

    protected abstract String[] textNames();

    protected String[] textPrefixes() {
      return null;
    }


    public MultipleText(String name, boolean required) {
      super(name, required);
    }

    /** Displays the query element. It is a tabled list of all possibilities
     * with an optional scrollbar on the right.
     */
    @Override protected String queryElement() {
      StringBuilder sb = new StringBuilder();
      sb.append("<div style='max-height:300px;overflow:auto'>");
      String[] prefixes = textPrefixes();
      String[] values = textValues();
      String[] names = textNames();
      if (prefixes == null)
        prefixes = names;
      if (values == null) {
        values = new String[prefixes.length];
        for (int i = 0; i < values.length; ++i)
          values[i] = "";
      }
      assert (prefixes.length == values.length);
      if (values.length == 0)
        sb.append("<div class='alert alert-error'>No editable controls under current setup</div>");
      for (int i = 0 ; i < values.length; ++i) {
        sb.append("<div class='input-append'>"
                + "<input class='span3' name='"+names[i]+"' id='"+_name+String.valueOf(i)+"' type='text' value='"+values[i]+"' placeholder='"+queryDescription()+"'>"
                + "<span class='add-on'>" + prefixes[i]+"</span>"
                + "</div>");
      }
      sb.append("</div>");
      return sb.toString();
    }

    /** Refresh is supported using standard jQuery change event. Each text
     * input is instrumented.
     */
    @Override protected String jsRefresh(String callbackName) {
      int size = textNames().length;
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < size; ++i)
        sb.append("$('#"+_name+String.valueOf(i)+"').change('"+_name+"',"+callbackName+");\n");
      return sb.toString();
    }

    /** Get value is supported by a JS function that enumerates over the
     * possibilities. If checked, the value of the possibility is appended to
     * a comma separated list.
     */
    @Override protected String jsValue() {
      int size = textNames().length;
      RString result = new RString(_multipleTextValueJS);
      result.replace("NUMITEMS",size);
      result.replace("NAME",_name);
      return result.toString();
    }
  }


  // ===========================================================================
  // UserDefinedArguments
  //
  // Place your used defined arguments here.
  //
  // ===========================================================================

  // ---------------------------------------------------------------------------
  // Str
  // ---------------------------------------------------------------------------

  /** A string value.
   *
   * Any string can be a proper value. If required, empty string is not allowed.
   */
  public class Str extends InputText<String> {

    public final String _defaultValue;

    public Str(String name) {
      super(name,true);
      _defaultValue = null;
    }

    public Str(String name, String defaultValue) {
      super(name, false);
      _defaultValue = defaultValue;
    }

    @Override protected String parse(String input) throws IllegalArgumentException {
      return input;
    }

    @Override protected String defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      return _required ? "Any non-empty string" : "Any string";
    }

  }

  public static class NumberSequence {
    public final double [] _arr;
    final String _str;

    public NumberSequence(double [] val, String str){
      _arr = val;
      _str = str;
    }

    public NumberSequence(String str, boolean mul, double defaultStep){
      this(parseArray(str,mul,defaultStep),str);
    }

    private static double [] parseArray(String input, boolean mul, double defaultStep){
      String str = input.trim().toLowerCase();
      if( str.startsWith("seq") ) {
        throw new Error("unimplemented");
      } if( str.contains(":") ) {
        String [] parts = str.split(":");
        if(parts.length != 2 &&  parts.length != 3 )throw new IllegalArgumentException("Value "+input+" is not a valid number sequence.");
        double step = defaultStep;

        if( parts.length == 3 ){
          step = Double.parseDouble(parts[2]);
        }
        double from = Double.parseDouble(parts[0]);
        double to = Double.parseDouble(parts[1]);
        if(to == from) return new double[]{from};
        if(to < from)throw new IllegalArgumentException("Value "+input+" is not a valid number sequence.");
        if(step == 0)throw new IllegalArgumentException("Value "+input+" is not a valid number sequence.");
        int n = mul
          ? (int)((Math.log(to) - Math.log(from))/Math.log(step))
          : (int)((         to  -          from )/         step );
        double [] res = new double[n];
        for( int i = 0; i < n; ++i ) {
          res[i] = from;
          if( mul) from *= step; else from += step;
        }
        return res;
      } else if( str.contains(",") ) {
        String [] parts = str.split(",");
        double [] res = new double[parts.length];
        for(int i = 0; i < parts.length; ++i)
          res[i] = Double.parseDouble(parts[i]);
        return res;
      } else {
        return new double [] {Double.parseDouble(str)};
      }

    }
    static NumberSequence parse(String input, boolean mul, double defaultStep){
      return new NumberSequence(parseArray(input, mul, defaultStep),null);
    }
    public String toString(){
      if(_str != null)return _str;
      if(_arr == null || _arr.length == 0)return"";

      StringBuilder res = new StringBuilder();
      res.append(_arr[0]);
      for(int i = 1; i < _arr.length; ++i)
        res.append("," + _arr[i]);
      return res.toString();
    }
  }

  public class RSeq extends InputText<NumberSequence> {
    boolean _multiplicative;
    NumberSequence _dVal;
    double _defaultStep;

    @Override
    public String queryComment(){
      return disabled()?"":"Comma separated list of values. Or range specified as from:to:step" + (_multiplicative?"(*).":"(+).");
    }

    public RSeq(String name, boolean req, boolean mul){
      this(name,req,null,mul);

    }
    public RSeq(String name, boolean req, NumberSequence dVal, boolean mul){
      super(name,req);
      _dVal = dVal;
      _multiplicative = mul;
      _defaultStep = mul?10:1;
    }

    @Override protected NumberSequence parse(String input) throws IllegalArgumentException {
      try {
        return NumberSequence.parse(input, _multiplicative, _defaultStep);
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Value "+input+" is not a valid number sequence.");
      }
    }

    @Override
    protected NumberSequence defaultValue() {
      return _dVal;
    }

    @Override
    protected String queryDescription() {
      return "Number sequence. Comma separated list of values. Or range specified as from:to:step.";
    }

  }



  // ---------------------------------------------------------------------------
  // Int
  // ---------------------------------------------------------------------------

  public class Int extends InputText<Integer> {

    public final Integer _defaultValue;

    public final int _min;
    public final int _max;

    public Int(String name) {
      this(name, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public Int(String name, int min, int max) {
      super(name,true);
      _defaultValue = null;
      _min = min;
      _max = max;
    }

    public Int(String name, Integer defaultValue) {
      this(name, defaultValue, Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    public Int(String name, Integer defaultValue, int min, int max) {
      super(name,false);
      _defaultValue = defaultValue;
      _min = min;
      _max = max;
    }

    @Override protected Integer parse(String input) throws IllegalArgumentException {
      try {
        int i = Integer.parseInt(input);
        if ((i< _min) || (i > _max))
          throw new IllegalArgumentException("Value "+i+" is not between "+_min+" and "+_max+" (inclusive)");
        return i;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Value "+input+" is not a valid integer.");
      }
    }

    @Override protected Integer defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      return ((_min == Integer.MIN_VALUE) && (_max == Integer.MAX_VALUE))
              ? "Integer value"
              : "Integer from "+_min+" to "+_max;
    }
  }

  // ---------------------------------------------------------------------------
  // LongInt
  // ---------------------------------------------------------------------------

  public class LongInt extends InputText<Long> {
    public final Long _defaultValue;
    public final long _min;
    public final long _max;
    public final String _comment;

    public LongInt(String name) {
      this(name, Long.MIN_VALUE, Long.MAX_VALUE);
    }

    public LongInt(String name, long min, long max) {
      super(name,true);
      _defaultValue = null;
      _min = min;
      _max = max;
      _comment = "";
    }

    public LongInt(String name, Long defaultValue, String comment) {
      this(name, defaultValue, Long.MIN_VALUE, Long.MAX_VALUE, comment);
    }

    public LongInt(String name, Long defaultValue, long min, long max, String comment) {
      super(name,false);
      _defaultValue = defaultValue;
      _min = min;
      _max = max;
      _comment = comment;
    }

    @Override protected Long parse(String input) throws IllegalArgumentException {
      try {
        long i = Long.parseLong(input);
        if ((i< _min) || (i > _max))
          throw new IllegalArgumentException("Value "+i+" is not between "+_min+" and "+_max+" (inclusive)");
        return i;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Value "+input+" is not a valid long integer.");
      }
    }

    @Override protected Long defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryComment() { return _comment; }

    @Override protected String queryDescription() {
      return ((_min == Long.MIN_VALUE) && (_max == Long.MAX_VALUE))
              ? "Integer value"
              : "Integer from "+_min+" to "+_max;
    }
  }

  // ---------------------------------------------------------------------------
  // Real
  // ---------------------------------------------------------------------------
  public class Real extends InputText<Double> {
    public final Double _defaultValue;
    public       double _min;
    public       double _max;
    public final String _comment;

    public Real(String name) {
      this(name, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY);
    }

    public Real(String name, double min, double max) {
      super(name,true);
      _defaultValue = null;
      _min = min;
      _max = max;
      _comment = "";
    }

    public Real(String name, Double defaultValue) {
      this(name, defaultValue, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, "");
    }

    public Real(String name, Double defaultValue, String comment) {
      this(name, defaultValue, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY, comment);
    }

    public Real(String name, Double defaultValue, double min, double max, String comment) {
      super(name,false);
      _defaultValue = defaultValue;
      _min = min;
      _max = max;
      _comment = comment;
    }

    @Override protected Double parse(String input) throws IllegalArgumentException {
      try {
        double i = Double.parseDouble(input);
        if ((i< _min) || (i > _max))
          throw new IllegalArgumentException("Value "+i+" is not between "+_min+" and "+_max+" (inclusive)");
        return i;
      } catch (NumberFormatException e) {
        throw new IllegalArgumentException("Value "+input+" is not a valid real number.");
      }
    }

    @Override protected Double defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryComment() { return _comment; }

    @Override protected String queryDescription() {
      return ((_min == Double.NEGATIVE_INFINITY) && (_max == Double.POSITIVE_INFINITY))
              ? "Real value"
              : "Real from "+_min+" to "+_max;
    }
  }

  public class CaseModeSelect extends EnumArgument<CaseMode> {
    public CaseModeSelect(H2OHexKey key,H2OHexKeyCol classCol, EnumArgument<Family> family, String name, CaseMode defaultValue) {
      super(name, defaultValue);
      addPrerequisite(_key = key);
      addPrerequisite(_classCol  = classCol);
      addPrerequisite(_family  = family);
      setRefreshOnChange();
    }

    public final H2OHexKey _key;
    public final H2OHexKeyCol _classCol;
    public final EnumArgument<Family> _family;

    @Override
    public String[] selectValues(){
      if(_key.value() == null || _classCol.value() == null || _key.value()._cols[_classCol.value()]._domain == null)
        return super.selectValues();
      return new String[]{CaseMode.eq.toString(), CaseMode.neq.toString()};
    }
    @Override
    public CaseMode defaultValue() {
      if(_family.value() == Family.binomial){
        Column c = _key.value()._cols[_classCol.value()];
        if(c._min < 0 || c._max > 1)
          return c._domain == null
            ?CaseMode.gt
            :CaseMode.eq;
      }
      return CaseMode.none;
    }

  }

  class LinkArg extends EnumArgument<Link> {
    final EnumArgument<Family> _f;

    public LinkArg(EnumArgument<Family> f ,String name) {
      super(name,f.defaultValue().defaultLink);
      addPrerequisite(_f = f);
    }

    @Override
    protected Link defaultValue() {
      return _f.value().defaultLink;
    }
  }

  // Binomial GLM 'case' selection.  Only useful for binomial GLM where the
  // response column is NOT 0/1 - names a value to be treated as 1 and all
  // other values are treated as zero.
  public class CaseSelect extends Real {
    public final H2OHexKey _key;
    public final H2OHexKeyCol _classCol;
    public final CaseModeSelect _caseMode;


    public CaseSelect(H2OHexKey key, H2OHexKeyCol classCol, CaseModeSelect mode, String name) {
      super(name);
      addPrerequisite(_key= key);
      addPrerequisite(_classCol=classCol);
      addPrerequisite(_caseMode = mode);
    }

    @Override protected Double defaultValue() {
      if(_caseMode.value() == CaseMode.none)return Double.NaN;
      Column c = _key.value()._cols[_classCol.value()];
      return (_caseMode.value() == CaseMode.eq || Double.isNaN(c._mean))?c._max:c._mean;
    }


    @Override protected Double parse(String input) throws IllegalArgumentException {
      // Set min & max at the last second, after key/column selection has been
      // cleared up
      ValueArray.Column C = _key.value()._cols[_classCol.value()];
      _min = C._min;
      _max = C._max;
      double x = super.parse(input); // Then the normal parsing step
      if( Double.isNaN(x) && (C._scale!=1 || _min != 0 || _max != 1) )
        throw new IllegalArgumentException("Class column is not boolean, 'case' needs to specify what value to treat as TRUE; valid values range from "+_min+" to "+_max);
      return x;
    }

    @Override protected String queryDescription() {
      return "Value from the " + _classCol._name + " column.";
    }
  }


  // ---------------------------------------------------------------------------
  // Bool
  // ---------------------------------------------------------------------------

  public class Bool extends InputCheckBox {

    public final String _description;

    public Bool(String name, boolean defaultValue, String description) {
      super(name, defaultValue);
      _description = description;
    }

    @Override protected String queryDescription() {
      return _description;
    }

  }

  // ---------------------------------------------------------------------------
  // EnumClass
  // ---------------------------------------------------------------------------

  public class EnumArgument<T extends Enum<T>> extends InputSelect<T> {

    protected final Class<T> _enumClass;
    private final T _defaultValue;


    public EnumArgument(String name, T defaultValue, boolean refreshOnChange) {
      this(name,defaultValue);
      if(refreshOnChange)setRefreshOnChange();
    }
    public EnumArgument(String name, T defaultValue) {
      super(name, false);
      _defaultValue = defaultValue;
      _enumClass = (Class<T>) defaultValue.getClass();
    }

    public EnumArgument(String name, Class enumClass) {
      super(name, true);
      _defaultValue = null;
      _enumClass = enumClass;
    }


    @Override protected String[] selectValues() {
      T[] _enums = _enumClass.getEnumConstants();
      String[] result = new String[_enums.length];
      for (int i = 0; i < _enums.length; ++i)
        result[i] = _enums[i].toString();
      return result;
    }

    @Override protected String selectedItemValue() {
      T v = value();
      if (v == null)
        return "";
      return v.toString();
    }

    @Override protected T parse(String input) throws IllegalArgumentException {
      for (T v : _enumClass.getEnumConstants())
        if (v.toString().equals(input))
          return v;
      throw new IllegalArgumentException("Only "+Arrays.toString(selectValues())+" accepted for argument "+_name);
    }

    @Override protected T defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      return "Any of "+Arrays.toString(selectValues());
    }

  }

  // ---------------------------------------------------------------------------
  // ExistingFile
  // ---------------------------------------------------------------------------

  public class ExistingFile extends TypeaheadInputText<File> {
    public ExistingFile(String name) {
      super(TypeaheadFileRequest.class, name, true);
    }

    @Override protected File parse(String input) throws IllegalArgumentException {
      File f = new File(input);
      if( !f.exists() )
        throw new IllegalArgumentException("File "+input+" not found!");
      return f;
    }

    @Override protected String queryDescription() {
      return "Existing file or directory";
    }

    @Override
    protected File defaultValue() {
      return null;
    }
  }

  // ---------------------------------------------------------------------------
  // H2OKey
  // ---------------------------------------------------------------------------
  public class H2OKey extends InputText<Key> {
    public final Key _defaultValue;
    public H2OKey(String name) {
      super(name, true);
      _defaultValue = null;
    }
    public H2OKey(String name, String keyName) {
      this(name, Key.make(keyName));
    }
    public H2OKey(String name, Key key) {
      super(name, false);
      _defaultValue = key;
    }

    @Override protected Key parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      return k;
    }

    @Override protected Key defaultValue() {
      return _defaultValue;
    }

    @Override protected String queryDescription() {
      return "Valid H2O key";
    }
  }

  // ---------------------------------------------------------------------------
  // H2OExistingKey
  // ---------------------------------------------------------------------------
  public class H2OExistingKey extends TypeaheadInputText<Value> {
    public final Key _defaultValue;
    public H2OExistingKey(String name) {
      super(TypeaheadKeysRequest.class, name, true);
      _defaultValue = null;
    }
    public H2OExistingKey(String name, String keyName) {
      this(name, Key.make(keyName));
    }
    public H2OExistingKey(String name, Key key) {
      super(TypeaheadKeysRequest.class, name, false);
      _defaultValue = key;
    }
    @Override protected Value parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null)
        throw new IllegalArgumentException("Key "+input+" not found!");
      return v;
    }

    @Override protected Value defaultValue() {
      if (_defaultValue == null)
        return null;
      return DKV.get(_defaultValue);
    }

    @Override protected String queryDescription() {
      return "An existing H2O key";
    }
  }

  // ---------------------------------------------------------------------------
  // H2OHexKey
  // ---------------------------------------------------------------------------
  public class H2OHexKey extends TypeaheadInputText<ValueArray> {
    public final Key _defaultKey;

    public H2OHexKey(String name) {
      super(TypeaheadHexKeyRequest.class, name, true);
      _defaultKey = null;
    }

    public H2OHexKey(String name, String keyName) {
      this(name, Key.make(keyName));
    }

    public H2OHexKey(String name, Key key) {
      super(TypeaheadHexKeyRequest.class, name, false);
      _defaultKey = key;
    }

    @Override protected ValueArray parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null)
        throw new IllegalArgumentException("Key "+input+" not found!");
      if (!v.isArray())
        throw new IllegalArgumentException("Key "+input+" is not a valid HEX key");
      return v.get();
    }

    @Override protected ValueArray defaultValue() {
      if(_defaultKey == null) return null;
      return DKV.get(_defaultKey).get();
    }

    @Override protected String queryDescription() {
      return "An existing H2O HEX key";
    }
  }

  // -------------------------------------------------------------------------
  public class H2OModelKey<TM extends Model, TK extends TypeaheadKeysRequest> extends TypeaheadInputText<TM> {
    public H2OModelKey(TK tkr, String name, boolean req) { super(tkr.getClass(), name, req); }
    @Override protected TM parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null)
        throw new IllegalArgumentException("Key "+input+" not found!");
      return v.get();
    }
    @Override protected String queryDescription() { return "An existing H2O Model key"; }
    @Override protected TM defaultValue() { return null; }
  }

  // -------------------------------------------------------------------------
  public class H2OGLMModelKey extends H2OModelKey<GLMModel, TypeaheadGLMModelKeyRequest> {
    public H2OGLMModelKey(String name, boolean req) {
      super(new TypeaheadGLMModelKeyRequest(),name, req);
    }
  }
  // -------------------------------------------------------------------------
  public class RFModelKey extends H2OModelKey<RFModel, TypeaheadRFModelKeyRequest> {
    public RFModelKey(String name) {
      super(new TypeaheadRFModelKeyRequest(),name, true);
    }
  }

  // ---------------------------------------------------------------------------
  // StringListArgument
  // ---------------------------------------------------------------------------

  // NO EMPTY string in values
  public class StringList extends InputSelect<String> {

    public final String[] _values;

    public final int _defaultIndex;

    public StringList(String name, String[] values) {
      super(name, true);
      _values = values;
      _defaultIndex = -1;
    }

    public StringList(String name, String[] values, int defaultIndex) {
      super(name, false);
      _values = values;
      _defaultIndex = defaultIndex;
    }

    @Override  protected String[] selectValues() {
      return _values;
    }

    @Override protected String selectedItemValue() {
      if (_required && (!valid()))
        return "";
      return value();
    }

    @Override protected String parse(String input) throws IllegalArgumentException {
      for (String s : _values)
        if (s.equals(input))
          return input;
      throw new IllegalArgumentException("Invalid value "+input+", only "+Arrays.toString(_values)+" allowed");
    }

    @Override
    protected String defaultValue() {
      if (_defaultIndex == -1)
        return null;
      return _values[_defaultIndex];
    }

    @Override protected String queryDescription() {
      return "Any of "+Arrays.toString(_values);
    }
  }

  // ---------------------------------------------------------------------------
  // H2OHexKeyCol
  // ---------------------------------------------------------------------------

  public class H2OHexKeyCol extends InputSelect<Integer> {
    public final int _defaultCol;
    public final H2OHexKey _key;

    public H2OHexKeyCol(String name, H2OHexKey key) {
      super(name, true);
      _key = key;
      _defaultCol = 0;
      addPrerequisite(key);
    }

    public H2OHexKeyCol(String name, H2OHexKey key, int defaultCol) {
      super(name, false);
      _key = key;
      _defaultCol = defaultCol;
      addPrerequisite(key);
    }

    @Override protected String[] selectValues() {
      ValueArray va = _key.value();
      String[] result = new String[va._cols.length];
      for(int i = 0; i < result.length; ++i)
        result[i] = va._cols[i]._name == null ? String.valueOf(i) : va._cols[i]._name;
      return result;
    }

    @Override protected String selectedItemValue() {
      if (value() == null) return "";
      if (_key.value() == null) return "";
      return String.valueOf(_key.value()._cols[value()]._name);
    }

    @Override protected Integer parse(String input) throws IllegalArgumentException {
      ValueArray va = _key.value();
      int colIdx = vaColumnNameToIndex(va, input);
      if (colIdx == -1)
        throw new IllegalArgumentException(input+" not a name of column, or a column index");
      return colIdx;
    }

    @Override protected Integer defaultValue() {
      if (_defaultCol>=0)
        return _defaultCol;
      return _key.value()._cols.length + _defaultCol;
    }

    @Override protected String queryDescription() {
      return "Column name";
    }

  }

  public class HexKeyClassCol extends H2OHexKeyCol {
    public HexKeyClassCol(String name, H2OHexKey key ) {
      super(name, key, -1);
    }

    @Override protected Integer parse(String input) throws IllegalArgumentException {
      Integer i = super.parse(input);
      // called for error checking
      vaCategoryNames(_key.value()._cols[i], Integer.MAX_VALUE);
      return i;
    }
  }

  public class HexColumnSelect extends MultipleSelect<int[]> {
    public final H2OHexKey _key;

    public HexColumnSelect(String name, H2OHexKey key) {
      super(name);
      addPrerequisite(_key = key);
    }

    public boolean shouldIgnore(int i, ValueArray.Column ca ) { return false; }
    public void checkLegality(int i, ValueArray.Column c) throws IllegalArgumentException { }

    protected Comparator<Integer> colComp(final ValueArray ary){
      return null;
    }

    ArrayList<Integer> _selectedCols; // All the columns I'm willing to show the user

    // Select which columns I'll show the user
    @Override protected String queryElement() {
      ValueArray va = _key.value();
      ArrayList<Integer> cols = Lists.newArrayList();
      for (int i = 0; i < va._cols.length; ++i)
        if( !shouldIgnore(i, va._cols[i]) )
          cols.add(i);
      Comparator<Integer> cmp = colComp(va);
      if(cmp != null)
        Collections.sort(cols,cmp);
      _selectedCols = cols;
      return super.queryElement();
    }

    // "values" to send back and for in URLs.  Use numbers for density (shorter URLs).
    @Override protected final String[] selectValues() {
      String [] res = new String[_selectedCols.size()];
      int idx = 0;
      for(int i : _selectedCols) res[idx++] = String.valueOf(i);
      return res;
    }

    // "names" to select in the boxes.
    @Override protected String[] selectNames() {
      ValueArray va = _key.value();
      String [] res = new String[_selectedCols.size()];
      int idx = 0;
      for(int i:_selectedCols) res[idx++] = va._cols[i]._name;
      return res;
    }

    @Override protected boolean isSelected(String value) {
      ValueArray va = _key.value();
      int[] val = value();
      if (val == null) return false;
      int idx = vaColumnNameToIndex(va, value);
      return Ints.contains(val, idx);
    }

    @Override protected int[] parse(String input) throws IllegalArgumentException {
      ValueArray va = _key.value();
      ArrayList<Integer> al = new ArrayList();
      for (String col : input.split(",")) {
        col = col.trim();
        int idx = vaColumnNameToIndex(va, col);
        if (idx == -1)
          throw new IllegalArgumentException("Column "+col+" not part of key "+va._key);
        if (al.contains(idx))
          throw new IllegalArgumentException("Column "+col+" is already ignored.");
        checkLegality(idx, va._cols[idx]);
        al.add(idx);
      }
      return Ints.toArray(al);
    }

    @Override protected int[] defaultValue() {
      return new int[0];
    }

    @Override protected String queryDescription() {
      return "Columns to select";
    }
  }

  public class HexNonClassColumnSelect extends HexColumnSelect {
    public final H2OHexKeyCol _classCol;

    public HexNonClassColumnSelect(String name, H2OHexKey key, H2OHexKeyCol classCol) {
      super(name, key);
      addPrerequisite(_classCol = classCol);
    }

    @Override
    public boolean shouldIgnore(int i, Column ca) {
      return i == _classCol.value();
    }

    @Override
    public void checkLegality(int i, Column c) throws IllegalArgumentException {
      if( i == _classCol.value() )
        throw new IllegalArgumentException("Class column "+i+" cannot be ignored");
    }
  }

  public class HexAllColumnSelect extends HexColumnSelect {
    public HexAllColumnSelect(String name, H2OHexKey key) {
      super(name, key);
    }

    @Override protected int[] defaultValue() {
      int[] cols = new int[_key.value()._cols.length];
      for( int i = 0; i < cols.length; i++ ) cols[i]=i;
      return cols;
    }
  }

  // By default, all on - except *constant* columns
  public class HexNonConstantColumnSelect extends HexNonClassColumnSelect {
    public HexNonConstantColumnSelect(String name, H2OHexKey key, H2OHexKeyCol classCol) {
      super(name, key, classCol);
    }

    @Override
    public String [] selectNames(){
      ValueArray va = _key.value();
      String [] res = new String [_selectedCols.size()];
      int idx = 0;
      for(int cid: _selectedCols){
        Column c = va._cols[cid];
        double ratio = c._n/(double)va._numrows;
        if(ratio < 0.99){
          res[idx++] = c._name  + " (" + Math.round((1-ratio)*100) + "% NAs)";
        } else
          res[idx++] = c._name;
      }
      return res;
    }


    @Override protected Comparator<Integer> colComp(final ValueArray ary){
      ValueArray va = _key.value();
      final double ratio = 1.0/va._numrows;
      return new Comparator<Integer>() {
        @Override
        public int compare(Integer x, Integer y) {
          Column xc = ary._cols[x];
          Column yc = ary._cols[y];
          double xRatio = xc._n*ratio;
          double yRatio = yc._n*ratio;
          if(xRatio > 0.9 && yRatio > 0.9) return 0;
          if(xRatio <= 0.9 && yRatio <= 0.9) return Double.compare(1-xRatio, 1-yRatio);
          if(xRatio <= 0.9) return 1;
          return -1;
        }
      };
    }
    double _maxNAsRatio = 0.1;
    ThreadLocal<TreeSet<String>> _constantColumns = new ThreadLocal<TreeSet<String>>();
    ThreadLocal<Integer> _badColumns = new ThreadLocal<Integer>();

    @Override
    public boolean shouldIgnore(int i, ValueArray.Column ca ) {
      if(ca._min == ca._max){
        if(_constantColumns.get() == null)
          _constantColumns.set(new TreeSet<String>());
        _constantColumns.get().add(Objects.firstNonNull(ca._name, String.valueOf(i)));
        return true;
      }
      return super.shouldIgnore(i, ca);
    }

    String _comment = "";
    @Override protected int[] defaultValue() {
      ValueArray va = _key.value();
      int [] res = new int[va._cols.length];
      int selected = 0;
      for(int i = 0; i < va._cols.length; ++i)
        if(!shouldIgnore(i,va._cols[i]))
          if((1.0 - (double)va._cols[i]._n/va._numrows) <= _maxNAsRatio)
            res[selected++] = i;
          else {
            int val = 0;
            if(_badColumns.get() != null) val = _badColumns.get();
            _badColumns.set(val+1);
          }

      return Arrays.copyOfRange(res,0,selected);
    }
    @Override
    public String queryComment(){
      if(_constantColumns.get() == null || _constantColumns.get().isEmpty())return "";
      TreeSet<String> ignoredCols = _constantColumns.get();
      if(_badColumns.get() != null && _badColumns.get() > 0)
        return "<div class='alert'><b> There were " + _badColumns.get() + " bad columns not selected by default. Ignoring " + _constantColumns.get().size() + " constant columns</b>: " + ignoredCols.toString() +"</div>";
      else
        return "<div class='alert'><b>Ignoring " + _constantColumns.get().size() + " constant columns</b>: " + ignoredCols.toString() +"</div>";
    }
  }

  // ---------------------------------------------------------------------------
  // H2OHexCategoryWeights
  // ---------------------------------------------------------------------------

  public class H2OCategoryWeights extends MultipleText<double[]> {
    public final RFModelKey   _modelKey;
    public final H2OHexKey    _key;
    public final H2OHexKeyCol _classCol;
    public final double       _defaultValue;

    public H2OCategoryWeights(String name, RFModelKey modelKey, H2OHexKey key, H2OHexKeyCol classCol, double defaultValue) {
      super(name,false);
      _modelKey = modelKey;
      _key      = key;
      _classCol = classCol;
      _defaultValue = defaultValue;
      if (modelKey!=null) addPrerequisite(modelKey);
      addPrerequisite(key);
      addPrerequisite(classCol);
    }

    public H2OCategoryWeights(String name, H2OHexKey key, H2OHexKeyCol classCol, double defaultValue) {
      this(name, null, key, classCol, defaultValue);
    }

    protected String[] determineColumnClassNames(int maxClasses) throws IllegalArgumentException {
      ValueArray va = _key.value();
      ValueArray.Column dataCol = va._cols[_classCol.value()];
      if (_modelKey!=null) {
        ValueArray.Column modelCol = _modelKey.value().response();
        return vaCategoryNames(modelCol, dataCol, maxClasses);
      } else {
        return vaCategoryNames(dataCol, maxClasses);
      }
    }

    @Override protected String[] textValues() {
      double[] val = value();
      String[] result = new String[val.length];
      for (int i = 0; i < val.length; ++i)
        result[i] = String.valueOf(val[i]);
      return result;
    }

    @Override protected String[] textNames() {
      try {
        return determineColumnClassNames(1024);
      } catch (IllegalArgumentException e) {
        return new String[0];
      }
    }

    @Override protected double[] parse(String input) throws IllegalArgumentException {
      // determine the arity of the column
      HashMap<String,Integer> classNames = new HashMap();
      String[] names = determineColumnClassNames(1024);
      for (int i = 0; i < names.length; ++i)
        classNames.put(names[i],i);
      double[] result = new double[names.length];
      for (int i = 0; i < result.length; ++i)
        result[i] = _defaultValue;
      // now parse the given string and update the weights
      int start = 0;
      byte[] bsource = input.getBytes();
      while (start < bsource.length) {
        while (start < bsource.length && bsource[start]==' ') ++start; // whitespace;
        String className;
        double classWeight;
        int end = 0;
        if (bsource[start] == ',') {
          ++start;
          end = input.indexOf(',',start);
          className = input.substring(start,end);
          ++end;

        } else {
          end = input.indexOf('=',start);
          className = input.substring(start,end);
        }
        start = end;
        while (start < bsource.length && bsource[start]==' ') ++start; // whitespace;
        if (bsource[start]!='=')
          throw new IllegalArgumentException("Expected = after the class name.");
        ++start;
        end = input.indexOf(',',start);
        try {
          if (end == -1) {
            classWeight = Double.parseDouble(input.substring(start));
            start = bsource.length;
          } else {
            classWeight = Double.parseDouble(input.substring(start,end));
            start = end + 1;
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid double format for weight value");
        }

        if (!classNames.containsKey(className))
          throw new IllegalArgumentException("Category "+className+" not found!");
        result[classNames.get(className)] = classWeight;
      }
      return result;
    }

    @Override protected double[] defaultValue() throws IllegalArgumentException {
      try {
        String[] names = determineColumnClassNames(1024);
        double[] result = new double[names.length];
        for (int i = 0; i < result.length; ++i)
          result[i] = _defaultValue;
        return result;
      } catch( IllegalArgumentException e ) {
        return new double[0];
      }
    }

    @Override protected String queryDescription() {
      return "Category weight (positive)";
    }

  }

  // ---------------------------------------------------------------------------
  // H2OCategoryStrata
  // ---------------------------------------------------------------------------

  public class H2OCategoryStrata extends MultipleText<int[]> {
    public final H2OHexKey _key;
    public final H2OHexKeyCol _classCol;
    public final int _defaultValue;

    public H2OCategoryStrata(String name, H2OHexKey key, H2OHexKeyCol classCol, int defaultValue) {
      super(name,false);
      _key = key;
      _classCol = classCol;
      _defaultValue = defaultValue;
      addPrerequisite(key);
      addPrerequisite(classCol);
    }

    protected String[] determineColumnClassNames(int maxClasses) throws IllegalArgumentException {
      ValueArray va = _key.value();
      ValueArray.Column classCol = va._cols[_classCol.value()];
      return vaCategoryNames(classCol, maxClasses);
    }

    @Override protected String[] textValues() {
      int[] val = value();
      String[] result = new String[val.length];
      for (int i = 0; i < val.length; ++i)
        result[i] = String.valueOf(val[i]);
      return result;
    }

    @Override protected String[] textNames() {
      try {
        return determineColumnClassNames(1024);
      } catch (IllegalArgumentException e) {
        return new String[0];
      }
    }

    @Override protected int[] parse(String input) throws IllegalArgumentException {
      // determine the arity of the column
      HashMap<String,Integer> classNames = new HashMap();
      String[] names = determineColumnClassNames(1024);
      for (int i = 0; i < names.length; ++i)
        classNames.put(names[i],i);
      int[] result = new int[names.length];
      for (int i = 0; i < result.length; ++i)
        result[i] = _defaultValue;
      int start = 0;
      byte[] bsource = input.getBytes();
      while (start < bsource.length) {
        while (start < bsource.length && bsource[start]==' ') ++start; // whitespace;
        String className;
        int classWeight;
        int end = 0;
        if (bsource[start] == ',') {
          ++start;
          end = input.indexOf(',',start);
          className = input.substring(start,end);
          ++end;

        } else {
          end = input.indexOf('=',start);
          className = input.substring(start,end);
        }
        start = end;
        while (start < bsource.length && bsource[start]==' ') ++start; // whitespace;
        if (bsource[start]!='=')
          throw new IllegalArgumentException("Expected = after the class name.");
        ++start;
        end = input.indexOf(',',start);
        try {
          if (end == -1) {
            classWeight = Integer.parseInt(input.substring(start));
            start = bsource.length;
          } else {
            classWeight = Integer.parseInt(input.substring(start,end));
            start = end + 1;
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid integer format for strata value");
        }
        if (!classNames.containsKey(className))
          throw new IllegalArgumentException("Category "+className+" not found!");
        result[classNames.get(className)] = classWeight;
      }
      return result;
    }

    @Override protected int[] defaultValue() {
      try {
        String[] names = determineColumnClassNames(1024);
        int[] result = new int[names.length];
        for (int i = 0; i < result.length; ++i)
          result[i] = _defaultValue;
        return result;
      } catch (IllegalArgumentException e) {
        return new int[0];
      }
    }

    @Override protected String queryDescription() {
      return "Category strata (integer)";
    }

    public Map<Integer,Integer> convertToMap() {
      int[] v = value();
      if ((v == null) || (v.length == 0))
        return null;
      Map<Integer,Integer> result = new HashMap();
      for (int i = 0; i < v.length; ++i) {
        if (v[i] != _defaultValue)
          result.put(i, v[i]);
      }
      return result;
    }

  }

  public class NTree extends Int {
    final RFModelKey _modelKey;

    public NTree(String name, final RFModelKey modelKey) {
      super(name, 50, 0, Integer.MAX_VALUE);
      _modelKey     = modelKey;

      addPrerequisite(modelKey);
    }
    @Override
    protected Integer parse(String input) throws IllegalArgumentException {
      Integer N = super.parse(input);
      RFModel model = _modelKey.value();
//      if (N > model.treeCount())
//        throw new IllegalArgumentException("Value "+N+" is higher than number of trees provided by the random forest model ("+model.treeCount()+")!");

      return N;
    }
    @Override
    protected Integer defaultValue() {
      RFModel model = _modelKey.value();
      return model != null ? model.treeCount() : 0;
    }
  }
}
