package water.api;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import dontweave.gson.JsonObject;
import water.*;
import water.Request2.TypeaheadKey;
import water.api.Request.Filter;
import water.api.Request.Validator;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Check;
import water.util.RString;
import water.util.Utils;

import java.io.File;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;

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


  protected static int frameColumnNameToIndex(Frame fr, String input, boolean namesOnly) {
    // first check if we have string match
    for (int i = 0; fr._names != null && i < fr._names.length; ++i) {
      String colName = fr._names[i];
      if (colName == null)
        colName = String.valueOf(i);
      if (colName.equals(input))
        return i;
    }
    try {
      if(!namesOnly) {
        int i = Integer.parseInt(input);
        if ((i<0) || (i>=fr.vecs().length))
          return -1;
        return i;
      }
    } catch (NumberFormatException e) {
    }
    return -1;
  }

  // ===========================================================================
  // Record
  // ===========================================================================

  /** List of arguments for the request. Automatically filled in by the argument
   * constructors.
   */
  protected transient ArrayList<Argument> _arguments = new ArrayList();
  public ArrayList<Argument> arguments() {
    return _arguments;
  }

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

  public abstract class Argument<T> extends Iced implements Filter {
    @Override public boolean run(Object value) {
      throw new RuntimeException("Should not be called for special case Argument");
    }

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

    /**
     * Can be a grid search parameter.
     */
    public boolean _gridable;

    /**
     * For keys. If specified, the key must exist.
     */
    public boolean _mustExist;

    /** Value validator. */
    public Validator<T> _validator;

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

    /** Returns a list of possible error strings, that could be thrown in an
     * IllegalArgumentException.
     */
    protected String[] errors() { return null; }

    /** Any query addons can be specified here. These will be displayed with
     * the query html code and should be used for instance for default value
     * calculators, etc.
     */
    protected String queryAddons() {
      return "";
    }

    public String getName() { return _name; }
    public String getDisplayName() { return _displayName; }

    /** Returns the html query for the given argument, including the full
     * formatting. That means not only the queryElement, but also the argument
     * name in front of it, etc.
     *
     * You may want to override this if you want different form layouts to be
     * present.
     */
    protected String query() {
      RString result = new RString(_queryHtml);
      result.replace("ID",_name);
      result.replace("NAME", _displayName != null ? _displayName : JSON2HTML(_name));
      if (disabled())
        result.replace("ELEMENT","<div class='alert alert-info' style='padding-top:4px;padding-bottom:4px;margin-bottom:5px'>"+record()._disabledReason+"</div>");
      else
        result.replace("ELEMENT",queryElement());
      result.replace("TOOLTIP_DESCRIPTION", queryDescription());
      if (!disabled())
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
    public String _name, _displayName;

    /** True if the argument is required, false if it may be skipped.
     */
    public boolean _required;

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
    public transient ArrayList<Argument<T>> _prerequisites = null;

    /** The thread local argument state record. Must be initialized at the
     * beginning of each request before it can be used.
     */
    private transient ThreadLocal<Record> _argumentRecord = new ThreadLocal();

    /**
     * If argument has been created reflectively from a request field.
     */
    public transient Field _field;

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
    public void setRefreshOnChange() {
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
      // return record().valid();
      return record() != null && record().valid();
    }

    /** Returns true if the argument is specified by the user. That is if the
     * argument value was submitted by the user and parsed correctly.
     */
    public final boolean specified() {
      return record() != null && record().specified();
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
    public void check(RequestQueries callInstance, String input) throws IllegalArgumentException {
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
      // check that we have all prerequisites properly initialized
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
          throw new IllegalArgumentException("Argument '"+_name+"' is required, but not specified");
        record._value = defaultValue();
        record._valid = true;
      // parse the argument, if parse throws we will still be invalid correctly
      } else {
        try {
          record._value = parse(input);
          record._valid = true;

          if(callInstance instanceof Request2)
            ((Request2) callInstance).set(this, input, record._value);
        } catch( IllegalArgumentException e) {
          //record._value = defaultValue();
          throw e;
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
      if (_name == "path" || _name == "key" || _name == "source" || _name == "data_key" || _name == "source_key" || _name == "model_key" || _name == "thresholds" || _name == "model") {
      return "<input autocomplete=\"off\"" + (_readOnly ? " disabled" : "")+ " class='span5' type='text' name='"+_name+"' id='"+_name+"' placeholder='"+queryDescription()+"' "+ (!value.isEmpty() ? (" value='"+value+"' />") : "/>");
      } else {
        return "<input " + (_readOnly ? " disabled" : "")+ " class='span5' type='text' name='"+_name+"' id='"+_name+"' placeholder='"+queryDescription()+"' "+ (!value.isEmpty() ? (" value='"+value+"' />") : "/>");
      }


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
    public final transient Boolean _defaultValue;

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
      throw new H2OIllegalArgumentException(this, input+" is not valid boolean value. Only 1 and 0 are allowed.");
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
      sb.append(" size='").append(Math.min(20, values.length)).append("'");
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

  private static final char JS_SEP = '=';
  private static final String _multipleTextValueJS =
            "  var str = ''\n"
          + "  for (var i = 0; i < %NUMITEMS; ++i) {\n"
          + "    var element = $('#%NAME'+i);\n"
          + "    if (element.val() != '') {\n"
          + "      if (str == '')\n"
          + "        str = element.attr('name') + '" + JS_SEP + "' +element.val();\n"
          + "      else\n"
          + "        str = str + ',' + element.attr('name') + '" + JS_SEP + "' + element.val();\n"
          + "    }\n"
          + "  }\n"
          + "  return str;\n"
          ;


  public abstract class MultipleText<T> extends Argument<T> {
    protected abstract String[] textValues();
    protected abstract String[] textNames();

    protected String[] textPrefixes() { return null; }
    protected String[] textSuffixes() { return null; }
    protected String   textSuffix()   { return null; }


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
      String[] values   = textValues();
      String[] names    = textNames();
      String[] suffixes = textSuffixes();
      if (prefixes == null) prefixes = names;
      if (suffixes == null && textSuffix() != null) {
        suffixes = new String[names.length];
        String suffix = textSuffix();
        for(int i = 0; i<names.length; i++) suffixes[i] = suffix;
      }
      if (values == null) {
        values = new String[prefixes.length];
        for (int i = 0; i < values.length; ++i)
          values[i] = "";
      }
      assert (prefixes.length == values.length);
      if (values.length == 0)
        sb.append("<div class='alert alert-error'>No editable controls under current setup</div>");
      for (int i = 0 ; i < values.length; ++i) {
        sb.append("<div class='input-prepend" + (suffixes!=null?" input-append":"") + "'>");
        sb.append("<span class='add-on'>" + prefixes[i]+"</span>");
        sb.append("<input autocomplete=\"off\" class='span3' name='"+names[i]+"' id='"+_name+String.valueOf(i)+"' type='text' value='"+values[i]+"' placeholder='"+queryDescription()+"'>");
        if (suffixes!=null) sb.append("<span class='add-on'>" + suffixes[i]+"</span>");
        sb.append("</div>");
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
    final boolean _ints;

    public NumberSequence(double [] val, String str, boolean ints) {
      _arr = val;
      _str = str;
      _ints = ints;
    }

    public NumberSequence(String str, boolean mul, double defaultStep) {
      this(parseArray(str,mul,defaultStep),str, false);
    }

    static double [] parseArray(String input, boolean mul, double defaultStep) {
      String str = input.trim().toLowerCase();
      if(str.startsWith("c(") && str.endsWith(")"))
        str = str.substring(2,str.length()-1);
      if( str.startsWith("seq") ) {
        throw new RuntimeException("unimplemented");
      } if( str.contains(":") )
        return parseGenerator(input, mul, defaultStep);
      else if( str.contains(",") ) {
        String [] parts = str.split(",");
        double [] res = new double[parts.length];
        for(int i = 0; i < parts.length; ++i)
          res[i] = Double.parseDouble(parts[i]);
        return res;
      } else {
        return new double [] {Double.parseDouble(str)};
      }
    }
    public static double[] parseGenerator(String input, boolean mul, double defaultStep) {
      String str = input.trim().toLowerCase();
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
      if(mul?(step <= 1):(step<=0))throw new IllegalArgumentException("Value "+input+" is not a valid number sequence.");
      // make sure we have format from < to

      double [] res = new double[1024];
      int i = 0;
      while(from <= to){
        res[i++] = from;
        if(i == res.length)res = Arrays.copyOf(res, res.length + Math.max(1, res.length >> 1));
        if( mul) from *= step; else from += step;
      }
      return Arrays.copyOf(res,i);
    }
    static NumberSequence parse(String input, boolean mul, double defaultStep) {
      return new NumberSequence(parseArray(input, mul, defaultStep),null, false);
    }
    @Override public String toString() {
      if(_str != null)return _str;
      if(_arr == null || _arr.length == 0)return"";

      StringBuilder res = new StringBuilder();
      for(int i = 0; i < _arr.length; ++i) {
        if(i > 0) res.append(",");
        res.append(_ints ? "" + (int) _arr[i] : _arr[i]);
      }
      return res.toString();
    }
  }

  public static class NumberSequenceFloat {
    public final float [] _arr;
    final String _str;
    final boolean _ints;

    public NumberSequenceFloat(float [] val, String str, boolean ints) {
      _arr = val;
      _str = str;
      _ints = ints;
    }

    public NumberSequenceFloat(String str, boolean mul, float defaultStep) {
      this(parseArray(str,mul,defaultStep),str, false);
    }

    static float [] parseArray(String input, boolean mul, float defaultStep) {
      String str = input.trim().toLowerCase();
      if(str.startsWith("c(") && str.endsWith(")"))
        str = str.substring(2,str.length()-1);
      if( str.startsWith("seq") ) {
        throw new RuntimeException("unimplemented");
      } if( str.contains(":") )
        return parseGenerator(input, mul, defaultStep);
      else if( str.contains(",") ) {
        String [] parts = str.split(",");
        float [] res = new float[parts.length];
        for(int i = 0; i < parts.length; ++i)
          res[i] = Float.parseFloat(parts[i]);
        return res;
      } else {
        return new float [] {Float.parseFloat(str)};
      }
    }
    public static float[] parseGenerator(String input, boolean mul, float defaultStep) {
      String str = input.trim().toLowerCase();
      String [] parts = str.split(":");
      if(parts.length != 2 &&  parts.length != 3 )throw new IllegalArgumentException("Value "+input+" is not a valid number sequence.");
      float step = defaultStep;

      if( parts.length == 3 ){
        step = Float.parseFloat(parts[2]);
      }
      float from = Float.parseFloat(parts[0]);
      float to = Float.parseFloat(parts[1]);
      if(to == from) return new float[]{from};
      if(to < from)throw new IllegalArgumentException("Value "+input+" is not a valid number sequence.");
      if(mul?(step <= 1):(step<=0))throw new IllegalArgumentException("Value "+input+" is not a valid number sequence.");
      // make sure we have format from < to

      float [] res = new float[1024];
      int i = 0;
      while(from <= to){
        res[i++] = from;
        if(i == res.length)res = Arrays.copyOf(res, res.length + Math.max(1, res.length >> 1));
        if( mul) from *= step; else from += step;
      }
      return Arrays.copyOf(res,i);
    }
    static NumberSequenceFloat parse(String input, boolean mul, float defaultStep) {
      return new NumberSequenceFloat(parseArray(input, mul, defaultStep),null, false);
    }
    @Override public String toString() {
      if(_str != null)return _str;
      if(_arr == null || _arr.length == 0)return"";

      StringBuilder res = new StringBuilder();
      for(int i = 0; i < _arr.length; ++i) {
        if(i > 0) res.append(",");
        res.append(_ints ? "" + (int) _arr[i] : _arr[i]);
      }
      return res.toString();
    }
  }

  public class RSeq extends InputText<NumberSequence> {
    boolean _multiplicative;
    transient NumberSequence _dVal;
    double _defaultStep;
    String _comment;

    @Override
    public String queryComment() {
      if( disabled() ) return "";
      if( _comment != null ) return _comment;
      return "Comma separated list of values. Or range specified as from:to:step" + (_multiplicative?"(*).":"(+).");
    }

    public RSeq(String name, boolean req, boolean mul){
      this(name,req,null,mul);
    }
    public RSeq(String seq, boolean mul){
      this("", false, new NumberSequence(seq, mul, 0), mul);
    }
    public RSeq(String name, boolean req, NumberSequence dVal, boolean mul){
      this(name, req, dVal, mul, null);
    }
    public RSeq(String name, boolean req, NumberSequence dVal, boolean mul, String comment){
      super(name,req);
      _dVal = dVal;
      _multiplicative = mul;
      _defaultStep = mul?10:1;
      _comment = comment;
    }

    @Override protected NumberSequence parse(String input) throws IllegalArgumentException {
      try {
        return NumberSequence.parse(input, _multiplicative, _defaultStep);
      } catch( NumberFormatException e) {
        // allow grid search number sequences to pass without an exception (if all numbers except for [(),] are valid)
        if (input.contains("(") && input.contains(")")) {
          try {
            String[] s = input.replaceAll("[()]", "").split(","); //remove ( and ) and split on ,
            for (String num : s) Double.parseDouble(num); //try to parse every number as Double
            return NumberSequence.parse(s[0], _multiplicative, _defaultStep); //HACK: report back the first number (to satisfy the UI)
          } catch (NumberFormatException e2) {
            throw new IllegalArgumentException("Value " + input + " is not a valid number sequence.");
          }
        }
        else
        throw new IllegalArgumentException("Value " + input + " is not a valid number sequence.");
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

  public class RSeqFloat extends InputText<NumberSequenceFloat> {
    boolean _multiplicative;
    transient NumberSequenceFloat _fVal;
    float _defaultStep;
    String _comment;

    @Override
    public String queryComment() {
      if( disabled() ) return "";
      if( _comment != null ) return _comment;
      return "Comma separated list of values. Or range specified as from:to:step" + (_multiplicative?"(*).":"(+).");
    }

    public RSeqFloat(String name, boolean req, boolean mul){
      this(name,req,null,mul);
    }
    public RSeqFloat(String seq, boolean mul){
      this("", false, new NumberSequenceFloat(seq, mul, 0), mul);
    }
    public RSeqFloat(String name, boolean req, NumberSequenceFloat fVal, boolean mul){
      this(name, req, fVal, mul, null);
    }
    public RSeqFloat(String name, boolean req, NumberSequenceFloat fVal, boolean mul, String comment){
      super(name,req);
      _fVal = fVal;
      _multiplicative = mul;
      _defaultStep = mul?10:1;
      _comment = comment;
    }

    @Override protected NumberSequenceFloat parse(String input) throws IllegalArgumentException {
      try {
        return NumberSequenceFloat.parse(input, _multiplicative, _defaultStep);
      } catch( NumberFormatException e) {
        throw new IllegalArgumentException("Value "+input+" is not a valid number sequence.");
      }
    }

    @Override
    protected NumberSequenceFloat defaultValue() {
      return _fVal;
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

    public final transient Integer _defaultValue;

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
          throw new H2OIllegalArgumentException(this, "Value "+i+" is not between "+_min+" and "+_max+" (inclusive)");
        return i;
      } catch( NumberFormatException e ) {
        throw new H2OIllegalArgumentException(this, "Value "+input+" is not a valid integer.");
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
    public final transient long _defaultValue;
    public final long _min;
    public final long _max;
    public final String _comment;

    public LongInt(String name, long min, long max) { this(name,false,0,min,max,""); }
    public LongInt(String name, long defaultValue, String comment) {
      this(name, false, defaultValue, Long.MIN_VALUE, Long.MAX_VALUE, comment);
    }
    public LongInt(String name, boolean req, long defaultValue, long min, long max, String comment) {
      super(name, req);
      _defaultValue = defaultValue;
      _min = min;
      _max = max;
      _comment = comment;
    }

    @Override protected Long parse(String input) throws IllegalArgumentException {
      long i = 0;
      try {
        i = Long.parseLong(input);
      } catch( NumberFormatException e ) {
        double d = Double.NaN;
        try {
          d = Double.parseDouble(input);
          i = (long)d;
        } catch ( NumberFormatException xe ) { d = i - 1; } // make d different from i
        if( i!=d ) throw new H2OIllegalArgumentException(this, "Value '"+input+"' is not a valid long integer.");
      }
      if ((i< _min) || (i > _max))
        throw new H2OIllegalArgumentException(this, "Value "+i+" is not between "+_min+" and "+_max+" (inclusive)");
      return i;
    }

    @Override protected Long defaultValue() { return _defaultValue; }
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
    public transient final Double _defaultValue;
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
      this(name, false, defaultValue, null, null, "");
    }

    public Real(String name, Double defaultValue, String comment) {
      this(name, false, defaultValue, null, null, comment);
    }

    public Real(String name, Double defaultValue, double min, double max, String comment) {
      this(name, false, defaultValue, min, max, comment);
    }

    public Real(String name, boolean req, Double defaultValue, Double min, Double max, String comment) {
      super(name,req);
      _defaultValue = defaultValue;
      _min = min != null ? min : Double.NEGATIVE_INFINITY;
      _max = max != null ? max : Double.POSITIVE_INFINITY;
      _comment = comment;
    }

    @Override protected Double parse(String input) throws IllegalArgumentException {
      try {
        double i = Double.parseDouble(input);
        if ((i< _min) || (i > _max))
         throw new H2OIllegalArgumentException(this, "Value "+i+" is not between "+_min+" and "+_max+" (inclusive)");
        return i;
      } catch( NumberFormatException e ) {
        throw new H2OIllegalArgumentException(this, "Value "+input+" is not a valid real number.");
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

  // ---------------------------------------------------------------------------
  // Bool
  // ---------------------------------------------------------------------------

  public class Bool extends InputCheckBox {
    public final String _description;
    public Bool(String name, boolean defaultValue, String description) {
      super(name, defaultValue);
      _description = description;
    }
    public void setValue(boolean b){
      record()._value = b;
      record()._valid = true;
      record()._originalValue = b?"1":"0";
    }
    @Override protected String queryDescription() {
      return _description;
    }
  }

  public class ClassifyBool extends Bool {
    private FrameClassVec _fcv;
    public ClassifyBool(String name, FrameClassVec fcv) {
      super(name,false,"Classification or Regression");
      addPrerequisite(_fcv=fcv);
      setRefreshOnChange();
    }
    @Override public Boolean parse(String input) {
      boolean b=false;
      if( false ) ;
      else if (input.equals("1"))     b= true;
      else if (input.equals("0"))     b= false;
      else if (input.equals("true"))  b= true;
      else if (input.equals("false")) b= false;
      else throw new H2OIllegalArgumentException(this, input+" is not valid boolean value. Only 1 and 0 are allowed.");
      Vec vec = _fcv.value();
      if( !vec.isInt() &&  b ) throw new H2OIllegalArgumentException(this, "Float response allows only regression!");
      if( vec.isEnum() && !b ) throw new H2OIllegalArgumentException(this, "Categorical response allows only classification!");
      return b;
    }
    @Override protected Boolean defaultValue() {
      return _fcv.value().isInt(); // Allows only float columns for regression
    }
  }

  public class DRFCopyDataBool extends Bool {
    private TypeaheadKey _frkey;
    public DRFCopyDataBool(String name, TypeaheadKey frkey) {
      super(name,false,"Run on one node only; no network overhead but fewer cpus used.  Suitable for small datasets.");
      addPrerequisite(_frkey=frkey);
      setRefreshOnChange();
    }
    protected Frame fr() { return DKV.get(_frkey.value()).get(); }
    @Override public Boolean parse(String input) {
      boolean b=false;
      if( false ) ;
      else if (input.equals("1"))     b= true;
      else if (input.equals("0"))     b= false;
      else if (input.equals("true"))  b= true;
      else if (input.equals("false")) b= false;
      else throw new H2OIllegalArgumentException(this, input+" is not valid boolean value. Only 1 and 0 are allowed.");
      return b;
    }
    @Override protected Boolean defaultValue() {
      // Can we allocate ALL of the dataset locally?
      long bs = fr().byteSize();
      if( !MemoryManager.tryReserveTaskMem(bs) ) return false;
      // Also, do we have enough chunks to run it well globally?
      if( fr().anyVec().nChunks() >= 2*H2O.CLOUD.size() ) return false;
      // Less than 2 chunks per node, and fits locally... default to local-only
      return true;
    }
  }

  // ---------------------------------------------------------------------------
  // EnumClass
  // ---------------------------------------------------------------------------

  public class EnumArgument<T extends Enum<T>> extends InputSelect<T> {

    protected transient final Class<T> _enumClass;
    private transient final T _defaultValue;

    public EnumArgument(T defaultValue) {
      this("", defaultValue, false);
    }
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
      throw new H2OIllegalArgumentException(this, "Only "+Arrays.toString(selectValues())+" accepted for argument "+_name);
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
        throw new H2OIllegalArgumentException(this, "File "+input+" not found");
      return f;
    }
    @Override protected String queryDescription() { return "Existing file or directory"; }
    @Override protected File defaultValue() { return null; }
    @Override protected String[] errors() { return new String[] { "File not found" }; }
  }

  public class GeneralFile extends TypeaheadInputText<String> {
    public GeneralFile() {this("");}
    public GeneralFile(String name) {
      super(TypeaheadFileRequest.class, name, true);
    }
    @Override protected String parse(String input) throws IllegalArgumentException {
      return input;
    }
    @Override protected String queryDescription() { return "File or directory, can be on NFS, HDFS or S3"; }
    @Override protected String defaultValue() { return ""; }
    @Override protected String[] errors() { return new String[] { "File not found" }; }
  }

  // ---------------------------------------------------------------------------
  // H2OKey
  // ---------------------------------------------------------------------------
  // key with autocompletion and autoconversion to frame
  public class H2OKey2 extends TypeaheadInputText<Key> {
    public final Key _defaultValue;
    public H2OKey2(String name, boolean required) { this(name,null,required); }
    public H2OKey2(String name, Key key) { this(name,key,false); }
    public H2OKey2(String name, Key key, boolean req) { super(TypeaheadKeysRequest.class,name, req); _defaultValue = key; }
    @Override protected Key parse(String input) { return Key.make(input); }
    @Override protected Key defaultValue() { return _defaultValue; }
    @Override protected String queryDescription() { return "Valid H2O key"; }
  }
  public class H2OKey extends InputText<Key> {
    public final Key _defaultValue;
    private final boolean _checkLegal;
    public H2OKey(String name, boolean required) { this(name,null,required); }
    public H2OKey(String name, boolean required, boolean checkLegal) { this(name,null,required,checkLegal); }
    public H2OKey(String name, Key key) { this(name,key,false); }
    public H2OKey(String name, Key key, boolean req) { super(name, req); _defaultValue = key; _checkLegal = false; }
    public H2OKey(String name, Key key, boolean req, boolean checkLegal) { super(name, req); _defaultValue = key; _checkLegal = checkLegal; }
    @Override protected Key parse(String input) {
      if (_checkLegal && Utils.contains(input, Key.ILLEGAL_USER_KEY_CHARS))
        throw new IllegalArgumentException("Key '" + input + "' contains illegal character! Please avoid these characters: " + Key.ILLEGAL_USER_KEY_CHARS);
    return Key.make(input); }
    @Override protected Key defaultValue() { return _defaultValue; }
    @Override protected String queryDescription() { return "Valid H2O key"; }
  }

  // ---------------------------------------------------------------------------
  // H2OExistingKey
  // ---------------------------------------------------------------------------
  public class H2OExistingKey extends TypeaheadInputText<Value> {
    public final Key _defaultValue;
    public H2OExistingKey(String name) {
      this(name,true);
    }
    public H2OExistingKey(String name,boolean required) {
      super(TypeaheadKeysRequest.class, name, required);
      setRefreshOnChange();
      _defaultValue = null;
    }
    public H2OExistingKey(String name, String keyName) {
      this(name, Key.make(keyName));
    }
    public void setValue(Value v){
      record()._value = v;
      record()._originalValue = v._key.toString();
    }
    public H2OExistingKey(String name, Key key) {
      super(TypeaheadKeysRequest.class, name, false);
      _defaultValue = key;
    }
    @Override protected Value parse(String input) throws IllegalArgumentException {
      Key k = Key.make(input);
      Value v = DKV.get(k);
      if (v == null)
        throw new H2OIllegalArgumentException(this, "Key "+input+" not found!");
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
      throw new H2OIllegalArgumentException(this, "Invalid value "+input+", only "+Arrays.toString(_values)+" allowed");
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
  // Fluid Vec Arguments
  // ---------------------------------------------------------------------------

  /** A Fluid Vec, via a column name in a Frame */
  public class FrameKeyVec extends InputSelect<Vec> {
    final TypeaheadKey _key;
    boolean _optional = false;
    final String _desc;
    protected transient ThreadLocal<Integer> _colIdx= new ThreadLocal();
    public FrameKeyVec(String name, TypeaheadKey key, String desc,boolean required) {
      super(name, required);
      addPrerequisite(_key=key);
      setRefreshOnChange();
      _desc = desc;
    }

    protected Frame fr() { return DKV.get(_key.value()).get(); }
    @Override protected String[] selectValues() {
      String [] vals = fr()._names;
      if(!_required)
        vals = Utils.append(new String[]{""},vals);
      return vals;
    }
    @Override protected String selectedItemValue() {
      Frame fr = fr();
      if( value() == null || fr == null ) {
        if(!refreshOnChange()) { // Not if has dependencies, or page doesn't refresh
          Vec defaultVec = defaultValue();
          if( defaultVec != null && fr != null )
            for( int i = 0; i < fr.vecs().length; i++ )
              if( fr.vecs()[i] == defaultVec )
                return fr._names[i];
        }
        return "";
      }
      if(_colIdx.get() == null)
        return "";
      return fr._names[_colIdx.get()];

    }
    @Override protected Vec parse(String input) throws IllegalArgumentException {
      int cidx = fr().find(input);
      if (cidx == -1) {
        try {
          cidx = Integer.parseInt(input);
        } catch( NumberFormatException e ) { cidx = -1; }
        if (cidx < 0 || cidx >= fr().numCols() )
          throw new H2OIllegalArgumentException(this, input+" not a name of column, or a column index");
      }
      _colIdx.set(cidx);
      return fr().vecs()[cidx];
    }
    @Override protected Vec defaultValue() { return null; }
    @Override protected String queryDescription() { return _desc; }
    @Override protected String[] errors() { return new String[] { "Not a name of column, or a column index" }; }
  }

  /** A Class Vec/Column within a Frame.  Limited to 1000 classes, just to prevent madness. */
  public class FrameClassVec extends FrameKeyVec {
    public FrameClassVec(String name, TypeaheadKey key ) { super(name, key,"response column name",true); }
    @Override protected String[] selectValues() {
      final Vec [] vecs = fr().vecs();
      String[] names = new String[vecs.length];
      int j = 0;
      for( int i = 0; i < vecs.length; ++i) {
        if( !vecs[i].isUUID() ) // No math on strings or UUIDs
          names[j++] = fr()._names[i];
      }
      return Arrays.copyOf(names, j);
    }
    @Override protected Vec defaultValue() {
      Frame fr = fr();
      return fr != null ? fr.vecs()[fr.vecs().length - 1] : null;
    }
  }

  public class FrameKeyMultiVec extends MultipleSelect<int[]> {
    final TypeaheadKey _key;
    final String _description;
    final boolean _namesOnly;
    final boolean _filterNAs;
    transient ArrayList<FrameKeyVec> _ignoredVecs = new ArrayList<FrameKeyVec>();
    protected transient ThreadLocal<Integer> _colIdx= new ThreadLocal();
    protected Frame fr() {
      Value v = DKV.get(_key.value());
      if(v == null) throw new H2OIllegalArgumentException(this, "Frame not found");
      return v.get();
    }
    public FrameKeyMultiVec(String name, TypeaheadKey key, FrameClassVec vec, String description, boolean namesOnly, boolean filterNAs) {
      super(name);
      addPrerequisite(_key = key);
      _description = description;
      _namesOnly = namesOnly;
      _filterNAs = filterNAs;
      if(vec != null) ignoreVec(vec);
    }
    public void ignoreVec(FrameKeyVec v) {
      for(FrameKeyVec vv:_ignoredVecs)
        if(vv == v)return;
      addPrerequisite(v);
      _ignoredVecs.add(v);
    }
    public boolean shouldIgnore(int i, Frame fr ) {
      if(fr.vecs()[i].isUUID())
        return true;
      for(FrameKeyVec v:_ignoredVecs)
        if(v.value() == fr.vecs()[i])
          return true;
      return false;
    }
    public void checkLegality(Vec v) throws IllegalArgumentException { }
    transient ArrayList<Integer> _selectedCols; // All the columns I'm willing to show the user

    @Override protected String queryElement() {
      Frame fr = fr();
      ArrayList<Integer> cols = Lists.newArrayList();
      for (int i = 0; i < fr.numCols(); ++i)
        if( !shouldIgnore(i, fr) )
          cols.add(i);
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
      Frame fr = fr();
      String [] res = new String[_selectedCols.size()];
      int idx = 0;
      for(int i:_selectedCols) {
        final Vec v = fr.vec(i);
        int naRatio = (int)(((double)v.naCnt())/v.length()*100);
        res[idx++] = fr._names[i] + (naRatio > 0?"(" + naRatio + "% NAs)":"");
      }
      return res;
    }

    @Override protected boolean isSelected(String value) {
      int[] val = value();
      return val != null && Ints.contains(val, frameColumnNameToIndex(fr(), value, _namesOnly));
    }

    @Override protected int[] parse(String input) throws IllegalArgumentException {
      Frame fr = fr();
      ArrayList<Integer> al = new ArrayList();
      for (String col : input.split(",")) {
        col = col.trim();
        int idx = frameColumnNameToIndex(fr(), col, _namesOnly);
        if (0 > idx || idx > fr.numCols())
          throw new H2OIllegalArgumentException(this, "Column "+col+" not part of key "+_key.value());
        if (al.contains(idx))
          throw new H2OIllegalArgumentException(this, "Column "+col+" is specified twice.");
        checkLegality(fr.vecs()[idx]);
        al.add(idx);
      }
      return Ints.toArray(al);
    }

    @Override protected int[] defaultValue() {
      final Vec [] vecs = fr().vecs();
      int [] res = new int[vecs.length];
      int j = 0;
      for( int i = 0; i < vecs.length; ++i) {
        if(!(vecs[i].min() < vecs[i].max()) ||
          (_filterNAs && ((double)vecs[i].naCnt())/vecs[i].length() > 0.1) ||
           vecs[i].isUUID() ) // No math on strings or UUIDs
          res[j++] = i; // ignore constant columns and columns with too many NAs
      }
      return Arrays.copyOf(res, j);
    }

    @Override protected String queryDescription() {
      return _description;
    }
  }

  public static class H2OIllegalArgumentException extends IllegalArgumentException {
    public H2OIllegalArgumentException(Argument a, String msg) {
      super("Field '" + (a!=null ? a.getName() : "<unknown>") + "' : " + msg);
    }
  }
}
