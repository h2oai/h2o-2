package water.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.Lists;
import dontweave.gson.*;

public class Check {
  private static final Pattern JSON_PATTERN = Pattern.compile("[_a-z0-9]*[/_a-z]*");
  private static final List<String> RESERVED_WORDS = Lists.newArrayList(
    // python reserved words
    "and", "assert", "break", "class", "continue", "def", "del", "elif",
    "else", "except", "exec", "finally", "for", "from", "global", "if",
    "import", "in", "is", "not", "or", "pass", "print", "raise",
    "return", "try", "while",
    // "lambda", - while lambda is a python reserved word, this word is also
    // the main term-of-the-art in GLM.  People expect to see 'lambda' in
    // reference to GLM.

    // java reserved words
    "public", "private", "protected", "static", "true", "false", "final",
    "volatile", "transient", "package", "catch"
  );

  public static boolean paramName(String s) {
    Matcher m = JSON_PATTERN.matcher(s);
    assert m.matches() : "Name " + s + " does not match convention: " + JSON_PATTERN;
    assert !RESERVED_WORDS.contains(s) : "Name " + s + " is a reserved word";
    return true;
  }

  public static boolean staticFinalStrings(Class<?> c) {
    try {
      for( Field f : c.getFields() ) {
        if( !Modifier.isFinal (f.getModifiers()) ) continue;
        if( !Modifier.isStatic(f.getModifiers()) ) continue;
        if( !f.getType().equals(String.class)    ) continue;
        Check.paramName((String) f.get(null));
      }
      return true;
    } catch( Exception e ) {
      throw  Log.errRTExcept(e);
    }
  }

  public static boolean jsonKeyNames(JsonArray a) {
    if( a == null ) return true;
    for(JsonElement v : a) {
      if( v.isJsonObject() ) {
        Check.jsonKeyNames(v.getAsJsonObject());
      } else if( v.isJsonArray() ) {
        Check.jsonKeyNames(v.getAsJsonArray());
      }
    }
    return true;
  }

  public static boolean jsonKeyNames(JsonObject o) {
    if( o == null ) return true;
    for(Entry<String, JsonElement> e : o.entrySet()) {
      Check.paramName(e.getKey());
      JsonElement v = e.getValue();
      if( v.isJsonObject() ) {
        Check.jsonKeyNames(v.getAsJsonObject());
      } else if( v.isJsonArray() ) {
        Check.jsonKeyNames(v.getAsJsonArray());
      }
    }
    return true;
  }
}
