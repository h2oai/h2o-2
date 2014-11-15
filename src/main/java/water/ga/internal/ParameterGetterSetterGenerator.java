package water.ga.internal;

import water.ga.GoogleAnalyticsParameter;

/**
 * A small library for interacting with Google Analytics Measurement Protocol.  This
 * copy is a back port of version 1.1.1 of the library.  This backport removes
 * the slf4j dependency, and modifies the code to work with the 4.1 version of the
 * Apache http client library.
 *
 * Original sources can be found at https://github.com/brsanthu/google-analytics-java.
 * All copyrights retained by original authors.
 *
 */
public class ParameterGetterSetterGenerator {

  public static void main(String[] args) {
    GoogleAnalyticsParameter[] enumConstants = GoogleAnalyticsParameter.class.getEnumConstants();
    for (GoogleAnalyticsParameter parameter : enumConstants) {
      String methodName = null;//CaseFormat.UPPER_UNDERSCORE.to(CaseFormat.LOWER_CAMEL, parameter.toString());
      String constName = parameter.toString();

      String type = "String";

      if (parameter.getType().equalsIgnoreCase("integer")) {
        type = "Integer";

      } else if (parameter.getType().equalsIgnoreCase("boolean")) {
        type = "Boolean";

      } else if (parameter.getType().equalsIgnoreCase("currency")) {
        type = "Double";
      }

      System.out.println("public T " + methodName  +"(" + type + " value) {");
      System.out.println("	set" + type + "(" + constName + ", value);");
      System.out.println("   	return (T) this;");
      System.out.println("}");

      System.out.println("public " + type + " " + methodName  +"() {");
      System.out.println("	return get" + type + "(" + constName + ");");
      System.out.println("}");
    }
  }
}
