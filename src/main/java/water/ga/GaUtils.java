package water.ga;

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

public class GaUtils {
  public static boolean isNotEmpty(String value) {
    return !isEmpty(value);
  }

  public static boolean isEmpty(String value) {
    return value == null || value.trim().length() == 0;
  }

  public static StringBuilder appendSystemProperty(StringBuilder sb, String property) {
    String value = System.getProperty(property);
    if (isNotEmpty(value)) {
      if (isNotEmpty(sb.toString())) {
        sb.append("/");
      }
      sb.append(value);
    }

    return sb;
  }


}
