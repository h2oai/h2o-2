package water.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class FSUtils {

  public static boolean isHdfs (String path) { return path.startsWith("hdfs://"); }
  public static boolean isS3N  (String path) { return path.startsWith("s3n://"); }
  public static boolean isS3   (String path) { return path.startsWith("s3://"); }
  public static boolean isHTTP (String path) { return path.startsWith("http://"); }
  public static boolean isHTTPS(String path) { return path.startsWith("https://"); }
  public static boolean isH2O  (String path) { return path.startsWith("h2o://"); }

  public static boolean isBareS3NBucketWithoutTrailingSlash(String s) {
    Pattern p = Pattern.compile("s3n://[^/]*");
    Matcher m = p.matcher(s);
    boolean b = m.matches();
    return b;
  }
}
