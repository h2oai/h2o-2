package water.genmodel;

import java.util.Arrays;

public class GenUtils {
  /**
   * Concatenate given list of arrays into one long array.
   *
   * <p>Expect not null array.</p>
   *
   * @param aa list of string arrays
   * @return a long array create by concatenation of given arrays.
   */
  public static String[] concat(String[] ...aa) {
    int l = 0;
    for (String[] a : aa) l += a.length;
    String[] r = new String[l];
    l = 0;
    for (String[] a : aa) {
      System.arraycopy(a, 0, r, l, a.length);
      l += a.length;
    }
    return r;
  }

  public static String[][] array(String[] ...aa) {
    return aa;
  }

  public static int find(String name, String[] ...aa) {
    int l = 0;
    for (String[] a : aa) {
      int ii = Arrays.binarySearch(a, name);
      if (ii>=0) return l + ii;
      l += a.length;
    }
    return -1;
  }

  public static int maxIndex(float[] from, int start) {
    int result = start;
    for (int i = start; i<from.length; ++i)
      if (from[i]>from[result]) result = i;
    return result;
  }
}
