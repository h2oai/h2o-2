package water.genmodel;

import java.util.Map;

/** This is a helper class to support Java generated models.
 */
public abstract class GeneratedModel implements IGeneratedModel {
  @Override public int      getNumCols()      { return getNames().length - 1; }
  @Override public int      getResponseIdx () { return getNames().length - 1; }
  @Override public String   getResponseName() { return getNames()[getResponseIdx()]; }
  @Override public int      getNumResponseClasses() { return getNumClasses(getResponseIdx()); }
  @Override public boolean  isClassifier() { return getNumResponseClasses()!=-1; }

  /**
   * Return <code>true</code> if the given index is in given bit array else false.
   *
   * <p>The method returns <code>false</code> if <code>idx</code> is less than
   * <code>offset</code>. It also returns <code>false</code> if the <code>idx</code>
   * is greater then length of given bit set!
   * </p>
   *
   * @param gcmp bit set array
   * @param offset number of bits skipped by default since there are 0
   * @param idx index of bit to be checked if it is in bitset
   * @return
   */
  public static boolean grpContains(byte[] gcmp, int offset, int idx) {
    if(offset < 0) throw new IndexOutOfBoundsException("offset < 0: " + offset);
    if(idx < offset) return false;
    idx = idx - offset;
    int max_idx = (gcmp.length << 3) - 1;
    if(idx > max_idx) return false;

    return (gcmp[idx >> 3] & ((byte)1 << (idx % 8))) != 0;
  }

  public String getHeader() { return null; }
  public boolean isAutoEncoder() { return false; }
  @Override public int getColIdx(String name) {
    String[] names = getNames();
    for (int i=0; i<names.length; i++) if (names[i].equals(name)) return i;
    return -1;
  }
  @Override public int getNumClasses(int i) {
    String[] domval = getDomainValues(i);
    return domval!=null?domval.length:-1;
  }
  @Override public String[] getDomainValues(String name) {
    int colIdx = getColIdx(name);
    return colIdx != -1 ? getDomainValues(colIdx) : null;
  }
  @Override public String[] getDomainValues(int i) {
    return getDomainValues()[i];
  }
  @Override public int mapEnum(int colIdx, String enumValue) {
    String[] domain = getDomainValues(colIdx);
    if (domain==null || domain.length==0) return -1;
    for (int i=0; i<domain.length;i++) if (enumValue.equals(domain[i])) return i;
    return -1;
  }

  @Override public int getPredsSize() {
    return isClassifier() ? 1+getNumResponseClasses() : 2;
  }

  /**
   * Takes a HashMap mapping column names to doubles.
   * <p>
   * Looks up the column names needed by the model, and places the doubles into the data array in
   * the order needed by the model. Missing columns use NaN.
   * </p>
   */
  public double[] map( Map<String, Double> row, double data[] ) {
    String[] colNames = getNames();
    for( int i=0; i<colNames.length-1; i++ ) {
      Double d = row.get(colNames[i]);
      data[i] = d==null ? Double.NaN : d;
    }
    return data;
  }

  // Does the mapping lookup for every row, no allocation
  public float[] predict( Map<String, Double> row, double data[], float preds[] ) {
    return predict(map(row,data),preds);
  }

  // Allocates a double[] for every row
  public float[] predict( Map<String, Double> row, float preds[] ) {
    return predict(map(row,new double[getNames().length]),preds);
  }

  // Allocates a double[] and a float[] for every row
  public float[] predict( Map<String, Double> row ) {
    return predict(map(row,new double[getNames().length]),new float[getNumResponseClasses()+1]);
  }

}
