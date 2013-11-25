package water.util;

import water.fvec.Frame;
import water.fvec.Vec;

public class JCodeGen {

  /**
   * Outputs given frame as static variable with given name.
   */
  public static SB toStaticVar(SB sb, Frame f, String varname, int nrows) {
    sb.i().p("public static final double[][] ").p(varname).p(" = new double[][] {").nl();
    if (f!=null) {
      Vec[] vecs = f.vecs();
      for( int row = 0; row < nrows; row++ ) {
        sb.i(1).p(row > 0 ? "," : "").p("new double[] {");
        for( int v = 0; v < vecs.length; v++ )
          sb.p(v > 0 ? "," : "").p(vecs[v].at(row));
        sb.p("}").nl();
      }
    }
    sb.i().p("};").nl();
    return sb;
  }

  public static SB toStaticVar(SB sb, String varname, int value) {
    return sb.i().p("public static final int ").p(varname).p(" = ").p(value).p(';').nl();
  }

  /**
   * Transform given string to legal java Identifier (see Java grammar http://docs.oracle.com/javase/specs/jls/se7/html/jls-3.html#jls-3.8)
   *
   */
  public static String toJavaId(String s) {
    return s.replace('-', '_');
  }
}
