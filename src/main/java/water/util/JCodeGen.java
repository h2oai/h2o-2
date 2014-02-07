package water.util;

import water.fvec.Frame;
import water.fvec.Vec;

public class JCodeGen {

  public static SB toClass(SB sb, String classSig, String varname, Frame f, int nrows, String comment) {
    sb.p(classSig).p(" {").nl().ii(1);
    toStaticVar(sb, varname, f, nrows, comment).di(1);
    return sb.p("}").nl();
  }

  /**
   * Outputs given frame as static variable with given name.
   */
  public static SB toStaticVar(SB sb, String varname, Frame f, int nrows, String comment) {
    if (comment!=null) sb.i(1).p("// ").p(comment).nl();
    sb.i(1).p("public static final double[][] ").p(varname).p(" = new double[][] {").nl();
    if (f!=null) {
      Vec[] vecs = f.vecs();
      for( int row = 0; row < Math.min(nrows,f.numRows()); row++ ) {
        sb.i(2).p(row > 0 ? "," : "").p("new double[] {");
        for( int v = 0; v < vecs.length; v++ )
          sb.p(v > 0 ? "," : "").p(vecs[v].at(row));
        sb.p("}").nl();
      }
    }
    sb.i(1).p("};").nl();
    return sb;
  }

  public static SB toStaticVar(SB sb, String varname, int value) {
    return toStaticVar(sb, varname, value,null);
  }
  public static SB toStaticVar(SB sb, String varname, int value, String comment) {
    if (comment!=null) sb.i(1).p("// ").p(comment).nl();
    return sb.i(1).p("public static final int ").p(varname).p(" = ").p(value).p(';').nl();
  }

  /**
   * Transform given string to legal java Identifier (see Java grammar http://docs.oracle.com/javase/specs/jls/se7/html/jls-3.html#jls-3.8)
   *
   */
  public static String toJavaId(String s) {
    StringBuilder sb = new StringBuilder(s);
    return Utils.replace(sb,
        "+-*/ !@#$%^&()={}[]|\\;:'\"<>,.?/",
        "_______________________________").toString();
  }
}
