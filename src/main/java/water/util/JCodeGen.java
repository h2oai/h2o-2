package water.util;

import water.fvec.Frame;
import water.fvec.Vec;

public class JCodeGen {

  /** Generates data sample as a dedicated class with static <code>double[][]</code> member. */
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
   *
   * @param sb
   * @param className
   * @param values
   * @return
   */
  public static SB toClassWithArray(SB sb, String modifiers, String className, String[] values) {
    sb.i().p(modifiers!=null ? modifiers+" ": "").p("class ").p(className).p(" {").nl().ii(1);
    sb.i().p("public static final String[] VALUES = ");
    if (values==null)
      sb.p("null;").nl();
    else {
      sb.p("new String[").p(values.length).p("];").nl();

      // Static part
      int s = 0;
      int remain = values.length;
      int its = 0;
      do {
        sb.i().p("static {").ii(1).nl();
          int len = Math.min(MAX_STRINGS_IN_CONST_POOL, remain);
          toArrayFill(sb, "VALUES", values, s, len);
          s += len;
          remain -= len;
        sb.di(1).i().p("}").nl();
        if (its>0) sb.di(1).i().p('}').nl();
        if (remain>0) {
          sb.i().p("static final class ").p(className).p("_").p(its++).p(" {").ii(1).nl();
        }
      } while (remain>0);
    }
    return sb.di(1).p("}").nl();
  }

  /** Maximum number of string generated per class (static initializer) */
  public static int MAX_STRINGS_IN_CONST_POOL = 3000;

  public static SB toArrayFill(SB sb, String hmName, String[] values, int start, int len) {
    for (int i=0; i<len; i++) {
      sb.i().p(hmName).p("[").p(start+i).p("] = ").ps(values[start+i]).p(";").nl();
    }
    return sb;
  }

  public static SB toField(SB sb, String modifiers, String type, String fname, String finit) {
    sb.i().p(modifiers).s().p(type).s().p(fname);
    if (finit!=null) sb.p(" = ").p(finit);
    sb.p(";").nl();
    return sb;
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
