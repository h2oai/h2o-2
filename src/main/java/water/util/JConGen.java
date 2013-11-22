package water.util;

import water.fvec.Frame;
import water.fvec.Vec;

public class JConGen {

  public static SB frameAsStaticVar(SB sb, Frame f, String varname, int nrows ) {
    Vec[] vecs = f.vecs();
    sb.p("public static final double[][] DATA = new double[][] {").nl();
    for (int row=0; row<nrows; row++) {
      sb.indent(1).p(row>0?",":"").p("new double[] {");
      for (int v=0; v<vecs.length;v++) sb.p(v>0?",":"").p(vecs[v].at(row));
      sb.p("}").nl();
    }
    sb.p("}").nl();
    return sb;
  }
}
