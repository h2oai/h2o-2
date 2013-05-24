package water.r.commands;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.RAny;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.*;

public class Matrix implements Invokable {
  @Override public String name() {
    return "matrix";
  }

  @Override public String[] parameters() {
    // TODO ranges instead?
    return new String[] { "key", "r1", "c1", "r2", "c2", "m", "n", "normalize" };
  }

  @Override public String[] requiredParameters() {
    return new String[] { "key" };
  }

  @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
    Key key = Key.make(Interop.asString(args[0]));
    ValueArray va = DKV.get(key).get();
    long r1 = ai.get(args, "r1", 1) - 1;
    long c1 = ai.get(args, "c1", 1) - 1;
    long r2 = ai.get(args, "r2", va._numrows);
    long c2 = ai.get(args, "c2", va._cols.length);
    int m = (int) ai.get(args, "m", r2 - r1);
    int n = (int) ai.get(args, "n", c2 - c1);
    String normalizeStr = ai.get(args, "normalize", null);
    boolean normalize = normalizeStr != null ? Boolean.parseBoolean(normalizeStr) : false;
    double[] res = new double[(int) ((c2 - c1) * (r2 - r1))];
    for( long r = r1; r < r2; r++ ) {
      long chknum = va.chknum(r);
      AutoBuffer bits = va.getChunk(chknum);
      int rowInChunk = va.rowInChunk(chknum, r);
      for( int c = (int) c1; c < c2; c++ ) {
        ValueArray.Column C = va._cols[c];
        double d = (va.isNA(bits, rowInChunk, C) ? C._mean : va.datad(bits, rowInChunk, C));
        if( normalize ) {
          d -= C._mean;
          d = (C._sigma == 0.0 || Double.isNaN(C._sigma)) ? d : d / C._sigma;
        }
        res[c] = d;
      }
    }
    String[] names = new String[(int) (c2 - c1 + 1)];
    for( int c = (int) c1; c < c2; c++ )
      names[c] =va._cols[c]._name;
    return Interop.makeDoubleVector(res, new int[] { m, n }, names);
  }
}