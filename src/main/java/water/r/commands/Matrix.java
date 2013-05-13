package water.r.commands;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.*;
import r.data.RArray.Names;
import r.data.internal.DoubleImpl;
import r.data.internal.ScalarDoubleImpl;
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
    long r1 = get(ai, args, 1, 1) - 1;
    long c1 = get(ai, args, 2, 1) - 1;
    long r2 = get(ai, args, 3, va._numrows);
    long c2 = get(ai, args, 4, va._cols.length);
    int m = (int) get(ai, args, 5, r2 - r1);
    int n = (int) get(ai, args, 6, c2 - c1);
    String normalizeStr = ai.provided(parameters()[7]) ? Interop.asString(args[7]) : null;
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
    RSymbol[] names = new RSymbol[(int) (c2 - c1 + 1)];
    for( int c = (int) c1; c < c2; c++ )
      names[c] = RSymbol.getSymbol(va._cols[c]._name);
    return new DoubleImpl(res, new int[] { m, n }, Names.create(names));
  }

  private long get(ArgumentInfo ai, RAny[] args, int i, long def) {
    String[] names = parameters();
    int pos = ai.position(names[i]);
    return pos >= 0 ? (long) ((ScalarDoubleImpl) (args[pos])).getDouble() : def;
  }
}