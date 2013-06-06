package water.r.commands;

import r.builtins.CallFactory.ArgumentInfo;
import r.data.*;
import r.ifc.Interop;
import r.ifc.Interop.Invokable;
import water.*;

public class VAWrite implements Invokable {
  @Override public String name() {
    return "va.write";
  }

  @Override public String[] parameters() {
    return new String[] { "key", "data" };
  }

  @Override public String[] requiredParameters() {
    return new String[] { "key", "data" };
  }

  // TODO merge with TestUtils version
  @Override public RAny invoke(ArgumentInfo ai, RAny[] args) {
    Key key = Key.make(Interop.asString(args[0]));
    RArray data = (RArray) args[1];
    int m = data.dimensions()[0];
    int n = data.dimensions()[1];
    UKV.remove(key);
    // Gather basic column info, 1 column per array
    ValueArray.Column cols[] = new ValueArray.Column[n];
    char off = 0;
    for( int i = 0; i < n; i++ ) {
      ValueArray.Column col = cols[i] = new ValueArray.Column();
      col._name = Integer.toString(i);
      col._off = off;
      col._scale = 1;
      col._min = Double.MAX_VALUE;
      col._max = Double.MIN_VALUE;
      col._mean = 0.0;
      if( args[1] instanceof RInt ) {
        col._size = 4;
        col._n = m;
      } else if( args[1] instanceof RDouble ) {
        col._size = -8;
        col._n = m;
      } else {
        throw H2O.unimpl();
      }
      off += Math.abs(col._size);
    }

    int rowsize = off;
    ValueArray ary = new ValueArray(key, m, rowsize, cols);
    int r = 0;

    for( int chunk = 0; chunk < ary.chunks(); chunk++ ) {
      // Compact data into VA format, and compute min/max/mean
      int rpc = ary.rpc(chunk);
      int limit = r + rpc;
      AutoBuffer ab = new AutoBuffer(rpc * rowsize);

      for( ; r < limit; r++ ) {
        for( int c = 0; c < n; c++ ) {
          ValueArray.Column col = cols[c];
          int i;
          double d;
          // @formatter:off
          switch( col._size ) {
            case  4: ab.put4( i = ((RInt)    data).getInt(   r + c * m)); d = i; break;
            case -8: ab.put8d(d = ((RDouble) data).getDouble(r + c * m));        break;
            default:
              throw H2O.unimpl();
          }
          // @formatter:on
          if( d > col._max ) col._max = d;
          if( d < col._min ) col._min = d;
          col._mean += d;
        }
      }

      Key ckey = ary.getChunkKey(chunk);
      DKV.put(ckey, new Value(ckey, ab.bufClose()));
    }

    // Sum to mean
    for( ValueArray.Column col : cols )
      col._mean /= col._n;

    // 2nd pass for sigma. Sum of squared errors, then divide by n and sqrt
    for( r = 0; r < m; r++ ) {
      for( int c = 0; c < n; c++ ) {
        ValueArray.Column col = cols[c];
        double d;
        // @formatter:off
        switch( col._size ) {
          case  4: d = ((RInt)    data).getInt(   r + c * m); break;
          case -8: d = ((RDouble) data).getDouble(r + c * m); break;
          default: throw H2O.unimpl();
        }
        // @formatter:on
        col._sigma += (d - col._mean) * (d - col._mean);
      }
    }
    // RSS to sigma
    for( ValueArray.Column col : cols )
      col._sigma = Math.sqrt(col._sigma / col._n);

    // Write out data & keys
    DKV.put(key, ary);
    DKV.write_barrier();
    return Interop.asRString(key.toString());
  }
}