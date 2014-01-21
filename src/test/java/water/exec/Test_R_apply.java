package water.exec;

import static org.junit.Assert.*;
import java.io.File;

import org.junit.Rule;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.rules.ExpectedException;
import water.*;
import water.fvec.*;

public class Test_R_apply extends TestUtil {
  @BeforeClass public static void stall() { stall_till_cloudsize(1); }

  @Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test public void testRowApply() {
    Key dest = Key.make("h.hex");
    try {
      //File file = TestUtil.find_test_file("smalldata/logreg/syn_2659x1049.csv");
      //File file = TestUtil.find_test_file("smalldata/iris/iris_wheader.csv");
      File file = TestUtil.find_test_file("smalldata/drugs.csv");
      Key fkey = NFSFileVec.make(file);
      Frame in = ParseDataset2.parse(dest,new Key[]{fkey});
      UKV.remove(fkey);
      // apply pre-defined function
      checkResult("apply(h.hex,1,sum)",  sum .apply(in));
      checkResult("apply(h.hex,1,max)",  max .apply(in));
      checkResult("apply(h.hex,1,min)",  min .apply(in));
      checkResult("apply(h.hex,1,c)",    c   .apply(in));
      checkResult("apply(h.hex,1,mean)", mean.apply(in));
      checkResult("apply(h.hex,1,is.na)",isna.apply(in));
      //apply custom function
      checkResult("apply(h.hex,1,function(x){c(x)})", c.apply(in));
      checkResult("apply(h.hex,1,function(x){cap=0; fn=function(x){ifelse(x<cap,x,cap)}; cap=2; fn(x)})", new Cap(2).apply(in));
      checkResult("apply(h.hex,1,function(x){x+1})", new Add(1).apply(in));
      checkResult("apply(h.hex,1,function(x){is.na(x)?0:x})", fillna.apply(in));

    } finally {
      UKV.remove(dest);         // Remove original hex frame key
    }
  }

  abstract static class Map {
    abstract double[] map(double ds[]);
    public double[][] apply(Frame fr) {
      double out[][] = new double[(int)fr.anyVec().length()][];
      double rowin[] = new double[fr.vecs().length];
      // apply to small data
      for (int i = 0; i < (int)fr.anyVec().length(); i++) {
        for (int c = 0; c < fr.vecs().length; c++) rowin[c] = fr.vecs()[c].at((long)i);
        out[i] = map(rowin);
      }
      return out;
    }
  }

  static Map fr2ary = new Map() {double[] map(double ds[]) { double out[] = new double[ds.length];
    for (int i=0;i<ds.length;i++) out[i]=ds[i]; return out; }  };
  static Map c      = fr2ary;
  static Map fillna = new Map() {double[] map(double ds[]) { double out[] = new double[ds.length];
    for (int i=0;i<ds.length;i++) out[i]=Double.isNaN(ds[i])?0:ds[i]; return out; }  };
  static Map isna   = new Map() {double[] map(double ds[]) { double out[] = new double[ds.length];
    for (int i=0;i<ds.length;i++) out[i]=Double.isNaN(ds[i])?1:0; return out; }  };
  static Map sum    = new Map() {double[] map(double ds[]) { double out[] = new double[1        ];
    for (int i=0;i<ds.length;i++) out[0]+=ds[i]; return out; }  };
  static Map max    = new Map() {double[] map(double ds[]) { double out[] = new double[1        ];
    out[0] = Double.NEGATIVE_INFINITY;
    for (int i=0;i<ds.length;i++) out[0]=Math.max(out[0],ds[i]); return out; }  };
  static Map min    = new Map() {double[] map(double ds[]) { double out[] = new double[1        ];
    out[0] = Double.POSITIVE_INFINITY;
    for (int i=0;i<ds.length;i++) out[0]=Math.min(out[0],ds[i]); return out; }  };
  static Map mean   = new Map() {double[] map(double ds[]) { double out[] = new double[1        ];
    for (int i=0;i<ds.length;i++) out[0]+=ds[i]; out[0]=out[0]/ds.length; return out; }  };

  static final class Cap extends Map {
    final double _c;
    public Cap(double c) { _c = c; }
    double[] map(double ds[]) {
      double out[] = new double[ds.length];
      for (int i=0;i<ds.length;i++) if (ds[i] >= _c) out[i] = _c;
      return out;
    }
  }

  static final class Add extends Map {
    final double _c;
    public Add(double c) { _c = c; }
    double[] map(double ds[]) {
      double out[] = new double[ds.length];
      for (int i=0;i<ds.length;i++) out[i] = ds[i] + _c;
      return out;
    }
  } 
  void checkStr( String s ) {
    Env env=null;
    try { 
      System.out.println(s);
      env = Exec2.exec(s); 
      if( env.isAry() ) {       // Print complete frames for inspection
        Frame res = env.popAry();
        String skey = env.key();
        System.out.println(res.toStringAll());
        env.subRef(res,skey);   // But then end lifetime
      } else {
        System.out.println( env.resultString() );
      }
    } 
    catch( IllegalArgumentException iae ) { System.out.println(iae.getMessage()); }
    if( env != null ) env.remove();
  }

  void checkResult( String s, double d ) {
    Env env = Exec2.exec(s);
    assertFalse( env.isAry() );
    assertFalse( env.isFcn() );
    double res = env.popDbl();
    assertEquals(d,res,d/1e8);
    env.remove();
  }

  void checkResult( String s, double expected[][] ) {
    Env env = Exec2.exec(s);
    assertFalse( env.isDbl() );
    assertFalse( env.isFcn() );
    Frame fr = env.peekAry();
    double result[][] = fr2ary.apply(fr);
    assertEquals(result.length,expected.length);
    for(int i = 0; i < result.length; i++) {
      assertEquals(result[i].length, expected[i].length);
      for (int c = 0; c < result[i].length; c++)
        System.out.print(result[i][c]);
      assertArrayEquals(result[i], expected[i], 1E-5);
    }
    env.remove();
  }

  void checkError( String s, String err ) {
    Env env = null;
    try {
      env = Exec2.exec(s);
      env.remove();
      fail(); // Supposed to throw; reaching here is an error
    } catch ( IllegalArgumentException e ) {
      assertEquals(err, e.getMessage());
    }
  }

}
