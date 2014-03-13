package hex;

import org.junit.BeforeClass;
import org.junit.Test;
import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

import static org.junit.Assert.assertEquals;

public class Summary2Test extends TestUtil {
  @BeforeClass
  public static void stall() { stall_till_cloudsize(3); }

  @Test public void testConstColumn() {
    Key key = Key.make("testConst.hex");
    Frame fr = parseFrame(key, "./smalldata/constantColumn.csv");

    Futures fs = new Futures();
    for( Vec vec : fr.vecs()) vec.rollupStats(fs);
    fs.blockForPending();

    Vec vec = fr.vecs()[0];
    Summary2.BasicStat basicStat = new Summary2.PrePass().doAll(fr).finishUp()._basicStats[0];
    Summary2 s = new Summary2(vec, "",basicStat);
    s.add(vec.chunkForRow(0));
    for (int i = 1; i < vec.nChunks(); i++) {
      Summary2 s1 = new Summary2(vec, "", basicStat); s1.add(vec.chunkForRow(i)); s.add(s1);
    }
    s.finishUp(vec);
    assertEquals(1, s.hcnt.length);
    assertEquals(528, s.hcnt[0]);
    for (double pv : s._pctile)
      assertEquals(0.1, pv, 0.00001);

    fr.delete();
  }

  @Test public void testEnumColumn() {
    Key key = Key.make("cars.hex");
    Frame fr = parseFrame(key, "./smalldata/cars.csv");
    Futures fs = new Futures();
    for( Vec vec : fr.vecs()) vec.rollupStats(fs);
    fs.blockForPending();
    Vec vec = fr.vecs()[fr.find("name")];
    Summary2.BasicStat basicStat = new Summary2.PrePass().doAll(fr).finishUp()._basicStats[fr.find("name")];
    Summary2 s = new Summary2(vec, "", basicStat);
    s.add(vec.chunkForRow(0));
    for( int i = 1; i < vec.nChunks(); i++ )
      { Summary2 s1 = new Summary2(vec, "", basicStat); s1.add(vec.chunkForRow(i)); s.add(s1); }
    s.finishUp(vec);

    assertEquals(306, s.hcnt.length);
    fr.delete();
  }

  @Test public void testIntColumn() {
    Key key = Key.make("cars.hex");
    Frame fr = parseFrame(key, "./smalldata/cars.csv");
    Futures fs = new Futures();
    for( Vec vec : fr.vecs()) vec.rollupStats(fs);
    fs.blockForPending();
    Vec vec = fr.vecs()[fr.find("cylinders")];
    Summary2.BasicStat basicStat = new Summary2.PrePass().doAll(fr).finishUp()._basicStats[fr.find("cylinders")];
    Summary2 s = new Summary2(vec, "", basicStat);
    s.add(vec.chunkForRow(0));
    for( int i = 1; i < vec.nChunks(); i++ )
      { Summary2 s1 = new Summary2(vec, "", basicStat); s1.add(vec.chunkForRow(i)); s.add(s1); }
    s.finishUp(vec);

    assertEquals(0, s.hcnt[4]); // no 7 cylinder cars
    // kbn 2/28. 1% quantile for 0 should expect 4
    // I changed Summary2 to be .1%, 1% ...99%, 99.9% quantiles. So answer is 3 for [0]
    assertEquals(3, (int)s._pctile[0]);
    assertEquals(8, (int)s._pctile[s._pctile.length - 1]);
    fr.delete();
  }

  public static void main(String[] args) throws Exception {
    water.Boot.main(Summary2Test.class, args);
  }

  public static void userMain(String[] args) throws Exception {
    H2O.main(args);
    Summary2Test test = new Summary2Test();
    test.testConstColumn();
    test.testEnumColumn();
    test.testIntColumn();
    System.exit(0);
  }
}

