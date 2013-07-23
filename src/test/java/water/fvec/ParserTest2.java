package water.fvec;

import org.junit.Assert;
import org.junit.Test;

import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;

public class ParserTest2 extends TestUtil {
  private double[] d(double... ds) { return ds; }
  private String[] s(String...ss) { return ss; }
  private final double NaN = Double.NaN;
  private final char[] SEPARATORS = new char[] {',', ' '};
  private Key k(String kname, String... data) {
    Key k = Vec.newKey(Key.make(kname));
    byte [][] chunks = new byte[data.length][];
    long [] espc = new long[data.length+1];
    for(int i = 0; i < chunks.length; ++i){
      chunks[i] = data[i].getBytes();
      espc[i+1] = espc[i] + data[i].length();
    }
    Futures fs = new Futures();
    ByteVec bv = new ByteVec(k,espc);
    DKV.put(k, bv, fs);
    for(int i = 0; i < chunks.length; ++i){
      Key chunkKey = bv.chunkKey(i);
      DKV.put(chunkKey, new Value(chunkKey,chunks[i].length,chunks[i],TypeMap.C1CHUNK,Value.ICE));
    }
    fs.blockForPending();
    return k;
  }
  public static boolean compareDoubles(double a, double b, double threshold){
    int e1 = 0;
    int e2 = 0;
    while(a > 1){
      a /= 10;
      ++e1;
    }
    while(b > 1){
       b /= 10;
       ++e2;
    }
    return ((e1 == e2) && Math.abs(a - b) < threshold);
  }
  public static void testParsed(Key k, double[][] expected, Key inputkey) {
    testParsed(k,expected,inputkey,expected.length);
  }
  public static void testParsed(Key k, double[][] expected, Key inputkey, int len) {
    Frame fr = DKV.get(k).get();
    Assert.assertEquals(len,fr.numRows());
    Assert.assertEquals(expected[0].length,fr.numCols());
    for (int i = 0; i < expected.length; ++i)
      for (int j = 0; j < fr.numCols(); ++j) {
        double parsedVal = fr._vecs[j].at(i);
        Assert.assertTrue((Double.isNaN(parsedVal) == Double.isNaN(expected[i][j])));
        Assert.assertTrue(Double.isNaN(expected[i][j]) || compareDoubles(expected[i][j],parsedVal,1e-8));
      }
    UKV.remove(k);
    UKV.remove(inputkey);
  }

  @Test public void testBasic() {
    String[] data = new String[] {
        "1|2|3\n1|2|3\n",
        "4|5|6\n",
        "4|5.2|\n",
        "asdf|qwer|1\n",
        "1.3\n",
        "1.1|2.7|3.4",
    };

    double[][] exp = new double[][] {
        d(1.0, 2.0, 3.0),
        d(1.0, 2.0, 3.0),
        d(4.0, 5.0, 6.0),
        d(4.0, 5.2, NaN),
        d(NaN, NaN, 1.0),
        d(1.3, NaN, NaN),
        d(1.1, 2.7, 3.4),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      StringBuilder sb = new StringBuilder();
      for( int i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\n");
      Key k = k(Key.make().toString(),sb.toString());
      Key r1 = Key.make("r1");
      ParseDataset2.parse(r1, new Key[]{k});
      testParsed(r1,exp,k);
      sb = new StringBuilder();
      for( int i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\r\n");
      k = k(k.toString(),sb.toString());
      Key r2 = Key.make("r2");
      ParseDataset2.parse(r2, new Key[]{k});
      testParsed(r2,exp,k);
    }
  }

  @Test public void testChunkBoundaries() {
    String[] data = new String[] {
        "1|2|3\n1|2|3\n",
        "1|2|3\n1|2", "|3\n1|1|1\n",
        "2|2|2\n2|3|", "4\n3|3|3\n",
        "3|4|5\n5",
        ".5|2|3\n5.","5|2|3\n55e-","1|2.0|3.0\n55e","-1|2.0|3.0\n55","e-1|2.0|3.0\n"

    };
    double[][] exp = new double[][] {
        d(1.0, 2.0, 3.0),
        d(1.0, 2.0, 3.0),
        d(1.0, 2.0, 3.0),
        d(1.0, 2.0, 3.0),
        d(1.0, 1.0, 1.0),
        d(2.0, 2.0, 2.0),
        d(2.0, 3.0, 4.0),
        d(3.0, 3.0, 3.0),
        d(3.0, 4.0, 5.0),
        d(5.5, 2.0, 3.0),
        d(5.5, 2.0, 3.0),
        d(5.5, 2.0, 3.0),
        d(5.5, 2.0, 3.0),
        d(5.5, 2.0, 3.0),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key k = k("ChunkBoundaries",dataset);
      Key r3 = Key.make();
      ParseDataset2.parse(r3,new Key[]{k});
      testParsed(r3,exp,k);
    }
  }

  @Test public void testChunkBoundariesMixedLineEndings() {
    String[] data = new String[] {
        "1|2|3\n4|5|6\n7|8|9",
        "\r\n10|11|12\n13|14|15",
        "\n16|17|18\r",
        "\n19|20|21\n",
        "22|23|24\n25|26|27\r\n",
        "28|29|30"
    };
    double[][] exp = new double[][] {
        d(1, 2, 3),
        d(4, 5, 6),
        d(7, 8, 9),
        d(10, 11, 12),
        d(13, 14, 15),
        d(16, 17, 18),
        d(19, 20, 21),
        d(22, 23, 24),
        d(25, 26, 27),
        d(28, 29, 30),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key k = k("ChunkBoundariesMixedLineEndings",dataset);
      Key r4 = Key.make();
      ParseDataset2.parse(r4,new Key[]{k});
      testParsed(r4,exp,k);
      checkLeakedKeys();
    }
  }

  @Test public void testNondecimalColumns() {
    String data[] = {
          "1| 2|one\n"
        + "3| 4|two\n"
        + "5| 6|three\n"
        + "7| 8|one\n"
        + "9| 10|two\n"
        + "11|12|three\n"
        + "13|14|one\n"
        + "15|16|\"two\"\n"
        + "17|18|\" four\"\n"
        + "19|20| three\n",
    };

    double[][] expDouble = new double[][] {
        d(1, 2, 1), // preserve order
        d(3, 4, 3),
        d(5, 6, 2),
        d(7, 8, 1),
        d(9, 10, 3),
        d(11,12, 2),
        d(13,14, 1),
        d(15,16, 3),
        d(17,18, 0),
        d(19,20, 2),
    };

    String[][] expString = new String[][] {
        s(null,null, "one"),
        s(null,null, "two"),
        s(null,null, "three"),
        s(null,null, "one"),
        s(null,null, "two"),
        s(null,null, "three"),
        s(null,null, "one"),
        s(null,null, "two"),
        s(null,null, " four"),
        s(null,null, "three"),
    };

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key key = k("NondecimalColumns",dataset);
      Key r = Key.make();
      ParseDataset2.parse(r,new Key[]{key});
      Frame fr = DKV.get(r).get();
      String[] cd = fr._vecs[2]._domain;
      Assert.assertEquals(" four",cd[0]);
      Assert.assertEquals("one",cd[1]);
      Assert.assertEquals("three",cd[2]);
      Assert.assertEquals("two",cd[3]);
      testParsed(r, expDouble,key);
    }
    checkLeakedKeys();
  }

  @Test public void testNumberFormats(){
    String [] data = {"+.6e102|+.7e102|+.8e102\n.6e102|.7e102|.8e102\n"};
    double[][] expDouble = new double[][] {
        d(+.6e102,.7e102,.8e102), // preserve order
        d(+.6e102, +.7e102,+.8e102),
    };
    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key key = k("NumberFormats",dataset);
      Key r = Key.make();
      ParseDataset2.parse(r,new Key[]{key});
      testParsed(r, expDouble,key);
    }
  }
 @Test public void testMultipleNondecimalColumns() {
    String data[] = {
        "foo| 2|one\n"
      + "bar| 4|two\n"
      + "foo| 6|three\n"
      + "bar| 8|one\n"
      + "bar|ten|two\n"
      + "bar| 12|three\n"
      + "foobar|14|one\n",
    };
    double[][] expDouble = new double[][] {
        d(1, 2, 0), // preserve order
        d(0, 4, 2),
        d(1, 6, 1),
        d(0, 8, 0),
        d(0, NaN, 2),
        d(0, 12, 1),
        d(2, 14, 0),
    };


    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key key = k("MultipleNondecimalColumns",dataset);
      Key r = Key.make();
      ParseDataset2.parse(r,new Key[]{key});
      Frame fr = DKV.get(r).get();
      String[] cd = fr._vecs[2]._domain;
      Assert.assertEquals("one",cd[0]);
      Assert.assertEquals("three",cd[1]);
      Assert.assertEquals("two",cd[2]);
      cd = fr._vecs[0]._domain;
      Assert.assertEquals("bar",cd[0]);
      Assert.assertEquals("foo",cd[1]);
      Assert.assertEquals("foobar",cd[2]);
      testParsed(r, expDouble,key);
    }
  }


  // Test if the empty column is correctly handled.
  // NOTE: this test makes sense only for comma separated columns
  @Test public void testEmptyColumnValues() {
    String data[] = {
        "1,2,3,foo\n"
      + "4,5,6,bar\n"
      + "7,,8,\n"
      + ",9,10\n"
      + "11,,,\n"
      + "0,0,0,z\n"
      + "0,0,0,z\n"
      + "0,0,0,z\n"
      + "0,0,0,z\n"
      + "0,0,0,z\n"
    };
    double[][] expDouble = new double[][] {
        d(1, 2, 3, 1),
        d(4, 5, 6, 0),
        d(7, NaN, 8, NaN),
        d(NaN, 9, 10, NaN),
        d(11, NaN, NaN, NaN),
        d(0, 0, 0, 2),
        d(0, 0, 0, 2),
        d(0, 0, 0, 2),
        d(0, 0, 0, 2),
        d(0, 0, 0, 2),
    };

    final char separator = ',';

    String[] dataset = getDataForSeparator(separator, data);
    Key key = k("EmptyColumnValues",dataset);
    Key r = Key.make();
    ParseDataset2.parse(r,new Key[]{key});
    Frame fr = DKV.get(r).get();
    String[] cd = fr._vecs[3]._domain;
    Assert.assertEquals("bar",cd[0]);
    Assert.assertEquals("foo",cd[1]);
    testParsed(r, expDouble,key);
  }


  @Test public void testBasicSpaceAsSeparator() {
    String[] data = new String[] {
        " 1|2|3",
        " 4 | 5 | 6",
        "4|5.2 ",
        "asdf|qwer|1",
        "1.1",
        "1.1|2.1|3.4",
    };
    double[][] exp = new double[][] {
        d(1.0, 2.0, 3.0),
        d(4.0, 5.0, 6.0),
        d(4.0, 5.2, NaN),
        d(NaN, NaN, 1.0),
        d(1.1, NaN, NaN),
        d(1.1, 2.1, 3.4),
    };
    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      int i = 0;
      StringBuilder sb = new StringBuilder();
      for( i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\n");
      Key k = k("test_"+separator,sb.toString());
      Key r5 = Key.make();
      ParseDataset2.parse(r5, new Key[]{k});
      testParsed(r5, exp,k);
    }
  }

  String[] getDataForSeparator(char sep, String[] data) {
    return getDataForSeparator('|', sep, data);
  }
  String[] getDataForSeparator(char placeholder, char sep, String[] data) {
    String[] result = new String[data.length];
    for (int i = 0; i < data.length; i++)
      result[i] = data[i].replace(placeholder, sep);
    return result;
  }

  @Test public void testTimeParse() {
    Key fkey = NFSFileVec.make(find_test_file("smalldata/kaggle/bestbuy_train_10k.csv.gz"));
    Key okey = Key.make("bestbuy.hex");
    ParseDataset2.parse(okey,new Key[]{fkey});
    UKV.remove(fkey);
    UKV.remove(okey);
  }

  @Test public void testMixedSeps() {
    double[][] exp = new double[][] {
      d(NaN,   1,   1),
      d(NaN,   2, NaN),
      d(  3, NaN,   3),
      d(  4, NaN, NaN),
      d(NaN, NaN, NaN),
      d(NaN, NaN, NaN),
      d(NaN, NaN,   6),
    };
    Key fkey = NFSFileVec.make(find_test_file("smalldata/test/is_NA.csv"));
    Key okey = Key.make("NA.fvec");
    ParseDataset2.parse(okey,new Key[]{fkey});
    testParsed(okey,exp,fkey,25);
  }

  void runTests(){
    System.out.println("testBasic");
    testBasic();
    System.out.println("testBasicSpaceAsSeparator");
    testBasicSpaceAsSeparator();
    System.out.println("testChunkBoundaries");
    testChunkBoundaries();
    System.out.println("testChunkBoundariesMixedLineEndings");
    testChunkBoundariesMixedLineEndings();
    System.out.println("testEmptyColumnValues");
    testEmptyColumnValues();
    System.out.println("testMixedSeps");
    testMixedSeps();
    System.out.println("testMultipleNondecimalColumns");
    testMultipleNondecimalColumns();
    System.out.println("testNondecimalColumns");
    testNondecimalColumns();
    System.out.println("testNumberFormats");
    testNumberFormats();
    System.out.println("testTimeParse");
    testTimeParse();
    checkLeakedKeys();
    System.out.println("DONE!!!");
  }

  public static void main(String [] args) throws Exception{
    System.out.println("Running ParserTest2");
    final int nnodes = 3;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    System.out.println("Cloud formed");
    new ParserTest2().runTests();
  }
}
