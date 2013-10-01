package water.fvec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

import water.*;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.parser.CustomParser;
import water.parser.CustomParser.ParserType;

public class ParserTest2 extends TestUtil {
  private double[] d(double... ds) { return ds; }
  private String[] s(String...ss) { return ss; }
  private final double NaN = Double.NaN;
  private final char[] SEPARATORS = new char[] {',', ' '};

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
        double parsedVal = fr.vecs()[j].at(i);
        Assert.assertTrue((Double.isNaN(parsedVal) == Double.isNaN(expected[i][j])));
        Assert.assertTrue(Double.isNaN(expected[i][j]) || compareDoubles(expected[i][j],parsedVal,1e-5));
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
      Key k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
      Key r1 = Key.make("r1");
      ParseDataset2.parse(r1, new Key[]{k});
      testParsed(r1,exp,k);
      sb = new StringBuilder();
      for( int i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\r\n");
      k = FVecTest.makeByteVec(k.toString(),sb.toString());
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
      Key k = FVecTest.makeByteVec("ChunkBoundaries",dataset);
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
      Key k = FVecTest.makeByteVec("ChunkBoundariesMixedLineEndings",dataset);
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
      Key key = FVecTest.makeByteVec("NondecimalColumns",dataset);
      Key r = Key.make();
      ParseDataset2.parse(r,new Key[]{key});
      Frame fr = DKV.get(r).get();
      String[] cd = fr.vecs()[2]._domain;
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
      Key key = FVecTest.makeByteVec("NumberFormats",dataset);
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
      Key key = FVecTest.makeByteVec("MultipleNondecimalColumns",dataset);
      Key r = Key.make();
      ParseDataset2.parse(r,new Key[]{key});
      Frame fr = DKV.get(r).get();
      String[] cd = fr.vecs()[2]._domain;
      Assert.assertEquals("one",cd[0]);
      Assert.assertEquals("three",cd[1]);
      Assert.assertEquals("two",cd[2]);
      cd = fr.vecs()[0]._domain;
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
    Key key = FVecTest.makeByteVec("EmptyColumnValues",dataset);
    Key r = Key.make();
    ParseDataset2.parse(r,new Key[]{key});
    Frame fr = DKV.get(r).get();
    String[] cd = fr.vecs()[3]._domain;
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
      Key k = FVecTest.makeByteVec("test_"+separator,sb.toString());
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

  @Test public void testNAs() {
    String [] data = new String[]{
        "'C1Chunk',C1SChunk, 'C2Chunk', 'C2SChunk',  'C4Chunk',  'C4FChunk',  'C8Chunk',  'C8DChunk',   'Enum'\n"  +
        "0,       0.0,          0,           0,           0,          0 ,          0,   8.878979,           A \n" ,
        "1,       0.1,          1,         0.1,           1,          1 ,          1,   1.985934,           B \n" ,
        "2,       0.2,          2,         0.2,           2,          2 ,          2,   3.398018,           C \n" ,
        "3,       0.3,          3,         0.3,           3,          3 ,          3,   9.329589,           D \n" ,
        "4,       0.4,          4,           4,           4,          4 , 2147483649,   0.290184,           A \n" ,
        "0,       0.5,          0,           0,     -100000,    1.234e2 ,-2147483650,   1e-30,              B \n" ,
      "254,       0.25,      2550,       6553.4,     100000,    2.345e-2,          0,   1e30,               C \n" ,
      "   ,           ,          ,            ,            ,            ,           ,       ,                 \n" ,
      "  ?,         NA,         ?,           ?,           ?,           ?,          ?,       ?,                \n" ,
    };

    //File file = find_test_file("./smalldata/test/na_test.zip");
    //Key key = NFSFileVec.make(file);
    Key rkey = FVecTest.makeByteVec("na_test",data);
    Key okey = Key.make("na_test.hex");
    Frame fr = ParseDataset2.parse(okey, new Key[]{rkey});
    int nlines = (int)fr.numRows();
    // This variable could be declared static, except that that causes an issue
    // with the weaver trying to load these classes before a Cloud is formed.
    Class [] expectedTypes = new Class[]{C1Chunk.class,C1SChunk.class,C2Chunk.class,C2SChunk.class,C4Chunk.class,C4FChunk.class,C8Chunk.class,C8DChunk.class, C1Chunk.class};
    assertTrue(fr.numCols() == expectedTypes.length);
//    for(int i = 0; i < expectedTypes.length; ++i)
//      assertTrue("unpextected vector type, got: " + fr.vecs()[i].elem2BV(0).getClass().getSimpleName() + ", expected: " + expectedTypes[i].getSimpleName(),expectedTypes[i].isInstance(fr.vecs()[i].elem2BV(0)));
    assertEquals(9,nlines);
    for(int i = 0; i < nlines-2; ++i)
      for( Vec v : fr.vecs() )
        assertTrue("error at line "+i+", vec " + v.elem2BV(0).getClass().getSimpleName(),
                   !Double.isNaN(v.at(i)) && !v.isNA(i) );
    int j = 0;
    for( Vec v:fr.vecs() ) {
      for( int i = nlines-2; i < nlines; ++i ) {
        assertTrue(i + ", " + j + ":" + v.at(i) + ", " + v.isNA(i),
                   Double.isNaN(v.at(i)) && v.isNA(i) );
//        v.replaceNAs(1.0, 2);
//        assertTrue(!v.isNA(v.at(i)) && !v.isNA(v.at8(i)));
//        assertTrue(v.at(i) == 1.0 && v.at8(i) == 2);
//        v.setNAs(3.0, 4);
//        assertTrue(v.isNA(v.at(i)) && v.isNA(v.at8(i)));
//        assertTrue(v.at(i) == 3.0 && v.at8(i) == 4);
      }
      ++j;
    }
    UKV.remove(rkey);
    UKV.remove(okey);
  }


  @Test public void testSingleQuotes(){
    String [] data  = new String[]{"Tomas's,test\n'Tomas''s,test2',test2\nlast,'line''","s, trailing, piece'"};
    CustomParser.ParserSetup gSetup = new CustomParser.ParserSetup(ParserType.CSV, (byte)',', false);
    Key k = FVecTest.makeByteVec(Key.make().toString(), data);
    Key r1 = Key.make("single_quotes_test");
    ParseDataset2.parse(r1, new Key[]{k},gSetup);
  }
  @Test public void testSVMLight() {
    String[] data = new String[] {
        "1 2:.2 5:.5 9:.9\n",
        "-1 7:.7 8:.8 9:.9\n",
        "+1 1:.1 5:.5 6:.6\n"
    };

    double[][] exp = new double[][] {
        d( 1., .0, .2, .0, .0, .5, .0, .0, .0, .9),
        d(-1., .0, .0, .0, .0, .0, .0, .7, .8, .9),
        d( 1., .1, .0, .0, .0, .5, .6, .0, .0, .0),
    };
    String[] dataset = data;
    StringBuilder sb = new StringBuilder();
    for( int i = 0; i < dataset.length; ++i ) sb.append(dataset[i]).append("\n");
    Key k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
    Key r1 = Key.make("r1");
    ParseDataset2.parse(r1, new Key[]{k});
    testParsed(r1,exp,k);
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
    System.out.println("testNAs");
    testNAs();
    checkLeakedKeys();
    System.out.println("DONE!!!");
  }

  public static void main(String [] args) throws Exception{
    System.out.println("Running ParserTest2");
    final int nnodes = 1;
    for( int i = 1; i < nnodes; i++ ) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    new ParserTest2().testSingleQuotes();
//    File f = new File("/Users/tomasnykodym/Downloads/140k_train_anonymised.zip");
//    Key fkey = NFSFileVec.make(f);
//    ByteVec v = DKV.get(fkey).get();
//    InputStream is = v.openStream(null);
//    InputStream is2 = new FileInputStream(f);
//    byte [] buff1 = new byte[256];
//    byte [] buff2 = new byte[256];
//    while(is.available() > 0 || is2.available() > 0){
//      assert is.read() == is2.read();
//      int off = (int)(buff1.length*Math.random());
//      int maxN = buff1.length-off;
//      int len = (int)(maxN*Math.random());
//      int l1 = is.read(buff1, off, len);
//      int l2 = is2.read(buff2, off, len);
//      while(l1 < l2 && is.available() > 0)
//        l1 += is.read(buff1, off+l1, l2-l1);
//      while(l2 < l1 && is2.available() > 0)
//        l2 += is2.read(buff2, off+l2, l1-l2);
//      if(l1 != l2 || !Arrays.equals(buff1, buff2)){
//        System.out.println(Arrays.toString(buff1));
//        System.out.println(Arrays.toString(buff2));
//        assert l1 == l2;
//        assert Arrays.equals(buff1, buff2);
//      }
//    }
//    is2.close();
////    is = v.openStream(null);
////    ZipInputStream zis = new ZipInputStream(is);
////    ZipEntry ze = zis.getNextEntry(); // Get the *FIRST* entry
////    // There is at least one entry in zip file and it is not a directory.
////    assert( ze != null && !ze.isDirectory() );
////    int i = 0;
////    while(zis.read() != -1)++i;
////    System.out.println("read " + i + " bytes");
////    zis.close();
/////    System.out.println("DONE!");
//    System.out.println("==========================================================================");
//    ParseDataset2.forkParseDataset(Key.make("haha"), new Key[]{fkey}, ParseDataset2.guessSetup(fkey, new ParserSetup(), true)).get();
//    Frame f1 = DKV.get(Key.make("haha")).get();
//    Vec v = DKV.get(fkey).get();
//    System.out.println("parsed nchunks = " + f1.anyVec().nChunks() + ", raw nchunks = " + v.nChunks());
//    assert f1.anyVec().nChunks() == v.nChunks();

////    new ParserTest2().testTimeParse();
//
//
//////    new ParserTest2().testSVMLight();
////    new ParserTest().testSVMLight();
    System.out.println("DONE!");

  }
}
