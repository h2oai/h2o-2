package water.fvec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Assert;
import org.junit.Test;

import water.*;
import water.api.FrameSplitPage;
import water.api.ImportFiles2;
import water.api.Parse2;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.parser.CustomParser;
import water.parser.GuessSetup;
import water.util.Log;

public class ParserTest2 extends TestUtil {
  private double[] d(double... ds) { return ds; }
  private final double NaN = Double.NaN;
  private final char[] SEPARATORS = new char[] {',', ' '};

  public static boolean compareDoubles(double a, double b, double threshold){
    if( a==b ) return true;
    if( ( Double.isNaN(a) && !Double.isNaN(b)) ||
        (!Double.isNaN(a) &&  Double.isNaN(b)) ) return false;
    if( Double.isInfinite(a) || Double.isInfinite(b) ) return false;
    return Math.abs(a-b)/Math.max(Math.abs(a),Math.abs(b)) < threshold;
  }
  public static void testParsed(Key inputKey, double[][] expected) {
    testParsed(ParseDataset2.parse(Key.make("parsed"), new Key[]{inputKey}),expected, expected.length);
  }

  public static void testParsed(Frame fr, double[][] expected, int len) {
    try {
      Assert.assertEquals(len,fr.numRows());
      Assert.assertEquals(expected[0].length,fr.numCols());
      for (int i = 0; i < expected.length; ++i)
        for (int j = 0; j < fr.numCols(); ++j) {
          double parsedVal = fr.vecs()[j].at(i);
          Assert.assertTrue((Double.isNaN(parsedVal) == Double.isNaN(expected[i][j])));
          Assert.assertTrue("Frame "+fr._key+", row="+i+", col="+j+", expect="+expected[i][j]+", found="+parsedVal,
                            Double.isNaN(expected[i][j]) || compareDoubles(expected[i][j],parsedVal,1e-5));
        }
    } finally {
      fr.delete();
    }
  }

  @Test public void testOverflow(){
    String [] data = new String[]{
      "-5.81504",
      "-4.77862e-13",
      "3.42349",
    };
    double [][] exp = new double[][]{{-5.81504},{-4.77862e-13},{3.42349}};
    StringBuilder sb = new StringBuilder();
    for( String s : data ) sb.append(s).append("\n");
    Key k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
    testParsed(k,exp);
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
      for( String s : dataset ) sb.append(s).append("\n");
      Key k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
      testParsed(k,exp);
      sb = new StringBuilder();
      for( String s : dataset ) sb.append(s).append("\r\n");
      k = FVecTest.makeByteVec(k.toString(),sb.toString());
      testParsed(k,exp);
    }
  }

  // test correctness of sparse chunks
  // added after failing to encode properly following data as
  // 0s were not considered when computing compression strategy and then
  // lemin was 6108 and there was Short overflow when encoding zeros.
  // So, the first column was compressed into C2SChunk with 0s causing short overflow,
  @Test public void testSparse2(){
    String data =
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "35351, 0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "6108,  0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "35351, 0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "6334,  0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n" +
        "0,     0,0,0,0,0\n";

    double[][] exp = new double[][] {
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(35351,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(6108,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(35351,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(6334,0,0,0,0,0),
        d(0,0,0,0,0,0),
        d(0,0,0,0,0,0),
    };
    Key k = FVecTest.makeByteVec(Key.make().toString(),data);
    testParsed(k,exp);

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
      testParsed(k,exp);
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
      testParsed(k, exp);
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

    for (char separator : SEPARATORS) {
      String[] dataset = getDataForSeparator(separator, data);
      Key key = FVecTest.makeByteVec("NondecimalColumns",dataset);
      Frame fr = ParseDataset2.parse(Key.make(),new Key[]{key});
      String[] cd = fr.vecs()[2]._domain;
      Assert.assertEquals(" four",cd[0]);
      Assert.assertEquals("one",cd[1]);
      Assert.assertEquals("three",cd[2]);
      Assert.assertEquals("two",cd[3]);
      testParsed(fr, expDouble,expDouble.length);
    }
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
      testParsed(key,expDouble);
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
      Frame fr = ParseDataset2.parse(r,new Key[]{key});
      String[] cd = fr.vecs()[2]._domain;
      Assert.assertEquals("one",cd[0]);
      Assert.assertEquals("three",cd[1]);
      Assert.assertEquals("two",cd[2]);
      cd = fr.vecs()[0]._domain;
      Assert.assertEquals("bar",cd[0]);
      Assert.assertEquals("foo",cd[1]);
      Assert.assertEquals("foobar",cd[2]);
      testParsed(fr, expDouble,expDouble.length);
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
    Frame fr = ParseDataset2.parse(r,new Key[]{key});
    String[] cd = fr.vecs()[3]._domain;
    Assert.assertEquals("bar",cd[0]);
    Assert.assertEquals("foo",cd[1]);
    testParsed(fr, expDouble, expDouble.length);
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
      StringBuilder sb = new StringBuilder();
      for( String s : dataset ) sb.append(s).append("\n");
      Key k = FVecTest.makeByteVec("test_"+separator,sb.toString());
      testParsed(k,exp);
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
    testParsed(ParseDataset2.parse(Key.make(),new Key[]{fkey}),exp,25);
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
//      assertTrue("unpextected vector type, got: " + fr.vecs()[i].chunkForChunkIdx(0).getClass().getSimpleName() + ", expected: " + expectedTypes[i].getSimpleName(),expectedTypes[i].isInstance(fr.vecs()[i].chunkForChunkIdx(0)));
    assertEquals(9,nlines);
    for(int i = 0; i < nlines-2; ++i)
      for( Vec v : fr.vecs() )
        assertTrue("error at line "+i+", vec " + v.chunkForChunkIdx(0).getClass().getSimpleName(),
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
    fr.delete();
  }


//  @Test public void testSingleQuotes(){
//    String [] data  = new String[]{"Tomas's,test\n'Tomas''s,test2',test2\nlast,'line''","s, trailing, piece'"};
//    CustomParser.ParserSetup gSetup = new CustomParser.ParserSetup(ParserType.CSV, (byte)',', false);
//    Key k = FVecTest.makeByteVec(Key.make().toString(), data);
//    Key r1 = Key.make("single_quotes_test");
//    ParseDataset2.parse(r1, new Key[]{k},gSetup);
//  }
  @Test public void testSingleQuotes() {
    Key k = Key.make("q.hex");
    try {
      Frame fr = TestUtil.parseFrame(k,"smalldata/test/test_quote.csv");
      Assert.assertEquals(fr.numCols(),11);
      Assert.assertEquals(fr.numRows(), 7);
    } finally {
      Lockable.delete(k);
    }
  }

  @Test public void testSVMLight() {
    String[] data = new String[] {
        "1 2:.2 5:.5 9:.9\n-1 1:.1 4:.4 8:.8\n",
        "1 2:.2 5:.5 9:.9\n1 3:.3 6:.6\n",
        "-1 7:.7 8:.8 9:.9\n1 20:2.\n",
        "+1 1:.1 5:.5 6:.6 10:1\n1 19:1.9\n",
      "1 2:.2 5:.5 9:.9\n-1 1:.1 4:.4 8:.8\n",
      "1 2:.2 5:.5 9:.9\n1 3:.3 6:.6\n",
      "-1 7:.7 8:.8 9:.9\n1 20:2.\n",
      "+1 1:.1 5:.5 6:.6 10:1\n1 19:1.9\n",
      "1 2:.2 5:.5 9:.9\n-1 1:.1 4:.4 8:.8\n",
      "1 2:.2 5:.5 9:.9\n1 3:.3 6:.6\n",
      "-1 7:.7 8:.8 9:.9\n1 20:2.\n",
      "+1 1:.1 5:.5 6:.6 10:1\n1 19:1.9\n"
    };

    double[][] exp = new double[][] {
        d(  1., .0, .2, .0, .0, .5, .0, .0, .0, .9, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
        d( -1., .1, .0, .0, .4, .0, .0, .0, .8, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
        d(  1., .0, .2, .0, .0, .5, .0, .0, .0, .9, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
        d(  1., .0, .0, .3, .0, .0, .6, .0, .0, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
        d( -1., .0, .0, .0, .0, .0, .0, .7, .8, .9, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
        d(  1., .0, .0, .0, .0, .0, .0, .0, .0, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0,2.0),
        d(  1., .1, .0, .0, .0, .5, .6, .0, .0, .0, 1, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
        d(  1., .0, .0, .0, .0, .0, .0, .0, .0, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0,1.9, .0),
      d(  1., .0, .2, .0, .0, .5, .0, .0, .0, .9, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d( -1., .1, .0, .0, .4, .0, .0, .0, .8, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d(  1., .0, .2, .0, .0, .5, .0, .0, .0, .9, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d(  1., .0, .0, .3, .0, .0, .6, .0, .0, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d( -1., .0, .0, .0, .0, .0, .0, .7, .8, .9, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d(  1., .0, .0, .0, .0, .0, .0, .0, .0, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0,2.0),
      d(  1., .1, .0, .0, .0, .5, .6, .0, .0, .0, 1, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d(  1., .0, .0, .0, .0, .0, .0, .0, .0, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0,1.9, .0),
      d(  1., .0, .2, .0, .0, .5, .0, .0, .0, .9, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d( -1., .1, .0, .0, .4, .0, .0, .0, .8, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d(  1., .0, .2, .0, .0, .5, .0, .0, .0, .9, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d(  1., .0, .0, .3, .0, .0, .6, .0, .0, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d( -1., .0, .0, .0, .0, .0, .0, .7, .8, .9, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d(  1., .0, .0, .0, .0, .0, .0, .0, .0, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0, .0,2.0),
      d(  1., .1, .0, .0, .0, .5, .6, .0, .0, .0, 1, 0, 0, 0, 0, 0, 0, 0, 0, .0, .0),
      d(  1., .0, .0, .0, .0, .0, .0, .0, .0, .0, 0, 0, 0, 0, 0, 0, 0, 0, 0,1.9, .0),
    };
    Key k = FVecTest.makeByteVec(Key.make("svmtest_bits").toString(),data);
    testParsed(k,exp);
  }

  // Test very sparse data
  @Test public void testSparse() {
    for (char separator : SEPARATORS) {

      // Build 100 zero's and 1 one.
      double[][] exp = new double[101][1];
      exp[50][0] = 1;
      StringBuilder sb = new StringBuilder();
      for( int i=0; i<50; i++ ) sb.append("0.0\n");
      sb.append("1.0\n");
      for( int i=0; i<50; i++ ) sb.append("0.0\n");
      Key k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
      testParsed(k,exp);

      // Build 100 zero's and 1 non-zero.
      exp = new double[101][1];
      exp[50][0] = 2;
      sb = new StringBuilder();
      for( int i=0; i<50; i++ ) sb.append("0\n");
      sb.append("2\n");
      for( int i=0; i<50; i++ ) sb.append("0\n");
      k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
      testParsed(k,exp);

      // Build 100 zero's and some non-zeros.  Last line is truncated.
      exp = new double[101][2];
      exp[ 50][0] = 2;
      exp[ 50][1] = 3;
      exp[100][0] = 0;          // Truncated final line
      exp[100][1] = Double.NaN;
      sb = new StringBuilder();
      for( int i=0; i<50; i++ ) sb.append("0,0\n");
      sb.append("2,3\n");
      for( int i=0; i<49; i++ ) sb.append("0,0\n");
      sb.append("0");           // Truncated final line
      k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
      testParsed(k,exp);

      // Build 100000 zero's and some one's
      sb = new StringBuilder();
      exp = new double[100100][1];
      for( int i=0; i<100; i++ ) {
        for( int j=0; j<1000; j++ )
          sb.append("0\n");
        sb.append("1\n");
        exp[i*1001+1000][0]=1;
      }
      k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
      testParsed(k,exp);

      // Build 100 zero's, then 100 mix of -1001 & 1001's (to force a
      // sparse-short, that finally inflates to a full dense-short).
      sb = new StringBuilder();
      for( int i=0; i<100; i++ ) sb.append("0\n");
      for( int i=0; i<100; i+=2 ) sb.append("-1001\n1001\n");
      exp = new double[200][1];
      for( int i=0; i<100; i+=2 ) { exp[i+100][0]=-1001; exp[i+101][0]= 1001; }
      k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
      testParsed(k,exp);

      // Build 100 zero's and 1 double.
      exp = new double[101][1];
      exp[50][0] = 1.23456789;
      sb = new StringBuilder();
      for( int i=0; i<50; i++ ) sb.append("0.0\n");
      sb.append("1.23456789\n");
      for( int i=0; i<50; i++ ) sb.append("0.0\n");
      k = FVecTest.makeByteVec(Key.make().toString(),sb.toString());
      testParsed(k,exp);
    }
  }

  // Mix of NA's, very large & very small, ^A Hive-style seperator, comments, labels
  @Test public void testParseMix() {
    double[][] exp = new double[][] {
      d( 0      ,  0.5    ,  1      , 0),
      d( 3      ,  NaN    ,  4      , 1),
      d( 6      ,  NaN    ,  8      , 0),
      d( 0.6    ,  0.7    ,  0.8    , 1),
      d(+0.6    , +0.7    , +0.8    , 0),
      d(-0.6    , -0.7    , -0.8    , 1),
      d(  .6    ,   .7    ,   .8    , 0),
      d(+ .6    ,  +.7    ,  +.8    , 1),
      d(- .6    ,  -.7    ,  -.8    , 0),
      d(+0.6e0  , +0.7e0  , +0.8e0  , 1),
      d(-0.6e0  , -0.7e0  , -0.8e0  , 0),
      d(  .6e0  ,   .7e0  ,   .8e0  , 1),
      d(+ .6e0  ,  +.7e0  ,  +.8e0  , 0),
      d( -.6e0  ,  -.7e0  ,  -.8e0  , 1),
      d(+0.6e00 , +0.7e00 , +0.8e00 , 0),
      d(-0.6e00 , -0.7e00 , -0.8e00 , 1),
      d(  .6e00 ,   .7e00 ,   .8e00 , 0),
      d( +.6e00 ,  +.7e00 ,  +.8e00 , 1),
      d( -.6e00 ,  -.7e00 ,  -.8e00 , 0),
      d(+0.6e-01, +0.7e-01, +0.8e-01, 1),
      d(-0.6e-01, -0.7e-01, -0.8e-01, 0),
      d(  .6e-01,   .7e-01,   .8e-01, 1),
      d( +.6e-01,  +.7e-01,  +.8e-01, 0),
      d( -.6e-01,  -.7e-01,  -.8e-01, 1),
      d(+0.6e+01, +0.7e+01, +0.8e+01, 0),
      d(-0.6e+01, -0.7e+01, -0.8e+01, 1),
      d(  .6e+01,   .7e+01,   .8e+01, 0),
      d( +.6e+01,  +.7e+01,  +.8e+01, 1),
      d( -.6e+01,  -.7e+01,  -.8e+01, 0),
      d(+0.6e102, +0.7e102, +0.8e102, 1),
      d(-0.6e102, -0.7e102, -0.8e102, 0),
      d(  .6e102,   .7e102,   .8e102, 1),
      d( +.6e102,  +.7e102,  +.8e102, 0),
      d( -.6e102,  -.7e102,  -.8e102, 1)
    };
    testParsed(TestUtil.parseFrame(null,"smalldata/test/test_parse_mix.csv"),exp,exp.length);
  }

  // Test of parsing numbers with many digits
  @Test public void testParseManyDigits1() {
    String pows10 = 
      "1\n"+
      "10\n"+
      "100\n"+
      "1000\n"+
      "10000\n"+
      "100000\n"+
      "1000000\n"+
      "10000000\n"+
      "100000000\n"+
      "1000000000\n"+
      "10000000000\n"+
      "100000000000\n"+
      "1000000000000\n"+
      "10000000000000\n"+
      "100000000000000\n"+
      "1000000000000000\n"+
      "10000000000000000\n"+
      "100000000000000000\n"+
      "1000000000000000000\n"+
      "10000000000000000000\n"+
      "100000000000000000000\n"+
      "1000000000000000000000\n"+
      "10000000000000000000000\n"+
      "100000000000000000000000\n";
    double[][] pows10_exp = new double[][] {
      d(1e0 ), d(1e1 ), d(1e2 ), d(1e3 ), d(1e4 ), d(1e5 ), d(1e6 ), d(1e7 ), d(1e8 ), d(1e9 ),
      d(1e10), d(1e11), d(1e12), d(1e13), d(1e14), d(1e15), d(1e16), d(1e17), d(1e18), d(1e19),
      d(1e20), d(1e21), d(1e22), d(1e23),
    };
    Key k = FVecTest.makeByteVec("test_digits.csv",pows10);
    testParsed(k,pows10_exp);
  }

  // Test of parsing numbers with many digits
  @Test public void testParseManyDigits2() {
    String pows10 = 
      "9\n"+
      "99\n"+
      "999\n"+
      "9999\n"+
      "99999\n"+
      "999999\n"+
      "9999999\n"+
      "99999999\n"+
      "999999999\n"+
      "9999999999\n"+
      "99999999999\n"+
      "999999999999\n"+
      "9999999999999\n"+
      "99999999999999\n"+
      "999999999999999\n"+
      "9999999999999999\n"+
      "99999999999999999\n"+
      "999999999999999999\n"+
      "9999999999999999999\n"+
      "99999999999999999999\n"+
      "999999999999999999999\n"+
      "9999999999999999999999\n"+
      "99999999999999999999999\n"+
      "999999999999999999999999\n";
    double[][] pows10_exp = new double[][] {
      d(9L),
      d(99L),
      d(999L),
      d(9999L),
      d(99999L),
      d(999999L),
      d(9999999L),
      d(99999999L),
      d(999999999L),
      d(9999999999L),
      d(99999999999L),
      d(999999999999L),
      d(9999999999999L),
      d(99999999999999L),
      d(999999999999999L),
      d(9999999999999999L),
      d(99999999999999999L),
      d(999999999999999999L),
      d(9.99999999999999999e18),
      d(9.99999999999999999e19),
      d(9.99999999999999999e20),
      d(9.99999999999999999e21),
      d(9.99999999999999999e22),
      d(9.99999999999999999e23),
    };
    Key k = FVecTest.makeByteVec("test_digits.csv",pows10);
    testParsed(k,pows10_exp);
  }

  // Test of parsing numbers with many digits
  @Test public void testParseManyDigits3() {
    String pows10 = 
      "0.00000000000001\n"+
      "1000001\n"+
      "2000001\n"+
      "";
    double[][] pows10_exp = new double[][] {
      d(1e-14),
      d(1000001L),
      d(2000001L),
    };
    Key k = FVecTest.makeByteVec("test_digits.csv",pows10);
    testParsed(k,pows10_exp);
  }

  // Test of parsing numbers with many digits
  @Test public void testParseManyDigits4() {
    String pows10 = 
      "3\n"+
      "1e-18\n"+
      "1e-34\n"+
      "";
    double[][] pows10_exp = new double[][] {
      d(3),
      d(1e-18),
      d(1e-34),
    };
    Key k = FVecTest.makeByteVec("test_digits.csv",pows10);
    testParsed(k,pows10_exp);
  }

  @Test public void testMultiFileParseSmall(){
    Key [] files = null,files2 = null;
    Frame f1 = null,f2 = null, f3=null;
    try {
      files = ImportFiles2.importPath("smalldata/parse_folder_test");
      // test we can run guess setup task (run by gui)
      new GuessSetup.GuessSetupTsk(new CustomParser.ParserSetup(),true).invoke(files);
      f1 = ParseDataset2.parse(Key.make("multifile"), files);
      files2 = ImportFiles2.importPath("smalldata/glm_test/prostate_cat_replaced.csv");
      f3 = ParseDataset2.parse(Key.make("singleFile"), files2);
      f2 = f1.makeCompatible(f3);
      DKV.put(f2._key,f2); // annoyingly, have to put the frame back into KV
      // can not assert on bit-identity, enums will generally have different types depending on how they were created,
      // since during parse they are first numbered racily (so the initial compression does not have to be optimal!)
      // and then renumbered later, but not recompressed (if they fit within the original compression scheme)
      assertTrue(f1.isIdentical(f2));
    } finally {
      if(f1 != null)f1.delete();
      if(f2 != null)f2.delete();
      if(f3 != null)f3.delete();
      if(files != null) for(Key k:files) {
        Frame f = UKV.get(k);
        if (f!=null) f.delete();
      }
      if(files2 != null) for(Key k:files2) {
        Frame f = UKV.get(k);
        if (f!=null) f.delete();
      }
    }
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
    System.out.println("testNAs");
    testNAs();
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
    new ParserTest2  ().runTests();
    new ParseTimeTest().runTests();
    System.out.println("DONE!");
  }

  public static class ParseAllSmalldata {
    @Test
    public void run() {
      //for i in `find smalldata -type f`; do echo \"$i\",; done
      String[] files = new String[]{
              "smalldata/1_100kx7_logreg.data.gz",
              "smalldata/2_100kx7_logreg.data.gz",
              "smalldata/Abalone.gz",
              "smalldata/adult.gz",
              "smalldata/AID362red_test.csv.gz",
              "smalldata/airlines/AirlinesTest.csv.zip",
              "smalldata/airlines/AirlinesTrain.csv.zip",
              "smalldata/airlines/airlineUUID.csv",
              "smalldata/airlines/allyears2k.zip",
              "smalldata/airlines/allyears2k_headers.zip",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ea_000000",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ed_000000",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ed_000001",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ed_000002",
//              "smalldata/airlines/hiveallyears2k/04c40d7c-33c8-486c-8f08-e24ebb8832ed_000005",
//              "smalldata/allstate/claim_prediction_dict.html",
              "smalldata/allstate/claim_prediction_train_set_10000.csv.gz",
              "smalldata/allstate/claim_prediction_train_set_10000_bool.csv.gz",
              "smalldata/allstate/claim_prediction_train_set_10000_int.csv.gz",
              "smalldata/anomaly/ecg_discord.csv",
              "smalldata/anomaly/ecg_discord_test.csv",
              "smalldata/anomaly/ecg_discord_train.csv",
              "smalldata/anomaly/toy_test.csv",
              "smalldata/anomaly/toy_train.csv",
              "smalldata/arcene/arcene_test.data",
              "smalldata/arcene/arcene_train.data",
              "smalldata/arcene/arcene_train_labels.labels",
              "smalldata/arcene/arcene_valid.data",
              "smalldata/arcene/arcene_valid_labels.labels",
              "smalldata/auto.csv",
              "smalldata/badchars.csv",
              "smalldata/baddata.data",
              "smalldata/bigburn.csv",
              "smalldata/boring.csv",
              "smalldata/BostonHousing.csv",
              "smalldata/cars.csv",
              "smalldata/cars_nice_header.csv",
              "smalldata/categoricals/30k_categoricals.csv.gz",
              "smalldata/categoricals/40k_categoricals.csv.gz",
              "smalldata/categoricals/AllBedrooms_Rent_Neighborhoods.csv.gz",
              "smalldata/categoricals/apartments_rec.csv",
              "smalldata/categoricals/Cleveland_heartDiseaseUCI_test.csv",
              "smalldata/categoricals/Cleveland_heartDiseaseUCI_train.csv",
              "smalldata/categoricals/TwoBedrooms_Rent_Neighborhoods.csv.gz",
              "smalldata/cebbinom.csv",
              "smalldata/cebexpanded.csv",
              "smalldata/cebexpandedREADME.rtf",
              "smalldata/chess/chess_1x2x1000/h2o/test.csv",
              "smalldata/chess/chess_1x2x1000/h2o/train.csv",
              "smalldata/chess/chess_2x1x1000/h2o/test.csv",
              "smalldata/chess/chess_2x1x1000/h2o/train.csv",
              "smalldata/chess/chess_2x2x10/h2o/test.csv",
              "smalldata/chess/chess_2x2x10/h2o/train.csv",
              "smalldata/chess/chess_2x2x10/R/test.csv",
              "smalldata/chess/chess_2x2x10/R/train.csv",
              "smalldata/chess/chess_2x2x10/rf.conf",
              "smalldata/chess/chess_2x2x10/weka/test.csv.arff",
              "smalldata/chess/chess_2x2x10/weka/train.csv.arff",
              "smalldata/chess/chess_2x2x100/h2o/test.csv",
              "smalldata/chess/chess_2x2x100/h2o/train.csv",
              "smalldata/chess/chess_2x2x100/R/test.csv",
              "smalldata/chess/chess_2x2x100/R/train.csv",
              "smalldata/chess/chess_2x2x100/rf.conf",
              "smalldata/chess/chess_2x2x100/weka/test.csv.arff",
              "smalldata/chess/chess_2x2x100/weka/train.csv.arff",
              "smalldata/chess/chess_2x2x1000/h2o/test.csv",
              "smalldata/chess/chess_2x2x1000/h2o/train.csv",
              "smalldata/chess/chess_2x2x1000/R/test.csv",
              "smalldata/chess/chess_2x2x1000/R/train.csv",
              "smalldata/chess/chess_2x2x1000/rf.conf",
              "smalldata/chess/chess_2x2x1000/weka/test.csv.arff",
              "smalldata/chess/chess_2x2x1000/weka/train.csv.arff",
              "smalldata/chess/chess_2x2x200/h2o/test.csv",
              "smalldata/chess/chess_2x2x200/h2o/train.csv",
              "smalldata/chess/chess_2x2x200/R/test.csv",
              "smalldata/chess/chess_2x2x200/R/train.csv",
              "smalldata/chess/chess_2x2x200/rf.conf",
              "smalldata/chess/chess_2x2x200/weka/test.csv.arff",
              "smalldata/chess/chess_2x2x200/weka/train.csv.arff",
              "smalldata/chess/chess_2x2x500/h2o/chess_2x2_500_int.csv",
              "smalldata/chess/chess_2x2x500/h2o/test.csv",
              "smalldata/chess/chess_2x2x500/h2o/train.csv",
              "smalldata/chess/chess_2x2x500/R/test.csv",
              "smalldata/chess/chess_2x2x500/R/train.csv",
              "smalldata/chess/chess_2x2x500/rf.conf",
              "smalldata/chess/chess_2x2x500/weka/test.csv.arff",
              "smalldata/chess/chess_2x2x500/weka/train.csv.arff",
              "smalldata/chess/chess_8x8x1000/R/test.csv",
              "smalldata/chess/chess_8x8x1000/R/train.csv",
              "smalldata/constantColumn.csv",
              "smalldata/covtype/covtype.20k.data",
              "smalldata/covtype/covtype.altered.gz",
              "smalldata/cuse.data.csv",
              "smalldata/cusedataREADME.rtf",
              "smalldata/cuseexpanded.csv",
              "smalldata/datagen1.csv",
              "smalldata/datetime/dd-mon-yr.csv",
              "smalldata/datetime/dd-mon-yy-with-other-cols.csv",
              "smalldata/drugs.csv",
              "smalldata/dummydata.csv",
              "smalldata/fail1_100x11000.csv.gz",
              "smalldata/fail2_24_100000_10.csv.gz",
              "smalldata/fish.csv",
              "smalldata/gaussian/sdss174052.csv.gz",
              "smalldata/gbm_test/CaliforniaHousing/cal_housing.data",
              "smalldata/gbm_test/CaliforniaHousing/cal_housing.domain",
              "smalldata/gbm_test/ecology_eval.csv",
              "smalldata/gbm_test/ecology_model.csv",
              "smalldata/gbm_test/Mfgdata_gaussian_GBM_testing.csv",
              "smalldata/gisette/Gisette_train_data.csv.gzip",
              "smalldata/gisette/Gisette_train_labels.csv.gzip",
              "smalldata/gisette/Gisette_valid_data.csv.gzip",
              "smalldata/gisette/Gisette_valid_labels.csv.gzip",
              "smalldata/glm_test/poisson_tst1.csv",
              "smalldata/glm_test/prostate_cat_replaced.csv",
              "smalldata/gt69436csv.data",
              "smalldata/handmade.csv",
              "smalldata/hex-443.parsetmp_1_0_0_0.data",
              "smalldata/hhp.cut3.214.data.gz",
              "smalldata/hhp_107_01.data.gz",
              "smalldata/hhp_9_17_12.predict.data.gz",
              "smalldata/histogram_test/30k_cattest.csv",
              "smalldata/histogram_test/50_cattest_test.csv",
              "smalldata/histogram_test/50_cattest_train.csv",
              "smalldata/histogram_test/alphabet_cattest.csv",
              "smalldata/histogram_test/bigcat_5000x2.csv",
              "smalldata/histogram_test/czechboard_300x300.csv",
              "smalldata/histogram_test/swpreds_1000x3.csv",
              "smalldata/housing.raw.txt",
              "smalldata/iris/iris.csv",
              "smalldata/iris/iris.csv.gz",
              "smalldata/iris/iris.csv.zip",
              "smalldata/iris/iris.xls",
              "smalldata/iris/iris.xlsx",
              "smalldata/iris/iris2.csv",
              "smalldata/iris/iris22.csv",
//              "smalldata/iris/iris_header.csv",
              "smalldata/iris/iris_test.csv",
              "smalldata/iris/iris_train.csv",
              "smalldata/iris/iris_wheader.csv",
              "smalldata/iris/iris_wheader.csv.gz",
              "smalldata/iris/iris_wheader.csv.zip",
              "smalldata/iris/iris_wheader.nonspd.csv",
              "smalldata/iris/leads.csv",
              "smalldata/jira/850.csv",
              "smalldata/jira/hex-1789.csv",
              "smalldata/jira/pub-180.csv",
              "smalldata/jira/pub-215.csv",
              "smalldata/jira/pub-35_test.csv",
              "smalldata/jira/pub-35_train.csv",
              "smalldata/jira/pub-569.csv",
              "smalldata/jira/pub_213.csv",
              "smalldata/jira/v-11.csv",
              "smalldata/jira/v-3.csv",
              "smalldata/kaggle/bestbuy_train_10k.csv.gz",
              "smalldata/kaggle/creditsample-test.csv.gz",
              "smalldata/kaggle/creditsample-training.csv.gz",
              "smalldata/kaggle/KDDTest.arff.gz",
              "smalldata/kaggle/KDDTrain.arff.gz",
              "smalldata/linreg/data.gz",
              "smalldata/logreg/100kx7_logreg.data.gz",
              "smalldata/logreg/benign.csv",
              "smalldata/logreg/benign.xls",
              "smalldata/logreg/failtoconverge_1000x501.csv.gz",
              "smalldata/logreg/failtoconverge_100x50.csv",
              "smalldata/logreg/logreg_trisum_int_cat_10000x10.csv",
              "smalldata/logreg/make_me_converge_10000x5.csv",
              "smalldata/logreg/princeton/copen.dat",
              "smalldata/logreg/princeton/cuse.dat",
              "smalldata/logreg/princeton/housing.raw",
              "smalldata/logreg/pros.xls",
              "smalldata/logreg/prostate.csv",
              "smalldata/logreg/prostate_long.csv.gz",
              "smalldata/logreg/prostate_test.csv",
              "smalldata/logreg/prostate_train.csv",
              "smalldata/logreg/syn_2659x1049.csv",
              "smalldata/logreg/syn_2659x1049x2enum.csv",
              "smalldata/logreg/syn_8686576441534898792_10000x100.csv",
              "smalldata/logreg/umass_chdage.csv",
              "smalldata/logreg/umass_statdata/cgd.dat",
              "smalldata/logreg/umass_statdata/cgd.txt",
              "smalldata/logreg/umass_statdata/chdage.dat",
              "smalldata/logreg/umass_statdata/chdage.txt",
              "smalldata/logreg/umass_statdata/chdage_cleaned.dat",
              "smalldata/logreg/umass_statdata/clslowbwt.dat",
              "smalldata/logreg/umass_statdata/clslowbwt.txt",
              "smalldata/logreg/umass_statdata/icu.dat",
              "smalldata/logreg/umass_statdata/icu.txt",
              "smalldata/logreg/umass_statdata/lowbwt.dat",
              "smalldata/logreg/umass_statdata/lowbwt.txt",
              "smalldata/logreg/umass_statdata/lowbwtm11.dat",
              "smalldata/logreg/umass_statdata/lowbwtm11.txt",
              "smalldata/logreg/umass_statdata/meexp.dat",
              "smalldata/logreg/umass_statdata/meexp.txt",
              "smalldata/logreg/umass_statdata/nhanes3.dat",
              "smalldata/logreg/umass_statdata/nhanes3.txt",
              "smalldata/logreg/umass_statdata/pbc.dat",
              "smalldata/logreg/umass_statdata/pbc.txt",
              "smalldata/logreg/umass_statdata/pharynx.dat",
              "smalldata/logreg/umass_statdata/pharynx.txt",
              "smalldata/logreg/umass_statdata/pros.dat",
              "smalldata/logreg/umass_statdata/pros.txt",
              "smalldata/logreg/umass_statdata/uis.dat",
              "smalldata/logreg/umass_statdata/uis.txt",
              "smalldata/logreg/why_perfect_training_100x500.csv",
              "smalldata/makedata.csv",
              "smalldata/marketing_naRemoved.csv",
              "smalldata/mixed_causes_NA.csv",
              "smalldata/mnist/readme.txt",
              "smalldata/mnist/test.csv.gz",
              "smalldata/mnist/train.csv.gz",
              "smalldata/mtcars.csv",
              "smalldata/Mushroom.gz",
              "smalldata/neural/Benchmark_dojo_test.data",
              "smalldata/neural/eightsq.data",
              "smalldata/neural/Readme.txt",
              "smalldata/neural/sin_pattern.data",
              "smalldata/neural/sumsigmoids.csv",
              "smalldata/neural/sumsigmoids_test.csv",
              "smalldata/neural/two_spiral.data",
              "smalldata/neural/two_spiral.png",
              "smalldata/parity_128_4_100_quad.data",
              "smalldata/parity_128_4_2_quad.data",
              "smalldata/parse_fail_double_space.csv",
              "smalldata/parse_folder_test/prostate_0.csv",
              "smalldata/parse_folder_test/prostate_1.csv",
              "smalldata/parse_folder_test/prostate_2.csv",
              "smalldata/parse_folder_test/prostate_3.csv",
              "smalldata/parse_folder_test/prostate_4.csv",
              "smalldata/parse_folder_test/prostate_5.csv",
              "smalldata/parse_folder_test/prostate_6.csv",
              "smalldata/parse_folder_test/prostate_7.csv",
              "smalldata/parse_folder_test/prostate_8.csv",
              "smalldata/parse_folder_test/prostate_9.csv",
              "smalldata/parse_zeros_100x8500.csv.gz",
              "smalldata/pca_test/AustraliaCoast.csv",
              "smalldata/pca_test/USArrests.csv",
              "smalldata/phbirths.raw.txt",
              "smalldata/pmml/cars-cater-rf-1tree.pmml",
              "smalldata/pmml/cars-cater-rf-50trees.pmml",
              "smalldata/pmml/cars-rf-1tree.pmml",
              "smalldata/pmml/cars-rf-50trees.pmml",
              "smalldata/pmml/copen-rf-1tree.pmml",
              "smalldata/pmml/copen-rf-50trees.pmml",
              "smalldata/pmml/iris_rf_1tree.pmml",
              "smalldata/pmml/iris_rf_500trees.pmml",
              "smalldata/pmml/SampleScorecard.pmml",
              "smalldata/poisson/Goalies.csv",
              "smalldata/poker/poker-hand-testing.data",
              "smalldata/poker/poker-hand.pl",
              "smalldata/poker/poker10",
              "smalldata/poker/poker100",
              "smalldata/poker/poker1000",
              "smalldata/prostate/prostate.bin.csv.zip",
              "smalldata/prostate/prostate.csv.zip",
              "smalldata/prostate/prostate.float.csv.zip",
              "smalldata/prostate/prostate.int.csv.zip",
              "smalldata/prostate/prostate.uuid.csv.zip",
              "smalldata/quantiles/breadth.csv",
              "smalldata/random1csv.data",
              "smalldata/randomdata2.csv",
              "smalldata/randomdata3.csv",
              "smalldata/runif.csv",
              "smalldata/runifA.csv",
              "smalldata/runifB.csv",
              "smalldata/runifC.csv",
              "smalldata/smtrees.csv",
              "smalldata/space_shuttle_damage.csv",
              "smalldata/stego/stego_testing.data",
              "smalldata/stego/stego_training.data",
              "smalldata/stego/stego_training_modified.data",
              "smalldata/swiss.csv",
              "smalldata/swiss_clean.csv",
              "smalldata/syn_binary10Kx100.csv.gz",
              "smalldata/syn_fp_prostate.csv",
              "smalldata/syn_sphere2.csv",
              "smalldata/syn_sphere3.csv",
              "smalldata/test/arit.csv",
              "smalldata/test/classifier/chess_test.csv",
              "smalldata/test/classifier/chess_train.csv",
              "smalldata/test/classifier/coldom_test_1.csv",
              "smalldata/test/classifier/coldom_test_1_2.csv",
              "smalldata/test/classifier/coldom_test_2.csv",
              "smalldata/test/classifier/coldom_test_3.csv",
              "smalldata/test/classifier/coldom_train_1.csv",
              "smalldata/test/classifier/coldom_train_2.csv",
              "smalldata/test/classifier/coldom_train_3.csv",
              "smalldata/test/classifier/iris_missing_values.csv",
              "smalldata/test/classifier/iris_test.csv",
              "smalldata/test/classifier/iris_test_extra.csv",
              "smalldata/test/classifier/iris_test_extra_with_na.csv",
              "smalldata/test/classifier/iris_test_missing.csv",
              "smalldata/test/classifier/iris_test_missing_extra.csv",
              "smalldata/test/classifier/iris_test_numeric.csv",
              "smalldata/test/classifier/iris_test_numeric_extra.csv",
              "smalldata/test/classifier/iris_test_numeric_extra2.csv",
              "smalldata/test/classifier/iris_test_numeric_missing.csv",
              "smalldata/test/classifier/iris_test_numeric_missing_extra.csv",
              "smalldata/test/classifier/iris_train.csv",
              "smalldata/test/classifier/iris_train_numeric.csv",
              "smalldata/test/classifier/multi_class.test.csv",
              "smalldata/test/classifier/multi_class.train.csv",
              "smalldata/test/cm/v1.csv",
              "smalldata/test/cm/v1n.csv",
              "smalldata/test/cm/v2.csv",
              "smalldata/test/cm/v2n.csv",
              "smalldata/test/cm/v3.csv",
              "smalldata/test/cm/v4.csv",
              "smalldata/test/cm/v4n.csv",
              "smalldata/test/HEX-287-small-files.data",
              "smalldata/test/hive.txt",
              "smalldata/test/HTWO-87-one-line-dataset-0.csv",
              "smalldata/test/HTWO-87-one-line-dataset-1dos.csv",
              "smalldata/test/HTWO-87-one-line-dataset-1unix.csv",
              "smalldata/test/HTWO-87-one-line-dataset-2dos.csv",
              "smalldata/test/HTWO-87-one-line-dataset-2unix.csv",
              "smalldata/test/HTWO-87-two-lines-dataset.csv",
              "smalldata/test/HTWO-87-two-unique-lines-dataset.csv",
              "smalldata/test/is_NA.csv",
              "smalldata/test/na_test.zip",
              "smalldata/test/R/titanic.csv",
//              "smalldata/test/rmodels/covtype-rf-50tree-as-factor-X5-20k.rdata",
//              "smalldata/test/rmodels/covtype-rf-50tree-as-factor-X5.rdata",
              "smalldata/test/rmodels/covtype.rf.2",
//              "smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-3.rdata",
//              "smalldata/test/rmodels/iris_x-iris-1-4_y-species_ntree-500.rdata",
//              "smalldata/test/rmodels/ozone.rf.10trees.rdata",
//              "smalldata/test/rmodels/prostate-rf-10tree-asFactorCapsule.rdata",
//              "smalldata/test/rmodels/prostate-rf-1tree-asFactorCapsule.rdata",
//              "smalldata/test/rmodels/prostate-rf-2tree-asFactorCapsule.rdata",
              "smalldata/test/rmodels/rf-iris-1tree.model",
              "smalldata/test/test1.dat",
              "smalldata/test/test_26cols_comma_sep.csv",
              "smalldata/test/test_26cols_multi_space_sep.csv",
              "smalldata/test/test_26cols_single_space_sep.csv",
              "smalldata/test/test_26cols_single_space_sep_2.csv",
              "smalldata/test/test_all_raw_top10rows.csv",
              "smalldata/test/test_enum_domain_size.csv",
              "smalldata/test/test_less_than_65535_unique_names.csv",
              "smalldata/test/test_manycol_tree.csv",
              "smalldata/test/test_more_than_65535_unique_names.csv",
              "smalldata/test/test_parse_mix.csv",
              "smalldata/test/test_percentiles_distns.csv.gz",
              "smalldata/test/test_percentiles_distns.R",
              "smalldata/test/test_quote.csv",
              "smalldata/test/test_time.csv",
              "smalldata/test/test_tree.csv",
              "smalldata/test/test_tree_minmax.csv",
//              "smalldata/test/test_uuid.csv", //PUB-1003
//              "smalldata/test/test_uuid_na.csv", //PUB-1003
//              "smalldata/airlines/uuid_airline.csv", //PUB-1003
              "smalldata/test/test_var.csv",
              "smalldata/Test_Arabic_Digit_short.data",
              "smalldata/tinyburn.csv",
              "smalldata/titanicalt.csv",
              "smalldata/tnc3.csv",
              "smalldata/tnc3_10.csv",
              "smalldata/tnc6.csv",
              "smalldata/toy_data_RF.csv",
              "smalldata/toykm.csv",
              "smalldata/trees.csv",
              "smalldata/Twitter2DB.txt",
              "smalldata/unbalanced/orange_small_test.data.zip",
              "smalldata/unbalanced/orange_small_train.data.zip",
              "smalldata/WBIsparsedata",
              "smalldata/weather.csv",
              "smalldata/wine.data",
              "smalldata/winesPCA.csv",
              "smalldata/wonkysummary.csv",
              "smalldata/zero_dot_zero_one.csv",
              "smalldata/zero_dot_zero_zero_one.csv",
              "smalldata/zinb.csv",
              "smalldata/zip_code/zip_code_database.csv.gz",
              "smalldata/zip_code/zipcodes",
              "smalldata/zipcodes",
      };
      for (String f : files) {
        Frame fr=null;
        Key dest = null;
        Key split1 = null;
        Key split2 = null;
        try {
          Log.info("Trying to parse " + f);
          Key fkey = NFSFileVec.make(find_test_file(f));
          fr = ParseDataset2.parse(Key.make(), new Key[]{fkey});

          Log.info("Trying to rebalance " + f);
          final int splits = Math.min((int)fr.numRows(), 4*H2O.NUMCPUS*H2O.CLOUD.size());
          dest = Key.make(f + ".balanced");
          H2O.submitTask(new RebalanceDataSet(fr, dest, splits)).join();

          Log.info("Trying to split " + f);
          FrameSplitPage fsp = new FrameSplitPage();
          fsp.source = fr;
          fsp.ratios = new float[]{0.75f};
          split1 = Key.make(f + ".one");
          split2 = Key.make(f + ".two");
          fsp.split_keys = new Key[]{split1, split2};
          fsp.invoke();

        } catch (IllegalArgumentException t) {
          Log.info("Caught non-fatal exception:");
          t.printStackTrace();
        } finally {
          if (fr != null) fr.delete();
          if (dest != null) {
            Frame balanced = UKV.get(dest);
            if (balanced != null) balanced.delete();
          }
          if (split1 != null) {
            Frame split = UKV.get(split1);
            if (split != null) split.delete();
          }
          if (split2 != null) {
            Frame split = UKV.get(split2);
            if (split != null) split.delete();
          }
        }
      }
    }
   }

  // test 0,N,Y -> NA,0,1
  @Test public void testBinaryCol1() {
    String data =
                  "1 A 0\n"+
                  "2 B 0\n"+
                  "3 C N\n"+
                  "4 A 0\n"+
                  "5 B 0\n"+
                  "6 C N\n"+
                  "7 A 0\n"+
                  "8 B 0\n"+
                  "9 C 0\n"+
                 "10 A Y\n"+
                 "11 B 0\n"+
                 "12 C 0\n";

    double[][] exp = new double[][] {
            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,1),
            d(11,1,NaN),
            d(12,2,NaN),
    };
    Key k = FVecTest.makeByteVec("test",data);
    testParsed(k,exp);
  }

  // test 0,F,T -> NA,0,1
  @Test public void testBinaryCol2() {
    String data =
            "1 A 0\n"+
                    "2 B 0\n"+
                    "3 C F\n"+
                    "4 A 0\n"+
                    "5 B 0\n"+
                    "6 C F\n"+
                    "7 A 0\n"+
                    "8 B 0\n"+
                    "9 C 0\n"+
                    "10 A T\n"+
                    "11 B 0\n"+
                    "12 C 0\n";

    double[][] exp = new double[][] {
            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,1),
            d(11,1,NaN),
            d(12,2,NaN),
    };
    Key k = FVecTest.makeByteVec("test",data);
    testParsed(k,exp);
  }

  // test 0,n,y -> NA,0,1
  @Test public void testBinaryCol3() {
    String data =
            "1 A 0\n"+
                    "2 B 0\n"+
                    "3 C n\n"+
                    "4 A 0\n"+
                    "5 B 0\n"+
                    "6 C n\n"+
                    "7 A 0\n"+
                    "8 B 0\n"+
                    "9 C 0\n"+
                    "10 A y\n"+
                    "11 B 0\n"+
                    "12 C 0\n";

    double[][] exp = new double[][] {
            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,1),
            d(11,1,NaN),
            d(12,2,NaN),
    };
    Key k = FVecTest.makeByteVec("test",data);
    testParsed(k,exp);
  }

  // test 0,f,t -> NA,0,1
  @Test public void testBinaryCol4() {
    String data =
            "1 A 0\n"+
                    "2 B 0\n"+
                    "3 C f\n"+
                    "4 A 0\n"+
                    "5 B 0\n"+
                    "6 C f\n"+
                    "7 A 0\n"+
                    "8 B 0\n"+
                    "9 C 0\n"+
                    "10 A t\n"+
                    "11 B 0\n"+
                    "12 C 0\n";

    double[][] exp = new double[][] {
            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,1),
            d(11,1,NaN),
            d(12,2,NaN),
    };
    Key k = FVecTest.makeByteVec("test",data);
    testParsed(k,exp);
  }

  // test 0,n,y -> NA,0,1
  @Test public void testBinaryCol5() {
    String data =
            "1 A 0\n"+
                    "2 B 0\n"+
                    "3 C n\n"+
                    "4 A 0\n"+
                    "5 B 0\n"+
                    "6 C n\n"+
                    "7 A 0\n"+
                    "8 B 0\n"+
                    "9 C 0\n"+
                    "10 A y\n"+
                    "11 B 0\n"+
                    "12 C 0\n";

    double[][] exp = new double[][] {
            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,1),
            d(11,1,NaN),
            d(12,2,NaN),
    };
    Key k = FVecTest.makeByteVec("test",data);
    testParsed(k,exp);
  }

  // test 0,n,y -> NA,0,1 with single quotes
  @Test public void testBinaryCol6() {
    String data =
            "1 A 0\n"+
                    "2 B 0\n"+
                    "3 C 'n'\n"+
                    "4 A 0\n"+
                    "5 B 0\n"+
                    "6 C 'n'\n"+
                    "7 A 0\n"+
                    "8 B 0\n"+
                    "9 C 0\n"+
                    "10 A 'y'\n"+
                    "11 B 0\n"+
                    "12 C 0\n";

    double[][] exp = new double[][] {
            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,1),
            d(11,1,NaN),
            d(12,2,NaN),
    };
    Key k = FVecTest.makeByteVec("test",data);
    testParsed(k,exp);
  }

  // test 0,n,y -> NA,0,1 with double quotes
  @Test public void testBinaryCol7() {
    String data =
            "1 A 0\n"+
                    "2 B 0\n"+
                    "3 C \"n\"\n"+
                    "4 A 0\n"+
                    "5 B 0\n"+
                    "6 C \"n\"\n"+
                    "7 A 0\n"+
                    "8 B 0\n"+
                    "9 C 0\n"+
                    "10 A \"y\"\n"+
                    "11 B 0\n"+
                    "12 C 0\n";

    double[][] exp = new double[][] {
            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,1),
            d(11,1,NaN),
            d(12,2,NaN),
    };
    Key k = FVecTest.makeByteVec("test",data);
    testParsed(k,exp);
  }

  // test 0,n,y -> NA,0,1
  @Test public void testBinaryColZipped() {

    Frame fr = parseFrame(null,"smalldata/test/is_NA2.csv.gz");
    double[][] exp = new double[][] {
            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,1),
            d(11,1,NaN),
            d(12,2,NaN),
    };
    Log.info(fr.toStringAll());
    testParsed(fr,exp,12);
  }


  // test 0,n,y -> NA,0,1
  @Test public void testBinaryColMultiFile() {
    Key [] files = ImportFiles2.importPath("smalldata/test/binaryCol");
    Frame f1 = ParseDataset2.parse(Key.make("multifile"), files);

    double[][] exp = new double[][] {
            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,NaN),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,NaN),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,NaN),
            d(11,1,NaN),
            d(12,2,NaN),

            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,1),
            d(11,1,NaN),
            d(12,2,NaN),

            d( 1,0,NaN),
            d( 2,1,NaN),
            d( 3,2,0),
            d( 4,0,NaN),
            d( 5,1,NaN),
            d( 6,2,0),
            d( 7,0,NaN),
            d( 8,1,NaN),
            d( 9,2,NaN),
            d(10,0,0),
            d(11,1,NaN),
            d(12,2,NaN),
    };
    Log.info(f1.toStringAll());
    testParsed(f1,exp,36);
  }

}
