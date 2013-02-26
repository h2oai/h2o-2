import java.util.*;
import java.io.*;

/**
 *  A Sample Main Class which embeddeds the H2O Scoring Engine
 *
 *  The scoring engine is a modular, seperable piece of H2O intended to be
 *  embedded into a larger user application.  It understands how to load and
 *  score all H2O models, but cannot do modeling by itself.
 *
 *  In the directory above this one (the 'embedded' package directory),
 *  compile this file:
 *     $ javac -cp ".;h2o.jar" embedded/ScoreMain.java
 *  And Go!
 *     $ java  -cp ".;h2o.jar" embedded/ScoreMain
 *     Sample App Init Code Goes Here; loading data to score
 *     Loading model to score
 *     [h2o] Warning: feature btr_port_nom used by the model is not in the provided feature list from the data
 *     Initial score0(HashMap)=30.2953306
 *     Initial score (HashMap)=30.2953306
 *     Initial score (Arrays )=30.2953306
 *     Timing...
 *     score(HashMap) 1000 in 2085678ns = 2085.678 nanosec/score
 *     score(Arrays ) 1000 in 835487ns = 835.487 nanosec/score
 *     score(HashMap) 1000000 in 442568040ns = 442.56804 nanosec/score
 *     score(Arrays ) 1000000 in 198211951ns = 198.211951 nanosec/score
 *     score(HashMap) 10000000 in 4311345978ns = 431.1345978 nanosec/score
 *     score(Arrays ) 10000000 in 1942011556ns = 194.2011556 nanosec/score
 *     score(HashMap) 100000000 in 43612466550ns = 436.1246655 nanosec/score
 *     score(Arrays ) 100000000 in 19390716132ns = 193.90716132 nanosec/score
 *     Sample App Shuts Down
 */
class ScoreMain {
  // A JavaScript-like data row; K/V pairs indexed by a String fieldname, and
  // returning either a String or a subclass of Number (Double or Long) or a
  // Boolean.
  static HashMap<String, Comparable> ROW;

  // A *efficient* but less convenient representation of a data row.  Features
  // are named as with the HashMap version, but we convert the feature String
  // into an index into these arrays; Strings go in the matching String array
  // and other values in the double array.  Booleans go in the double array as
  // 0 or 1.  Missing values are null in the Strings or NaNs in the doubles.
  // As long as the *set of features* does not change, then this mapping can be
  // invariant across many data rows.
  static String[] FEATURES;
  static String[] SS;
  static double[] DS;
  // A mapping from the dense columns desired by the model, to the above
  // feature list, computed by asking the model for a mapping (given a list of
  // features).  Some features may be unused and won't appear in the mapping.
  // If the data row features list does not mention all the features the model
  // needs, then this map will contain a -1 for the missing feature index.
  static int[] MAP;

  public static void main(String[] args) throws Exception {
    // Prep the app; get data available
    sampleAppInit(args);

    // Load a PMML model
    System.out.println("Loading model to score");
    InputStream fis = new BufferedInputStream(new FileInputStream("../../../../demo/SampleScorecard.pmml"));
    water.score.ScoreModel scm = water.parser.PMMLParser.parse(fis);

    // Pre-compute the row data into arrays.  The expectation is that this
    // mapping is done early by the data producer, and a row of data is passed
    // about as a pair of SS/DS arrays.  The mapping is invariant between the
    // data rows and the model (i.e., changing either the model or the data
    // layout requires computing a new mapping).
    FEATURES = ROW.keySet().toArray(new String[0]);
    SS = new String[FEATURES.length];
    DS = new double[FEATURES.length];
    for( int i=0; i<FEATURES.length; i++ ) {
      SS[i] = null;
      DS[i] = Double.NaN;
      Object o = ROW.get(FEATURES[i]);
      if( o == null ) ;
      else if( o instanceof String ) SS[i]=(String)o;
      else if( o instanceof Number ) DS[i]=((Number)o).doubleValue();
      else if( o instanceof Boolean) DS[i]=((Boolean)o).booleanValue() ? 1 : 0;
      else throw new IllegalArgumentException("Unknown datatype "+o.getClass());
    }
    // Given a feature list, map them to columns in the model
    MAP = scm.columnMapping(FEATURES);

    sampleAppDoesStuff(scm);

    System.out.println("Sample App Shuts Down");
    System.exit(0);
  }

  public static void sampleAppInit(String[] args) throws Exception {
    System.out.println("Sample App Init Code Goes Here; loading data to score");

    // Make data available.  In this case, parse a simple text file and inject
    // pairs into a HashMap.
    File f = new File("../../../../demo/SampleData.txt");
    String text = new Scanner( f ).useDelimiter("\\A").next();
    ROW = new HashMap<String, Comparable>();
    String[] toks = text.split("[,\\{\\}]");
    for( String tok : toks ) {
      if( tok.length() > 0 ) {
        String pair[] = tok.split("[=:]");
        if( pair.length==2 ) {
          String feature = trim(pair[0]);
          ROW.put(feature,asNum(trim(pair[1])));
        }
      }
    }
  }

  static String trim( String x ) { return x.trim().replaceAll("\"",""); }
  static Comparable asNum( String x ) {
    if( "true" .equals(x) ) return new Boolean(true );
    if( "false".equals(x) ) return new Boolean(false);
    try { return new Long   (Long  .valueOf(x)); }
    catch( NumberFormatException nfe ) { }
    try { return new Double (Double.valueOf(x)); }
    catch( NumberFormatException nfe ) { }
    return x;
  }

  public static void sampleAppDoesStuff(water.score.ScoreModel scm) {
    System.out.println("Initial score_interpreter(HashMap)="+
                       ((water.score.ScorecardModel)scm).score_interpreter(ROW));
    System.out.println("Initial score(HashMap)="+scm.score(ROW));
    System.out.println("Initial score(Arrays )="+scm.score(MAP,SS,DS));

    for( int i=0; i<1000; i++ ) {
      loop1000(scm,true );
      loop1000(scm,false);
    }
    try { Thread.sleep(1000); } catch( Exception e ) { }
    System.out.println("Timing...");

    timeTwo(scm,1);             // thousand
    timeTwo(scm,1000);          // 1 million
    timeTwo(scm,10000);         // 10 million
    timeTwo(scm,100000);        // 100 million
  }

  public static void timeTwo(water.score.ScoreModel scm, int iter ) {
    timeOne(scm,iter,true );
    timeOne(scm,iter,false);
  }

  public static void timeOne(water.score.ScoreModel scm, int iter, boolean mapOrAry) {
    //long start = System.currentTimeMillis();
    long start = System.nanoTime();
    for( int i=0; i<iter; i++ )
      loop1000(scm,mapOrAry);
    //long now = System.currentTimeMillis();
    long now = System.nanoTime();
    long delta = now-start;
    //double nanos = delta*1000.0*1000.0;
    double nanos = (double)delta;
    System.out.println("score("+(mapOrAry?"HashMap":"Arrays ")+") "+(iter*1000)+" in "+(long)nanos+"ns = "+(nanos/(iter*1000.0))+" nanosec/score");
  }

  public static void loop1000(water.score.ScoreModel scm, boolean mapOrAry) {
    if( mapOrAry ) 
      for( int i=0; i<1000; i++ )
        scm.score(ROW);
    else
      for( int i=0; i<1000; i++ )
        scm.score(MAP,SS,DS);
  }
}
