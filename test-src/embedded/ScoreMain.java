package embedded;

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
 *  extract the h2o_core.jar file.
 *     $ jar xf h2o.jar h2o_core.jar
 *  Compile this file:
 *     $ javac -cp ".;h2o_core.jar" embedded/ScoreMain.java
 *  And Go!
 *     $ java -cp ".;h2o_core.jar" embedded/ScoreMain
 *     Sample App Init Code Goes Here; loading data to score
 *     Loading model to score
 *     Initial score=62.1729083
 *     100000 in 328ms = 3280.0 microsec/score
 *     Sample App Shuts Down
 */
class ScoreMain {
  static HashMap<String, Comparable> ROW;

  public static void main(String[] args) throws Exception {
    // Prep the app; get data available
    sampleAppInit(args);

    // Load a PMML model
    System.out.println("Loading model to score");
    FileInputStream fis = new FileInputStream("../../demo/SampleScorecard.pmml");
    water.score.ScorecardModel scm = water.parser.PMMLParser.load(fis);
    
    
    sampleAppDoesStuff(scm);

    System.out.println("Sample App Shuts Down");
    System.exit(0);
  }

  public static void sampleAppInit(String[] args) throws Exception {
    System.out.println("Sample App Init Code Goes Here; loading data to score");

    // Make data available.  In this case, parse a simple text file and inject
    // pairs into a HashMap.
    File f = new File("../../demo/SampleData.txt");
    String text = new Scanner( f ).useDelimiter("\\A").next();
    ROW = new HashMap<String, Comparable>();
    String[] toks = text.split("[,\\{\\}]");
    for( String tok : toks ) {
      if( tok.length() > 0 ) {
        String pair[] = tok.split("[=:]");
        if( pair.length==2 ) {
          ROW.put(trim(pair[0]),asNum(trim(pair[1])));
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

  public static void sampleAppDoesStuff(water.score.ScorecardModel scm) {
    System.out.println("Initial score="+scm.score(ROW));
    for( int i=0; i<1000; i++ )
      loop1000(scm);
    try { Thread.sleep(1000); } catch( Exception e ) { }
    System.out.println("Timing...");
    long start = System.currentTimeMillis();
    final int ITER=100000;
    for( int i=0; i<ITER; i++ )
      loop1000(scm);
    long now = System.currentTimeMillis();
    long delta = now-start;
    System.out.println(""+ITER+" in "+delta+"ms = "+((double)delta*1000.0*1000.0/(ITER*1000))+" microsec/score");

  }

  public static void loop1000(water.score.ScorecardModel scm) {
    for( int i=0; i<1000; i++ )
      scm.score(ROW);
  }
}
