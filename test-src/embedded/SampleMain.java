package embedded;

import java.io.FileInputStream;

import H2OInit.Boot;

/**
 *  A Sample Main Class which embeddeds H2O.
 *
 *  H2O is *not* run from its main, and thus requires an initialization call.
 *  After that, the H2OInit.External API can be called, including e.g.
 *  loading a Model and running H2O as an embedded Scoring Engine.
 *
 *  Compile:   javac -cp ".;h2o.jar" SampleMain.java
 *  Run:       java  -cp ".;h2o.jar" SampleMain
 */
class SampleMain {

  public static void main(String[] args) throws Exception {
    sampleAppInit(args);

    // Start H2O before using H2O.
    Boot.main(new String[]{"-name","MySampleAppCloud","-port","12345"});

    sampleAppDoesStuff();

    System.out.println("Sample App Shuts Down");
    System.exit(0);
  }

  public static void sampleAppInit(String[] args) {
    System.out.println("Sample App Init Code Goes Here");
  }

  public static void sampleAppDoesStuff() throws Exception {
    System.out.println("Sample App Does Stuff");

    // Put a model
    Object modelKey = H2OInit.External.makeKey("irisModel");
    Object model = H2OInit.External.ingestRFModelFromR(modelKey,new FileInputStream("../../smalldata/test/rmodels/rf-iris-1tree.model"));

    double[] row = new double[]{2.3,1.2,4.4,5.5};
    double res1 = H2OInit.External.scoreKey(modelKey, new String[]{"sepal"}, row);
    System.out.println(res1);

    double res2 = H2OInit.External.scoreModel(model, new String[]{"sepal"}, row);
    System.out.println(res2);

    try { Thread.sleep(5*1000); }
    catch( InterruptedException ie ) { }
  }
}
