package hex.rf;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import water.Arguments;
import water.TestUtil;
import water.util.Utils;

/** Launch RF in a new vm and records  results.
 */
public class RFRunner {

  static final long MAX_RUNNING_TIME = 400 * 60000; // max runtime is 20 mins
  static final int ERROR_IDX = 3;
  static enum patterns { FINISH, NTREES, FEATURES, ERROR, DEPTH, LEAVES, ROWS };

  static class iMatch {
    static final Pattern
      pfinish = Pattern.compile("Random forest finished in[ ]*(.*)"),
      pntrees = Pattern.compile("Number of trees:[ ]*([0-9]+)"),
      pfeatures = Pattern.compile("No of variables tried at each split:[ ]*([0-9]+)"),
      perror = Pattern.compile("Estimate of err. rate:[ ]*([0-9]+)%[ ]*\\(([.0-9]+)\\)"),
      pdepth = Pattern.compile("Avg tree depth \\(min, max\\):[ ]*([0-9]+).*"),
      pleaves = Pattern.compile("Avg tree leaves \\(min, max\\):[ ]*([0-9]+).*"),
      prows = Pattern.compile("Validated on \\(rows\\):[ ]*([0-9]+).*"),
      EXCEPTION = Pattern.compile("Exception in thread \"(.*\") (.*)"),
      ERROR = Pattern.compile("java.lang.(.*?Error.*)"),
      DONE = Pattern.compile("[RF] Validation done");
    String _finish;
    int _ntrees=-1, _features=-1,  _avgdepth=-1, _mindepth=-1, _maxdepth=-1, _avgleaf=-1,_minleaf=-1,_maxleaf=-1,_rows=-1;
    double _err=-1, _class_err=-1;
    boolean match(String s) {
      Matcher m = null;
      m = pfinish.matcher(s);  if (m.find()) { _finish = m.group(1); return false; }
      m = pntrees.matcher(s);  if (m.find()) { _ntrees = Integer.parseInt(m.group(1)); return false; }
      m = pfeatures.matcher(s);if (m.find()) { _features = Integer.parseInt(m.group(1)); return false; }
      m = perror.matcher(s);   if (m.find()) { double v =  ((int) (Double.parseDouble(m.group(2)) * 10000)/100.0);
          if(_err == -1) _err = v; else _class_err = v; return false; }
      m = pleaves.matcher(s);  if (m.find()) { _avgleaf = Integer.parseInt(m.group(1)); return false; }
      m = pdepth.matcher(s);   if (m.find()) { _avgdepth = Integer.parseInt(m.group(1)); return false; }
      m = prows.matcher(s);    if (m.find()) { _rows = Integer.parseInt(m.group(1)); return false; }
      m = DONE.matcher(s);     if (m.find()) return true; else return false;
    }
    String exception(String s) {
      Matcher m = EXCEPTION.matcher(s);
      if (m.find()) return "thread=" + m.group(1) + ", exception = " + m.group(2);
      m = ERROR.matcher(s);  if (m.find()) return  m.group(1);   return null;
    }
    void print(FileWriter fw) throws IOException {
        fw.write(_err+"," +_class_err+"," +_ntrees +"," +_features+"," +0+ "," +_avgleaf+","+ _rows+"," +_finish);
    }
  }
  static final String[] RESULTS = new String[] { "err", "classif err", "ntrees", "nvars",  "avg.depth", "avg.leaves", "rows", "time" };
  static final String JAVA         = "java";
  static final String JAR          = "-jar target/h2o.jar";
  static final String MAIN         = "-mainClass hex.rf.RandomForest";
  static final String[] stat_types = new String[] { "gini", "entropy" };
  static final int[] ntrees        = new int[] { 1, 50, 100, 300 };
  static final int[] sizeMultiples = new int[]{ 1, 4, 12, 24, 32, 64};

  static class RFArgs extends Arguments.Opt {
    String file;                // data
    String parsedKey;           //
    String rawKey;              //
    String validationFile;      // validation data
    int ntrees = 10;            // number of trees
    int depth = Integer.MAX_VALUE; // max depth of trees
    int sample = 67;  // sampling rate
    int binLimit = 1024;
    String statType = "gini";// split type
    int seed = 42;              // seed
    String ignores;
    int features = -1;
    boolean stratify;
    String strata;
  }

  static class OptArgs extends Arguments.Opt {
    String files = "smalldata/poker/poker-hand-testing.data"; // dataset
    int     maxConcat = 16;                                    // how big should we go?
    String rawKeys;
    String parsedKeys;
    String h2oArgs = "";                                      // args for the spawned h2o
    String jvmArgs = " -Xmx3g";                               // args for the spawned jvm
    String resultDB = "/tmp/results.csv";                     // output file
  }

  static iMatch _im;

  /**
   * Represents spawned process with H2O running RF. Hooks stdout and stderr and
   * looks for exceptions (failed run) and results.
   */
  static class RFProcess extends Thread {
    Process _process;
    BufferedReader _rd;
    BufferedReader _rdErr;
    String exception;
    PrintStream _stdout = System.out;
    PrintStream _stderr = System.err;

    /* Creates RFPRocess and spawns new process. */
    RFProcess(String cmd) throws Exception {
      Utils.pln("'"+JAVA+" "+cmd+"'");
      List<String> c = new ArrayList<String>();
      c.add(JAVA);  for(String s : cmd.split(" "))  { s = s.trim(); if (s.length()>0) c.add(s); }
      ProcessBuilder bldr = new ProcessBuilder(c);
      bldr.redirectErrorStream(true);
      _process = bldr.start();
      _rd = new BufferedReader(new InputStreamReader(_process.getInputStream()));
      _rdErr = new BufferedReader(new InputStreamReader(_process.getErrorStream()));
    }

    /** Kill the spawned process. And kill the thread. */
    void cleanup() { _process.destroy(); try { _process.waitFor(); } catch( InterruptedException e ) { } }

    /** Read stdout and stderr of the spawned process. Read by lines and print
     * them to our stdout. Look for exceptions (fail) and result (error rate and
     * running time). */
    @Override public void run() {
      try {
        String _line;
        _im = new iMatch();
        while( (_line = _rd.readLine()) != null ) {
          _stdout.println(_line);
          boolean done = _im.match(_line);
          if (done) { System.out.println("Error: " + _im._err + "%"); break; }
          exception = _im.exception(_line);
          if (exception!=null) break;
        }
      } catch( Exception e ) { throw new Error(e); }
    }
  }

  /** look for input files. If the path ends with '*' all files will be used. */
  static Collection<File> parseDatasetArg(String str) {
    ArrayList<File> files = new ArrayList<File>();
    StringTokenizer tk = new StringTokenizer(str, ",");
    while( tk.hasMoreTokens() ) {
      String path = tk.nextToken();
      if( path.endsWith("*") ) {
        path = path.substring(0, path.length() - 1);
        File f = TestUtil.find_test_file(path);
        if( !f.isDirectory() ) throw new Error("invalid path '" + path + "*'");
        for( File x : f.listFiles() ) {
          if( x.isFile() && (x.getName().endsWith(".csv") || x.getName().endsWith(".data")) )
            files.add(x);
          else if( x.isDirectory() )
            files.addAll(parseDatasetArg(x.getAbsolutePath() + "*"));
        }
      } else files.add(TestUtil.find_test_file(path));
    }
    return files;
  }


  static boolean runTest(String cmd, String resultDB, PrintStream stdout, boolean check) throws Exception {
    RFProcess p = new RFProcess(cmd);
    p._stdout = stdout;
    p.start(); p.join(MAX_RUNNING_TIME); p.cleanup();
    p.join(); // in case we timed out...
    File resultFile = new File(resultDB);
    boolean writeColumnNames = !resultFile.exists();
    FileWriter fw = new FileWriter(resultFile, true);
    if( writeColumnNames ) {
      fw.write("Timestamp, Command");
      for( String s : RESULTS )
        fw.write(", " + s);
        fw.write("\n");
    }
    fw.write(new SimpleDateFormat("yyyy.MM.dd HH:mm").format(new Date(System.currentTimeMillis())) + ","+cmd+",");
    if (check) {
      String expect = errorRates.get(cmd);
     // if (expect != null)
      //  assertEquals(expect,p.exception);
    }
    if( p.exception != null ) {
      System.err.println("Error: " + p.exception);
      fw.write("," + p.exception);
    } else _im.print(fw);
    fw.write("\n");
    fw.close();
    return true;
  }

  private static int[] size(int[] multiples, int max){
    int off =0; while(multiples[off]<max) off++;
    return Arrays.copyOf(multiples, off+1);
  }

  /** Builds a list of test input files */
  static String[] makeFiles(String[] files, int[] multiples) {
    LinkedList<String> ls = new LinkedList();
    for(String f:files)
      try { ls.addAll( Arrays.asList(makeFile(f,multiples)) ); } catch (IOException e) { throw new Error(e); }
    String[] res = new String[ls.size()];
    ls.toArray(res);
    return res;
  }
  /** Creates a multiply repeated file */
  static String[] makeFile(String fn,int[] multiples) throws IOException {
    File f = new File(fn);
    String name = f.getName();
    StringBuilder bldr = new StringBuilder();
    Reader r = new FileReader(f);
    char[] buf = new char[1024 * 1024];
    int n = 0;
    String[] names = new String[multiples.length];
    while( (n = r.read(buf)) > 0 ) bldr.append(buf, 0, n);
    r.close();
    String content = bldr.toString();
    bldr = null;
    int off=0;
    for( int m : multiples ) {
      File f2 = new File(names[off++] = "/tmp/"+ name + "."+ m);
      FileWriter fw = new FileWriter(f2, true);
      for( int i = 0; i <= m; ++i )  fw.write(content);
      fw.close();
    }
    return names;
  }


  static void runTests(String javaCmd, PrintStream out, OptArgs args,boolean check) throws Exception {
    int[] szMultiples = size(sizeMultiples, args.maxConcat);
    int[] szTrees = ntrees;
    String[] stats  = stat_types;
    boolean[] threading = new boolean[]{true,false};
    int[] seeds = new int[]{ 3, 42, 135};
    String[] files = makeFiles(args.files.split(","),szMultiples);

    int experiments = files.length * szTrees.length*stats.length*threading.length*seeds.length;
    String[] commands = new String[experiments];
    int i = 0;
    for(String f : files)
        for (int sz :szTrees)
          for(String stat : stats)
            for(int seed : seeds) {
              RFArgs rfa = new RFArgs();
              rfa.seed = seed; rfa.statType = stat; rfa.file = f;
              rfa.ntrees = sz;
              commands[i++] = javaCmd + " " + rfa;
            }

    for( String cmd : commands)
       runTest(cmd, args.resultDB, out,check);
  }

  static HashMap<String,String> special = new HashMap<String,String>();
  static HashMap<String,String> errorRates = new HashMap<String,String>();

  static {
    special.put("smalldata//stego/stego_training.data", "-classcol=1");
    special.put("smalldata//stego/stego_testing.data", "-classcol=1");

    errorRates.put(" -Xmx4g -jar build/h2o.jar -mainClass hex.rf.RandomForest  -file=smalldata//cars.csv -ntrees=10 -depth=2147483647 -cutRate=-1.0 -statType=gini -seed=3"
, "28%");
  };

  public static void basicTests(String javaCmd, PrintStream out, OptArgs args) throws Exception {
    String[][] files = new String[][]{
        {"smalldata//cars.csv",null},
        {"smalldata//hhp_9_17_12.predict.100rows.data",null},
        {"smalldata//iris/iris2.csv",null},
        {"smalldata//logreg/benign.csv",null},
        {"smalldata//logreg/prostate.csv",null},
        {"smalldata//poker/poker-hand-testing.data",null},
        {"smalldata//poker/poker10",null},
        {"smalldata//poker/poker100",null},
        {"smalldata//poker/poker1000",null},
        {"smalldata//stego/stego_testing.data",null},
        {"smalldata//stego/stego_training.data",null},
        {"smalldata//test/arit.csv",null},
        {"smalldata//test/test_all_raw_top10rows.csv",null},
        {"smalldata//test/test_domains_and_column_names.csv",null},
        {"smalldata//test/test_less_than_65535_unique_names.csv",null},
        {"smalldata//test/test_more_than_65535_unique_names.csv",null},
        {"smalldata//test/test_var.csv",null} };
    testIt( "", files, //files
        new int[]{50}/*trees*/, new int[]{10000} /*bin*/, null/*sample*/,
         new String[]{"entropy"}, null/*ignore*/, null, null,null);// seeds ,  staras    int[] szTrees = new int[]{10};
  }

  private static final String path = "../";

  public static void cT() {
    testIt( path+"datasets/bench/covtype/h2o", new String[][]{{"train.csv","test.csv"}}, //files
       fromToStride(50,500,10)/*trees*/, new int[]{10000} /*bin*/, new int[]{50}/*sample*/,
        new String[]{"entropy"}, null/*ignore*/, null, null,null);// seeds ,  staras
  }

  public static void cT2() {
    testIt( path+"datasets/bench/covtype/h2o", new String[][]{{"train.csv","test.csv"}}, //files
       fromToStride(50,500,10)/*trees*/, new int[]{10000} /*bin*/, new int[]{50}/*sample*/,
        new String[]{"entropy"}, null/*ignore*/, null, new String[]{null,"2:5000,3:5000,4:5000,5:5000,6:5000","0:5000,1:5000,2:5000,3:5000,4:5000,5:5000,6:5000"},null);// seeds ,  staras
  }

  public static void cT3() {
    testIt( path+"datasets/bench/covtype/h2o", new String[][]{{"train.csv","test.csv"}}, //files
        new int[]{150} /*trees*/, new int[]{10000} /*bin*/, fromToStride(1,99,1)/*sample*/,
        new String[]{"entropy"}, null/*ignore*/, null, null,null);// seeds ,  staras
  }

  public static void cT4() {
    testIt( path+"datasets/bench/covtype/h2o", new String[][]{{"train.csv","test.csv"}}, //files
        new int[]{150} /*trees*/, new int[]{10000} /*bin*/, new int[]{50} /*sample*/,
        new String[]{"entropy"}, stringIt(fromToStride(0,53,1))/*ignore*/, null, null,null);// seeds ,  staras
  }
  public static void cT5() {
    testIt( path+"datasets/bench/covtype/h2o", new String[][]{{"train.csv","test.csv"}}, //files
        new int[]{200,300} /*trees*/, new int[]{10000} /*bin*/, new int[]{50,60,70,80} /*sample*/,
        new String[]{"entropy"}, new String[]{"1,2,6,7,8"}/*ignore*/, null, null,null);// seeds ,  staras
  }
  public static void cT6() {
    testIt( path+"datasets/bench/covtype/h2o", new String[][]{{"train.csv","test.csv"}}, //files
        new int[]{200,300} /*trees*/, new int[]{10000} /*bin*/, new int[]{50,60,70,80} /*sample*/,
        new String[]{"entropy"}, new String[]{"1,2,6,7,8"}/*ignore*/, null, null,fromToStride(1,53,1));// seeds ,  staras
  }
  public static void cT7() {
    testIt( path+"datasets/bench/covtype/h2o", new String[][]{{"train.csv","test.csv"}}, //files
        new int[]{400} /*trees*/, new int[]{10000} /*bin*/, new int[]{50,60,70,80,90} /*sample*/,
        new String[]{"entropy"}, new String[]{"1,2,6,7,8"}/*ignore*/, null, null,new int[]{30});// seeds ,  staras
  }

  public static void kCS1() {
    special.put(path+"datasets/bench/kaggle.creditsample/h2o/train.csv","-classcol=0");
    testIt( path+"datasets/bench/kaggle.creditsample/h2o", new String[][]{{"train.csv","test.csv"}}, //files
        new int[]{50,100} /*trees*/, new int[]{100,200,500,1000} /*bin*/, new int[]{80} /*sample*/,
        new String[]{"entropy"}, null/*ignore*/, null, null,null);// seeds ,  staras
  }


  static String javaCmd_;
  static PrintStream out_;
  static OptArgs args_;

  public static String[] stringIt(int[] vs) {
    String[] res = new String[vs.length];
    for(int i=0;i<res.length;i++) res[i]=Integer.toString(vs[i]);
    return res;
  }
  public static void  testIt(String path, String[][] files, int[] tree_size,
                             int[] bin_limits, int[] samples, String[] stats,
                             String[] ignores, int[] seeds, String[] stratas,
                             int[]features)  {
    if (path==null) path="";
    if (files==null) files = new String[][]{{"smalldata/iris/iris.csv",null}};
    if (tree_size==null) tree_size=new int[]{100};
    if (bin_limits==null) bin_limits = new int[]{10000};
    if (samples==null) samples=new int[]{67};
    if (stats==null) stats=new String[]{"entropy"};
    if (ignores==null) ignores=new String[]{null};
    if (seeds==null) seeds = new int[]{3};
    if (stratas==null) stratas = new String[]{null};
    if (features==null) features = new int[]{-1};
    int experiments = files.length*tree_size.length*stats.length*samples.length*seeds.length *
          bin_limits.length*ignores.length*stratas.length*features.length;
    String[] commands = new String[experiments];
    for(int i=0;i<files.length;i++) {
      files[i][0]=path+"/"+files[i][0]; if (files[i][1]!=null) files[i][1]=path+"/"+files[i][1];
    }
    int i = 0;
    for(String ig : ignores) for(String [] f : files)  for (int sz :tree_size)
      for(String stat : stats) for(int  smpl : samples)  for(int  bl : bin_limits)
            for(int seed : seeds) for(String strata: stratas) for(int feat:features) {
              RFArgs rfa = new RFArgs();
              rfa.seed = seed; rfa.statType = stat; rfa.file = f[0];
              if (f[1]!=null) rfa.validationFile = f[1];
              rfa.ntrees = sz; rfa.sample= smpl; rfa.binLimit = bl;
              if (ig!=null) rfa.ignores=ig; rfa.features= feat;
              if (strata!= null) { rfa.stratify = true; rfa.strata = strata;}
              String add = special.get(f[0])==null? "" : (" "+special.get(f[0]));
              commands[i++] = javaCmd_ + " " + rfa + add;
            }
    try { for( String cmd : commands)runTest(cmd, args_.resultDB, out_, true);
    } catch (Exception e)  { throw new Error(e); }
  }

  public static int[] fromToStride(int f, int t, int s){
    int sz = (t-f)/s; int[] ret = new int[sz+1]; int j = 0;
    for(int i = f; i<=t; i+=s) ret[j++]=i;
    return ret;
  }


  public static void quickTests(String javaCmd, PrintStream out, OptArgs args) throws Exception {
    String[] files = new String[]{
    "smalldata//cars.csv",
    "smalldata//hhp_9_17_12.predict.100rows.data",
    "smalldata//iris/iris2.csv", };

    int[] szTrees = new int[]{10};
    String[] stats  = new String[]{"entropy"};
    boolean[] threading = new boolean[]{true};
    int[] seeds = new int[]{ 3};
    int experiments = files.length * szTrees.length*stats.length*threading.length*seeds.length;
    String[] commands = new String[experiments];
    int i = 0;
    for(String f : files)
        for (int sz :szTrees)
          for(String stat : stats)
            for(int seed : seeds) {
              RFArgs rfa = new RFArgs();
              rfa.seed = seed; rfa.statType = stat; rfa.file = f;  rfa.ntrees = sz;
              String add = special.get(f)==null? "" : (" "+special.get(f));
              commands[i++] = javaCmd + " " + rfa + add;
            }
    for( String cmd : commands) runTest(cmd, args.resultDB, out, true);
  }


  public static void main(String[] args) throws Exception {
    OptArgs ARGS = args_ = new OptArgs();
    new Arguments(args).extract(ARGS);
    out_= new PrintStream(new File("/tmp/RFRunner.stdout.txt"));
    javaCmd_ =   ARGS.jvmArgs + " " + JAR + " " + MAIN;
    kCS1(); //cT7(); //kCS1(); //cT5();// cT2(); cT3(); cT4();
  }

  @org.junit.Test
  public void test_Others() throws Exception {
    final OptArgs ARGS        = new OptArgs();
    PrintStream out = new PrintStream(new File("/tmp/RFRunner.stdout.txt"));
    String javaCmd =   ARGS.jvmArgs + " " + RFRunner.JAR + " " + RFRunner.MAIN;
    try { RFRunner.quickTests(javaCmd, out, ARGS ); } finally { out.close(); }
  }
}
