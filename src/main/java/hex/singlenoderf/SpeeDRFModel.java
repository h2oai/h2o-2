package hex.singlenoderf;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import water.*;
import water.api.DocGen;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Counter;
import java.util.Arrays;
import java.util.Random;


public class SpeeDRFModel extends Model implements Job.Progress {

  @API(help = "Number of features these trees are built for.")
  int features;

  @API(help = "Sampling strategy used for model")
  Sampling.Strategy sampling_strategy;

  @API(help = " Sampling rate used when building trees.")
  float sample;

  @API(help = "Strata sampling rate used for local-node strata-sampling")
  float[] strata_samples;

  @API(help = "Number of split features defined by user.")
  int mtry;

  @API(help = " Number of computed split features per node.")
  int[] node_split_features;

  @API(help = "Number of keys the model expects to be built for it.")
  int total_trees;

  @API(help = "Max depth to grow trees to")
  int depth;

  @API(help = "All the trees in the model.")
  Key[]     t_keys;

  @API(help = "Local forests produced by nodes.")
  Key[][]   local_forests;

  @API(help = "Total time in seconds to produce the model.")
  long time;

  @API(help = "Frame being operated on.")
  Frame fr;

  @API(help = "Response Vector.")
  Vec response;

  @API(help = "Class weights.")
  double[] weights;

  @API(help = "bin limit")
  int bin_limit;

  @API(help = "Raw tree data. for faster classification passes.")
  transient byte[][] trees;

  @API(help = "Job key")
  Key jobKey;

  @API(help = "")
  String current_status;

//  @API(help = "No CM")
//  boolean _noCM;
//
//  @API(help = "Iterative CM")
//  boolean iterative_cm;


  @API(help = "Out of bag error estimate.")
  boolean oobee;

  @API(help = "Class column idx.")
  int classcol;

  @API(help = "Data Key")
  Key dataKey;

  public static final String KEY_PREFIX = "__RFModel_";
  public static final String JSON_CONFUSION_KEY   = "confusion_key";
//  public static final String JSON_CLEAR_CM        = "clear_confusion_matrix";
//  public static final String JSON_REFRESH_THRESHOLD_CM = "refresh_threshold_cm";

  public static final String JSON_CM_TYPE         = "type";
  public static final String JSON_CM_HEADER       = "header";
  public static final String JSON_CM_MATRIX       = "scores";
  public static final String JSON_CM_TREES        = "used_trees";
  public static final String JSON_CM_CLASS_ERR    = "classification_error";
  public static final String JSON_CM_ROWS         = "rows";
  public static final String JSON_CM_ROWS_SKIPPED = "rows_skipped";
  public static final String JSON_CM_CLASSES_ERRORS = "classes_errors";


  public SpeeDRFModel(Key selfKey, Key jobKey, Key dataKey, Frame fr, Vec response, Key[] t_keys) {
    super(selfKey, dataKey, fr);
    int csize = H2O.CLOUD.size();
    this.fr = fr;
    this.response = response;
    this.time = 0;
    this.local_forests = new Key[csize][];
    for(int i=0;i<csize;i++) this.local_forests[i] = new Key[0];
    this.t_keys = t_keys;
    this.node_split_features = new int[csize];
    for( Key tkey : t_keys ) assert DKV.get(tkey)!=null;
    this.jobKey = jobKey;
    this.classcol = fr.find(response);
    this.dataKey = dataKey;
    this.current_status = "Initializing Model";
  }

  public Vec get_response() {
    return response;
  }

  public int treeCount() { return t_keys.length; }
  public int size()      { return t_keys.length; }
  public int classes()   { return (int)(response.max() - response.min() + 1); }

  public static Key makeKey() {
    return Key.make(KEY_PREFIX + Key.make());
  }

  static public SpeeDRFModel make(SpeeDRFModel old, Key tkey, int nodeIdx) {
    SpeeDRFModel m = (SpeeDRFModel)old.clone();
    m.t_keys = Arrays.copyOf(old.t_keys, old.t_keys.length + 1);
    m.t_keys[m.t_keys.length-1] = tkey;

    m.local_forests[nodeIdx] = Arrays.copyOf(old.local_forests[nodeIdx],old.local_forests[nodeIdx].length+1);
    m.local_forests[nodeIdx][m.local_forests[nodeIdx].length-1] = tkey;
    return m;
  }

  public String name(int atree) {
    if( atree == -1 ) atree = size();
    assert atree <= size();
    return _key.toString() + "[" + atree + "]";
  }

  /** Return the bits for a particular tree */
  public byte[] tree(int tree_id) {
    byte[][] ts = trees;
    if( ts == null ) trees = ts = new byte[tree_id+1][];
    if( tree_id >= ts.length ) trees = ts = Arrays.copyOf(ts,tree_id+1);
    if( ts[tree_id] == null ) ts[tree_id] = DKV.get(t_keys[tree_id]).memOrLoad();
    return ts[tree_id];
  }

  /** Free all internal tree keys. */
  @Override public Futures delete_impl(Futures fs) {
    for( Key k : t_keys )
      UKV.remove(k,fs);
    return fs;
  }

  /**
   * Classify a row according to one particular tree.
   * @param tree_id  the number of the tree to use
   * @param chunks    the chunk we are using
   * @param row      the row number in the chunk
   * @param modelDataMap  mapping from model/tree columns to data columns
   * @return the predicted response class, or class+1 for broken rows
   */
  public short classify0(int tree_id, Frame fr, Chunk[] chunks, int row, int modelDataMap[], short badrow) {
    return Tree.classify(new AutoBuffer(tree(tree_id)), fr, chunks, row, modelDataMap, badrow);
  }

  private void vote(Frame fr, Chunk[] chks, int row, int modelDataMap[], int[] votes) {
    int numClasses = classes();
    assert votes.length == numClasses + 1 /* +1 to catch broken rows */;
    for( int i = 0; i < treeCount(); i++ )
      votes[classify0(i, fr, chks, row, modelDataMap, (short) numClasses)]++;
  }

  public short classify(Frame fr, Chunk[] chks, int row, int modelDataMap[], int[] votes, double[] classWt, Random rand ) {
    // Vote all the trees for the row
    vote(fr, chks, row, modelDataMap, votes);
    return classify(votes, classWt, rand);
  }

  public short classify(int[] votes, double[] classWt, Random rand) {
    // Scale the votes by class weights: it as-if rows of the weighted classes
    // were replicated many times so get many votes.
    if( classWt != null )
      for( int i=0; i<votes.length-1; i++ )
        votes[i] = (int) (votes[i] * classWt[i]);
    // Tally results
    int result = 0;
    int tied = 1;
    for( int i = 1; i < votes.length - 1; i++ )
      if( votes[i] > votes[result] ) { result=i; tied=1; }
      else if( votes[i] == votes[result] ) { tied++; }
    if( tied == 1 ) return (short) result;
    // Tie-breaker logic
    int j = rand == null ? 0 : rand.nextInt(tied); // From zero to number of tied classes-1
    int k = 0;
    for( int i = 0; i < votes.length - 1; i++ )
      if( votes[i]==votes[result] && (k++ >= j) )
        return (short)i;
    throw H2O.unimpl();
  }

  // The seed for a given tree
  long seed(int ntree) { return UDP.get8(tree(ntree), 4); }
  // The producer for a given tree
  byte producerId(int ntree) { return tree(ntree)[12]; }

  // Lazy initialization of tree leaves, depth
  private transient Counter _tl, _td;

  /** Internal computation of depth and number of leaves. */
  public void find_leaves_depth() {
    if( _tl != null ) return;
    _td = new Counter();
    _tl = new Counter();
    for( Key tkey : t_keys ) {
      long dl = Tree.depth_leaves(new AutoBuffer(DKV.get(tkey).memOrLoad()));
      _td.add((int) (dl >> 32));
      _tl.add((int) dl);
    }
  }
  public Counter leaves() { find_leaves_depth(); return _tl; }
  public Counter depth()  { find_leaves_depth(); return _td; }

  public int[] colMap(String[] names) {
    int res[] = new int[names.length];
    for(int i = 0; i < res.length; i++) {
      res[i] = fr.find(names[i]);
    }
    return res;
  }

  @Override
  protected float[] score0(double[] data, float[] preds) {
    int numClasses = classes();
    int votes[] = new int[numClasses + 1/* +1 to catch broken rows */];
    for( int i = 0; i < treeCount(); i++ )
      votes[(int) Tree.classify(new AutoBuffer(tree(i)), data, numClasses)]++;
    return new float[]{(float) (classify(votes, null, null) + get_response().min())};
  }

  @Override
  public float progress() {
    return (float) t_keys.length / (float) total_trees;
  }

  static String[] cfDomain(final CMTask.CMFinal cm, int maxClasses) {
    String[] dom = cm.domain();
    if (dom.length > maxClasses)
      throw new IllegalArgumentException("The column has more than "+maxClasses+" values. Are you sure you have that many classes?");
    return dom;
  }

  public void generateHTML(String title, StringBuilder sb) {
    int tasks;
    int finished;
    double[] weights = this.weights;
    // Finish refresh after rf model is done and confusion matrix for all trees is computed

    tasks    = this.total_trees;
    finished = this.size();


    if(title != null && !title.isEmpty()) DocGen.HTML.title(sb, title);
    DocGen.HTML.paragraph(sb, "Model Key: " + _key);

    DocGen.HTML.section(sb, "SpeeDRF Output:");
    sb.append("Current Status: " + this.current_status);


    int modelSize = tasks * 25/100;
    modelSize = modelSize == 0 || finished==tasks ? finished : modelSize * (finished/modelSize);


    if (tasks * 1. / (1.*finished) > 0.50) {
      CMTask cmTask = new CMTask(this, modelSize, weights, this.oobee);
      cmTask.doAll(this.fr);
      CMTask.CMFinal confusion = CMTask.CMFinal.make(cmTask._matrix, this, cmTask.domain(), cmTask._errorsPerTree, this.oobee);
      if (confusion!=null && confusion.valid() && modelSize > 0) {
        //finished += 1;
        JsonObject cm       = new JsonObject();
        JsonArray  cmHeader = new JsonArray();
        JsonArray  matrix   = new JsonArray();
        cm.addProperty(JSON_CM_TYPE, oobee ? "OOB error estimate" : "full scoring");
        cm.addProperty(JSON_CM_CLASS_ERR, confusion.classError());
        cm.addProperty(JSON_CM_ROWS_SKIPPED, confusion.skippedRows());
        cm.addProperty(JSON_CM_ROWS, confusion.rows());
        // create the header
        for (String s : cfDomain(confusion, 1024))
          cmHeader.add(new JsonPrimitive(s));
        cm.add(JSON_CM_HEADER,cmHeader);
        // add the matrix
        final int nclasses = confusion.dimension();
        JsonArray classErrors = new JsonArray();
        for (int crow = 0; crow < nclasses; ++crow) {
          JsonArray row  = new JsonArray();
          int classHitScore = 0;
          for (int ccol = 0; ccol < nclasses; ++ccol) {
            row.add(new JsonPrimitive(confusion.matrix(crow,ccol)));
            if (crow!=ccol) classHitScore += confusion.matrix(crow,ccol);
          }
          // produce infinity members in case of 0.f/0
          classErrors.add(new JsonPrimitive((float)classHitScore / (classHitScore + confusion.matrix(crow,crow))));
          matrix.add(row);
        }
        cm.add(JSON_CM_CLASSES_ERRORS, classErrors);
        cm.add(JSON_CM_MATRIX,matrix);
        cm.addProperty(JSON_CM_TREES,modelSize);
        // Signal end only and only if all trees were generated and confusion matrix is valid

      DocGen.HTML.section(sb, "Confusion Matrix:");

      if (cm.has(JSON_CM_MATRIX)) {
        sb.append("<h3>Confusion matrix - ").append(cm.get(JSON_CM_TYPE).getAsString()).append("</h3>");
        sb.append("<dl class='dl-horizontal'>");
        sb.append("<dt>classification error</dt><dd>").append(String.format("%5.3f %%", 100*cm.get(JSON_CM_CLASS_ERR).getAsFloat())).append("</dd>");
        long rows = cm.get(JSON_CM_ROWS).getAsLong();
        long skippedRows = cm.get(JSON_CM_ROWS_SKIPPED).getAsLong();
        sb.append("<dt>used / skipped rows </dt><dd>").append(String.format("%d / %d (%3.1f %%)", rows, skippedRows, (double)skippedRows*100/(skippedRows+rows))).append("</dd>");
        sb.append("<dt>trees used</dt><dd>").append(cm.get(JSON_CM_TREES).getAsInt()).append("</dd>");
        sb.append("</dl>");
        sb.append("<table class='table table-striped table-bordered table-condensed'>");
        sb.append("<tr><th>Actual \\ Predicted</th>");
        JsonArray header = (JsonArray) cm.get(JSON_CM_HEADER);
        for (JsonElement e: header)
          sb.append("<th>").append(e.getAsString()).append("</th>");
        sb.append("<th>Error</th></tr>");
        int classes = header.size();
        long[] totals = new long[classes];
        JsonArray matrix2 = (JsonArray) cm.get(JSON_CM_MATRIX);
        long sumTotal = 0;
        long sumError = 0;
        for (int crow = 0; crow < classes; ++crow) {
          JsonArray row = (JsonArray) matrix2.get(crow);
          long total = 0;
          long error = 0;
          sb.append("<tr><th>").append(header.get(crow).getAsString()).append("</th>");
          for (int ccol = 0; ccol < classes; ++ccol) {
            long num = row.get(ccol).getAsLong();
            total += num;
            totals[ccol] += num;
            if (ccol == crow) {
              sb.append("<td style='background-color:LightGreen'>");
            } else {
              sb.append("<td>");
              error += num;
            }
            sb.append(num);
            sb.append("</td>");
          }
          sb.append("<td>");
          sb.append(String.format("%5.3f = %d / %d", (double)error/total, error, total));
          sb.append("</td></tr>");
          sumTotal += total;
          sumError += error;
        }
        sb.append("<tr><th>Totals</th>");
        for (long total : totals) sb.append("<td>").append(total).append("</td>");
        sb.append("<td><b>");
        sb.append(String.format("%5.3f = %d / %d", (double)sumError/sumTotal, sumError, sumTotal));
        sb.append("</b></td></tr>");
        sb.append("</table>");
      } else {
        sb.append("<div class='alert alert-info'>");
        sb.append("Confusion matrix is being computed into the key:</br>");
        sb.append(cm.get(JSON_CONFUSION_KEY).getAsString());
        sb.append("</div>");
      }
      }
    }
  }
}