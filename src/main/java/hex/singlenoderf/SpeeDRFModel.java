package hex.singlenoderf;

import static hex.singlenoderf.VariableImportance.asVotes;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import hex.ConfusionMatrix;
import hex.VarImp;
import hex.gbm.DTree.TreeModel.TreeStats;
import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Counter;
import water.util.ModelUtils;

import java.util.Arrays;
import java.util.Random;


public class SpeeDRFModel extends Model implements Job.Progress {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.


  /**
   * Model Parameters
   */
  /* Number of features these trees are built for */                      int features;
  /* Sampling strategy used for model */                                  Sampling.Strategy sampling_strategy;
  @API(help = " Sampling rate used when building trees.")                 float sample;
  @API(help = "Strata sampling rate used for local-node strata-sampling") float[] strata_samples;
  @API(help = "Number of split features defined by user.")                int mtry;
  /* Number of computed split features per node */                        int[] node_split_features;
  @API(help = "Number of keys the model expects to be built for it.")     int N;
  @API(help = "Max depth to grow trees to")                               int max_depth;
  @API(help = "All the trees in the model.")                              Key[] t_keys;
  /* Local forests produced by nodes */                                   Key[][] local_forests;
  /* Total time in seconds to produce the model */                        long time;
  /* Frame being operated on */                                           Frame fr;
  /* Response Vector */                                                   Vec response;
  /* Class weights */                                                     double[] weights;
  @API(help = "bin limit")                                                int nbins;
  /* Raw tree data. for faster classification passes */                   transient byte[][] trees;
  @API(help = "Job key")                                                  Key jobKey;
  /* Destination Key */                                                   Key dest_key;
  /* Current model status */                                              String current_status;
  @API(help = "MSE by tree")                                              float[] errs;
  /* Statistic Type */                                                    Tree.StatType statType;
  /* Adapted Validation Frame */                                          Frame test_frame;
  @API(help = "Test Key")                                                 Key testKey;
  /* Out of bag error estimate */                                         boolean oobee;
  /* Seed */                                                              protected long zeed;
  /* Variable Importance */                                               boolean importance;
  /* Final Confusion Matrix */                                            CMTask.CMFinal confusion;
  @API(help = "Confusion Matrices")                                       ConfusionMatrix[] cms;
  /* Confusion Matrix */                                                  long[][] cm;
  @API(help = "Tree Statistics")                                          TreeStats treeStats;
  @API(help = "cmDomain")                                                 String[] cmDomain;
  @API(help = "AUC")                                                      public AUC validAUC;
  @API(help = "Variable Importance")                                      public VarImp varimp;
  /* Regression or Classification */                                      boolean regression;
  /* Score each iteration? */                                             boolean score_each;

  /**
   * Extra helper variables.
   */
  private transient VariableImportance.TreeMeasures[/*features*/] _treeMeasuresOnOOB;
  // Tree votes/SSE per individual features on permutated OOB rows
  private transient VariableImportance.TreeMeasures[/*features*/] _treeMeasuresOnSOOB;

  public static final String JSON_CONFUSION_KEY   = "confusion_key";
  public static final String JSON_CM_TYPE         = "type";
  public static final String JSON_CM_HEADER       = "header";
  public static final String JSON_CM_MATRIX       = "scores";
  public static final String JSON_CM_TREES        = "used_trees";
  public static final String JSON_CM_CLASS_ERR    = "classification_error";
  public static final String JSON_CM_ROWS         = "rows";
  public static final String JSON_CM_ROWS_SKIPPED = "rows_skipped";
  public static final String JSON_CM_CLASSES_ERRORS = "classes_errors";

  @API(help = "Model parameters", json = true)
  private final SpeeDRF parameters;

  @Override public final SpeeDRF get_params() { return parameters; }
  @Override public final Request2 job() {
    return get_params();
  }

  public SpeeDRFModel(Key selfKey, Key dataKey, Frame fr, SpeeDRF params) {
    super(selfKey, dataKey, fr);
    this.dest_key = selfKey;
    this.parameters = params;
    score_each = params.score_each_iteration;
    regression = !(params.classification);
    _domain = regression ? null : fr.lastVec().toEnum().domain();
  }

  public Vec get_response() {
    return response;
  }

  public int treeCount() { return t_keys.length; }
  public int size()      { return t_keys.length; }
  public int classes()   {
    return regression ? 1 : (int)(response.max() - response.min() + 1);
  }

  //FIXME: Model._domain should be used for nclasses() and classNames()
  static String[] _domain = null;
  @Override public int nclasses() { return classes(); }
  @Override public String[] classNames() { return regression ? null : _domain; }

  private static boolean doScore(SpeeDRFModel m) {
    return m.score_each || m.t_keys.length == 1 || m.t_keys.length == m.N;
  }

  public static SpeeDRFModel make(SpeeDRFModel old, Key tkey, int nodeIdx) {

    // Create a new model for atomic update
    SpeeDRFModel m = (SpeeDRFModel)old.clone();

    // Update the tree keys with the new one (tkey)
    m.t_keys = Arrays.copyOf(old.t_keys, old.t_keys.length + 1);
    m.t_keys[m.t_keys.length-1] = tkey;

    // Update the local_forests
    m.local_forests[nodeIdx] = Arrays.copyOf(old.local_forests[nodeIdx],old.local_forests[nodeIdx].length+1);
    m.local_forests[nodeIdx][m.local_forests[nodeIdx].length-1] = tkey;

    // Do not score every time because it's slow and isn't necessary.
    // Only score the first tree and when the whole forest is available.
    boolean shouldScore = doScore(m);

    if (shouldScore) {

      // Gather the results
      CMTask cmTask = new CMTask(m, m.size(), m.weights, m.oobee);
      cmTask.doAll(m.test_frame == null ? m.fr : m.test_frame, true);

      // Perform the regression scoring
      if (m.regression) {
        float mse = cmTask._ss / ( (float) (cmTask._rowcnt) );
        m.errs = Arrays.copyOf(old.errs, old.errs.length+1);
        m.errs[m.errs.length - 1] = mse;
        m.cms = Arrays.copyOf(old.cms, old.cms.length+1);
        m.cms[m.cms.length-1] = null;

      // Perform the classification scoring
      } else {
        _domain = cmTask.domain();
        m.confusion = CMTask.CMFinal.make(cmTask._matrix, m, cmTask.domain(), cmTask._errorsPerTree, m.oobee, cmTask._sum, cmTask._cms);
        m.cm = cmTask._matrix._matrix;

        m.errs = Arrays.copyOf(old.errs, old.errs.length+1);
        m.errs[m.errs.length - 1] = m.confusion.mse();
        m.cms = Arrays.copyOf(old.cms, old.cms.length+1);
        ConfusionMatrix new_cm = new ConfusionMatrix(m.confusion._matrix);
        m.cms[m.cms.length-1] = new_cm;

        // Create the ROC Plot
        if (m.classes() == 2) {
          m.validAUC= makeAUC(toCMArray(m.confusion._cms), ModelUtils.DEFAULT_THRESHOLDS, m.cmDomain);
        }

        // Launch a Variable Importance Task
        if (m.importance && !m.regression)
          m.varimp = m.doVarImpCalc(m);
      }
    } else {
      m.errs = Arrays.copyOf(old.errs, old.errs.length+1);
      m.errs[m.errs.length - 1] = -1.f;
      m.cms = Arrays.copyOf(old.cms, old.cms.length+1);
      m.cms[m.cms.length-1] = null;
    }

    // Tree Statistics
    JsonObject trees = new JsonObject();
    trees.addProperty(Constants.TREE_COUNT,  m.size());
    if( m.size() > 0 ) {
      trees.add(Constants.TREE_DEPTH,  m.depth().toJson());
      trees.add(Constants.TREE_LEAVES, m.leaves().toJson());
    }
    TreeStats treeStats = new TreeStats();
    double[] depth_stats = stats(trees.get(Constants.TREE_DEPTH));
    double[] leaf_stats = stats(trees.get(Constants.TREE_LEAVES));

    if(depth_stats != null) {
      treeStats.minDepth = (int)depth_stats[0];
      treeStats.meanDepth = (float)depth_stats[1];
      treeStats.maxDepth = (int)depth_stats[2];
      treeStats.minLeaves = (int)leaf_stats[0];
      treeStats.meanLeaves = (float)leaf_stats[1];
      treeStats.maxLeaves = (int)leaf_stats[2];
    } else {
      treeStats = null;
    }
    m.treeStats = treeStats;
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
  public float classify0(int tree_id, Frame fr, Chunk[] chunks, int row, int modelDataMap[], short badrow, boolean regression) {
    return Tree.classify(new AutoBuffer(tree(tree_id)), fr, chunks, row, modelDataMap, badrow, regression);
  }

  private void vote(Frame fr, Chunk[] chks, int row, int modelDataMap[], int[] votes) {
    int numClasses = classes();
    assert votes.length == numClasses + 1 /* +1 to catch broken rows */;
    for( int i = 0; i < treeCount(); i++ )
      votes[(int)classify0(i, fr, chks, row, modelDataMap, (short) numClasses, false)]++;
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
      long dl = Tree.depth_leaves(new AutoBuffer(DKV.get(tkey).memOrLoad()), regression);
      _td.add((int) (dl >> 32));
      _tl.add((int) dl);
    }
  }
  public Counter leaves() { find_leaves_depth(); return _tl; }
  public Counter depth()  { find_leaves_depth(); return _td; }


  private static int find(String n, String[] names) {
    if( n == null ) return -1;
    for( int j = 0; j<names.length; j++ )
      if( n.equals(names[j]) )
        return j;
    return -1;
  }

  public int[] colMap(String[] names) {
    int res[] = new int[fr._names.length]; //new int[names.length];
    for(int i = 0; i < res.length; i++) {
      res[i] = find(fr.names()[i], names);
    }
    return res;
  }

  @Override
  protected float[] score0(double[] data, float[] preds) {
    int numClasses = classes();
    if (numClasses == 1) {
      float p = 0.f;
      for (int i = 0; i < treeCount(); ++i) {
        p += Tree.classify(new AutoBuffer(tree(i)), data, numClasses, true);
      }
      return new float[]{p / (float)(1. * treeCount())};
    } else {
      int votes[] = new int[numClasses + 1/* +1 to catch broken rows */];
      preds = new float[numClasses + 1];
      for( int i = 0; i < treeCount(); i++ )
        votes[(int) Tree.classify(new AutoBuffer(tree(i)), data, numClasses, false)]++;
      float s = 0.f;
      for (int v : votes) s += (float)v;
      for (int i = 0; i  < votes.length - 1; ++i)
        preds[i+1] = (float)votes[i] / s;
      preds[0] = (float) (classify(votes, null, null) + get_response().min());
      return preds;
    }
  }

  @Override
  public float progress() {
    return (float) t_keys.length / (float) N;
  }

  static String[] cfDomain(final CMTask.CMFinal cm, int maxClasses) {
    String[] dom = cm.domain();
    if (dom.length > maxClasses)
      throw new IllegalArgumentException("The column has more than "+maxClasses+" values. Are you sure you have that many classes?");
    return dom;
  }

  public void generateHTML(String title, StringBuilder sb) {
    String style = "<style>\n"+
                    "td, th { min-width:60px;}\n"+
                    "</style>\n";
    sb.append(style);
    DocGen.HTML.title(sb,title);
    sb.append("<div class=\"alert\">").append("Actions: ");
    sb.append(Inspect2.link("Inspect training data (" + _dataKey.toString() + ")", _dataKey)).append(", ");
    if (this.test_frame != null)
      sb.append(Inspect2.link("Inspect testing data (" + testKey.toString() + ")", testKey)).append(", ");
    sb.append(Predict.link(_key, "Score on dataset" ));
    if (this.size() > 0 && this.size() < N && !Job.findJob(jobKey).isCancelledOrCrashed()) {
      sb.append(", ");
      sb.append("<i class=\"icon-stop\"></i>&nbsp;").append(Cancel.link(jobKey, "Cancel training"));
    }
    sb.append("</div>");
    DocGen.HTML.paragraph(sb,"Model Key: "+_key);
    DocGen.HTML.paragraph(sb,"Max max_depth: "+max_depth+", Nbins: "+nbins+", Trees: " + this.size());
    DocGen.HTML.paragraph(sb, "Sample Rate: "+sample + ", Seed: "+zeed+", mtry: "+mtry);
    sb.append("</pre>");

    if (this.size() > 0 && this.size() < N) sb.append("Current Status: ").append("Building Random Forest");
    else {
      if (this.size() == N) {
        sb.append("Current Status: ").append("Complete.");
      } else  {
        if( Job.findJob(jobKey).isCancelledOrCrashed()) {
          sb.append("Current Status: ").append("Cancelled.");
        } else {
          sb.append("Current Status: ").append(this.current_status);
        }
      }
    }

    //build cm
    if(!regression)
      buildCM(sb);
    sb.append("<br />");
    if( errs != null && this.size() > 0) {
      DocGen.HTML.section(sb,"Mean Squared Error by Tree");
      DocGen.HTML.arrayHead(sb);
      sb.append("<tr style='min-width:60px'><th>Trees</th>");
      int last = this.size(); // + 1;
      for( int i=last; i>=0; i-- )
        sb.append("<td style='min-width:60px'>").append(i).append("</td>");
      sb.append("</tr>");
      sb.append("<tr style='min-width: 60px;'><th style='min-width: 60px;' class='warning'>MSE</th>");
      for( int i=last; i>=0; i-- )
        sb.append( (!(Double.isNaN(errs[i]) || errs[i] <= 0.f)) ? String.format("<td style='min-width:60px'>%5.5f</td>",errs[i]) : "<td style='min-width:60px'>---</td>");
      sb.append("</tr>");
      DocGen.HTML.arrayTail(sb);
    }
    sb.append("<br/>");
    JsonObject trees = new JsonObject();
    trees.addProperty(Constants.TREE_COUNT,  this.size());
    if( this.size() > 0 ) {
      trees.add(Constants.TREE_DEPTH,  this.depth().toJson());
      trees.add(Constants.TREE_LEAVES, this.leaves().toJson());
    }
    generateHTMLTreeStats(sb, trees);

    if (validAUC != null) {
      generateHTMLAUC(sb);
    }
    if (varimp != null) {
      generateHTMLVarImp(sb);
    }
    printCrossValidationModelsHTML(sb);
  }

  static final String NA = "---";
  public void generateHTMLTreeStats(StringBuilder sb, JsonObject trees) {
    DocGen.HTML.section(sb,"Tree stats");
    DocGen.HTML.arrayHead(sb);
    sb.append("<tr><th>&nbsp;</th>").append("<th>Min</th><th>Mean</th><th>Max</th></tr>");

    TreeStats treeStats = new TreeStats();
    double[] depth_stats = stats(trees.get(Constants.TREE_DEPTH));
    double[] leaf_stats = stats(trees.get(Constants.TREE_LEAVES));

    sb.append("<tr><th>Depth</th>")
            .append("<td>").append(depth_stats != null ? depth_stats[0]  : NA).append("</td>")
            .append("<td>").append(depth_stats != null ? depth_stats[1] : NA).append("</td>")
            .append("<td>").append(depth_stats != null ? depth_stats[2] : NA).append("</td></tr>");
    sb.append("<th>Leaves</th>")
            .append("<td>").append(leaf_stats != null ? leaf_stats[0]  : NA).append("</td>")
            .append("<td>").append(leaf_stats != null ? leaf_stats[1] : NA).append("</td>")
            .append("<td>").append(leaf_stats != null ? leaf_stats[2]  : NA).append("</td></tr>");
    DocGen.HTML.arrayTail(sb);

    if(depth_stats != null && leaf_stats != null) {
      treeStats.minDepth = (int)depth_stats[0];
      treeStats.meanDepth = (float)depth_stats[1];
      treeStats.maxDepth = (int)depth_stats[2];
      treeStats.minLeaves = (int)leaf_stats[0];
      treeStats.meanLeaves = (float)leaf_stats[1];
      treeStats.maxLeaves = (int)leaf_stats[2];
    } else {
      treeStats = null;
    }
    this.treeStats = treeStats;
  }

  private static double[] stats(JsonElement json) {
    if( json == null ) {
      return null;
    } else {
      JsonObject obj = json.getAsJsonObject();
      return new double[]{
              obj.get(Constants.MIN).getAsDouble(),
              obj.get(Constants.MEAN).getAsDouble(),
              obj.get(Constants.MAX).getAsDouble()};
    }
  }

  public void buildCM(StringBuilder sb) {
    int tasks    = this.N;
    int finished = this.size();
    int modelSize = tasks * 25/100;
    modelSize = modelSize == 0 || finished==tasks ? finished : modelSize * (finished/modelSize);

    if (confusion!=null && confusion.valid() && modelSize > 0) {
      //finished += 1;
      JsonObject cm       = new JsonObject();
      JsonArray  cmHeader = new JsonArray();
      JsonArray  matrix   = new JsonArray();
      cm.addProperty(JSON_CM_TYPE, oobee ? "OOB" : "training");
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
      if (testKey != null)
        sb.append("<div class=\"alert\">Reported on ").append(Inspect2.link(testKey.toString(), testKey)).append("</div>");
      else
        sb.append("<div class=\"alert\">Reported on ").append(cm.get(JSON_CM_TYPE).getAsString()).append(" data</div>");

      if (cm.has(JSON_CM_MATRIX)) {
        sb.append("<dl class='dl-horizontal'>");
        sb.append("<dt>classification error</dt><dd>").append(String.format("%5.5f %%", 100*cm.get(JSON_CM_CLASS_ERR).getAsFloat())).append("</dd>");
        long rows = cm.get(JSON_CM_ROWS).getAsLong();
        long skippedRows = cm.get(JSON_CM_ROWS_SKIPPED).getAsLong();
        sb.append("<dt>used / skipped rows </dt><dd>").append(String.format("%d / %d (%3.1f %%)", rows, skippedRows, (double)skippedRows*100/(skippedRows+rows))).append("</dd>");
        sb.append("<dt>trees used</dt><dd>").append(cm.get(JSON_CM_TREES).getAsInt()).append("</dd>");
        sb.append("</dl>");
        sb.append("<table class='table table-striped table-bordered table-condensed'>");
        sb.append("<tr style='min-width: 60px;'><th style='min-width: 60px;'>Actual \\ Predicted</th>");
        JsonArray header = (JsonArray) cm.get(JSON_CM_HEADER);
        for (JsonElement e: header)
          sb.append("<th style='min-width: 60px;'>").append(e.getAsString()).append("</th>");
        sb.append("<th style='min-width: 60px;'>Error</th></tr>");
        int classes = header.size();
        long[] totals = new long[classes];
        JsonArray matrix2 = (JsonArray) cm.get(JSON_CM_MATRIX);
        long sumTotal = 0;
        long sumError = 0;
        for (int crow = 0; crow < classes; ++crow) {
          JsonArray row = (JsonArray) matrix2.get(crow);
          long total = 0;
          long error = 0;
          sb.append("<tr style='min-width: 60px;'><th style='min-width: 60px;'>").append(header.get(crow).getAsString()).append("</th>");
          for (int ccol = 0; ccol < classes; ++ccol) {
            long num = row.get(ccol).getAsLong();
            total += num;
            totals[ccol] += num;
            if (ccol == crow) {
              sb.append("<td style='background-color:LightGreen; min-width: 60px;'>");
            } else {
              sb.append("<td styile='min-width: 60px;'>");
              error += num;
            }
            sb.append(num);
            sb.append("</td>");
          }
          sb.append("<td style='min-width: 60px;'>");
          sb.append(String.format("%.05f = %,d / %d", (double)error/total, error, total));
          sb.append("</td></tr>");
          sumTotal += total;
          sumError += error;
        }
        sb.append("<tr style='min-width: 60px;'><th style='min-width: 60px;'>Totals</th>");
        for (long total : totals) sb.append("<td style='min-width: 60px;'>").append(total).append("</td>");
        sb.append("<td style='min-width: 60px;'><b>");
        sb.append(String.format("%.05f = %,d / %d", (double)sumError/sumTotal, sumError, sumTotal));
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

  private static ConfusionMatrix[] toCMArray(long[][][] cms) {
    int n = cms.length;
    ConfusionMatrix[] res = new ConfusionMatrix[n];
    for (int i = 0; i < n; i++) res[i] = new ConfusionMatrix(cms[i]);
    return res;
  }

  protected static water.api.AUC makeAUC(ConfusionMatrix[] cms, float[] threshold, String[] cmDomain) {
    return cms != null ? new AUC(cms, threshold, cmDomain) : null;
  }

  protected void generateHTMLAUC(StringBuilder sb) {
    validAUC.toHTML(sb);
  }
  protected void generateHTMLVarImp(StringBuilder sb) {
    if (varimp!=null) {
      // Set up variable names for importance
      varimp.setVariables(Arrays.copyOf(_names, _names.length-1));
      varimp.toHTML(this, sb);
    }
  }

  protected VarImp doVarImpCalc(final SpeeDRFModel model) {
    _treeMeasuresOnOOB  = new VariableImportance.TreeVotes[model.fr.numCols() - 1];
    _treeMeasuresOnSOOB = new VariableImportance.TreeVotes[model.fr.numCols() - 1];
    for (int i=0; i<model.fr.numCols() - 1; i++) _treeMeasuresOnOOB[i] = new VariableImportance.TreeVotes(model.treeCount());
    for (int i=0; i<model.fr.numCols() - 1; i++) _treeMeasuresOnSOOB[i] = new VariableImportance.TreeVotes(model.treeCount());
    final int ncols = model.fr.numCols();
    final int trees = model.treeCount();
    for (int i=0; i<ncols - 1; i++) _treeMeasuresOnSOOB[i] = new VariableImportance.TreeVotes(trees);
    Futures fs = new Futures();
    for (int var=0; var<ncols - 1; var++) {
      final int variable = var;
      H2O.H2OCountedCompleter task4var = new H2O.H2OCountedCompleter() {
        @Override public void compute2() {
          VariableImportance.TreeVotes[] cd = VariableImportance.collectVotes(trees, model.classes(), model.fr, ncols - 1, model.sample, variable, model);
          asVotes(_treeMeasuresOnOOB[variable]).append(cd[0]);
          asVotes(_treeMeasuresOnSOOB[variable]).append(cd[1]);
          tryComplete();
        }
      };
      H2O.submitTask(task4var);
      fs.add(task4var);
    }
    fs.blockForPending();

    // Compute varimp for individual features (_ncols)
    final float[] varimp   = new float[ncols - 1]; // output variable importance
    float[] varimpSD = new float[ncols - 1]; // output variable importance sd
    for (int var=0; var<ncols - 1; var++) {
      long[] votesOOB = asVotes(_treeMeasuresOnOOB[var]).votes();
      long[] votesSOOB = asVotes(_treeMeasuresOnSOOB[var]).votes();
      float imp = 0.f;
      float v = 0.f;
      long[] nrows = asVotes(_treeMeasuresOnOOB[var]).nrows();
      for (int i = 0; i < votesOOB.length; ++i) {
        double delta = ((float) (votesOOB[i] - votesSOOB[i])) / (float) nrows[i];
        imp += delta;
        v  += delta * delta;
      }
      imp /= model.treeCount();
      varimp[var] = imp;
      varimpSD[var] = (float)Math.sqrt( (v/model.treeCount() - imp*imp) / model.treeCount() );
    }
    return new VarImp.VarImpMDA(varimp, varimpSD, model.treeCount());
  }

  public static float[] computeVarImpSD(float[][] vote_diffs) {
    float[] res = new float[vote_diffs.length];
    for (int var = 0; var < vote_diffs.length; ++var) {
      float mean_diffs = 0.f;
      float r = 0.f;
      for (float d: vote_diffs[var]) mean_diffs += d / (float) vote_diffs.length;
      for (float d: vote_diffs[var]) {
        r += (d - mean_diffs) * (d - mean_diffs);
      }
      r *= 1.f / (float)vote_diffs[var].length;
      res[var] = (float) Math.sqrt(r);
    }
    return res;
  }

  @Override protected void setCrossValidationError(Job.ValidatedJob job, double cv_error, water.api.ConfusionMatrix cm, water.api.AUC auc, water.api.HitRatio hr) {
    _have_cv_results = true;
    // TODO: Update the model
  }
}
