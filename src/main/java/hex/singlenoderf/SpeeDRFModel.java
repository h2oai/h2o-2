package hex.singlenoderf;

import dontweave.gson.JsonArray;
import dontweave.gson.JsonElement;
import dontweave.gson.JsonObject;
import dontweave.gson.JsonPrimitive;
import hex.ConfusionMatrix;
import hex.VarImp;
import hex.gbm.DTree;
import hex.gbm.DTree.TreeModel.TreeStats;
import water.*;
import water.api.*;
import water.api.Request.API;
import water.fvec.Chunk;
import water.fvec.Frame;
import water.fvec.NewChunk;
import water.fvec.Vec;
import water.util.Counter;
import water.util.ModelUtils;

import java.util.Arrays;
import java.util.Random;

import static hex.singlenoderf.VariableImportance.asVotes;


public class SpeeDRFModel extends Model implements Job.Progress {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.


  /**
   * Model Parameters
   */
  /* Number of features these trees are built for */                      int features;
  /* Sampling strategy used for model */                                  Sampling.Strategy sampling_strategy;
  /* Key name */                                                          String src_key;
  @API(help = " Sampling rate used when building trees.")                 float sample;
  @API(help = "Strata sampling rate used for local-node strata-sampling") float[] strata_samples;
  @API(help = "Number of split features defined by user.")                int mtry;
  /* Number of computed split features per node */                        int[] node_split_features;
  @API(help = "Number of keys the model expects to be built for it.")     int N;
  @API(help = "Max depth to grow trees to")                               int max_depth;
  @API(help = "All the trees in the model.")                              Key[] t_keys;
  /* Local forests produced by nodes */                                   Key[][] local_forests;
  /* Errors Per Tree                 */                                   long[] errorsPerTree;
  /* Total time in seconds to produce the model */                        long time;
  /* Is there a validation set?*/                                         boolean validation;
  /* Response Min    */                                                   int resp_min;
  /* Class weights */                                                     double[] weights;
  @API(help = "bin limit")                                                int nbins;
  /* Raw tree data. for faster classification passes */                   transient byte[][] trees;
  @API(help = "Job key")                                                  Key jobKey;
  /* Destination Key */                                                   Key dest_key;
  /* Current model status */                                              String current_status;
  @API(help = "MSE by tree")                                              double[] errs;
  /* Statistic Type */                                                    Tree.StatType statType;
  @API(help = "Test Key")                                                 Key testKey;
  /* Out of bag error estimate */                                         boolean oobee;
  /* Seed */                                                              protected long zeed;
  /* Variable Importance */                                               boolean importance;
  /* Final Confusion Matrix */                                            CMTask.CMFinal confusion;
  @API(help = "Confusion Matrices")                                       ConfusionMatrix[] cms;
  /* Confusion Matrix */                                                  long[][] cm;
  @API(help = "Tree Statistics")                                          TreeStats treeStats;
  @API(help = "cmDomain")                                                 String[] cmDomain;
  @API(help = "AUC")                                                      public AUCData validAUC;
  @API(help = "Variable Importance")                                      public VarImp varimp;
  /* Regression or Classification */                                      boolean regression;
  /* Score each iteration? */                                             boolean score_each;
  @API(help = "CV Error")                                                 public double cv_error;
  @API(help = "Verbose Mode")                                             public boolean verbose;
  @API(help = "Verbose Output")                                           public String[] verbose_output;
  @API(help = "Use non-local data")                                       public boolean useNonLocal;
  @API(help = "Dtree keys")                                               public Key[/*ntree*/][/*nclass*/] dtreeKeys;
  @API(help = "DTree Model")                                              public SpeeDRFModel_DTree dtreeTreeModel = null;
  @API(help = "score_pojo boolean")                                       public boolean score_pojo;

  private float _ss; private float _cnt;
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
  @Override public final Request2 job() { return get_params(); }
  @Override public final VarImp varimp() { return varimp; }

  public float[] priordist() { return _priorClassDist; }
  public float[] modeldist() { return _modelClassDist; }

  public SpeeDRFModel(Key selfKey, Key dataKey, Frame fr, String[] domain, SpeeDRF params, float[] priorDist) {
    super(selfKey, dataKey, fr, priorDist);
    this.dest_key = selfKey;
    this.parameters = params;
    score_each = params.score_each_iteration;
    regression = !(params.classification);
  }

  protected SpeeDRFModel(SpeeDRFModel model, double err, ConfusionMatrix cm, VarImp varimp, AUCData auc) {
    super(model._key,model._dataKey,model._names,model._domains, model._priorClassDist,model._modelClassDist,model.training_start_time,model.training_duration_in_ms);
    this.features = model.features;
    this.sampling_strategy = model.sampling_strategy;
    this.sample = model.sample;
    this.strata_samples = model.strata_samples;
    this.mtry = model.mtry;
    this.node_split_features = model.node_split_features;
    this.N = model.N;
    this.max_depth = model.max_depth;
    this.t_keys = model.t_keys;
    this.local_forests = model.local_forests;
    this.time = model.time;
    this.weights = model.weights;
    this.nbins = model.nbins;
    this.trees = model.trees;
    this.jobKey = model.jobKey;
    this.dest_key = model.dest_key;
    this.current_status = model.current_status;
    this.errs = model.errs;
    this.statType = model.statType;
    this.testKey = model.testKey;
    this.oobee = model.oobee;
    this.zeed = model.zeed;
    this.importance = model.importance;
    this.confusion = model.confusion;
    this.cms = Arrays.copyOf(model.cms, model.cms.length+1);
    this.cms[this.cms.length-1] = cm;
    this.parameters = model.parameters;
    this.cm = cm._arr;
    this.treeStats = model.treeStats;
    this.cmDomain = model.cmDomain;
    this.validAUC = auc;
    this.varimp = varimp;
    this.regression = model.regression;
    this.score_each = model.score_each;
    this.cv_error = err;
    this.verbose = model.verbose;
    this.verbose_output = model.verbose_output;
    this.useNonLocal = model.useNonLocal;
    this.errorsPerTree = model.errorsPerTree;
    this.resp_min = model.resp_min;
    this.validation = model.validation;
    this.src_key = model.src_key;
    this.score_pojo = model.score_pojo;
  }

  public int treeCount() { return t_keys.length; }
  public int size()      { return t_keys.length; }
  public int classes()   { return nclasses(); }

  @Override public ConfusionMatrix cm() { return validAUC == null ? cms[cms.length-1] : validAUC.CM(); }

  private void scoreOnTest(Frame fr, Vec modelResp) {

    Frame scored = score(fr);
    water.api.ConfusionMatrix cm = new water.api.ConfusionMatrix();
    cm.vactual = fr.lastVec();
    cm.vpredict = scored.anyVec();
    cm.invoke();



    // Regression scoring
    if (regression) {
      float mse = (float) cm.mse;
      errs[errs.length - 1] = mse;
      cms[cms.length - 1] = null;

      // Classification scoring
    } else {
      Vec lv = scored.lastVec();
      double mse = CMTask.MSETask.doTask(scored.add("actual", fr.lastVec()));
      this.cm = cm.cm;
      errs[errs.length - 1] = (float)mse;
      ConfusionMatrix new_cm = new ConfusionMatrix(this.cm);
      cms[cms.length - 1] = new_cm;

      // Create the ROC Plot
      if (classes() == 2) {
        Vec v = null;
        Frame fa = null;
        if (lv.isInt()) {
          fa = new MRTask2() {
            @Override public void map(Chunk[] cs, NewChunk nchk) {
              int rows = cs[0]._len;
              int cols = cs.length - 1;
              for (int r = 0; r < rows; ++r) {
                nchk.addNum(cs[cols].at0(r) == 0 ? 1e-10 : 1.0 - 1e-10);
              }
            }
          }.doAll(1, scored).outputFrame(null,null);
          v = fa.anyVec();
        }
        AUC auc_calc = new AUC();
        auc_calc.vactual = cm.vactual;
        auc_calc.vpredict = v == null ? lv : v; // lastVec is class1
        auc_calc.invoke();
        validAUC = auc_calc.data();
        if (v != null) UKV.remove(v._key);
        if (fa != null) fa.delete();
        UKV.remove(lv._key);
      }
    }
    scored.remove("actual");
    scored.delete();
  }

  private void scoreOnTrain(Frame fr, Vec modelResp) {
    final CMTask cmTask = CMTask.scoreTask(this, treeCount(), oobee, fr, modelResp);
    if (regression) {
      float mse = cmTask._ss / ( (float) (cmTask._rowcnt));
      errs[errs.length - 1] = mse;
      cms[cms.length - 1] = null;
    } else {
      confusion = CMTask.CMFinal.make(cmTask._matrix, this, classNames(), cmTask._errorsPerTree, oobee, cmTask._sum, cmTask._cms);
      this.cm = cmTask._matrix._matrix;
      errorsPerTree = cmTask._errorsPerTree;
      errs[errs.length - 1] = confusion.mse();
      cms[cms.length - 1] = new ConfusionMatrix(confusion._matrix);
      if (classes() == 2) validAUC =  makeAUC(toCMArray(confusion._cms), ModelUtils.DEFAULT_THRESHOLDS, cmDomain);
    }
  }

  void scoreAllTrees(Frame fr, Vec modelResp) {
    if (this.validation) scoreOnTest(fr, modelResp);
    else scoreOnTrain(fr, modelResp);
  }

  void variableImportanceCalc(Frame fr, Vec modelResp) { varimp = doVarImpCalc(fr, this, modelResp); }

  public static SpeeDRFModel make(SpeeDRFModel old, Key tkey, Key dtKey, int nodeIdx, String tString, int tree_id) {

    // Create a new model for atomic update
    SpeeDRFModel m = (SpeeDRFModel)old.clone();

    // Update the tree keys with the new one (tkey)
    m.t_keys = Arrays.copyOf(old.t_keys, old.t_keys.length + 1);
    m.t_keys[m.t_keys.length-1] = tkey;

    // Update the dtree keys with the new one (dtkey)
    m.dtreeKeys[tree_id][0] = dtKey;

    // Update the local_forests
    m.local_forests[nodeIdx][tree_id] = tkey;

    // Update the treeStrings?
    if (old.verbose_output.length < 2) {
      m.verbose_output = Arrays.copyOf(old.verbose_output, old.verbose_output.length + 1);
      m.verbose_output[m.verbose_output.length - 1] = tString;
    }

    m.errs = Arrays.copyOf(old.errs, old.errs.length+1);
    m.errs[m.errs.length - 1] = -1.0;
    m.cms = Arrays.copyOf(old.cms, old.cms.length+1);
    m.cms[m.cms.length-1] = null;

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
    for( Key k : t_keys ) UKV.remove(k,fs);
    for (Key[] ka : local_forests) for (Key k : ka) if (k != null) UKV.remove(k, fs);
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
  public float classify0(int tree_id, Chunk[] chunks, int row, int modelDataMap[], short badrow, boolean regression) {
    return Tree.classify(new AutoBuffer(tree(tree_id)), chunks, row, modelDataMap, badrow, regression);
  }

  private void vote(Chunk[] chks, int row, int modelDataMap[], int[] votes) {
    int numClasses = classes();
    assert votes.length == numClasses + 1 /* +1 to catch broken rows */;
    for( int i = 0; i < treeCount(); i++ )
      votes[(int)classify0(i, chks, row, modelDataMap, (short) numClasses, false)]++;
  }

  public short classify(Chunk[] chks, int row, int modelDataMap[], int[] votes, double[] classWt, Random rand ) {
    // Vote all the trees for the row
    vote(chks, row, modelDataMap, votes);
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
//    if( _tl != null ) return;
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

  public int[] colMap(Frame df) {
    int res[] = new int[df._names.length]; //new int[names.length];
    for(int i = 0; i < res.length; i++) {
      res[i] = find(df.names()[i], _names);
    }
    return res;
  }

  @Override protected float[] score0(double[] data, float[] preds) {
    int numClasses = classes();
    if (numClasses == 1) {
      float p = 0.f;
      for (int i = 0; i < treeCount(); ++i) {
        p += Tree.classify(new AutoBuffer(tree(i)), data, 0.0, true) / (1. * treeCount());
      }
      return new float[]{p};
    } else {
      int votes[] = new int[numClasses + 1/* +1 to catch broken rows */];
      preds = new float[numClasses + 1];
      for( int i = 0; i < treeCount(); i++ ) {
//        DTree.TreeModel.CompressedTree t = UKV.get(dtreeKeys[i][0]);
        votes[(int) Tree.classify(new AutoBuffer(tree(i)), data, numClasses, false)]++;
      }

      float s = 0.f;
      for (int v : votes) s += (float)v;

      if (get_params().balance_classes) {
        for (int i = 0; i  < votes.length - 1; ++i)
          preds[i+1] = ( (float)votes[i] / treeCount());
        return preds;
      }

      for (int i = 0; i  < votes.length - 1; ++i)
        preds[i+1] = ( (float)votes[i] / (float)treeCount());
//      preds[0] = (float) (classify(votes, null, null) + resp_min);
      preds[0] = ModelUtils.getPrediction(preds, data);
      float[] rawp = new float[preds.length + 1];
      for (int i = 0; i < votes.length; ++i) rawp[i+1] = (float)votes[i];
      return preds;
    }
  }

  @Override public float progress() { return get_params().cv_progress(t_keys.length / (float) N); }

  static String[] cfDomain(final CMTask.CMFinal cm, int maxClasses) {
    String[] dom = cm.domain();
    if (dom.length > maxClasses)
      throw new IllegalArgumentException("The column has more than "+maxClasses+" values. Are you sure you have that many classes?");
    return dom;
  }

  private boolean errsNotNull() {
    boolean allMinus1 = true;
    if (errs == null) return false;
    for (double err : errs) {
      if (err > -1) allMinus1 = false;
    }
    return !allMinus1;
  }

  public void generateHTML(String title, StringBuilder sb) {
    String style = "<style>\n"+
                    "td, th { min-width:60px;}\n"+
                    "</style>\n";
    sb.append(style);
    DocGen.HTML.title(sb,title);
    sb.append("<div class=\"alert\">").append("Actions: ");
    sb.append(Inspect2.link("Inspect training data (" + _dataKey.toString() + ")", _dataKey)).append(", ");
    if (validation)
      sb.append(Inspect2.link("Inspect testing data (" + testKey.toString() + ")", testKey)).append(", ");
    sb.append(Predict.link(_key, "Score on dataset" ));
    if (this.size() > 0 && this.size() < N && !Job.findJob(jobKey).isCancelledOrCrashed()) {
      sb.append(", ");
      sb.append("<i class=\"icon-stop\"></i>&nbsp;").append(Cancel.link(jobKey, "Cancel training"));
    }
    sb.append("</div>");
    DocGen.HTML.paragraph(sb,"Model Key: "+_key);
    DocGen.HTML.paragraph(sb,"Max max_depth: "+max_depth+", Nbins: "+nbins+", Trees: " + this.size());
    DocGen.HTML.paragraph(sb, "Sample Rate: "+sample + ", User Seed: "+get_params().seed+ ", Internal Seed: "+zeed+", mtry: "+mtry);
    sb.append("</pre>");

    if (this.size() > 0 && this.size() < N) sb.append("Current Status: ").append("Building Random Forest");
    else {
      if (this.size() == N && !this.current_status.equals("Performing Variable Importance Calculation.")) {
        sb.append("Current Status: ").append("Complete.");
      } else  {
        if( Job.findJob(jobKey).isCancelledOrCrashed()) {
          sb.append("Current Status: ").append("Cancelled.");
        } else {
          sb.append("Current Status: ").append(this.current_status);
        }
      }
    }

    if (_have_cv_results) {
      sb.append("<div class=\"alert\">Scoring results reported for ").append(this.parameters.n_folds).append("-fold cross-validated training data ").append(Inspect2.link(_dataKey.toString(), _dataKey)).append("</div>");
    } else {
      if (testKey != null)
        sb.append("<div class=\"alert\">Reported on ").append(Inspect2.link(testKey.toString(), testKey)).append("</div>");
      else
        sb.append("<div class=\"alert\">Reported on ").append( oobee ? "OOB" : "training" ).append(" data</div>");
    }

    //build cm
    if(!regression) {
//      if (confusion != null && confusion.valid() && (this.N * .25 > 0) && classes() >= 2) {
//        buildCM(sb);
//      } else {
      if (this.cms[this.cms.length - 1] != null && (this.N * .25 > 0 && classes() >= 2) ) {
        this.cms[this.cms.length - 1].toHTML(sb, this.cmDomain);
//        }
      }
    }

    sb.append("<br />");
    if( errsNotNull() && this.size() > 0) {
      DocGen.HTML.section(sb,"Mean Squared Error by Tree");
      DocGen.HTML.arrayHead(sb);
      sb.append("<tr style='min-width:60px'><th>Trees</th>");
      int last = this.size(); // + 1;
      for( int i=last; i>=0; i-- )
        sb.append("<td style='min-width:60px'>").append(i).append("</td>");
      sb.append("</tr>");
      sb.append("<tr style='min-width: 60px;'><th style='min-width: 60px;' class='warning'>MSE</th>");
      for( int i=last; i>=0; i-- )
        sb.append( (!(Double.isNaN(errs[i]) || errs[i] <= 0.0)) ? String.format("<td style='min-width:60px'>%5.5f</td>",errs[i]) : "<td style='min-width:60px'>---</td>");
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

    if (validAUC != null) {
      generateHTMLAUC(sb);
    }

    generateHTMLTreeStats(sb, trees);
    if (varimp != null) {
      generateHTMLVarImp(sb);
    }
    printCrossValidationModelsHTML(sb);
  }

  public DTree.TreeModel transform2DTreeModel() {
    if (dtreeTreeModel != null) {
      dtreeTreeModel = new SpeeDRFModel_DTree(dtreeTreeModel, dtreeKeys, treeStats); //freshen the dtreeTreeModel
      return dtreeTreeModel;
    }
    Key key = Key.make();
    Key model_key = _key;
    Key dataKey = _dataKey;
    Key testKey = null;
    String[] names = _names;
    String[][] domains = _domains;
    String[] cmDomain = this.cmDomain;
    int ntrees = treeCount();
    int min_rows = 0;
    int nbins = this.nbins;
    int mtries = this.mtry;
    long seed = -1;
    int num_folds = 0;
    float[] priorClassDist = null;
    float[] classDist = null;

    // dummy model
    dtreeTreeModel = new SpeeDRFModel_DTree(model_key, model_key, dataKey,testKey,names,domains,cmDomain,ntrees, max_depth, min_rows, nbins, mtries, num_folds, priorClassDist, classDist);
    // update the model
    dtreeTreeModel = new SpeeDRFModel_DTree(dtreeTreeModel, dtreeKeys, treeStats);
    dtreeTreeModel.isFromSpeeDRF=true; // tells the toJava method the model is translated from a speedrf model.
    return dtreeTreeModel;
  }

  public static class SpeeDRFModel_DTree extends DTree.TreeModel {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    Key modelKey;

    public SpeeDRFModel_DTree(Key key, Key modelKey, Key dataKey, Key testKey, String names[], String domains[][], String[] cmDomain, int ntrees, int max_depth, int min_rows, int nbins, int mtries, int num_folds, float[] priorClassDist, float[] classDist) {
      super(key,dataKey,testKey,names,domains,cmDomain,ntrees, max_depth, min_rows, nbins, num_folds, priorClassDist, classDist);
      this.modelKey = modelKey;
    }

    public SpeeDRFModel_DTree(SpeeDRFModel_DTree prior, Key[][] treeKeys, TreeStats tstats) {
      super(prior, treeKeys, null, prior.cms, tstats, null, null);
    }

    @Override
    protected void generateModelDescription(StringBuilder sb) { }
  }

  @Override public ModelAutobufferSerializer getModelSerializer() {
    // Return a serializer which knows how to serialize keys
    return new ModelAutobufferSerializer() {
      @Override protected AutoBuffer postSave(Model m, AutoBuffer ab) {
        int ntrees = N;
        ab.put4(ntrees);
        // must fill out t_keys and dtreeKeys
        for (int i = 0; i < ntrees; ++i) {
          byte[] bits = tree(i);
          ab.putA1(bits);
          for (int j = 0; j < nclasses(); ++j) {
            if (dtreeKeys[i][j] == null) continue;
            Value v = DKV.get(dtreeKeys[i][j]);
            if (v == null) continue;
            DTree.TreeModel.CompressedTree t = v.get();
            ab.put(t);
          }
        }
        return ab;
      }
      @Override protected AutoBuffer postLoad(Model m, AutoBuffer ab) {
        int ntrees = ab.get4();
        Futures fs = new Futures();
        for (int i = 0; i < ntrees; ++i) {
          DKV.put(t_keys[i],new Value(t_keys[i],ab.getA1()), fs);
          for (int j = 0; j < nclasses(); ++j) {
            if (dtreeKeys[i][j] == null) continue;
            UKV.put(dtreeKeys[i][j], new Value(dtreeKeys[i][j], ab.get(DTree.TreeModel.CompressedTree.class)), fs);
          }
        }
        fs.blockForPending();
        return ab;
      }
    };
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
            .append("<td>").append(depth_stats != null ? (int)depth_stats[0]  : NA).append("</td>")
            .append("<td>").append(depth_stats != null ? depth_stats[1] : NA).append("</td>")
            .append("<td>").append(depth_stats != null ? (int)depth_stats[2] : NA).append("</td></tr>");
    sb.append("<th>Leaves</th>")
            .append("<td>").append(leaf_stats != null ? (int)leaf_stats[0]  : NA).append("</td>")
            .append("<td>").append(leaf_stats != null ? leaf_stats[1] : NA).append("</td>")
            .append("<td>").append(leaf_stats != null ? (int)leaf_stats[2]  : NA).append("</td></tr>");
    DocGen.HTML.arrayTail(sb);

    if(depth_stats != null && leaf_stats != null) {
      treeStats.minDepth = (int)depth_stats[0];
      treeStats.meanDepth = (float)depth_stats[1];
      treeStats.maxDepth = (int)depth_stats[2];
      treeStats.minLeaves = (int)leaf_stats[0];
      treeStats.meanLeaves = (float)leaf_stats[1];
      treeStats.maxLeaves = (int)leaf_stats[2];
      treeStats.setNumTrees(N);
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
              Math.round(obj.get(Constants.MIN).getAsDouble() * 1000.0) / 1000.0,
              Math.round(obj.get(Constants.MEAN).getAsDouble() * 1000.0) / 1000.0,
              Math.round(obj.get(Constants.MAX).getAsDouble() * 1000.0) / 1000.0};
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

  protected static AUCData makeAUC(ConfusionMatrix[] cms, float[] threshold, String[] cmDomain) {
    return cms != null ? new AUC(cms, threshold, cmDomain).data() : null;
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

  protected VarImp doVarImpCalc(final Frame fr, final SpeeDRFModel model, final Vec resp) {
    _treeMeasuresOnOOB  = new VariableImportance.TreeVotes[fr.numCols() - 1];
    _treeMeasuresOnSOOB = new VariableImportance.TreeVotes[fr.numCols() - 1];
    for (int i=0; i<fr.numCols() - 1; i++) _treeMeasuresOnOOB[i] = new VariableImportance.TreeVotes(treeCount());
    for (int i=0; i<fr.numCols() - 1; i++) _treeMeasuresOnSOOB[i] = new VariableImportance.TreeVotes(treeCount());
    final int ncols = fr.numCols();
    final int trees = treeCount();
    for (int i=0; i<ncols - 1; i++) _treeMeasuresOnSOOB[i] = new VariableImportance.TreeVotes(trees);
    Futures fs = new Futures();
    for (int var=0; var<ncols - 1; var++) {
      final int variable = var;
      H2O.H2OCountedCompleter task4var = new H2O.H2OCountedCompleter() {
        @Override public void compute2() {
          VariableImportance.TreeVotes[] cd = VariableImportance.collectVotes(trees, classes(), fr, ncols - 1, sample, variable, model, resp);
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

  @Override protected void setCrossValidationError(Job.ValidatedJob job, double cv_error, water.api.ConfusionMatrix cm, AUCData auc, HitRatio hr) {
    _have_cv_results = true;
    SpeeDRFModel drfm = ((SpeeDRF)job).makeModel(this, cv_error, cm.cm == null ? null : new ConfusionMatrix(cm.cm, this.nclasses()), this.varimp, auc);
    drfm._have_cv_results = true;
    DKV.put(this._key, drfm); //overwrite this model
  }
}
