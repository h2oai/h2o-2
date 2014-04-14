package hex.singlenoderf;

import hex.singlenoderf.Sampling;
import hex.singlenoderf.Tree;
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
  final int features;

  @API(help = "Sampling strategy used for model")
  final Sampling.Strategy sampling_strategy;

  @API(help = " Sampling rate used when building trees.")
  final float sample;

  @API(help = "Strata sampling rate used for local-node strata-sampling")
  final float[] strata_samples;

  @API(help = "Number of split features defined by user.")
  final int mtry;

  @API(help = " Number of computed split features per node.")
  final int[] node_split_features;

  @API(help = "Number of keys the model expects to be built for it.")
  final int total_trees;

  @API(help = "All the trees in the model.")
  Key[]     t_keys;

  @API(help = "Local forests produced by nodes.")
  final Key[][]   local_forests;

  @API(help = "Total time in seconds to produce the model.")
  long time;

  @API(help = "Frame being operated on.")
  final Frame fr;

  @API(help = "Response Vector.")
  final Vec response;

  @API(help = "Class weights.")
  final double[] weights;

  @API(help = "Raw tree data. for faster classification passes.")
  transient byte[][] trees;


  public static final String KEY_PREFIX = "__RFModel_";
  public static final String JSON_CONFUSION_KEY   = "confusion_key";
  public static final String JSON_CLEAR_CM        = "clear_confusion_matrix";
  public static final String JSON_REFRESH_THRESHOLD_CM = "refresh_threshold_cm";

  // JSON keys
  public static final String JSON_CM              = "confusion_matrix";
  public static final String JSON_CM_TYPE         = "type";
  public static final String JSON_CM_HEADER       = "header";
  public static final String JSON_CM_MATRIX       = "scores";
  public static final String JSON_CM_TREES        = "used_trees";
  public static final String JSON_CM_CLASS_ERR    = "classification_error";
  public static final String JSON_CM_ROWS         = "rows";
  public static final String JSON_CM_ROWS_SKIPPED = "rows_skipped";
  public static final String JSON_CM_CLASSES_ERRORS = "classes_errors";

  public SpeeDRFModel(Key selfKey, Key dataKey, Frame fr, int mtry, Sampling.Strategy sampling_strategy, float sample_rate, float[] strata_samples,
                      int features, int total_trees, Key[] t_keys, long time, Vec response, double[] weights) {
    super(selfKey, dataKey, fr);
    int csize = H2O.CLOUD.size();
    this.features = features;
    this.sampling_strategy = sampling_strategy;
    this.sample = sample_rate;
    this.fr = fr;
    this.response = response;
    this.weights = weights;
    this.time = time;
    this.local_forests = new Key[csize][];
    for(int i=0;i<csize;i++) this.local_forests[i] = new Key[0];
    this.t_keys = t_keys;
    this.total_trees = total_trees;
    this.node_split_features = new int[csize];
    this.mtry = mtry;
    this.strata_samples = strata_samples;
    for( Key tkey : t_keys ) assert DKV.get(tkey)!=null;
  }

  public SpeeDRFModel(Key selfKey, Frame fr, Key dataKey, Key[] t_keys, int features, float sample, Vec response, double[] weights, float[] strata_samples) {
    super(selfKey, dataKey, fr._names, fr.domains());
    this.features       = features;
    this.sample         = sample;
    this.mtry  = features;
    this.total_trees     = t_keys.length;
    this.t_keys          = t_keys;
    this.sampling_strategy   = Sampling.Strategy.RANDOM;
    int csize = H2O.CLOUD.size();
    this.node_split_features = new int[csize];
    this.local_forests       = new Key[csize][];
    for(int i=0;i<csize;i++) this.local_forests[i] = new Key[0];
    for( Key tkey : t_keys ) assert DKV.get(tkey)!=null;
    this.time = 0;
    this.weights = weights;
    this.strata_samples = strata_samples;
    this.trees = null;
    this.fr = fr;
    this.response = response;
    assert classes() > 0;
  }

  public Vec get_response() {
    return response;
  }

  public int treeCount() { return t_keys.length; }
  public int size()      { return t_keys.length; }
  public int classes()   { return (int)(response.max() - response.min() + 1); }

  public static final Key makeKey() {
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

  /** Return the random seed used to sample this tree. */
  public long getTreeSeed(int i) {  return Tree.seed(tree(i)); }

  public int[] colMap(String[] names) {
    int res[] = new int[names.length];
    for(int i = 0; i < res.length; i++) {
      res[i] = fr.find(names[i]);
    }
    return res;
  }

  @Override
  protected float[] score0(double[] data, float[] preds) {
    return new float[0];  //To change body of implemented methods use File | Settings | File Templates.
  }

  @Override
  public float progress() {
    return (float) t_keys.length / (float) total_trees;
  }

  public void generateHTML(String title, StringBuilder sb) {
    if(title != null && !title.isEmpty()) DocGen.HTML.title(sb, title);
    DocGen.HTML.paragraph(sb, "Model Key: " + _key);
//    sb.append("<div class='alert'>Actions: " + Predict.link(_key, "Predict on dataset") + ", "
//            + NaiveBayes.link(_dataKey, "Compute new model") + "</div>");

    DocGen.HTML.section(sb, "SpeeDRF Output:");

    //Log Pooled Variances...
//    DocGen.HTML.section(sb, "Log of the Pooled Cluster Within Sum of Squares per value of k");
//    sb.append("<span style='display: inline-block;'>");
//    sb.append("<table class='table table-striped table-bordered'>");
//
//    double[] log_wks = wks();
//
//    sb.append("<tr>");
//    for (int i = 0; i <log_wks.length; ++i) {
//      if (log_wks[i] == 0) continue;
//      sb.append("<th>").append(i).append("</th>");
//    }
//    sb.append("</tr>");
//
//    sb.append("<tr>");
//    for (int i = 0; i < log_wks.length; ++i) {
//      if (log_wks[i] == 0) continue;
//      sb.append("<td>").append(log_wks[i]).append("</td>");
//    }
//    sb.append("</tr>");
//    sb.append("</table></span>");
//
//
//    //Monte Carlo Bootstrap averages
//    DocGen.HTML.section(sb, "Monte Carlo Bootstrap Replicate Averages of the Log of the Pooled Cluster Within SS per value of k");
//    sb.append("<span style='display: inline-block;'>");
//    sb.append("<table class='table table-striped table-bordered'>");
//
//    double[] log_wkbs = wkbs();
//
//    sb.append("<tr>");
//    for (int i = 0; i <log_wkbs.length; ++i) {
//      if (log_wkbs[i] == 0) continue;
//      sb.append("<th>").append(i).append("</th>");
//    }
//    sb.append("</tr>");
//
//    sb.append("<tr>");
//    for (int i = 0; i < log_wkbs.length; ++i) {
//      if (log_wkbs[i] == 0) continue;
//      sb.append("<td>").append(log_wkbs[i]).append("</td>");
//    }
//    sb.append("</tr>");
//    sb.append("</table></span>");
//
//    //standard errors
//    DocGen.HTML.section(sb, "Standard Error for the Monte Carlo Bootstrap Replicate Averages of the Log of the Pooled Cluster Within SS per value of k");
//    sb.append("<span style='display: inline-block;'>");
//    sb.append("<table class='table table-striped table-bordered'>");
//
//    double[] sks = sk();
//
//    sb.append("<tr>");
//    for (int i = 0; i <sks.length; ++i) {
//      if (sks[i] == 0) continue;
//      sb.append("<th>").append(i).append("</th>");
//    }
//    sb.append("</tr>");
//
//    sb.append("<tr>");
//    for (int i = 0; i < sks.length; ++i) {
//      if (sks[i] == 0) continue;
//      sb.append("<td>").append(sks[i]).append("</td>");
//    }
//    sb.append("</tr>");
//    sb.append("</table></span>");
//
//    //Gap computation
//    DocGen.HTML.section(sb, "Gap Statistic per value of k");
//    sb.append("<span style='display: inline-block;'>");
//    sb.append("<table class='table table-striped table-bordered'>");
//
//    double[] gap_stats = gaps();
//
//    sb.append("<tr>");
//    for (int i = 0; i < log_wkbs.length; ++i) {
//      if (log_wkbs[i] == 0) continue;
//      sb.append("<th>").append(i).append("</th>");
//    }
//    sb.append("</tr>");
//
//    sb.append("<tr>");
////    double prev_val = Double.NEGATIVE_INFINITY;
//
//    for (int i = 0; i < log_wkbs.length; ++i) {
//      if (log_wkbs[i] == 0) continue;
//
//      sb.append("<td>").append(gap_stats[i]).append("</td>");
//    }
//    sb.append("</tr>");
//    sb.append("</table></span>");
//
//
//    //compute k optimal: smallest k such that GAP_(k) >= (GAP_(k+1) - s_(k+1)
//    int kmin = -1;
//    for(int i = 0; i < gap_stats.length; ++i) {
//      int cur_k = i + 1;
//      if( i == gap_stats.length - 1) {
//        kmin = cur_k;
//        break;
//      }
//      if (gap_stats[i] >= (gap_stats[i+1] - sks[i+1])) {
//        kmin = cur_k;
//        break; // every other k is larger...
//      }
//    }
//
//    if (log_wks[log_wks.length -1] != 0) {
//      DocGen.HTML.section(sb, "Best k:");
//      if (kmin < 0) {
//        sb.append("k = " + "No k computed!");
//      } else {
//        sb.append("k = " + kmin);
//      }
//    } else {
//      DocGen.HTML.section(sb, "Best k so far:");
//      if (kmin < 0) {
//        sb.append("k = " + "No k computed yet...");
//      } else {
//        sb.append("k = " + kmin);
//      }
//    }
  }
}