package hex.deepfeatures;

import hex.deeplearning.DeepLearningModel;
import water.Job;
import water.Key;
import water.UKV;
import water.api.DocGen;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;

import java.util.HashSet;

/**
 * Deep Learning Based Feature Extractor
 * For each row in the input frame (source), make predictions with the Auto-Encoder model
 * and extract the last hidden layer's neuron activation values as new features.
 */
public class DeepFeatures extends Job.FrameJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "Deep Learning Feature Extractor";

  @API(help = "Deep Learning Model", required=true, filter= Default.class, json = true)
  public Key dl_model;

  @API(help = "Which hidden layer (index) to extract (default: -1 -> last hidden layer)", required=true, lmin=-1, filter= Default.class, json = true)
  public int layer = -1;

  @Override
  protected final void execImpl() {
    if (dl_model == null) throw new IllegalArgumentException("Deep Learning Model must be specified.");
    DeepLearningModel dlm = UKV.get(dl_model);
    if (dlm == null) throw new IllegalArgumentException("Deep Learning Model not found.");

    StringBuilder sb = new StringBuilder();
    if (layer < -1 || layer > dlm.get_params().hidden.length-1) throw new IllegalArgumentException("Layer must be either -1 or between 0 and " + (dlm.get_params().hidden.length-1));
    if (layer == -1) layer = dlm.get_params().hidden.length-1;
    int features = dlm.get_params().hidden[layer];
    sb.append("\nTransforming frame '" + source._key.toString() + "' with " + source.numCols() + " into " + features + " features with model '" + dl_model + "'\n");

    Frame df = dlm.scoreDeepFeatures(source, layer);
    sb.append("Storing the new features under: " + dest() + ".\n");
    Frame output = new Frame(dest(), df.names(), df.vecs());
    output.delete_and_lock(null);
    output.unlock(null);
  }

}
