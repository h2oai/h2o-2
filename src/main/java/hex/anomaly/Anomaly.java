package hex.anomaly;

import hex.deeplearning.DeepLearningModel;
import water.*;
import water.api.*;
import water.fvec.Frame;
import water.fvec.Vec;
import water.util.Log;

import java.util.HashSet;

/**
 * Deep Learning Based Anomaly Detection
 */
public class Anomaly extends Job.FrameJob {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  public static DocGen.FieldDoc[] DOC_FIELDS;
  public static final String DOC_GET = "Anomaly Detection via Deep Learning";

  @API(help = "Deep Learning Auto-Encoder Model ", required=true, filter= Default.class, json = true)
  public Key dl_autoencoder_model;

  @API(help = "(Optional) Threshold of reconstruction error for rows to be displayed in logs (default: 10x training MSE)", filter= Default.class, json = true)
  public double thresh = -1;

  @Override
  protected final void execImpl() {
    if (dl_autoencoder_model == null) throw new IllegalArgumentException("Deep Learning Model must be specified.");
    DeepLearningModel dlm = UKV.get(dl_autoencoder_model);
    if (dlm == null) throw new IllegalArgumentException("Deep Learning Model not found.");
    if (!dlm.get_params().autoencoder) throw new IllegalArgumentException("Deep Learning Model must be build with autoencoder = true.");

    if (thresh == -1) {
      Log.info("Mean reconstruction error (MSE) of model on training data: " + dlm.mse());
      thresh = 10*dlm.mse();
      Log.info("Setting MSE threshold for anomaly to: " + thresh + ".");
    }

    StringBuilder sb = new StringBuilder();
    sb.append("\nFinding outliers in frame " + source._key.toString() + ".\n");

    Frame mse = dlm.scoreAutoEncoder(source);
    sb.append("Storing the reconstruction error (MSE) for all rows under: " + dest() + ".\n");
    Frame output = new Frame(dest(), new String[]{"Reconstruction.MSE"}, new Vec[]{mse.vecs()[0]});
    output.delete_and_lock(null);
    output.unlock(null);
    final Vec mse_test = mse.anyVec();
    sb.append("Mean reconstruction error (MSE): " + mse_test.mean() + ".\n");

    // print stats and potential outliers
    sb.append("The following data points have a reconstruction error greater than " + thresh + ":\n");
    HashSet<Long> outliers = new HashSet<Long>();
    for( long i=0; i<mse_test.length(); i++ ) {
      if (mse_test.at(i) > thresh) {
        outliers.add(i);
        sb.append(String.format("row %d : MSE = %5f\n", i, mse_test.at(i)));
      }
    }
    Log.info(sb);
  }

}
