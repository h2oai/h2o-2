package water.util;

import hex.NFoldFrameExtractor;
import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

public class CrossValUtils {

  /**
   * Cross-Validate a ValidatedJob
   * @param job (must contain valid entries for n_folds, validation, destination_key, source, response)
   */
  public static void crossValidate(Job.ValidatedJob job) {
    if (job.state != Job.JobState.RUNNING) return; //don't do cross-validation if the full model builder failed
    if (job.validation != null)
      throw new IllegalArgumentException("Cannot provide validation dataset and n_folds > 0 at the same time.");
    if (job.n_folds <= 1)
      throw new IllegalArgumentException("n_folds must be >= 2 for cross-validation.");
    final String basename = job.destination_key.toString();
    long[] offsets = new long[job.n_folds +1];
    Frame[] cv_preds = new Frame[job.n_folds];
    try {
      for (int i = 0; i < job.n_folds; ++i) {
        if (job.state != Job.JobState.RUNNING) break;
        Key[] destkeys = new Key[]{Key.make(basename + "_xval" + i + "_train"), Key.make(basename + "_xval" + i + "_holdout")};
        NFoldFrameExtractor nffe = new NFoldFrameExtractor(job.source, job.n_folds, i, destkeys, Key.make() /*key used for locking only*/);
        H2O.submitTask(nffe);
        Frame[] splits = nffe.getResult();
        // Cross-validate individual splits
        try {
          job.crossValidate(splits, cv_preds, offsets, i); //this removes the enum-ified response!
          job._cv_count++;
        } finally {
          // clean-up the results
          if (!job.keep_cross_validation_splits) for(Frame f : splits) f.delete();
        }
      }
      if (job.state != Job.JobState.RUNNING)
        return;
      final int resp_idx = job.source.find(job._responseName);
      Vec response = job.source.vecs()[resp_idx];
      boolean put_back = UKV.get(job.response._key) == null; // In the case of rebalance, rebalance response will be deleted
      if (put_back) {
        job.response = response;
        if (job.classification)
          job.response = job.response.toEnum();
        DKV.put(job.response._key, job.response); //put enum-ified response back to K-V store
      }
      ((Model)UKV.get(job.destination_key)).scoreCrossValidation(job, job.source, response, cv_preds, offsets);
      if (put_back) UKV.remove(job.response._key);
    } finally {
      // clean-up prediction frames for splits
      for(Frame f: cv_preds) if (f!=null) f.delete();
    }
  }
}
