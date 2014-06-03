package water.util;

import hex.NFoldFrameExtractor;
import water.*;
import water.fvec.Frame;

public class CrossValUtils {

  /**
   * Cross-Validate a ValidatedJob
   * @param job (must contain valid entries for n_folds, validation, destination_key, source, response)
   */
  public static void crossValidate(Job.ValidatedJob job) {
    if (job.state != Job.JobState.DONE) return; //don't do cross-validation if the full model builder failed
    if (job.validation != null)
      throw new IllegalArgumentException("Cannot provide validation dataset and n_folds > 0 at the same time.");
    if (job.n_folds <= 1)
      throw new IllegalArgumentException("n_folds must be >= 2 for cross-validation.");
    final String basename = job.destination_key.toString();
    long[] offsets = new long[job.n_folds +1];
    Frame[] cv_preds = new Frame[job.n_folds];
    for (int i = 0; i < job.n_folds; ++i) {
      Key[] destkeys = new Key[]{Key.make(basename + "_xval" + i + "_train"), Key.make(basename + "_xval" + i + "_holdout")};
      NFoldFrameExtractor nffe = new NFoldFrameExtractor(job.source, job.n_folds, i, destkeys, null);
      H2O.submitTask(nffe);
      Frame[] splits = nffe.getResult();
      job.crossValidate(splits, cv_preds, offsets, i); //this removes the enum-ified response!
      if (!job.keep_cross_validation_splits) for(Frame f : splits) f.delete();
    }
    boolean put_back = UKV.get(job.response._key) == null;
    if (put_back) DKV.put(job.response._key, job.response); //put enum-ified response back to K-V store
    ((Model)UKV.get(job.destination_key)).scoreCrossValidation(job, job.source, job.response, cv_preds, offsets);
    if (put_back) UKV.remove(job.response._key);
  }
}
