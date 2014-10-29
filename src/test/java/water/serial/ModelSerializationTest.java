package water.serial;

import hex.*;
import hex.FrameTask.DataInfo;
import hex.drf.DRF;
import hex.drf.DRF.DRFModel;
import hex.gbm.GBM;
import hex.gbm.GBM.GBMModel;
import hex.glm.*;
import hex.glm.GLM2.Source;
import hex.glm.GLMParams.Family;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import water.*;
import water.fvec.Frame;
import water.fvec.Vec;

public class ModelSerializationTest extends TestUtil {

  private static int[] EIA = new int[] {};

  @Test public void testSimpleModel() throws IOException {
    // Create a model
    Model model = new BlahModel(null, null, ar("ColumnBlah", "response"), new String[2][]);
    // Create a serializer, save a model and reload it
    Model loadedModel = saveAndLoad(model);
    // And compare
    assertModelEquals(model, loadedModel);
  }

  @Test
  public void testGBMModelMultinomial() throws IOException {
    GBMModel model = null, loadedModel = null;
    try {
      model = prepareGBMModel("smalldata/iris/iris.csv", EIA, 4, true, 5);
      loadedModel = saveAndLoad(model);
      // And compare
      assertTreeModelEquals(model, loadedModel);
      assertModelBinaryEquals(model, loadedModel);
    } finally {
      if (model!=null) model.delete();
      if (loadedModel!=null) loadedModel.delete();
    }
  }

  @Test
  public void testGBMModelBinomial() throws IOException {
    GBMModel model = null, loadedModel = null;
    try {
      model = prepareGBMModel("smalldata/logreg/prostate.csv", ari(0), 1, true, 5);
      loadedModel = saveAndLoad(model);
      // And compare
      assertTreeModelEquals(model, loadedModel);
      assertModelBinaryEquals(model, loadedModel);
    } finally {
      if (model!=null) model.delete();
      if (loadedModel!=null) loadedModel.delete();
    }
  }

  @Test
  public void testDRFModelMultinomial() throws IOException {
    DRFModel model = null, loadedModel = null;
    try {
      model = prepareDRFModel("smalldata/iris/iris.csv", EIA, 4, true, 5);
      loadedModel = saveAndLoad(model);
      // And compare
      assertTreeModelEquals(model, loadedModel);
      assertModelBinaryEquals(model, loadedModel);
    } finally {
      if (model!=null) model.delete();
      if (loadedModel!=null) loadedModel.delete();
    }
  }

  @Test
  public void testDRFModelBinomial() throws IOException {
    DRFModel model = null, loadedModel = null;
    try {
      model = prepareDRFModel("smalldata/logreg/prostate.csv", ari(0), 1, true, 5);
      loadedModel = saveAndLoad(model);
      // And compare
      assertTreeModelEquals(model, loadedModel);
      assertModelBinaryEquals(model, loadedModel);
    } finally {
      if (model!=null) model.delete();
      if (loadedModel!=null) loadedModel.delete();
    }
  }

  @Test
  public void testGLMModel() throws IOException {
    GLMModel model = null, loadedModel = null;
    try {
      model = prepareGLMModel("smalldata/cars.csv", EIA, 4, Family.poisson);
      loadedModel = saveAndLoad(model);
      assertModelBinaryEquals(model, loadedModel);
      GLMTest2.testHTML(loadedModel);
    } finally {
      if (model!=null) model.delete();
      if (loadedModel!=null) loadedModel.delete();
    }
  }

  private GBMModel prepareGBMModel(String dataset, int[] ignores, int response, boolean classification, int ntrees) {
    Frame f = parseFrame(dataset);
    try {
      GBM gbm = new GBM();
      Vec respVec = f.vec(response);
      gbm.source = f;
      gbm.response = respVec;
      gbm.classification = classification;
      gbm.ntrees = ntrees;
      gbm.score_each_iteration = true;
      gbm.invoke();
      return UKV.get(gbm.dest());
    } finally {
      if (f!=null) f.delete();
    }
  }

  private DRFModel prepareDRFModel(String dataset, int[] ignores, int response, boolean classification, int ntrees) {
    Frame f = parseFrame(dataset);
    try {
      DRF drf = new DRF();
      Vec respVec = f.vec(response);
      drf.source = f;
      drf.response = respVec;
      drf.classification = classification;
      drf.ntrees = ntrees;
      drf.score_each_iteration = true;
      drf.invoke();
      return UKV.get(drf.dest());
    } finally {
      if (f!=null) f.delete();
    }
  }

  private GLMModel prepareGLMModel(String dataset, int[] ignores, int response, Family family) {
    Frame f = parseFrame(dataset);
    Key modelKey = Key.make("GLM_model_for_"+dataset);
    try {
      new GLM2("GLM test on "+dataset,Key.make(),modelKey,new Source(f,f.vec(response),true),family).doInit().fork().get();
      return DKV.get(modelKey).get();
    } finally {
      if (f!=null) f.delete();
    }
  }

  static class BlahModel extends Model {
    //static final int DEBUG_WEAVER = 1;
    final Key[] keys;
    final VarImp varimp;

    public BlahModel(Key selfKey, Key dataKey, String[] names, String[][] domains) {
      super(selfKey, dataKey, names, domains, null, null);
      keys = new Key[3];
      varimp = new VarImp.VarImpRI(arf(1f, 1f, 1f));
    }
    @Override protected float[] score0(double[] data, float[] preds) {
      throw new RuntimeException("TODO Auto-generated method stub");
    }
  }

  private <M extends Model> M saveAndLoad(M model) throws IOException {
    return saveAndLoad(model,true);
  }
  private <M extends Model> M saveAndLoad(M model, boolean deleteModel) throws IOException {
    // Serialize to a file
    File file = File.createTempFile("H2O_ModelSerializationTest", ".model");
    file.deleteOnExit();
    new Model2FileBinarySerializer().save(model, file);
    // Delete model
    if (deleteModel) model.delete();
    // Deserialize
    M m = (M) new Model2FileBinarySerializer().load(file);
    // And return
    return m;
  }
}
