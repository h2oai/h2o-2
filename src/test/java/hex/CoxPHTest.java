package hex;

import hex.CoxPH.*;
import water.*;
import water.api.CoxPHModelView;
import water.deploy.Node;
import water.deploy.NodeVM;
import water.fvec.Frame;
import water.fvec.NFSFileVec;
import water.fvec.ParseDataset2;

import java.io.File;
import java.util.concurrent.ExecutionException;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class CoxPHTest extends TestUtil {

  public static void testHTML(CoxPHModel m) {
    StringBuilder sb = new StringBuilder();
    CoxPHModelView modelView = new CoxPHModelView();
    modelView.coxph_model = m;
    modelView.toHTML(sb);
    assert(sb.length() > 0);
  }

  private static Frame getFrameForFile(Key outputKey, String path) {
    File f = TestUtil.find_test_file(path);
    Key  k = NFSFileVec.make(f);
    return ParseDataset2.parse(outputKey, new Key[]{k});
  }

  @Test
  public void testCoxPHEfron1Var() throws InterruptedException, ExecutionException {
    Key parsed   = Key.make("coxph_efron_test_data_parsed");
    Key modelKey = Key.make("coxph_efron_test");
    CoxPHModel model = null;
    Frame fr = null;
    try {
      fr = getFrameForFile(parsed, "smalldata/heart.csv");
      CoxPH job = new CoxPH();
      job.destination_key  = modelKey;
      job.source           = fr;
      job.start_column     = fr.vec("start");
      job.stop_column      = fr.vec("stop");
      job.event_column     = fr.vec("event");
      job.x_columns        = new int[] {fr.find("age")};
      job.ties             = CoxPHTies.efron;
      job.fork();
      job.get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      assertEquals(model.coef[0],        0.0307077486571334,   1e-8);
      assertEquals(model.var_coef[0][0], 0.000203471477951459, 1e-8);
      assertEquals(model.null_loglik,    -298.121355672984,    1e-8);
      assertEquals(model.loglik,         -295.536762216228,    1e-8);
      assertEquals(model.score_test,     4.64097294749287,     1e-8);
      assert(model.iter >= 1);
      assertEquals(model.x_mean_num[0],  -2.48402655078554,    1e-8);
      assertEquals(model.n,              172);
      assertEquals(model.total_event,    75);
      assertEquals(model.wald_test,      4.6343882547245,      1e-8);
    } finally {
      if (fr != null)
        fr.delete();
      if (model != null)
        model.delete();
    }
  }

  @Test
  public void testCoxPHBreslow1Var() throws InterruptedException, ExecutionException {
    Key parsed   = Key.make("coxph_efron_test_data_parsed");
    Key modelKey = Key.make("coxph_efron_test");
    CoxPHModel model = null;
    Frame fr = null;
    try {
      fr = getFrameForFile(parsed, "smalldata/heart.csv");
      CoxPH job = new CoxPH();
      job.destination_key  = modelKey;
      job.source           = fr;
      job.start_column     = fr.vec("start");
      job.stop_column      = fr.vec("stop");
      job.event_column     = fr.vec("event");
      job.x_columns        = new int[] {fr.find("age")};
      job.ties             = CoxPHTies.breslow;
      job.fork();
      job.get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      assertEquals(model.coef[0],        0.0306910411003801,   1e-8);
      assertEquals(model.var_coef[0][0], 0.000203592486905101, 1e-8);
      assertEquals(model.null_loglik,    -298.325606736463,    1e-8);
      assertEquals(model.loglik,         -295.745227177782,    1e-8);
      assertEquals(model.score_test,     4.63317821557301,     1e-8);
      assert(model.iter >= 1);
      assertEquals(model.x_mean_num[0],  -2.48402655078554,    1e-8);
      assertEquals(model.n,              172);
      assertEquals(model.total_event,    75);
      assertEquals(model.wald_test,      4.62659510743282,     1e-8);
    } finally {
      if (fr != null)
        fr.delete();
      if (model != null)
        model.delete();
    }
  }

  @Test
  public void testCoxPHEfron1VarNoStart() throws InterruptedException, ExecutionException {
    Key parsed   = Key.make("coxph_efron_test_data_parsed");
    Key modelKey = Key.make("coxph_efron_test");
    CoxPHModel model = null;
    Frame fr = null;
    try {
      fr = getFrameForFile(parsed, "smalldata/heart.csv");
      CoxPH job = new CoxPH();
      job.destination_key  = modelKey;
      job.source           = fr;
      job.start_column     = null;
      job.stop_column      = fr.vec("stop");
      job.event_column     = fr.vec("event");
      job.x_columns        = new int[] {fr.find("age")};
      job.ties             = CoxPHTies.efron;
      job.fork();
      job.get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      assertEquals(model.coef[0],        0.0289468187293998,   1e-8);
      assertEquals(model.var_coef[0][0], 0.000210975113029285, 1e-8);
      assertEquals(model.null_loglik,    -314.148170059513,    1e-8);
      assertEquals(model.loglik,         -311.946958322919,    1e-8);
      assertEquals(model.score_test,     3.97716015008595,     1e-8);
      assert(model.iter >= 1);
      assertEquals(model.x_mean_num[0],  -2.48402655078554,    1e-8);
      assertEquals(model.n,              172);
      assertEquals(model.total_event,    75);
      assertEquals(model.wald_test,      3.97164529276219,     1e-8);
    } finally {
      if (fr != null)
        fr.delete();
      if (model != null)
        model.delete();
    }
  }

  @Test
  public void testCoxPHBreslow1VarNoStart() throws InterruptedException, ExecutionException {
    Key parsed   = Key.make("coxph_efron_test_data_parsed");
    Key modelKey = Key.make("coxph_efron_test");
    CoxPHModel model = null;
    Frame fr = null;
    try {
      fr = getFrameForFile(parsed, "smalldata/heart.csv");
      CoxPH job = new CoxPH();
      job.destination_key  = modelKey;
      job.source           = fr;
      job.start_column     = null;
      job.stop_column      = fr.vec("stop");
      job.event_column     = fr.vec("event");
      job.x_columns        = new int[] {fr.find("age")};
      job.ties             = CoxPHTies.breslow;
      job.fork();
      job.get();
      model = DKV.get(modelKey).get();
      testHTML(model);
      assertEquals(model.coef[0],        0.0289484855901731,   1e-8);
      assertEquals(model.var_coef[0][0], 0.000211028794751156, 1e-8);
      assertEquals(model.null_loglik,    -314.296493366900,    1e-8);
      assertEquals(model.loglik,         -312.095342077591,    1e-8);
      assertEquals(model.score_test,     3.97665282498882,     1e-8);
      assert(model.iter >= 1);
      assertEquals(model.x_mean_num[0],  -2.48402655078554,    1e-8);
      assertEquals(model.n,              172);
      assertEquals(model.total_event,    75);
      assertEquals(model.wald_test,      3.97109228128153,     1e-8);
    } finally {
      if (fr != null)
        fr.delete();
      if (model != null)
        model.delete();
    }
  }

  public static void main(String [] args) throws Exception{
    System.out.println("Running ParserTest2");
    final int nnodes = 1;
    for (int i = 0; i < nnodes; i++) {
      Node n = new NodeVM(args);
      n.inheritIO();
      n.start();
    }
    H2O.waitForCloudSize(nnodes);
    System.out.println("Running...");
    new CoxPHTest().testCoxPHEfron1Var();
    new CoxPHTest().testCoxPHBreslow1Var();
    new CoxPHTest().testCoxPHEfron1VarNoStart();
    new CoxPHTest().testCoxPHBreslow1VarNoStart();
    System.out.println("DONE!");
  }
}
