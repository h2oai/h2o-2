package hex.singlenoderf;

import hex.gbm.DTree;
import water.AutoBuffer;
import water.DKV;
import water.Key;
import water.Request2;
import water.api.DocGen;
import water.api.Request;
import water.util.Log;


public class SpeeDRFModelView extends Request2 {
  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help="SpeeDRF Model Key", required = true, filter = SpeeDRFKeyFilter.class)
  Key _modelKey;
  class SpeeDRFKeyFilter extends H2OKey { public SpeeDRFKeyFilter() { super("",true); } }

  @API(help="SpeeDRF Model")
  SpeeDRFModel speedrf_model;

  public static String link(String txt, Key model) {
    return "<a href='/2/SpeeDRFModelView.html?_modelKey=" + model + "'>" + txt + "</a>";
  }

  public static Response redirect(Request req, Key modelKey) {
    return Response.redirect(req, "/2/SpeeDRFModelView", "_modelKey", modelKey);
  }

  @Override public boolean toHTML(StringBuilder sb){
    speedrf_model.generateHTML("", sb);
    final double[][] sample = new double[][]{
                                             {5.1, 3.5, 1.4, 0.2},
                                             {7,   3.2, 4.5, 1.5},
                                             {6.3, 3.3, 6,   2.5}};
    for ( int i = 0; i < speedrf_model.treeCount(); i++) {
      DTree.TreeModel.CompressedTree compressedTreeTest = new DTree.TreeModel.CompressedTree(Tree.toDTreeCompressedTreeAB(speedrf_model.tree(i), false), 3, -1);
      AutoBuffer singleTreeBuffer = new AutoBuffer(speedrf_model.tree(i));
      for (int j=0; j < sample.length; j++) {
        singleTreeBuffer.position(0);
        double result2 = Tree.classify(singleTreeBuffer, sample[j], -123, false);
        float result1 = compressedTreeTest.score(sample[j]);

        Log.info(Log.Tag.Sys.RANDF, "Result of DTREE on "+ j + " is: " + result1);
        Log.info(Log.Tag.Sys.RANDF, "Result of SINGLETREE on "+ j +" is: " + result2);
      }
    }



    return true;
  }

//  @Override public void toJava(StringBuilder sb) { speedrf_model.transform2DTreeModel().toJavaHtml(sb); }

  @Override protected Response serve() {
    speedrf_model = DKV.get(_modelKey).get();
    return Response.done(this);
  }
}
