package water.web;
import hex.rf.RFModel;
import hex.rf.Tree;

import java.util.Properties;

import water.*;
import water.util.TreeRenderer;

public class RFTreeView extends H2OPage {
  @Override public String[] requiredArguments() {
    return new String[] { "modelKey" };
  }

  @Override protected String serveImpl(Server server, Properties args, String sessionID) throws PageError {
    Key modelKey = ServletUtil.check_key(args,"modelKey");
    RFModel model = UKV.get(modelKey, new RFModel());
    if( model == null ) throw new PageError("Model key is missing");

    // Which tree?
    final int n = getAsNumber(args,"n",0);
    if( !(0 <= n && n < model.size()) ) return wrap(error("Tree number of out bounds"));

    byte[] tbits = model.tree(n);

    long dl = Tree.depth_leaves(new AutoBuffer(tbits));
    int depth = (int)(dl>>>32);
    int leaves= (int)(dl&0xFFFFFFFFL);

    RString response = new RString(html());
    int nodeCount = leaves*2-1; // funny math: total nodes is always 2xleaves-1
    response.replace("modelKey", modelKey);
    response.replace("n", n);
    response.replace("nodeCount", nodeCount);
    response.replace("leafCount", leaves);
    response.replace("depth",     depth);

    ValueArray ary = ServletUtil.check_array(args,"dataKey");
    int clz = getAsNumber(args, "class", ary._cols.length-1);
    TreeRenderer renderer = new TreeRenderer(model, n, ary, clz);
    String graph = renderer.graphviz();
    String code = renderer.code();
    return response.toString() + graph + code;
  }

  //use a function instead of a constant so that a debugger can live swap it
  private String html() {
    return
        "\n%modelKey view of Tree %n\n<p>" +
        "%depth depth with %nodeCount nodes and %leafCount leaves.<p>" +
    "";
  }
}
