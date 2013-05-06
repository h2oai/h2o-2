package water.util;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import hex.rf.*;
import java.io.File;
import java.io.PrintWriter;
import org.apache.commons.codec.binary.Base64;
import water.*;
import water.ValueArray.Column;

public class TreeRenderer {
  private static final String DOT_PATH;
  static {
    File f = new File("/usr/local/bin/dot");
    if( !f.exists() ) f = new File("/usr/bin/dot");
    // graphviz is currently at 2.30. hack to support minor revs coming up until
    // someone figures out a better way.
    // Also, 2.30 apparently got rid of a space on Win7 at least. So try those too.
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz 2.28\\bin\\dot.exe");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz 2.29\\bin\\dot.exe");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz 2.30\\bin\\dot.exe");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz 2.31\\bin\\dot.exe");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz 2.32\\bin\\dot.exe");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz2.28\\bin\\dot.exe");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz2.29\\bin\\dot.exe");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz2.30\\bin\\dot.exe");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz2.31\\bin\\dot.exe");
    if( !f.exists() ) f = new File("C:\\Program Files (x86)\\Graphviz2.32\\bin\\dot.exe");
    DOT_PATH = f.exists() ? f.getAbsolutePath() : null;
  }

  /** Response column domain. */
  private final String[] _domain;
  /** RF Model columns set - only columns which were used for model building. */
  private final Column[] _columns;
  /** Mapping from RF model' columns into dataset columns. */
  private final int[]    _mapping;
  private final byte[]   _treeBits;
  private final int      _nodeCount;

  public TreeRenderer(RFModel model, int treeNum, ValueArray ary) {
    _treeBits = model.tree(treeNum);
    _mapping  = model.columnMapping(ary.colNames());
    _columns  = model._va._cols;
    _domain   = model.response()._domain;

    long dl = Tree.depth_leaves(new AutoBuffer(_treeBits));
    int leaves= (int)(dl&0xFFFFFFFFL);
    _nodeCount = leaves*2-1;
  }

  public String code() {
    if( _nodeCount > 10000 )
      return "<div class='alert'>Tree is too large to print psuedo code</div>";
    try {
      StringBuilder sb = new StringBuilder();
      sb.append("<pre><code>");
      new CodeTreePrinter(sb, _columns, _mapping, _domain).dumpColumnConstants()
                                                .dumpClassConstants()
                                                .walkSerializedTree(new AutoBuffer(_treeBits));
      sb.append("</code></pre>");
      return sb.toString();
    } catch( Exception e ) {
      return errorRender(e);
    }
  }

  public String graphviz() {
    if( DOT_PATH == null )
      return "<div class='alert'>Install <a href=\"http://www.graphviz.org/\">graphviz</a> to " +
      "see visualizations of small trees</div>";
    if( _nodeCount > 1000 )
      return "<div class='alert'>Tree is too large to graph.</div>";

    try {
      RString img = new RString("<img src=\"data:image/svg+xml;base64,%rawImage\" width='80%%' ></img><p>");
      Process exec = Runtime.getRuntime().exec(new String[] { DOT_PATH, "-Tsvg" });
      new GraphvizTreePrinter(exec.getOutputStream(), _columns, _mapping, _domain).walk_serialized_tree(new AutoBuffer(_treeBits));
      exec.getOutputStream().close();
      byte[] data = ByteStreams.toByteArray(exec.getInputStream());
      img.replace("rawImage", new String(Base64.encodeBase64(data), "UTF-8"));
      return img.toString();
    } catch( Exception e ) {
      return errorRender(e);
    }
  }

  private String errorRender(Exception e) {
    StringBuilder sb = new StringBuilder();
    sb.append("<div class='alert alert-error'>");
    sb.append("Error Generating Dot file:\n<pre>");
    e.printStackTrace(new PrintWriter(CharStreams.asWriter(sb)));
    sb.append("</pre>");
    sb.append("</div>");
    return sb.toString();
  }
}
