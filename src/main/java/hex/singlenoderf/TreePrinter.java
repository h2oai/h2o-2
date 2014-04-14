package hex.singlenoderf;
import hex.singlenoderf.*;
import hex.singlenoderf.Tree.ExclusionNode;
import hex.singlenoderf.Tree.LeafNode;
import hex.singlenoderf.Tree.SplitNode;
import water.ValueArray.Column;

import java.io.IOException;

public abstract class TreePrinter {
  protected final Column[] _cols;
  protected final String[] _classNames;
  protected final int[]    _colMapping;

  public TreePrinter(Column[] columns, int[] colMapping, String[] classNames) {
    _cols       = columns;
    _classNames = classNames;
    _colMapping = colMapping;
  }

  public abstract void printTree(hex.singlenoderf.Tree t) throws IOException;
  abstract void printNode(LeafNode t) throws IOException;
  abstract void printNode(SplitNode t) throws IOException;
  abstract void printNode(ExclusionNode t) throws IOException;
}
