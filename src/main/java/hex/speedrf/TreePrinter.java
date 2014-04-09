package hex.speedrf;

import hex.speedrf.Tree.ExclusionNode;
import hex.speedrf.Tree.LeafNode;
import hex.speedrf.Tree.SplitNode;
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

  public abstract void printTree(Tree t) throws IOException;
  abstract void printNode(LeafNode t) throws IOException;
  abstract void printNode(SplitNode t) throws IOException;
  abstract void printNode(ExclusionNode t) throws IOException;
}
