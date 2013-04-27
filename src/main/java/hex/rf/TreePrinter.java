package hex.rf;

import hex.rf.Tree.ExclusionNode;
import hex.rf.Tree.LeafNode;
import hex.rf.Tree.SplitNode;

import java.io.IOException;

import water.ValueArray.Column;

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
