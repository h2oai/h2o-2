package hex.singlenoderf;

import hex.singlenoderf.Tree.ExclusionNode;
import hex.singlenoderf.Tree.LeafNode;
import hex.singlenoderf.Tree.SplitNode;

import java.io.IOException;

public abstract class TreePrinter {
  protected final String[] _classNames;
  protected final int[]    _colMapping;

  public TreePrinter(int[] colMapping, String[] classNames) {
    _classNames = classNames;
    _colMapping = colMapping;
  }

  abstract void printNode(LeafNode t) throws IOException;
  abstract void printNode(SplitNode t) throws IOException;
  abstract void printNode(ExclusionNode t) throws IOException;
}
