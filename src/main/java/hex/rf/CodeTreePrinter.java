package hex.rf;

import hex.rf.Tree.ExclusionNode;
import hex.rf.Tree.LeafNode;
import hex.rf.Tree.SplitNode;

import java.io.*;

import water.AutoBuffer;
import water.ValueArray.Column;
import water.util.IndentingAppender;


public class CodeTreePrinter extends TreePrinter {
  private final IndentingAppender _dest;

  public CodeTreePrinter(OutputStream dest, Column[] columns, int[] mapping, String[] classNames) {
    this(new OutputStreamWriter(dest), columns, mapping, classNames);
  }

  public CodeTreePrinter(Appendable dest, Column[] columns, int[] mapping, String[] classNames) {
    super(columns, mapping, classNames);
    _dest = new IndentingAppender(dest);
  }

  public void printTree(Tree t) throws IOException {
    _dest.append("int classify(Row row) {\n");
    _dest.incrementIndent();
    t._tree.print(this);
    _dest.decrementIndent().append("}").flush();
  }

  void printNode(LeafNode t) throws IOException {
    _dest.append("return ").append(Integer.toString(t._class)).append('\n');
  }

  void printNode(SplitNode t) throws IOException {
    _dest.append("if (row.getColumnClass(").append(_cols[t._column]._name).append(") <= ");
    _dest.append(Integer.toString(t._split)).append(")\n");
    _dest.incrementIndent();
    t._l.print(this);
    _dest.decrementIndent().append("else\n").incrementIndent();
    t._r.print(this);
    _dest.decrementIndent();
  }

  void printNode(ExclusionNode t) throws IOException {
    // return r.getColumnClass(_column) <= _split ? _l.classify(r) : _r.classify(r);
    _dest.append("if (row.getColumnClass(").append(_cols[t._column]._name).append(") == ");
    _dest.append(Integer.toString(t._split)).append(")\n");
    _dest.incrementIndent();
    t._l.print(this);
    _dest.decrementIndent().append("else\n").incrementIndent();
    t._r.print(this);
    _dest.decrementIndent();
  }

  public CodeTreePrinter dumpColumnConstants() {
    try {
      _dest.append("// Column constants\n");
      for (int i=0; i<_cols.length-1; i++)
        _dest.append(String.format("int %s = %d;\n", colNameConstant(i), _colMapping[i]));
    } catch (IOException e) { throw new Error(e); }
    return this;
  }

  private String colNameConstant(int colNum) {
    return "COL_" + _cols[colNum]._name.toUpperCase().replace(' ', '_').replace('.', '_');
  }

  public CodeTreePrinter dumpClassConstants() {
    return this;
  }

  public CodeTreePrinter walkSerializedTree( AutoBuffer tbits ) {
    try {
      _dest.append("int classify(float fs[]) {\n");
      _dest.incrementIndent();
      new Tree.TreeVisitor<IOException>(tbits) {
        public Tree.TreeVisitor leaf(int tclass ) throws IOException {
        String x = _classNames != null && tclass < _classNames.length
            ? String.format("return %s;\n",_classNames[tclass])
            : String.format("return %d;\n",tclass);
          _dest.append(x);
          return this;
        }
        protected Tree.TreeVisitor pre (int col, float fcmp, int off0, int offl, int offr ) throws IOException {
          byte b = (byte) _ts.get1(off0);
          _dest.append(String.format("if( fs[%s] %s %f ) \n",colNameConstant(col),((b=='E')?"==":"<="), fcmp)).incrementIndent();
          return this;
        }
        protected Tree.TreeVisitor mid (int col, float fcmp ) throws IOException {
          _dest.decrementIndent().append("else\n").incrementIndent();
          return this;
        }
        protected Tree.TreeVisitor post(int col, float fcmp ) throws IOException {
          _dest.decrementIndent();
          return this;
        }
      }.visit();
      _dest.decrementIndent().append("}").flush();
    } catch( IOException e ) { throw new Error(e); }

    return this;
  }
}
