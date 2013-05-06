package water.score;

import hex.rf.Tree;
import java.util.*;
import water.AutoBuffer;
import water.H2O;
import water.parser.PMMLParser.*;
import water.parser.PMMLParser;
import water.score.ScoreModel;

public class RFScoreModel extends ScoreModel {

  final String[] _classes;      // Array of prediction values/enums
  int _tnum;                    // Tree number
  byte[][] _trees;              // The packed trees
  ArrayList<String> _cols;      // Columns in-use

  RFScoreModel(String name, String[] classes ) {
    super(name,null);
    if( classes == null ) throw new ParseException("Unknown response variable");
    _classes = classes;
    _cols = new ArrayList();    // Collect columns as we parse
  }

  // Simple internal tree node
  private static class RFNode {
    Predicate _pred;
    RFNode _l, _r;
    short _class;               // Class for leafs
    short _colnum;              // Column number being tested
    int _size;                  // Size in bytes of packed representation
    public String toString() { return toString(new StringBuilder(),0).toString(); }
    public StringBuilder toString(StringBuilder sb, int d) {
      for( int i=0; i<d; i++ ) sb.append("  ");
      if( _l == null && _r == null )
        return sb.append("return ").append(_class).append(";\n");
      sb.append("if( ");
      _l._pred.toJavaNum(sb,_l._pred.unique_name()).append(" ) {\n");
      if( _l != null ) _l.toString(sb,d+1);
      for( int i=0; i<d; i++ ) sb.append("  ");
      //sb.append("} else if( ");
      //_r._pred.toJavaNum(sb,_r._pred.unique_name()).append(" ) {\n");
      sb.append("} else {\n");
      if( _r != null ) _r.toString(sb,d+1);
      for( int i=0; i<d; i++ ) sb.append("  ");
      return sb.append("}\n");
    }
    final int size( ) { return _size==0 ? (_size=size_impl()) : _size;  }
    public int size_impl( ) {
      if( _l == null ) return 1+1; // Leaf?  1 byte indicator, 1 byte class
      // Size is: 1 byte indicator, 2 bytes col, 4 bytes val, the skip, then left, right
      return 1+2+4+(( _l.size() <= 254 ) ? 1 : 4)+_l.size()+_r.size();
    }

    public void toPackedTree(AutoBuffer ab) {
      if( _l == null ) {
        assert _r == null;
        ab.put1('[');
        ab.put1(_class);
        return;
      }
      ab.put1('S');             // 'E' for equals, this is always '>'
      ab.put2(_l._colnum);      // Column number
      if( _l._pred instanceof IsIn ) throw new ParseException("IsIn predicate not handled in RF Scoring yet");
      assert _l._pred instanceof LessOrEqual;
      assert _r._pred instanceof GreaterThan;
      ab.put4f((float)((LessOrEqual)_l._pred)._num);
      int skip = _l.size();
      if( skip <= 254 )  ab.put1(skip);
      else { ab.put1(0); ab.put3(skip); }
      _l.toPackedTree(ab);
      _r.toPackedTree(ab);
    }
  }

  public static RFScoreModel parse( PMMLParser pmml ) {
    HashMap<String,String> attrs = pmml.attrs();
    pmml.expect('>');
    pmml.skipWS().expect('<');
    String cls = pMiningSchema(pmml);
    RFScoreModel model = new RFScoreModel(attrs.get("modelName"),pmml._enums.get(cls));
    pmml.skipWS().expect('<');
    model._trees = model.pSegmentation(pmml);
    pmml.skipWS().expect("</MiningModel>");
    return model;
  }

  // Extract the class column name
  private static String pMiningSchema(PMMLParser pmml) {
    pmml.expect("MiningSchema>");
    String cls = null;
    while( pmml.skipWS().expect('<').peek() != '/' ) cls = pMiningField(pmml,cls);
    pmml.expect("/MiningSchema>");
    return cls;
  }

  // Extract the class column name
  private static String pMiningField(PMMLParser pmml, String cls) {
    HashMap<String,String> attrs = pmml.expect("MiningField").attrs();
    pmml.expect("/>");
    if( cls==null && "predicted".equals(attrs.get("usageType")) )
      return attrs.get("name");
    assert !"predicted".equals(attrs.get("usageType"));
    return cls;
  }


  private byte[][] pSegmentation( PMMLParser pmml ) {
    HashMap<String,String> attrs = pmml.expect("Segmentation").attrs();
    pmml.expect('>');
    assert "majorityVote".equals(attrs.get("multipleModelMethod"));
    ArrayList<byte[]> trees = new ArrayList();
    while( pmml.skipWS().expect('<').peek() != '/' ) trees.add(pSegment(pmml));
    pmml.skipWS().expect("/Segmentation>");
    return trees.toArray(new byte[0][]);
  }

  private byte[] pSegment( PMMLParser pmml ) {
    pmml.expect("Segment").skipAttrs();
    pmml.expect('>').skipWS().expect("<True/>").skipWS().expect('<');
    RFNode rfn = pTreeModel(pmml);
    AutoBuffer ab = new AutoBuffer();
    ab.put4(_tnum++);           // Tree number
    ab.put8(0);                 // Seed
    rfn.toPackedTree(ab);       // Packed tree bits
    pmml.skipWS().expect("</Segment>");
    return ab.buf();
  }

  private RFNode pTreeModel( PMMLParser pmml ) {
    pmml.expect("TreeModel").skipAttrs();
    pmml.expect('>').skipWS().expect('<').pGeneric("MiningSchema").skipWS().expect('<');
    RFNode rfn = pRFNode(pmml);
    pmml.skipWS().expect("</TreeModel>");
    return rfn;
  }

  private RFNode pRFNode( PMMLParser pmml ) {
    HashMap<String,String> attrs = pmml.expect("Node").attrs();
    RFNode rfn = new RFNode();
    String cls = attrs.get("score");
    if( cls != null ) {
      rfn._class = (short)Arrays.binarySearch(_classes,cls,null);
      if( rfn._class < 0 ) throw new ParseException("Unknown class "+cls);
    } else {
      rfn._class = -1;          // Not a leaf node
    }
    rfn._pred = pmml.expect('>').skipWS().expect('<').pPredicate();
    // Convert column/field string to a number right now
    String col = rfn._pred.unique_name();
    if( !col.isEmpty() ) {
      rfn._colnum = (short)_cols.indexOf(col);
      if( rfn._colnum == -1 ) {   // Not found?  insert into list
        rfn._colnum = (short)_cols.size();
        _cols.add(col);
      }
    }
    // Parse left & right children
    if( pmml.skipWS().expect('<').peek() == 'N' ) {
      rfn._l = pRFNode(pmml);
      pmml.skipWS().expect('<');
      rfn._r = pRFNode(pmml);
      pmml.skipWS().expect('<');
    }
    pmml.expect("/Node>");
    return rfn;
  }

  @Override public double score(final HashMap<String, Comparable> row ) {
    double[] ds = new double[_cols.size()];
    for( int i=0; i<ds.length; i++ ) {
      Double D = (Double)row.get(_cols.get(i));
      if( D == null ) throw new RuntimeException("row is missing column "+_cols.get(i)+", contains "+row.keySet());
      ds[i] = D;
    }
    int votes[] = new int[_classes.length+1];
    for( byte[] tree : _trees )
      votes[(int)Tree.classify(new AutoBuffer(tree),ds,_classes.length)]++;
    // Tally results
    int result = 0;
    int tied = 1;
    for( int i = 1; i<votes.length-1; i++)
      if( votes[i] > votes[result] ) { result=i; tied=1; }
      else if( votes[i] == votes[result] ) { tied++; }
    if( tied==1 ) return (short)result;
    // Tie-breaker logic
    Random rand = null;
    int j = rand == null ? 0 : rand.nextInt(tied); // From zero to number of tied classes-1
    int k = 0;
    for( int i=0; i<votes.length-1; i++ )
      if( votes[i]==votes[result] && (k++ >= j) )
        return (short)i;
    throw H2O.unimpl();
  }
  @Override public double score(int[] MAP, String[] SS, double[] DS) {
    throw H2O.unimpl();
  }

}
