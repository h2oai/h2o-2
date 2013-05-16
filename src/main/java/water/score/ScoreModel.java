package water.score;

import java.util.HashMap;
import java.util.HashSet;

import water.util.Log;
import water.util.Log.Tag.Sys;

/**
 * Embedded Scoring model
 */
public abstract class ScoreModel {
  public final String _name;
  public final String _colNames[]; // Column names

  ScoreModel( String name, String colNames[] ) {
    _name = name;
    _colNames = colNames;
  }

  // Convert an XML name to a java name
  protected static String xml2jname( String xml ) {
    // Convert pname to a valid java name
    StringBuilder nn = new StringBuilder();
    char[] cs = xml.toCharArray();
    if( !Character.isJavaIdentifierStart(cs[0]) )
      nn.append('X');
    for( char c : cs ) {
      if( !Character.isJavaIdentifierPart(c) ) {
        nn.append('_');
      } else {
        nn.append(c);
      }
    }
    String jname = nn.toString();
    return jname;
  }

  // The list of JIT'd classes, each a specific subclass of ScorecardModel
  // representing the optimized version of a particular set of scoring rules.
  final static HashSet<String> CLASS_NAMES = new HashSet<String>();

  // Make a unique class name for jit'd subclasses of ScoreModel
  protected static String uniqueClassName(String name) {
    // Make a unique class name
    String cname = xml2jname(name);
    if( CLASS_NAMES.contains(cname) ) {
      int i=0;
      while( CLASS_NAMES.contains(cname+i) ) i++;
      cname = cname+i;
    }
    CLASS_NAMES.add(cname);
    return cname;
  }

  // A mapping from the dense columns desired by the model, to the above
  // feature list, computed by asking the model for a mapping (given a list of
  // features).  Some features may be unused and won't appear in the mapping.
  // If the data row features list does not mention all the features the model
  // needs, then this map will contain a -1 for the missing feature index.
  public int[] columnMapping( String[] features ) {
    int[] map = new int[_colNames.length];
    for( int i=0; i<_colNames.length; i++ ) {
      map[i] = -1;              // Assume it is missing
      for( int j=0; j<features.length; j++ ) {
        if( _colNames[i].equals(features[j]) ) {
          if( map[i] != -1 ) throw new IllegalArgumentException("duplicate feature "+_colNames[i]);
          map[i] = j;
        }
      }
      if( map[i] == -1 ) Log.warn(Sys.SCORM,"Model feature "+_colNames[i]+" not in the provided feature list from the data");
    }
    return map;
  }

  /** Score this model on the specified row of data, where the data is
   *  specified as a collection of K/V pairs - Values are one of String or
   *  Boolean or Number (or subclasses of Number) */
  public abstract double score(final HashMap<String, Comparable> row );


  /** Score this model on the specified row of data, where the data is
   *  specified as the members of arrays.  MAP is used to map between the SS/DS
   *  columns and the columns desired by the Model; this map can be made by a
   *  single call to columnMapping.  SS/DS hold either String values (for
   *  enum/categorical data) or a primitive double.  This format exchanges a
   *  HashMap lookup for a bare array access, and can be faster (perhaps much
   *  faster) for models that are alread quick to score.
   */
  public abstract double score(int[] MAP, String[] SS, double[] DS);
}

