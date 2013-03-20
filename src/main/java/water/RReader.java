package water;

import hex.rf.RFModel;
import hex.rf.Tree;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.zip.GZIPInputStream;

public class RReader {
  private static final String ASCII_MAGIC_HEADER  = "RDA2\n";
  private static final String BINARY_MAGIC_HEADER = "RDB2\n";
  private static final String XDR_MAGIC_HEADER    = "RDX2\n";
  private static final byte   ASCII_FORMAT        = 'A';
  private static final byte   BINARY_FORMAT       = 'B';
  private static final byte   XDR_FORMAT          = 'X';
  private static final int    VERSION             = 2;
  private static final int    HAS_ATTR_BIT_MASK   = (1 << 9);
  private static final int    HAS_TAG_BIT_MASK    = (1 << 10);

  private static final int    SYMSXP              = 1;            // symbols
  private static final int    LISTSXP             = 2;            // lists of dotted pairs
  private static final int    CLOSXP              = 3;            // closures
  private static final int    ENVSXP              = 4;            // environments
  private static final int    PROMSXP             = 5;            // promises: [un]evaluated closure arguments
  private static final int    LANGSXP             = 6;            // language constructs (special lists)
  private static final int    SPECIALSXP          = 7;            // special forms
  private static final int    BUILTINSXP          = 8;            // builtin non-special forms
  private static final int    CHARSXP             = 9;            // "scalar" string type (internal only)
  private static final int    LGLSXP              = 10;           // logical vectors
  private static final int    INTSXP              = 13;           // integer vectors
  private static final int    REALSXP             = 14;           // real variables
  private static final int    CPLXSXP             = 15;           // complex variables
  private static final int    STRSXP              = 16;           // string vectors
  private static final int    DOTSXP              = 17;           // dot-dot-dot object
  private static final int    VECSXP              = 19;           // generic vectors
  private static final int    EXPRSXP             = 20;           // expressions vectors
  private static final int    BCODESXP            = 21;           // byte code
  private static final int    EXTPTRSXP           = 22;           // external pointer
  private static final int    WEAKREFSXP          = 23;           // weak reference
  private static final int    RAWSXP              = 24;           // raw bytes
  private static final int    S4SXP               = 25;           // S4, non-vector
  private static final int    REFSXP              = 255;
  private static final int    NILVALUE_SXP        = 254;
  private static final int    GLOBALENV_SXP       = 253;
  private static final int    UNBOUNDVALUE_SXP    = 252;
  private static final int    MISSINGARG_SXP      = 251;
  private static final int    BASENAMESPACE_SXP   = 250;
  private static final int    NAMESPACESXP        = 249;
  private static final int    PACKAGESXP          = 248;
  private static final int    PERSISTSXP          = 247;
  private static final int    CLASSREFSXP         = 246;
  private static final int    GENERICREFSXP       = 245;
  private static final int    EMPTYENV_SXP        = 242;
  private static final int    BASEENV_SXP         = 241;

  private static final int    LATIN1_MASK         = (1 << 2);
  private static final int    UTF8_MASK           = (1 << 3);

  private static final int    INDEX_CLASSES       = 7;
  private static final int    INDEX_FOREST        = 14;

  private static final int    FOREST_TYPES        = 1;
  private static final int    FOREST_COLS         = 2;
  private static final int    FOREST_NEXT         = 3;
  private static final int    FOREST_CLASS        = 4;
  private static final int    FOREST_VALS         = 5;
  private static final int    FOREST_COLUMNS      = 8;
  private static final int    FOREST_MAX_TREE     = 10;
  private static final int    FOREST_COUNT        = 11;

  private static final String PLUG                = "Unsupported";

  public static void run(Key dest, InputStream stream) {
    try {
      DataInputStream in = new DataInputStream(new GZIPInputStream(stream));
      header(in);

      HashMap<Object, String[]> attribs = new HashMap<Object, String[]>();
      Object value = read(in, attribs);
      // The deserialized R object (ignoring all sorts of attributes and stuff)
      Object[] values = (Object[]) ((Object[]) value)[0];
      // Class names (factors or enums)
      String[] classes = (String[]) values[INDEX_CLASSES];
      // A subset of the R object which contains the trees
      Object[] forest = (Object[]) values[INDEX_FOREST];
      // Size of max tree, in nodes
      int[] jump = (int[]) forest[FOREST_MAX_TREE];
      // Count of trees
      double[] count = (double[]) forest[FOREST_COUNT];
      int num_trees = (int) count[0];
      // Column headers
      String[] headers = attribs.get(forest[FOREST_COLUMNS]);

      // Dump the trees into the KV store
      Key[] keys = new Key[num_trees];
      for( int i = 0; i < num_trees; i++ ) {
        AutoBuffer buffer = build(classes, forest, jump[0], i);
        Key key = Key.make();
        keys[i] = key;
        DKV.put(key, new Value(key, buffer.buf()));
      }

      RFModel model = new RFModel(dest, headers, classes, keys, 1, 100.0f);
      UKV.put(dest, model);
    } catch( Exception ex ) {
      UKV.put(dest, new Job.Fail(ex.toString()));
    }
  }

  private static void header(DataInputStream in) throws Exception {
    byte[] buffer = new byte[7];
    buffer[0] = (byte) in.read();
    buffer[1] = (byte) in.read();
    byte format = -1;

    if( buffer[1] == '\n' ) {
      switch( buffer[0] ) {
      case XDR_FORMAT:
      case ASCII_FORMAT:
      case BINARY_FORMAT:
        format = buffer[0];
        break;
      default:
        throw new Exception("Unsupported format");
      }
    }

    if( format < 0 ) {
      for( int i = 2; i != 7; ++i )
        buffer[i] = (byte) in.read();
      String header = new String(buffer, 0, 5);
      if( header.equals(ASCII_MAGIC_HEADER) )
        format = ASCII_FORMAT;
      else if( header.equals(BINARY_MAGIC_HEADER) )
        format = BINARY_FORMAT;
      else if( header.equals(XDR_MAGIC_HEADER) )
        format = XDR_FORMAT;
      else
        throw new Exception("Unsupported format");

      if( format != XDR_FORMAT )
        throw new Exception("Unsupported format");

      if( in.readInt() != VERSION )
        throw new Exception("Unsupported version");
    }

    // Read & Write versions
    in.readInt();
    in.readInt();
  }

  private static AutoBuffer build(String[] classes, Object[] forest, int jump, int tree) throws Exception {
    AutoBuffer buffer = new AutoBuffer();
    buffer.put4(0);
    buffer.put8(0);
    write(buffer, forest, jump, tree * jump, 0);
    return buffer;
  }

  private static void write(AutoBuffer b, Object[] forest, int jump, int base, int i) {
    int[] types = (int[]) forest[FOREST_TYPES];
    if( types[base + i] > 0 ) {
      int[] cols = (int[]) forest[FOREST_COLS];
      int[] next = (int[]) forest[FOREST_NEXT];
      double[] vals = (double[]) forest[FOREST_VALS];
      b.put1('S');
      b.put2((short) (cols[base + i] - 1));
      b.put4f((float) vals[base + i]);
      int pos = b.position();
      b.positionWithResize(pos + 4);
      write(b, forest, jump, base, next[2 * base + i] - 1);
      int length = b.position() - pos - 4;
      if( length <= 254 ) {
        b.shift(pos + 4, pos + 1, length); // Move data to have length on 1 byte
        b.put1(pos, length);
        b.position(pos + 1 + length);
      } else {
        b.put1(0);
        b.put3(length);
      }
      write(b, forest, jump, base, next[2 * base + jump + i] - 1);
    } else if( types[base + i] < 0 ) {
      int[] clas = (int[]) forest[FOREST_CLASS];
      int pred = clas[base + i];
      b.put1('[');
      assert pred > 0 && pred < 256 : pred;
      b.put1((byte) (pred - 1));
    }
  }

  private static Object read(DataInputStream in, HashMap<Object, String[]> attribs) throws Exception {
    int flags = in.readInt();
    switch( flags & 255 ) {
    case NILVALUE_SXP:
      return null;
    case EMPTYENV_SXP:
      return PLUG;
    case BASEENV_SXP:
      return PLUG;
    case GLOBALENV_SXP:
      return PLUG;
    case UNBOUNDVALUE_SXP:
      return PLUG;
    case MISSINGARG_SXP:
      return PLUG;
    case BASENAMESPACE_SXP:
      return PLUG;
    case REFSXP: {
      int i = flags >> 8;
      if( i == 0 )
        System.out.println(in.readInt());
      return null;
    }
    case PERSISTSXP:
      throw new UnsupportedOperationException();
    case SYMSXP:
      return read(in, attribs);
    case PACKAGESXP:
      throw new UnsupportedOperationException();
    case NAMESPACESXP:
      throw new UnsupportedOperationException();
    case ENVSXP:
      throw new UnsupportedOperationException();
    case LISTSXP:
      attributes(flags, true, true, in);
      return new Object[] { read(in, attribs), read(in, attribs) };
    case LANGSXP:
      attributes(flags, true, true, in);
      return new Object[] { read(in, attribs), read(in, attribs) };
    case CLOSXP:
      throw new UnsupportedOperationException();
    case PROMSXP:
      throw new UnsupportedOperationException();
    case DOTSXP:
      throw new UnsupportedOperationException();
    case EXTPTRSXP:
      throw new UnsupportedOperationException();
    case WEAKREFSXP:
      throw new UnsupportedOperationException();
    case SPECIALSXP:
    case BUILTINSXP:
      throw new UnsupportedOperationException();
    case CHARSXP: {
      int length = in.readInt();
      if( length == -1 )
        return "" + null;
      byte[] bytes = new byte[length];
      in.readFully(bytes);
      int levels = flags >> 12;
      if( (levels & UTF8_MASK) != 0 )
        return new String(bytes, "UTF8");
      if( (levels & LATIN1_MASK) != 0 )
        return new String(bytes, "Latin1");
      return new String(bytes);
    }
    case LGLSXP: {
      int[] array = new int[in.readInt()];
      for( int i = 0; i != array.length; ++i )
        array[i] = in.readInt();
      attributes(flags, true, false, in);
      return array;
    }
    case INTSXP: {
      int[] array = new int[in.readInt()];
      for( int i = 0; i != array.length; ++i )
        array[i] = in.readInt();
      String[] a = attributes(flags, true, false, in);
      if( attribs != null && a != null )
        attribs.put(array, a);
      return array;
    }
    case REALSXP: {
      double[] array = new double[in.readInt()];
      for( int i = 0; i != array.length; ++i )
        array[i] = in.readDouble();
      attributes(flags, true, false, in);
      return array;
    }
    case CPLXSXP:
      throw new UnsupportedOperationException();
    case STRSXP: {
      String[] array = new String[in.readInt()];
      for( int i = 0; i != array.length; ++i )
        array[i] = (String) read(in, attribs);
      attributes(flags, true, false, in);
      return array;
    }
    case VECSXP: {
      Object[] array = new Object[in.readInt()];
      for( int i = 0; i != array.length; ++i )
        array[i] = read(in, attribs);
      attributes(flags, true, false, in);
      return array;
    }
    case EXPRSXP:
      throw new UnsupportedOperationException();
    case BCODESXP:
      throw new UnsupportedOperationException();
    case CLASSREFSXP:
      throw new UnsupportedOperationException();
    case GENERICREFSXP:
      throw new UnsupportedOperationException();
    case RAWSXP:
      throw new UnsupportedOperationException();
    case S4SXP:
      throw new UnsupportedOperationException();
    default:
      throw new Exception("unknown type ");
    }
  }

  private static String[] attributes(int flags, boolean attributes, boolean tag, DataInputStream in) throws Exception {
    boolean hasAttributes = (flags & HAS_ATTR_BIT_MASK) != 0;
    boolean hasTag = (flags & HAS_TAG_BIT_MASK) != 0;
    String[] res = null;

    if( attributes && hasAttributes ) {
      Object o = read(in, null);
      if( o instanceof Object[] ) {
        Object[] a = (Object[]) o;
        if( a[0] instanceof String[] )
          res = (String[]) a[0];
      }
    }

    if( tag && hasTag )
      read(in, null);

    return res;
  }

  // Debug

  static void debug(Object o, int indent) {
    String s = Log.padRight("", indent);

    if( o instanceof int[] ) {
      // System.out.println(((int[]) o).length);
      String a = Arrays.toString((int[]) o);
      s += a.substring(0, Math.min(10000, a.length()));
    } else if( o instanceof double[] ) {
      // System.out.println(((double[]) o).length);
      String a = Arrays.toString((double[]) o);
      s += a.substring(0, Math.min(10000, a.length()));
    } else
      s += o;

    System.out.println(s);

    if( o instanceof Object[] )
      for( Object i : (Object[]) o )
        debug(i, indent + 2);
  }

  static void debug(AutoBuffer buffer) throws Exception {
    final StringBuilder sb = new StringBuilder();
    final String[] classNames = new String[] { "c1", "c2", "c3" };
    sb.append("digraph {\n");
    new Tree.TreeVisitor(buffer) {
      protected Tree.TreeVisitor leaf(int tclass) throws IOException {
        String x = classNames != null && tclass < classNames.length ? String.format("%d [label=\"%s\"];\n",
            _ts.position() - 2, classNames[tclass]) : String.format("%d [label=\"Class %d\"];\n", _ts.position() - 2,
            tclass);
        sb.append(x);
        return this;
      }

      protected Tree.TreeVisitor pre(int col, float fcmp, int off0, int offl, int offr) throws IOException {
        byte b = (byte) _ts.get1(off0);
        sb.append(String.format("%d [label=\"%s %s %f\"];\n", off0, "col" + col, ((b == 'E') ? "==" : "<="), fcmp));
        sb.append(String.format("%d -> %d;\n", off0, offl));
        sb.append(String.format("%d -> %d;\n", off0, offr));
        return this;
      }
    }.visit();
    sb.append("}");
    System.out.println(sb.toString());
  }
}
