package water.web;

import java.util.Properties;

import water.DKV;
import water.Key;
import water.Value;
import water.ValueArray;
import water.parser.CsvParser;
import water.web.Page.PageError;

/**
 * Utility holding common code for checking errors on servlet request pages
 */
public class ServletUtil {

  // Returns a structured ValueArray or an error String
  public static ValueArray check_array(Properties args, String s) throws PageError {
    Key key = check_key(args,s);
    // Distributed get
    Value val = DKV.get(key);
    if( val == null ) throw new PageError("Key not found: "+ key);
    if( val._isArray == 0 )
      throw new PageError("Key not a structured (parsed) array");
    ValueArray ary = ValueArray.value(val);
    return ary;
  }

  // Returns a Key or an error String
  public static Key check_key(Properties args, String s) throws PageError {
    String skey = args.getProperty(s);
    if( skey == null ) throw new PageError("Missing argument key: "+ s);
    try {
      return Key.make(skey);
    } catch( IllegalArgumentException e ) {
      throw new PageError("Not a valid key: "+ skey);
    }
  }

  // Task to pass the static task execution function
  public static abstract class RunnableTask {
    public abstract String run(ValueArray ary, int colA, int colB);
  }

  public static void createBestEffortSummary(Key key, RString row, long len) {
    final int maxCols = 100;
    // Guess any separator
    byte[] bs = DKV.get(key).getFirstBytes();
    int[] rows_cols = CsvParser.inspect(bs);
    // Inject into the HTML
    if (rows_cols != null) {
      int rows = rows_cols[0];  // Rows in this first bit of data
      double bytes_per_row = (double)bs.length/rows_cols[0]; // Estimated bytes/row
      row.replace("rows",(long)((double)len/bytes_per_row));
      row.replace("cols",rows_cols[1]);
    }
    row.append();
  }
}

