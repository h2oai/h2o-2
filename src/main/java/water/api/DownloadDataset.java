package water.api;

import water.*;
import water.fvec.Frame;

import java.io.InputStream;
import java.util.Properties;

/**
 * @author tomasnykodym
 */
public class DownloadDataset extends Request2 {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  // This Request supports the HTML 'GET' command, and this is the help text
  // for GET.
  static final String DOC_GET = "Download a Frame as a CSV file";

  @API(help="An existing H2O Frame or VA key.", filter=Default.class)
  Key src_key;

  @API(help="Emit double values in a machine readable lossless format with Double.toHexString().", filter=Default.class)
  boolean hex_string = false;

  public static String link(Key k, String content){
    return  "<a href='/2/DownloadDataset?src_key=" + k.toString() + "'>" + content + "</a>";
  }

  @SuppressWarnings("resource")
  @Override final public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    // Needs to be done also for help to initialize or argument records
    checkArguments(args, type);
    if (DKV.get(src_key) == null) throw new IllegalArgumentException(src_key.toString() + " not found.");
    Object value = DKV.get(src_key).get();
    InputStream csv = ((Frame) value).toCSV(true, hex_string);
    NanoHTTPD.Response res = server.new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_DEFAULT_BINARY, csv);
    // Clean up Key name back to something resembling a file system name.  Hope
    // the user's browser actually asks for what to do with the suggested
    // filename.  Without this code, my FireFox would claim something silly
    // like "no helper app installed", then fail the download.
    String s = src_key.toString();
    int x = s.length()-1;
    boolean dot=false;
    for( ; x >= 0; x-- )
      if( !Character.isLetterOrDigit(s.charAt(x)) && s.charAt(x)!='_' )
        if( s.charAt(x)=='.' && !dot ) dot=true;
        else break;
    String suggested_fname = s.substring(x+1).replace(".hex", ".csv");
    if( !suggested_fname.endsWith(".csv") )
      suggested_fname = suggested_fname+".csv";
    res.addHeader("Content-Disposition", "filename=" + suggested_fname);
    return res;
  }

  @Override protected Response serve() {
    return Response.doneEmpty();
  }
}
