package water.api;

import java.util.Properties;

import water.*;
import water.ValueArray.CsvVAStream;


/**
 *
 * @author tomasnykodym
 *
 */
public class DownloadDataset extends Request {

  protected final H2OHexKey _dataKey = new H2OHexKey(KEY);



  @Override public String href(){
    return "downloadCsv";
  }

  public static String link(Key k, String content){
    return  "<a href='/downloadCsv?key=" + k.toString() + "'>" + content + "</a>";
  }


  @SuppressWarnings("resource")
  @Override final public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
 // Needs to be done also for help to initialize or argument records
    checkArguments(args, type);
    ValueArray ary = _dataKey.value();
    NanoHTTPD.Response res = server.new Response(NanoHTTPD.HTTP_OK,NanoHTTPD.MIME_DEFAULT_BINARY,new CsvVAStream(ary,null));
    System.out.println("filename=" + ary._key.toString().replace(".hex", ".csv"));
    res.addHeader("Content-Disposition", "filename=" + ary._key.toString().replace(".hex", ".csv"));
    return res;
  }

  @Override protected Response serve() {
    return Response.doneEmpty();
  }
}
