package water.api;

import java.io.*;
import java.nio.CharBuffer;
import java.util.Properties;

import water.NanoHTTPD;

public class StaticHTMLPage extends Request {
  private final String _html;
  private final String _href;
  public StaticHTMLPage(File f, String href) {
    _href = href;
    try {
      FileReader r = new FileReader(f);
      try{
        CharBuffer cb = CharBuffer.allocate((int)f.length()+1);
        while(r.read(cb) != -1);
        _html = cb.flip().toString();
      }finally{r.close();}
    }catch(IOException e){
      throw new RuntimeException(e);
    }
  }
  public NanoHTTPD.Response serve(NanoHTTPD server, Properties args, RequestType type) {
    return wrap(server,_html);
  }
  @Override protected Response serve() {
    throw new RuntimeException("should never be called!");
  }
  protected String href() {
    return _href;
  }
}
