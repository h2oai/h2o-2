package r;

import java.io.*;

public class RTest {
  public static void run() throws Exception {
    final String r = "" //
        + "a=load('smalldata/covtype/covtype.20k.data')\n" // ;
        + "b=parse(destination_key='covtype.hex', source_key='covtype.20k.data')\n" //
        + "c=kmeans(k='3', destination_key='covtype.kmeans', source_key='covtype.hex', cols='0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54', epsilon='1.0E-2')\n";

    Console.interactive(new BufferedReader(new Reader() {
      boolean _first = true;

      @Override public int read(char[] cbuf, int off, int len) throws IOException {
        if( _first ) {
          for( int i = 0; i < r.length(); i++ )
            cbuf[i] = r.charAt(i);
          _first = false;
          return r.length();
        } else return new InputStreamReader(System.in).read(cbuf, off, len);
      }

      @Override public void close() throws IOException {}
    }, 0xffff));
  }
}
