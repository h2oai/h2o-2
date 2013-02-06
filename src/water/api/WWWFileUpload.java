package water.api;

import java.io.IOException;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.util.Arrays;
import java.util.UUID;

import org.apache.http.*;
import org.apache.http.entity.HttpEntityWrapper;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.DefaultHttpServerConnection;
import org.apache.http.message.BasicHttpResponse;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;

import water.*;

import com.google.gson.*;

public class WWWFileUpload extends JSONOnlyRequest {
  // we are explicitly breaking the naming convention here to maintain compat
  // with the shared javascript between the old and new web api
  protected final Str _key = new Str(KEY, "");
  protected final Str _file = new Str(FILE, "file");

  // Maximal waiting time for client connection.
  // If the timeout is reached, server socket is closed.
  private static final int ACCEPT_TIMEOUT_MS = 60*1000; // = 1mins

  public static int uploadFile(String filename, String key) throws Exception {
    // Open a new port to listen by creating a server socket to permit upload.
    // The socket is closed by the uploader thread.
    ServerSocket serverSocket = new ServerSocket(0/*any port*/, 1/*queue len*/);
    serverSocket.setSoTimeout(ACCEPT_TIMEOUT_MS);
    serverSocket.setReuseAddress(true);
    int port = serverSocket.getLocalPort();

    // Launch uploader thread which retrieve a byte stream from client and
    // store it to key. If the client is not connected within a specified
    // timeout, the thread is destroyed.
    new WWWFileUpload.UploaderThread(serverSocket, filename, key).start();
    return port;
  }

  @Override protected Response serve() {
    try {
      String key = _key.value();
      if( key.isEmpty()) key = UUID.randomUUID().toString(); // additional check for empty Key-field since the Key-field can be returned as a part of form
      String fname = _file.value(); // TODO: send file name
      int port = uploadFile(fname, key);
      JsonObject res = new JsonObject();
      res.addProperty("port", port);
      return Response.done(res);
    } catch (Exception e) {
      return Response.error(e.getMessage());
    }
  }

  // Thread handling upload of a (possibly large) file.
  private static class UploaderThread extends Thread {
    // Server socket
    ServerSocket ssocket;
    // Key properties
    String filename;
    String keyname;

    public UploaderThread(ServerSocket ssocket, String filename, String keyname) {
      super("Uploader thread for: " + filename);
      this.ssocket = ssocket;
      this.filename = filename;
      this.keyname = keyname;
    }

    @Override
    public void run() {

      try {
        // Since we do cross-site request we need to handle to requests - 1st OPTIONS, 2nd is POST
        while(true) {
          // Wait for the 1st connection and handle connection in this thread.
          DefaultHttpServerConnection conn = new DefaultHttpServerConnection();
          conn.bind(ssocket.accept(), new BasicHttpParams());
          HttpRequest request = conn.receiveRequestHeader();
          RequestLine requestLine = request.getRequestLine();

          try {
            HttpResponse response = new BasicHttpResponse(HttpVersion.HTTP_1_1, HttpStatus.SC_OK, null);
            boolean finish = false;
            if (requestLine.getMethod().equals("OPTIONS")) {
              finish = handleOPTIONS(conn, request, response);
            } else if (requestLine.getMethod().equals("POST")) {
              finish = handlePOST(conn, request, response);
            }
            // Consume entity if necessary.
            if (request instanceof HttpEntityEnclosingRequest) EntityUtils.consume(((HttpEntityEnclosingRequest) request).getEntity());
            // Send response.
            conn.sendResponseHeader(response);
            conn.sendResponseEntity(response);
            // If the file was upload successfully then finish
            if (finish) break;
          } finally { // shutdown connection
            try { conn.close(); } catch( IOException e ) { }
          }
        }
      } catch (SocketTimeoutException ste) {
        // The client does not connect during the socket timeout => it is not interested in upload.
      } catch (IOException e) {
      } catch (HttpException e) {
      } finally {
        // shutdown server
        try { if (ssocket != null) ssocket.close(); } catch( IOException e ) { }
      }
    }

    protected boolean handleOPTIONS( final HttpServerConnection conn,
                                     final HttpRequest request,
                                     final HttpResponse response) throws HttpException, IOException {
      response.setStatusCode(HttpStatus.SC_OK);
      response.setReasonPhrase("OK");
      addDefaultHeaders(response);
      for (Header header: request.getHeaders("Access-Control-Request-Headers")) { // to support chunked uploads
        if (header.getValue().contains("x-file-name")) {
          response.addHeader("Access-Control-Allow-Headers", "x-file-name,x-file-size,x-file-type");
        }
      }

      return false;
    }

    protected boolean handlePOST( final HttpServerConnection conn,
                                  final HttpRequest request,
                                  final HttpResponse response) throws HttpException, IOException {
      // TODO: support chunked uploads
      Header contentTypeHeader = request.getFirstHeader("Content-Type");
      if (contentTypeHeader == null || !contentTypeHeader.getValue().startsWith("multipart/form-data")) { // File is not received
        sendError(response, HttpStatus.SC_BAD_REQUEST, "Request including multipart/form-data is expected");
        return true;
      }

      String boundary = null; // Get file boundary.
      for(HeaderElement el : contentTypeHeader.getElements()) {
        NameValuePair nvp = el.getParameterByName("boundary");
        if (nvp != null) { boundary = nvp.getValue(); break; }
      }
      if (boundary == null) { sendError(response, HttpStatus.SC_BAD_REQUEST, "Boundary is not included in request"); return true; }

      // Get http entity.
      conn.receiveRequestEntity((HttpEntityEnclosingRequest)request);
      if (request instanceof HttpEntityEnclosingRequest) {
        WWWFileUpload.HttpMultipartEntity entity = new WWWFileUpload.HttpMultipartEntity( ((HttpEntityEnclosingRequest)request).getEntity(), boundary.getBytes() );
        // Skip content header
        entity.skipHeader();

        // Read directly from stream and create a key
        Key key = ValueArray.readPut(keyname, entity.getContent());
        JsonElement jsonResult = getJsonResult(key);
        String result = jsonResult.toString();

        response.setStatusCode(HttpStatus.SC_OK);
        response.setReasonPhrase("OK");
        response.setEntity(new StringEntity(result, "application/json", HTTP.DEFAULT_CONTENT_CHARSET));
        addDefaultHeaders(response);
        response.addHeader("Content-Type", "application/json");
        response.addHeader("Content-Length", String.valueOf(result.length()));

        return true;
      } else {
        sendError(response, HttpStatus.SC_BAD_REQUEST, "Wrong request !?");
        return true;
      }
    }

    protected void sendError(final HttpResponse response, final int code, final String msg) {
      response.setStatusCode(code); response.setReasonPhrase(msg);
    }

    protected void addDefaultHeaders(final HttpResponse response) {
      response.addHeader("Access-Control-Allow-Origin", "*");
      response.addHeader("Access-Control-Allow-Methods", "OPTIONS,PUT,POST");
    }

    protected JsonElement getJsonResult(final Key key) {
      Value val = DKV.get(key);
      // The returned JSON object should follow structure of jquery-upload plugin
      JsonArray jsonResult = new JsonArray();
      JsonObject jsonFile = new JsonObject();
      jsonFile.addProperty(NAME, filename);
      jsonFile.addProperty(SIZE, val.length());
      jsonFile.addProperty( URL, "/Get?"+KEY+"=" + key.toString());
      jsonFile.addProperty( KEY, key.toString());
      jsonResult.add(jsonFile);

      return jsonResult;
    }
  }

  static class HttpMultipartEntity extends HttpEntityWrapper {
    private static final byte[] BOUNDARY_PREFIX = { '\r', '\n', '-', '-' };
    byte[] boundary;

    public HttpMultipartEntity(HttpEntity wrapped, byte[] boundary) {
      super(wrapped);
      this.boundary = Arrays.copyOf(BOUNDARY_PREFIX, BOUNDARY_PREFIX.length + boundary.length);
      System.arraycopy(boundary, 0, this.boundary, BOUNDARY_PREFIX.length, boundary.length);
    }

    public void skipHeader() throws IOException {
      InputStream is = wrappedEntity.getContent();
      // Skip the content disposition header
      skipContentDispositionHeader(is);
    }

    private void skipContentDispositionHeader(InputStream is) throws IOException {
      byte mode = 0; // 0 = nothing, 1=\n, 2=\n\n, 11=\r, 12=\r\n, 13=\r\n\r, 14=\r\n\r\n

      int c;
      while ((c = is.read()) != -1) {
        switch( mode ) {
        case 0 : if (c=='\n') mode= 1; else if (c=='\r') mode=11; else mode = 0; break;
        case 1 : if (c=='\n') return; else if (c=='\r') mode= 0; else mode = 0; break;
        case 11: if (c=='\n') mode=12; else if (c=='\r') mode=11; else mode = 0; break;
        case 12: if (c=='\n') mode= 0; else if (c=='\r') mode=13; else mode = 0; break;
        case 13: if (c=='\n') return; else if (c=='\r') mode=11; else mode = 0; break;
        }
      }
    }

    @Override
    public InputStream getContent() throws IOException {
      InputStream is = wrappedEntity.getContent();
      return new WWWFileUpload.HttpMultipartEntity.InputStreamWrapper(is);
    }

    class InputStreamWrapper extends InputStream {
      InputStream wrappedIs;

      byte[] lookAheadBuf;
      int lookAheadLen;

      public InputStreamWrapper(InputStream is) {
        this.wrappedIs = is;
        this.lookAheadBuf = new byte[boundary.length];
        this.lookAheadLen = 0;
      }

      @Override public void close() throws IOException { wrappedIs.close(); }
      @Override public int available() throws IOException { return wrappedIs.available(); }
      @Override public long skip(long n) throws IOException { return wrappedIs.skip(n); }
      @Override public void mark(int readlimit) { wrappedIs.mark(readlimit); }
      @Override public void reset() throws IOException { wrappedIs.reset(); }
      @Override public boolean markSupported() { return wrappedIs.markSupported(); }

      @Override public int read() throws IOException { throw new UnsupportedOperationException(); }
      @Override public int read(byte[] b) throws IOException { return read(b, 0, b.length); }
      @Override public int read(byte[] b, int off, int len) throws IOException {
        int readLen = readInternal(b, off, len);
        if (readLen != -1) {
          int pos = findBoundary(b, off, readLen);
          if (pos != -1) {
            while (wrappedIs.read()!=-1) ; // read the rest of stream
            return pos - off;
          }
        }
        return readLen;
      }

      private int readInternal(byte b[], int off, int len) throws IOException {
        if (len < lookAheadLen ) {
          System.arraycopy(lookAheadBuf, 0, b, off, len);
          lookAheadLen -= len;
          System.arraycopy(lookAheadBuf, len, lookAheadBuf, 0, lookAheadLen);
          return len;
        }

        if (lookAheadLen > 0) {
          System.arraycopy(lookAheadBuf, 0, b, off, lookAheadLen);
          off += lookAheadLen;
          len -= lookAheadLen;
          int r = Math.max(wrappedIs.read(b, off, len), 0) + lookAheadLen;
          lookAheadLen = 0;
          return r;
        } else {
          return wrappedIs.read(b, off, len);
        }
      }

      // Find boundary in read buffer
      private int findBoundary(byte[] b, int off, int len) throws IOException {
        int bidx = -1; // start index of boundary
        int idx = 0; // actual index in boundary[]
        for(int i = off; i < off+len; i++) {
          if (boundary[idx] != b[i]) { // reset
            idx = 0;
            bidx = -1;
          }
          if (boundary[idx] == b[i]) {
            if (idx == 0) bidx = i;
            if (++idx == boundary.length) return bidx; // boundary found
          }
        }
        if (bidx != -1) { // it seems that there is boundary but we did not match all boundary length
          assert lookAheadLen == 0; // There should not be not read lookahead
          lookAheadLen = boundary.length - idx;
          int readLen = wrappedIs.read(lookAheadBuf, 0, lookAheadLen);
          if (readLen < boundary.length - idx) { // There is not enough data to match boundary
            lookAheadLen = readLen;
            return -1;
          }
          for (int i = 0; i < boundary.length - idx; i++) {
            if (boundary[i+idx] != lookAheadBuf[i]) return -1; // There is not boundary => preserve lookahed buffer
          }
          // Boundary found => do not care about lookAheadBuffer since all remaining data are ignored
        }

        return bidx;
      }
    }
  }

}
