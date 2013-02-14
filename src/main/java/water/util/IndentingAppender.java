package water.util;

import java.io.*;

public class IndentingAppender implements Appendable, Flushable, Closeable {
  private final String _indent;
  private final Appendable _a;
  private boolean _pendingNewline = false;
  private int _l = 0;

  public IndentingAppender(Appendable base) {
    _a = base;
    _indent = "  ";
  }

  public IndentingAppender incrementIndent() { ++_l; return this; }
  public IndentingAppender decrementIndent() { --_l; return this; }
  public IndentingAppender appendln(CharSequence csq) throws IOException {
    return append(csq, 0, csq.length()).append('\n');
  }

  @Override public IndentingAppender append(CharSequence csq) throws IOException {
    return append(csq, 0, csq.length());
  }

  @Override public IndentingAppender append(CharSequence csq, int start, int end) throws IOException {
    for( int i = start; i < end; ++i ) append(csq.charAt(i));
    return this;
  }

  @Override
  public IndentingAppender append(char c) throws IOException {
    handlePending();
    if( c == '\n' ) {
      _pendingNewline = true;
    } else {
      _a.append(c);
    }
    return this;
  }

  @Override
  public void flush() throws IOException {
    handlePending();
    if( _a instanceof Flushable ) ((Flushable) _a).flush();
  }

  @Override
  public void close() throws IOException {
    flush();
    if( _a instanceof Closeable ) ((Closeable) _a).close();
  }

  private void handlePending() throws IOException {
    if( _pendingNewline ) {
      _a.append('\n');
      for( int i = 0; i < _l; ++i ) _a.append(_indent);
    }
    _pendingNewline = false;
  }
}
