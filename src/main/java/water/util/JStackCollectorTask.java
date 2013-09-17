package water.util;

import java.util.Map;
import java.util.Map.Entry;
import water.DRemoteTask;
import water.H2O;

public class JStackCollectorTask extends DRemoteTask<JStackCollectorTask> {
  public String[] _result; // for each node in the cloud it contains all threads stack traces

  @Override public void reduce(JStackCollectorTask that) {
    if( _result == null ) _result = that._result;
    else for (int i=0; i<_result.length; ++i)
      if (_result[i] == null)
        _result[i] = that._result[i];
  }

  @Override public void lcompute() {
    _result = new String[H2O.CLOUD.size()];
    Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
    StringBuilder sb = new StringBuilder();
    for (Entry<Thread,StackTraceElement[]> el : allStackTraces.entrySet()) {
      append(sb, el.getKey());
      append(sb, el.getValue());
      sb.append('\n');
    }
    _result[H2O.SELF.index()] = sb.toString();
    tryComplete();
  }

  @Override public byte priority() { return H2O.GUI_PRIORITY; }

  private void append(final StringBuilder sb, final Thread t) {
    sb.append('"').append(t.getName()).append('"');
    if (t.isDaemon()) sb.append(" daemon");
    sb.append(" prio=").append(t.getPriority());
    sb.append(" tid=").append(t.getId());
    sb.append(" java.lang.Thread.State: ").append(t.getState());
    sb.append('\n');
  }

  private void append(final StringBuilder sb, final StackTraceElement[] trace) {
    for (int i=0; i < trace.length; i++)
      sb.append("\tat ").append(trace[i]).append('\n');
  }
}
