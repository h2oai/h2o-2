package water.util;

import java.util.Map;
import java.util.Map.Entry;

import water.DRemoteTask;
import water.H2O;

public class JStackCollectorTask extends DRemoteTask {
  public String[] _result; // for each node in the cloud it contains all threads stack traces

  public JStackCollectorTask() {}

  @Override
  public void reduce(DRemoteTask drt) {
    JStackCollectorTask another = (JStackCollectorTask) drt;
    if( _result == null ) _result = another._result;
    else for (int i=0; i<_result.length; ++i)
      if (_result[i] == null)
        _result[i] = another._result[i];
  }

  @Override public void lcompute() {
    _result = new String[H2O.CLOUD._memary.length];
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
    sb.append('"'); sb.append(t.getName()); sb.append('"');
    if (t.isDaemon()) sb.append(" daemon");
    sb.append(" prio="); sb.append(t.getPriority());
    sb.append(" tid="); sb.append(t.getId());
    sb.append("\n  java.lang.Thread.State: "); sb.append(t.getState());
    sb.append('\n');
  }

  private void append(final StringBuilder sb, final StackTraceElement[] trace) {
    for (int i=0; i < trace.length; i++) {
      sb.append("\tat "); sb.append(trace[i]); sb.append('\n');
    }
  }
}