package water.util;

import java.util.Map;
import java.util.Map.Entry;

import water.DRemoteTask;
import water.H2O;

public class JStackCollectorTask extends DRemoteTask {
  public String[] result; // for each node in the cloud it contains all threads stack traces

  public JStackCollectorTask() {
    result = new String[H2O.CLOUD._memary.length];
  }

  @Override
  public void reduce(DRemoteTask drt) {
    JStackCollectorTask another = (JStackCollectorTask) drt;
    for (int i=0; i<result.length; ++i) {
      if (result[i] == null)
        result[i] = another.result[i];
    }
  }

  @Override
  public void compute() {
    Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
    StringBuilder sb = new StringBuilder();
    for (Entry<Thread,StackTraceElement[]> el : allStackTraces.entrySet()) {
      append(sb, el.getKey());
      append(sb, el.getValue());
      sb.append('\n');
    }
    result[H2O.SELF.index()] = sb.toString();
    tryComplete();
  }

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