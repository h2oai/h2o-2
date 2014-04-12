package water.util;

import water.DRemoteTask;
import water.H2O;
import water.Iced;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class ProfileCollectorTask extends DRemoteTask<ProfileCollectorTask> {
  public static class NodeProfile extends Iced {
    NodeProfile(int len) {
      _stacktraces = new String[len];
      _counts = new int[len];
    }
    public String[] _stacktraces;
    public int[] _counts;
  }

  public NodeProfile[] _result;

  @Override public void reduce(ProfileCollectorTask that) {
    if( _result == null ) _result = that._result;
    else for (int i=0; i<_result.length; ++i)
      if (_result[i] == null)
        _result[i] = that._result[i];
  }

  @Override public void lcompute() {
    int idx = H2O.SELF.index();
    _result = new NodeProfile[H2O.CLOUD.size()];

    Map<String, Integer> countedStackTraces = new HashMap<String, Integer>();

    final int repeats = 100;
    for (int i=0; i<repeats; ++i) {
      Map<Thread, StackTraceElement[]> allStackTraces = Thread.getAllStackTraces();
      for (Entry<Thread, StackTraceElement[]> el : allStackTraces.entrySet()) {
        StringBuilder sb = new StringBuilder();
        int j=0;
        for (StackTraceElement ste : el.getValue()) {
          sb.append(ste.toString());
          sb.append("\n");
          j++;
          if (j==3) break;
        }
        String st = sb.toString();
        boolean found = false;
        for (Entry<String, Integer> entry : countedStackTraces.entrySet()) {
          if (entry.getKey().equals(st)) {
            entry.setValue(entry.getValue() + 1);
            found = true;
            break;
          }
        }
        if (!found) countedStackTraces.put(st, 1);
      }
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    int i=0;
    _result[idx] = new NodeProfile(countedStackTraces.size());
    for (Entry<String, Integer> entry : countedStackTraces.entrySet()) {
      _result[idx]._stacktraces[i] = entry.getKey();
      _result[idx]._counts[i] = entry.getValue();
      i++;
    }
    tryComplete();
  }

  @Override public byte priority() { return H2O.GUI_PRIORITY; }
}
