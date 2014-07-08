package water.util;

import water.DRemoteTask;
import water.H2O;
import water.Iced;
import water.api.DocGen;
import water.api.Request;

import java.util.*;
import java.util.Map.Entry;

public class ProfileCollectorTask extends DRemoteTask<ProfileCollectorTask> {
  public ProfileCollectorTask(int stack_depth) {
    _stack_depth = stack_depth;
  }

  public static class NodeProfile extends Iced {
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    NodeProfile(int len) {
      stacktraces = new String[len];
      counts = new int[len];
    }
    @Request.API(help="Stack traces")
    public String[] stacktraces;
    @Request.API(help="Stack trace counts")
    public int[] counts;
  }

  public NodeProfile[] _result;
  public final int _stack_depth;

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
          String val = ste.toString();
          // filter out unimportant stuff
          if( j==0 && (   val.equals("sun.misc.Unsafe.park(Native Method)")
                       || val.equals("java.lang.Object.wait(Native Method)")
                       || val.equals("java.lang.Thread.sleep(Native Method)")
                       || val.equals("java.lang.Thread.yield(Native Method)")
                       || val.equals("java.net.PlainSocketImpl.socketAccept(Native Method)")
                       || val.equals("sun.nio.ch.ServerSocketChannelImpl.accept0(Native Method)")
                       || val.equals("sun.nio.ch.DatagramChannelImpl.receive0(Native Method)")
                       || val.equals("java.lang.Thread.dumpThreads(Native Method)")
          ) ) { break; }

          sb.append(ste.toString());
          sb.append("\n");
          j++;
          if (j==_stack_depth) break;
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
      _result[idx].stacktraces[i] = entry.getKey();
      _result[idx].counts[i] = entry.getValue();
      i++;
    }

    // sort it
    Map<Integer, String> sorted = new TreeMap<Integer, String>(Collections.reverseOrder());
    for (int j=0; j<_result[idx].counts.length; ++j) {
      if (_result[idx].stacktraces[j] != null && _result[idx].stacktraces[j].length() > 0)
        sorted.put(_result[idx].counts[j], _result[idx].stacktraces[j]);
    }

    // overwrite results
    String[] sorted_stacktraces = new String[sorted.entrySet().size()];
    int[] sorted_counts = new int[sorted.entrySet().size()];
    i=0;
    for (Map.Entry<Integer, String> e : sorted.entrySet()) {
      sorted_stacktraces[i] = e.getValue();
      sorted_counts[i] = e.getKey();
      i++;
    }
    _result[idx].stacktraces = sorted_stacktraces;
    _result[idx].counts = sorted_counts;
    tryComplete();
  }

  @Override public byte priority() { return H2O.GUI_PRIORITY; }
}
