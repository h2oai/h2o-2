package water;

import jsr166y.CountedCompleter;
import jsr166y.ForkJoinPool;
import jsr166y.ForkJoinTask;
import jsr166y.RecursiveAction;
import water.H2O.H2OCountedCompleter;
import water.api.DocGen;
import water.fvec.Vec;
import water.util.Log;
import water.util.Utils;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;


public class UDPDropTest extends Func {
  static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Message sizes", filter = Default.class, json=true)
  public int[] msg_sizes = new int[]{1,32,64,128,256,512,1024,AutoBuffer.MTU-100}; //INPUT



  @API(help = "Nodes", json=true)
  public String[] nodes; //OUTPUT

  @API(help = "Drop rates between each (ordered) pair of nodes for different message sizes", json = true)
  public UDPDropMatrix [] dropRates;

  private static class UDPPing extends DTask<UDPPing>{
    boolean _done;
    int _retries = -1;
    final long _t1;
    long _t2;
    byte [] _payload;

    public UDPPing(){_t1 = -1;}
    public UDPPing(int sz){
      assert sz <= AutoBuffer.MTU:"msg size does not fit into UDP";
      _payload = MemoryManager.malloc1(sz);
      Random rnd = new Random();
      for(int i = 0; i < _payload.length; ++i)
        _payload[i] = (byte)rnd.nextInt();
      _t1 = System.currentTimeMillis();
    }
    @Override
    public void compute2() { tryComplete();}

    @Override public synchronized UDPPing read(AutoBuffer ab){
      if(_done)return this;
      _done = true;
      _t2 = System.currentTimeMillis();
      _retries = ab.get4();
      byte [] bs = ab.getA1();
      _payload = bs;
      return this;
    }

    @Override public synchronized AutoBuffer write(AutoBuffer ab){
      if(!_done) ++_retries;
      ab.put4(_retries); // count the number of retries as number of serialization calls
      ab.putA1(_payload);
      return ab;
    }

    @Override public void copyOver(Freezable f){
      UDPPing u = (UDPPing)f;
      _retries = u._retries;
      _payload = u._payload;
    }
  }


  private static class TCPTester extends DTask<TCPTester> {
    public final int _srcId;
    public final int _tgtId;
    public final int _N;
    private final int[] _msgSzs;
    private transient RPC<UDPPing>[][] _pings;
    double[] _dropRates;


    int[] _droppedPackets;

    public TCPTester(H2ONode src, H2ONode tgt, int[] msgSzs, int ntests) {
      _srcId = src.index();
      _tgtId = tgt.index();
      _msgSzs = msgSzs;
      _N = ntests;
    }

    private transient boolean _done;

    private final void doTest() {
      _droppedPackets = new int[_N];
      Arrays.fill(_droppedPackets, -1);
      _pings = new RPC[_msgSzs.length][_N];
//      addToPendingCount(_msgSzs.length*_N - 1);
      for (int i = 0; i < _msgSzs.length; ++i)
        for (int j = 0; j < _N; ++j)  // instead of synchronization, just wait for predetermined amount of time
          _pings[i][j] = new RPC(H2O.CLOUD._memary[_tgtId], new UDPPing(_msgSzs[i]))/*.addCompleter(this)*/.call();
      try {
        Thread.sleep(5000);
      } catch (InterruptedException e) {
      }
      // if not done yet, finish no matter what (racy but we don't care here - only a debug tool, does not have to be precise)
//      setPendingCount(0);

    }

    @Override
    public synchronized void onCompletion(CountedCompleter caller) {
      if (!_done) { // only one completion
        _done = true;
        _dropRates = MemoryManager.malloc8d(_msgSzs.length);
        // compute the drop rates
        for (int i = 0; i < _msgSzs.length; ++i) {
          double sum = 0;
          for (int j = 0; j < _N; ++j) {
            RPC<UDPPing> rpc = _pings[i][j];
            sum += (rpc._dt._retries == -1 ? Double.POSITIVE_INFINITY : rpc._dt._retries);
          }
          _dropRates[i] = 1 - _N / (_N + sum);
        }
      }
    }

    @Override
    public void compute2() {

    }
  }

  private static class UDPDropTester extends DTask<UDPDropTester> {
    public final int _srcId;
    public final int _tgtId;
    public final int _N;
    private final int [] _msgSzs;
    private transient RPC<UDPPing> [][] _pings;
    double [] _dropRates;


    int [] _droppedPackets;

    public UDPDropTester(H2ONode src, H2ONode tgt, int [] msgSzs, int ntests){
      _srcId = src.index();
      _tgtId = tgt.index();
      _msgSzs = msgSzs;
      _N = ntests;
    }
    private transient boolean _done;
    private final void doTest(){
      _droppedPackets = new int[_N];
      Arrays.fill(_droppedPackets,-1);
      _pings = new RPC[_msgSzs.length][_N];
//      addToPendingCount(_msgSzs.length*_N - 1);
      for(int i = 0; i < _msgSzs.length; ++i)
        for(int j = 0; j < _N; ++j)  // instead of synchronization, just wait for predetermined amount of time
          _pings[i][j] = new RPC(H2O.CLOUD._memary[_tgtId],new UDPPing(_msgSzs[i]))/*.addCompleter(this)*/.call();
      try { Thread.sleep(5000); } catch (InterruptedException e) {}
      // if not done yet, finish no matter what (racy but we don't care here - only a debug tool, does not have to be precise)
//      setPendingCount(0);

    }

    @Override public synchronized void onCompletion(CountedCompleter caller){
      if(!_done){ // only one completion
        _done = true;
        _dropRates = MemoryManager.malloc8d(_msgSzs.length);
        // compute the drop rates
        for(int i = 0; i < _msgSzs.length; ++i) {
          double sum = 0;
          for (int j = 0; j < _N; ++j) {
            RPC<UDPPing> rpc = _pings[i][j];
            sum += (rpc._dt._retries == -1 ? Double.POSITIVE_INFINITY : rpc._dt._retries);
          }
          _dropRates[i] = 1 - _N/(_N+sum);
        }
      }
    }
    @Override
    public void compute2() {
      if(_srcId == H2O.SELF.index()) {
        doTest();
        tryComplete();
      } else {
        _done = true;
        final UDPDropTester t = (UDPDropTester) clone();
        new RPC(H2O.CLOUD._memary[_srcId], t).addCompleter(new H2OCountedCompleter(this) {
          @Override
          public void compute2() {
          }

          @Override
          public void onCompletion(CountedCompleter cc) {
            copyOver(t);
          }
        }).call();
      }
    }
  }
  private static class UDPDropMatrix extends Iced {
    static final int API_WEAVER=1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

    @API(help="message size")
    public final int messageSz;
    @API(help="meassured drop rates")
    public final double [][] dropRates;

    public UDPDropMatrix(int msgSz, double [][] dropRates){
      messageSz = msgSz;
      this.dropRates = dropRates;
    }
    @Override
    public String toString(){
      return " drop rates at " + messageSz + " bytes\n" + Utils.pprint(dropRates);
    }
  }
  @Override protected void execImpl() {
    logStart();
    Log.debug("NetworkTester testing udp drops");
    final UDPDropTester [] dropTests = new UDPDropTester[H2O.CLOUD.size()*H2O.CLOUD.size()-H2O.CLOUD.size()];
    H2O.submitTask(new H2OCountedCompleter() {
      @Override
      public void compute2() {
        int k = 0;
        for(int i = 0; i < H2O.CLOUD.size(); ++i)
          for(int j = 0; j < H2O.CLOUD.size(); ++j){
            if(i == j) continue;
            dropTests[k++] = new UDPDropTester(H2O.CLOUD._memary[i],H2O.CLOUD._memary[j],msg_sizes,10);
          }
        ForkJoinTask.invokeAll(dropTests);
        tryComplete();
      }
    }).join();
    dropRates = new UDPDropMatrix[msg_sizes.length];

    for(int m = 0; m < msg_sizes.length; ++m){
      double [][] ds = new double[H2O.CLOUD.size()][H2O.CLOUD.size()];
      int k = 0;
      for(int i = 0; i < H2O.CLOUD.size(); ++i)
        for(int j = 0; j < H2O.CLOUD.size(); ++j){
          if(i == j) continue;
          ds[i][j] = dropTests[k++]._dropRates[m];
        }
      dropRates[m] = new UDPDropMatrix(msg_sizes[m],ds);
    }
    Log.debug("Network test udp drop rates: ");
    for(UDPDropMatrix m:dropRates)
      Log.debug(m.toString());
    // now do the tcp bandwith test

    // print out
  }



  @Override
  public boolean toHTML(StringBuilder sb) {
    try {
      DocGen.HTML.section(sb, "UDP Drop rates");
      for(int i = 0; i < msg_sizes.length; ++i){
        sb.append("<h4>" + "Message size = " + msg_sizes[i] + " bytes</h4>");
        sb.append("<div>");
        UDPDropMatrix d = dropRates[i];
        sb.append("<table class='table table-bordered table-condensed'>\n");
        sb.append("<tr>");
        sb.append("<th></th>");
        for(int j = 0 ; j < H2O.CLOUD.size(); ++j)
          sb.append("<th>" + j + "</th>");
        sb.append("</tr>\n");
        for(int j = 0 ; j < H2O.CLOUD.size(); ++j){
          sb.append("<tr><td>" + j + "</td>");
          for(int k = 0; k < d.dropRates[j].length; ++k){
            sb.append("<td>" + (int)(100*d.dropRates[j][k]) + "&#37;</td>");
          }
          sb.append("</tr>\n");
        }
        sb.append("</table>");
        sb.append("</div>");
      }
    } catch(Throwable t){
      t.printStackTrace();
    }
    return true;
  }

}
