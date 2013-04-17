package water;
import java.io.IOException;
import java.lang.reflect.Array;
import java.net.*;
import java.nio.*;
import java.nio.channels.*;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import water.api.Timeline;

/**
 * A ByteBuffer backed mixed Input/OutputStream class.
 *
 * Reads/writes empty/fill the ByteBuffer as needed.  When it is empty/full it
 * we go to the ByteChannel for more/less.  Because DirectByteBuffers are
 * expensive to make, we keep a few pooled.
 *
 * @author <a href="mailto:cliffc@0xdata.com"></a>
 */
public final class AutoBuffer {
  public static final int TCP_WRITE_ATTEMPTS = 2;
  // The direct ByteBuffer for schlorping data about
  public ByteBuffer _bb;

  // The ByteChannel for schlorping more data in or out.  Could be a
  // SocketChannel (for a TCP connection) or a FileChannel (spill-to-disk) or a
  // DatagramChannel (for a UDP connection).
  private ByteChannel _chan;

  // If we need a SocketChannel, raise the priority so we get the I/O over
  // with.  Do not want to have some TCP socket open, blocking the TCP channel
  // and then have the thread stalled out.  If we raise the priority - be sure
  // to lower it again.  Note this is for TCP channels ONLY, and only because
  // we are blocking another Node with I/O.
  private int _oldPrior = -1;
  // Count of concurrent TCP requests both incoming and outgoing
  public static final AtomicInteger TCPS = new AtomicInteger(0);

  // Where to send or receive data via TCP or UDP (choice made as we discover
  // how big the message is); used to lazily create a Channel.  If NULL, then
  // _chan should be a pre-existing Channel, such as a FileChannel.
  public final H2ONode _h2o;

  // TRUE for read-mode.  FALSE for write-mode.  Can be flipped for rapid turnaround.
  private boolean _read;

  // TRUE if this AutoBuffer has never advanced past the first "page" of data.
  // The UDP-flavor, port# and task fields are only valid until we read over
  // them when flipping the ByteBuffer to the next chunk of data.
  private boolean _firstPage;

  // Total size written out from 'new' to 'close'.  Only updated when actually
  // reading or writing data, or after close().  For profiling only.
  public int _size;
  // More profiling: start->close msec, plus nano's spent in blocking I/O
  // calls.  The difference between (close-start) and i/o msec is the time the
  // i/o thread spends doing other stuff (e.g. allocating Java objects or
  // (de)serializing).
  public long _time_start_ms, _time_close_ms, _time_io_ns;
  // I/O persistence flavor: Value.ICE, NFS, HDFS, S3, TCP
  public final byte _persist;

  // The assumed max UDP packetsize
  public static final int MTU = 1500-8/*UDP packet header size*/;

  // Incoming UDP request.  Make a read-mode AutoBuffer from the open Channel,
  // figure the originating H2ONode from the first few bytes read.
  public AutoBuffer( DatagramChannel sock ) throws IOException {
    _chan = null;
    _bb = bbMake();
    _read = true;               // Reading by default
    _firstPage = true;
    // Read a packet; can get H2ONode from 'sad'?
    Inet4Address addr = null;
    SocketAddress sad = sock.receive(_bb);
    if( sad instanceof InetSocketAddress ) {
      InetAddress address = ((InetSocketAddress) sad).getAddress();
      if( address instanceof Inet4Address ) {
        addr = (Inet4Address) address;
      }
    }
    _bb.flip();                 // Set limit=amount read, and position==0

    if( addr == null ) throw new Error("Unhandled socket type: " + sad);
    // Read Inet from socket, port from the stream, figure out H2ONode
    _h2o = H2ONode.intern(addr, getPort());
    _firstPage = true;
    assert _h2o != null;
    _persist = 0;               // No persistance
  }

  // Incoming TCP request.  Make a read-mode AutoBuffer from the open Channel,
  // figure the originating H2ONode from the first few bytes read.
  public AutoBuffer( SocketChannel sock ) throws IOException {
    TCPS.incrementAndGet();
    _chan = sock;
    raisePriority();            // Make TCP priority high
    _bb = bbMake();
    _bb.flip();
    _read = true;               // Reading by default
    _firstPage = true;
    // Read Inet from socket, port from the stream, figure out H2ONode
    _h2o = H2ONode.intern(sock.socket().getInetAddress(), getPort());
    _firstPage = true;          // Yes, must reset this.
    assert _h2o != null && _h2o != H2O.SELF;
    _time_start_ms = System.currentTimeMillis();
    _persist = Value.TCP;
  }

  // Make an AutoBuffer to write to an H2ONode.  Requests for full buffer will
  // open a TCP socket and roll through writing to the target.  Smaller
  // requests will send via UDP.
  public AutoBuffer( H2ONode h2o ) {
    _bb = bbMake();
    _chan = null;               // Channel made lazily only if we write alot
    _h2o = h2o;
    _read = false;              // Writing by default
    _firstPage = true;          // Filling first page
    assert _h2o != null;
    _time_start_ms = System.currentTimeMillis();
    _persist = Value.TCP;
  }

  // Spill-to/from-disk request.
  public AutoBuffer( FileChannel fc, boolean read, byte persist ) {
    _bb = bbMake();
    _chan = fc;                 // Write to read/write
    _h2o = null;                // File Channels never have an _h2o
    _read = read;               // Mostly assert reading vs writing
    if( read ) _bb.flip();
    _time_start_ms = System.currentTimeMillis();
    _persist = persist;         // One of Value.ICE, NFS, S3, HDFS
  }

  // Read from UDP multicast.  Same as the byte[]-read variant, except there is an H2O.
  public AutoBuffer( DatagramPacket pack ) {
    _bb = ByteBuffer.wrap(pack.getData(), 0, pack.getLength()).order(ByteOrder.nativeOrder());
    _bb.position(0);
    _read = true;
    _firstPage = true;
    _chan = null;
    _h2o = H2ONode.intern(pack.getAddress(), getPort());
    _persist = 0;               // No persistance
  }

  /** Read from a fixed byte[]; should not be closed. */
  public AutoBuffer( byte[] buf ) { this(buf,0); }
  /** Read from a fixed byte[]; should not be closed. */
  public AutoBuffer( byte[] buf, int off ) {
    _bb = ByteBuffer.wrap(buf).order(ByteOrder.nativeOrder());
    _bb.position(off);
    _chan = null;
    _h2o = null;
    _read = true;
    _firstPage = true;
    _persist = 0;               // No persistance
  }

  /**  Write to an ever-expanding byte[].  Instead of calling {@link #close()},
   *  call {@link #buf()} to retrieve the final byte[].
   */
  public AutoBuffer( ) {
    _bb = ByteBuffer.wrap(new byte[16]).order(ByteOrder.nativeOrder());
    _chan = null;
    _h2o = null;
    _read = false;
    _firstPage = true;
    _persist = 0;               // No persistance
  }

  /** Write to a known sized byte[].  Instead of calling close(), call
   * {@link #bufClose()} to retrieve the final byte[].
   */
  public AutoBuffer( int len ) {
    _bb = ByteBuffer.wrap(MemoryManager.malloc1(len)).order(ByteOrder.nativeOrder());
    _chan = null;
    _h2o = null;
    _read = false;
    _firstPage = true;
    _persist = 0;               // No persistance
  }

  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("[AB ").append(_read ? "read " : "write ");
    sb.append(_firstPage?"first ":"2nd ").append(_h2o);
    sb.append(" ").append(Value.nameOfPersist(_persist));
    sb.append(" 0 <= ").append(_bb.position()).append(" <= ").append(_bb.limit());
    sb.append(" <= ").append(_bb.capacity());
    return sb.append("]").toString();
  }

  // Fetch a DBB from an object pool... they are fairly expensive to make
  // because a native call is required to get the backing memory.  I've
  // included BB count tracking code to help track leaks.  As of 12/17/2012 the
  // leaks are under control, but figure this may happen again so keeping these
  // counters around.
  private static final boolean DEBUG = Boolean.getBoolean("h2o.find-ByteBuffer-leaks");
  private static final AtomicInteger BBMAKE = new AtomicInteger(0);
  private static final AtomicInteger BBFREE = new AtomicInteger(0);
  private static final AtomicInteger BBCACHE= new AtomicInteger(0);
  private static final LinkedBlockingDeque<ByteBuffer> BBS = new LinkedBlockingDeque<ByteBuffer>();
  public static final int BBSIZE = 64*1024; // Bytebuffer "common big size"
  private static void bbstats( AtomicInteger ai ) {
    if( !DEBUG ) return;
    if( (ai.incrementAndGet()&511)==511 ) {
      System.err.println("BB make="+BBMAKE.get()+" free="+BBFREE.get()+" cache="+BBCACHE.get()+" size="+BBS.size());
    }
  }

  private static final ByteBuffer bbMake() {
    ByteBuffer bb = null;
    try { bb = BBS.pollFirst(0,TimeUnit.SECONDS); }
    catch( InterruptedException ie ) { throw new Error(ie); }
    if( bb != null ) {
      bbstats(BBCACHE);
      return bb;
    }
    bbstats(BBMAKE);
    return ByteBuffer.allocateDirect(BBSIZE).order(ByteOrder.nativeOrder());
  }
  private final int bbFree() {
    if( _bb.isDirect() ) {
      bbstats(BBFREE);
      _bb.clear();
      BBS.offerFirst(_bb);
    }
    _bb = null;
    return 0;                   // Flow-coding
  }


  // For reads, just assert all was read and close and release resources.
  // (release ByteBuffer back to the common pool).  For writes, force any final
  // bytes out.  If the write is to an H2ONode and is short, send via UDP.
  // AutoBuffer close calls order; i.e. a reader close() will block until the
  // writer does a close().
  public final int close() {
    assert _h2o != null || _chan != null;      // Byte-array backed should not be closed
    try {
      if( _chan == null ) {     // No channel?
        if( _read ) return bbFree();
        // For small-packet write, send via UDP.  Since nothing is sent until
        // now, this close() call trivially orders - since the reader will not
        // even start (much less close()) until this packet is sent.
        if( _bb.position() < MTU ) return udpSend();
      }
      // Force AutoBuffer 'close' calls to order; i.e. block readers until
      // writers do a 'close' - by writing 1 more byte in the close-call which
      // the reader will have to wait for.
      if( _h2o != null ) {      // TCP connection?
        if( _read ) {           // Reader?
          int x = get1();       // Read 1 more byte
          assert x == 0xab : "AB.close instead of 0xab sentinel got "+x+", "+this;
        } else {                // Writer?
          put1(0xab);           // Write one-more byte
        }
      }
      if( !_read ) sendPartial(); // Finish partial writes
      _chan.close();
      _time_close_ms = System.currentTimeMillis();
      TimeLine.record_IOclose(this,_persist); // Profile TCP connections
    } catch( IOException e ) {  // Dunno how to handle so crash-n-burn
      throw new RuntimeException(e);
    } finally {
      restorePriority();        // And if we raised priority, lower it back
      if( _chan instanceof SocketChannel )
        TCPS.decrementAndGet();
    }
    return bbFree();
  }

  public void drainClose() {
    try {
      while( _chan.read(_bb) != -1 )
        _bb.clear();
      _chan.close();
      restorePriority();        // And if we raised priority, lower it back
      TCPS.decrementAndGet();
      bbFree();
    } catch( IOException e ) {  // Dunno how to handle so crash-n-burn
      throw new RuntimeException(e);
    }
  }

  // Need a sock for a big read or write operation
  private void tcpOpen() throws IOException {
    assert _firstPage && _bb.limit() >= 1+2+4; // At least something written
    assert _chan == null;

    SocketChannel sock;
    while(true) {             // Loop, in case we get socket open problems
      IOException ex = null;
      try {
        // We expect the socket open to be fast, but if the receiver is very
        // overwhelmed he might not respond for a long long time.  In this
        // case we simply keep retrying (after a sleep period to let the
        // receiver catch up).
        sock = SocketChannel.open();
        sock.socket().setSendBufferSize(BBSIZE);
        sock.connect( _h2o._key );
        //sock = SocketChannel.open( _h2o._key );
        break;
      } // Explicitly ignore the following exceptions but fail on the rest
      catch (ConnectException e)       { ex = e; }
      catch (SocketTimeoutException e) { ex = e; }
      catch (IOException e)            { ex = e; }
      finally {
        if( ex != null ) {
          H2O.ignore(ex, "[h2o,Autobuffer] TCP open problem, waiting and retrying...", false);
          try { Thread.sleep(500); } catch (InterruptedException ie) {}
        }
      }
    }

    assert sock.isConnected();   // Supposed to be a blocking channel
    assert sock.isOpen();        // Supposed to be an open channel
    _chan = sock;
    TCPS.incrementAndGet();
    raisePriority();
  }

  // True if we opened a TCP channel, or will open one to close-and-send
  boolean hasTCP() { return _chan != null || (_bb != null && _bb.position() >= MTU); }

  // True if we are in read-mode
  boolean readMode() { return _read; }
  // Size in bytes sent, after a close()
  int size() { return _size; }

  // Available bytes in this buffer to read
  public int remaining() { return _bb.remaining(); }
  public int position () { return _bb.position (); }
  public void position(int pos) { _bb.position(pos); }
  public int limit() { return _bb.limit(); }

  public void positionWithResize(int value) {
    putSp(value - position());
    position(value);
  }

  // Return byte[] from a writable AutoBuffer
  public final byte[] buf() {
    assert _h2o==null && _chan==null && _read==false && !_bb.isDirect();
    return MemoryManager.arrayCopyOfRange(_bb.array(), _bb.arrayOffset(), _bb.position());
  }
  public final byte[] bufClose() {
    assert eof();
    byte[] res = _bb.array();
    bbFree();
    return res;
  }
  public final boolean eof() {
    assert _h2o==null && _chan==null;
    return _bb.position()==_bb.limit();
  }

  // For TCP sockets ONLY, raise the thread priority.  We assume we are
  // blocking other Nodes with our network I/O, so try to get the I/O
  // over with.
  private void raisePriority() {
    if(_oldPrior == -1){
      assert _chan instanceof SocketChannel && _oldPrior == -1;
      _oldPrior = Thread.currentThread().getPriority();
      Thread.currentThread().setPriority(Thread.MAX_PRIORITY-1);
    }
  }

  private void restorePriority() {
    if( _oldPrior == -1 ) return;
    Thread.currentThread().setPriority(_oldPrior);
    _oldPrior = -1;
  }

  // Send via UDP socket.  Unlike eg TCP sockets, we only need one for sending
  // so we keep a global one.  Also, we do not close it when done, and we do
  // not connect it up-front to a target - but send the entire packet right now.
  private int udpSend() throws IOException {
    assert _chan == null;
    TimeLine.record_send(this,false);
    _size += _bb.position();
    _bb.flip();                 // Flip for sending
    if( _h2o==H2O.SELF ) {      // SELF-send is the multi-cast signal
      H2O.multicast(_bb);
    } else {                    // Else single-cast send
      H2O.CLOUD_DGRAM.send(_bb, _h2o._key);
    }
    return bbFree();
  }

  // Flip to write-mode
  public AutoBuffer clearForWriting() {
    assert _read == true;
    _read = false;
    _bb.clear();
    _firstPage = true;
    return this;
  }
  // Flip to read-mode
  public AutoBuffer flipForReading() {
    assert _read == false;
    _read = true;
    _bb.flip();
    _firstPage = true;
    return this;
  }


  /** Ensure the buffer has space for sz more bytes */
  private ByteBuffer getSp( int sz ) { return sz > _bb.remaining() ? getImpl(sz) : _bb; }

  /** Ensure buffer has at least sz bytes in it.
   * - Also, set position just past this limit for future reading. */
  private ByteBuffer getSz(int sz) {
    assert _firstPage : "getSz() is only valid for early UDP bytes";
    if( sz > _bb.limit() ) getImpl(sz);
    _bb.position(sz);
    return _bb;
  }

  private ByteBuffer getImpl( int sz ) {
    assert _read : "Reading from a buffer in write mode";
    assert _chan != null : "Read to much data from a byte[] backed buffer";
    _bb.compact();            // Move remaining unread bytes to start of buffer; prep for reading
    // Its got to fit or we asked for too much
    assert _bb.position()+sz <= _bb.capacity() : "("+_bb.position()+"+"+sz+" <= "+_bb.capacity()+")";
    long ns = System.nanoTime();
    while( _bb.position() < sz ) { // Read until we got enuf
      try {
        int res = _chan.read(_bb); // Read more
        // Readers are supposed to be strongly typed and read the exact expected bytes
        if( res == -1 ) throw new RuntimeException("EOF while reading "+sz+" bytes");
        if( res ==  0 ) throw new RuntimeException("Reading zero bytes - so no progress?");
      } catch( IOException e ) {  // Dunno how to handle so crash-n-burn
        throw new RuntimeException(e);
      }
    }
    _time_io_ns += (System.nanoTime()-ns);
    _size += _bb.position();    // What we read
    _bb.flip();                 // Prep for handing out bytes
    _firstPage = false;         // First page of data is gone gone gone
    return _bb;
  }

  /** Put as needed to keep from overflowing the ByteBuffer. */
  private ByteBuffer putSp( int sz ) {
    assert !_read;
    if( sz <= _bb.remaining() ) return _bb;
    return sendPartial();
  }
  // Do something with partial results, because the ByteBuffer is full.
  // If we are byte[] backed, double the backing array size.
  // If we are doing I/O, ship the bytes we have now and flip the ByteBuffer.
  private ByteBuffer sendPartial() {
    // Writing into an expanding byte[]?
    if( _h2o==null && _chan == null ) {
      // This is a byte[] backed buffer; expand the backing byte[].
      byte[] ary = _bb.array();
      int newlen = ary.length<<1; // New size is 2x old size
      int oldpos = _bb.position();
      _bb = ByteBuffer.wrap(MemoryManager.arrayCopyOfRange(ary,0,newlen),oldpos,newlen-oldpos)
        .order(ByteOrder.nativeOrder());
      return _bb;
    }
    // Doing I/O with the full ByteBuffer - ship partial results
    _size += _bb.position();
    if( _chan == null )
      TimeLine.record_send(this,true);
    _bb.flip(); // Prep for writing.
    _bb.mark();
    try{
      if( _chan == null)
        tcpOpen(); // This is a big operation.  Open a TCP socket as-needed.
      long ns = System.nanoTime();
      while( _bb.hasRemaining() )
        _chan.write(_bb);
      _time_io_ns += (System.nanoTime()-ns);
    } catch( IOException e ) {   // Can't open the connection, try again later
      System.err.println("TCP Open/Write failed: " + e.getMessage()+" talking to "+_h2o);
      throw new Error(e);
    }
    if( _bb.capacity() < 16*1024 ) _bb = bbMake();
    _firstPage = false;
    _bb.clear();
    return _bb;
  }

  public int peek1() {
    if (eof())
      return 0;
    getSp(1);
    return get1(position());
  }
  public String getStr(int off, int len) {
    return new String(_bb.array(), _bb.arrayOffset()+off, len);
  }

  // -----------------------------------------------
  // Utility functions to get various Java primitives
  public boolean getZ() { return get1()!=0; }
  public int    get1 () { return getSp(1).get      ()&0xFF; }
  public char   get2 () { return getSp(2).getChar  ();      }
  public int    get4 () { return getSp(4).getInt   ();      }
  public float  get4f() { return getSp(4).getFloat ();      }
  public long   get8 () { return getSp(8).getLong  ();      }
  public double get8d() { return getSp(8).getDouble();      }


  public int get3() {
    return (0xff & get1()) << 0 |
           (0xff & get1()) << 8 |
           (0xff & get1()) << 16;
  }

  public AutoBuffer put3( int x ) {
    assert (-1<<24) <= x && x < (1<<24);
    return put1((x >> 0)&0xFF).put1((x >> 8)&0xFF).put1(x >> 16);
  }


  public int    get1 (int off) { return _bb.get (off)&0xFF; }
  public char   get2 (int off) { return _bb.getChar  (off); }
  public int    get4 (int off) { return _bb.getInt   (off); }
  public float  get4f(int off) { return _bb.getFloat (off); }
  public long   get8 (int off) { return _bb.getLong  (off); }
  public double get8d(int off) { return _bb.getDouble(off); }

  public AutoBuffer put1 (int off, int    v) { _bb.put      (off, (byte)(v&0xFF)); return this; }
  public AutoBuffer put2 (int off, char   v) { _bb.putChar  (off, v);              return this; }
  public AutoBuffer put2 (int off, short  v) { _bb.putShort (off, v);              return this; }
  public AutoBuffer put4 (int off, int    v) { _bb.putInt   (off, v);              return this; }
  public AutoBuffer put4f(int off, float  v) { _bb.putFloat (off, v);              return this; }
  public AutoBuffer put8 (int off, long   v) { _bb.putLong  (off, v);              return this; }
  public AutoBuffer put8d(int off, double v) { _bb.putDouble(off, v);              return this; }

  public AutoBuffer putZ (boolean b){ return put1(b?1:0); }
  public AutoBuffer put1 (   int b) { assert b >= -128 && b <= 255 : ""+b+" is not a byte";
                                      putSp(1).put((byte)b); return this; }
  public AutoBuffer put2 (  char c) { putSp(2).putChar  (c); return this; }
  public AutoBuffer put2 ( short s) { putSp(2).putShort (s); return this; }
  public AutoBuffer put4 (   int i) { putSp(4).putInt   (i); return this; }
  public AutoBuffer put4f( float f) { putSp(4).putFloat (f); return this; }
  public AutoBuffer put8 (  long l) { putSp(8).putLong  (l); return this; }
  public AutoBuffer put8d(double d) { putSp(8).putDouble(d); return this; }

  public AutoBuffer put(Freezable f) {
    if( f == null ) return put2(TypeMap.NULL);
    put2((short) f.frozenType());
    return f.write(this);
  }
  public AutoBuffer put(Iced f) {
    if( f == null ) return put2(TypeMap.NULL);
    put2((short) f.frozenType());
    return f.write(this);
  }
  public AutoBuffer putA(Iced[] fs) {
    if( fs == null ) return put4(-1);
    put4(fs.length);
    for( Iced f : fs ) put(f);
    return this;
  }
  public AutoBuffer putAA(Iced[][] fs) {
    if( fs == null ) return put4(-1);
    put4(fs.length);
    for( Iced[] f : fs ) putA(f);
    return this;
  }

  public <T extends Freezable> T get(Class<T> t) {
    short id = (short)get2();
    if( id == TypeMap.NULL ) return null;
    return TypeMap.newFreezable(id).read(this);
  }
  public <T extends Iced> T get() {
    short id = (short)get2();
    if( id == TypeMap.NULL ) return null;
    return TypeMap.newInstance(id).read(this);
  }
  public <T extends Iced> T[] getA(Class<T> tc) {
    int len = get4(); if( len == -1 ) return null;
    T[] ts = (T[]) Array.newInstance(tc, len);
    for( int i = 0; i < len; ++i ) ts[i] = get();
    return ts;
  }
  public <T extends Iced> T[][] getAA(Class<T> tc) {
    int len = get4(); if( len == -1 ) return null;
    Class<T[]> tcA = (Class<T[]>) Array.newInstance(tc, 0).getClass();
    T[][] ts = (T[][]) Array.newInstance(tcA, len);
    for( int i = 0; i < len; ++i ) ts[i] = getA(tc);
    return ts;
  }

  public AutoBuffer putAStr(String[] fs)    {
    if( fs == null ) return put4(-1);
    put4(fs.length);
    for( String s : fs ) putStr(s);
    return this;
  }
  public String[] getAStr() {
    int len = get4(); if( len == -1 ) return null;
    String[] ts = new String[len];
    for( int i = 0; i < len; ++i ) ts[i] = getStr();
    return ts;
  }

  // Read the smaller of _bb.remaining() and len into buf.
  // Return bytes read, which could be zero.
  public int read( byte[] buf, int off, int len ) {
    int sz = Math.min(_bb.remaining(),len);
    _bb.get(buf,off,sz);
    return sz;
  }


  // -----------------------------------------------
  // Utility functions to handle common UDP packet tasks.
  // Get the 1st control byte
  public int  getCtrl( ) { return getSz(1).get(0)&0xFF; }
  // Get the port in next 2 bytes
  public int  getPort( ) { return getSz(1+2).getChar(1); }
  // Get the task# in the next 4 bytes
  public int  getTask( ) { return getSz(1+2+4).getInt(1+2); }
  // Get the flag in the next 1 byte
  public int  getFlag( ) { return getSz(1+2+4+1).get(1+2+4); }

  // Set the ctrl, port, task.  Ready to write more bytes afterwards
  public AutoBuffer putUdp (UDP.udp type) {
    assert _bb.position()==0;
    putSp(1+2);
    _bb.put    ((byte)type.ordinal());
    _bb.putChar((char)H2O.UDP_PORT  ); // Outgoing port is always the sender's (me) port
    assert _bb.position()==1+2;
    return this;
  }

  public AutoBuffer putTask(UDP.udp type, int tasknum) {
    return putUdp(type).put4(tasknum);
  }
  public AutoBuffer putTask(int ctrl, int tasknum) {
    assert _bb.position()==0;
    putSp(1+2+4);
    _bb.put((byte)ctrl).putChar((char)H2O.UDP_PORT).putInt(tasknum);
    return this;
  }

  // -----------------------------------------------
  // Utility functions to read & write arrays
  public byte[] getA1( ) {
    int len = get4();
    assert len < 10000000 : "getA1 size=0x"+Integer.toHexString(len);
    return len == -1 ? null : getA1(len);
  }
  public byte[] getA1( int len ) {
    byte[] buf = MemoryManager.malloc1(len);
    int sofar = 0;
    while( sofar < len ) {
      int more = Math.min(_bb.remaining(), len - sofar);
      _bb.get(buf, sofar, more);
      sofar += more;
      if( sofar < len ) getSp(Math.min(_bb.capacity(), len-sofar));
    }
    return buf;
  }

  public short[] getA2( ) {
    int len = get4(); if( len == -1 ) return null;
    short[] buf = new short[len];
    int sofar = 0;
    while( sofar < buf.length ) {
      ShortBuffer as = _bb.asShortBuffer();
      int more = Math.min(as.remaining(), len - sofar);
      as.get(buf, sofar, more);
      sofar += more;
      _bb.position(_bb.position() + as.position()*2);
      if( sofar < len ) getSp(Math.min(_bb.capacity()-1, (len-sofar)*2));
    }
    return buf;
  }

  public int[] getA4( ) {
    int len = get4(); if( len == -1 ) return null;
    int[] buf = new int[len];
    int sofar = 0;
    while( sofar < buf.length ) {
      IntBuffer as = _bb.asIntBuffer();
      int more = Math.min(as.remaining(), len - sofar);
      as.get(buf, sofar, more);
      sofar += more;
      _bb.position(_bb.position() + as.position()*4);
      if( sofar < len ) getSp(Math.min(_bb.capacity()-3, (len-sofar)*4));
    }
    return buf;
  }
  public float[] getA4f( ) {
    int len = get4(); if( len == -1 ) return null;
    float[] buf = new float[len];
    int sofar = 0;
    while( sofar < buf.length ) {
      FloatBuffer as = _bb.asFloatBuffer();
      int more = Math.min(as.remaining(), len - sofar);
      as.get(buf, sofar, more);
      sofar += more;
      _bb.position(_bb.position() + as.position()*4);
      if( sofar < len ) getSp(Math.min(_bb.capacity()-3, (len-sofar)*4));
    }
    return buf;
  }
  public long[] getA8( ) {
    int len = get4(); if( len == -1 ) return null;
    long[] buf = new long[len];
    int sofar = 0;
    while( sofar < buf.length ) {
      LongBuffer as = _bb.asLongBuffer();
      int more = Math.min(as.remaining(), len - sofar);
      as.get(buf, sofar, more);
      sofar += more;
      _bb.position(_bb.position() + as.position()*8);
      if( sofar < len ) getSp(Math.min(_bb.capacity()-7, (len-sofar)*8));
    }
    return buf;
  }
  public double[] getA8d( ) {
    int len = get4(); if( len == -1 ) return null;
    double[] buf = new double[len];
    int sofar = 0;
    while( sofar < len ) {
      DoubleBuffer as = _bb.asDoubleBuffer();
      int more = Math.min(as.remaining(), len - sofar);
      as.get(buf, sofar, more);
      sofar += more;
      _bb.position(_bb.position() + as.position()*8);
      if( sofar < len ) getSp(Math.min(_bb.capacity()-7, (len-sofar)*8));
    }
    return buf;
  }
  public byte[][] getAA1( ) {
    int len = get4();  if( len == -1 ) return null;
    byte[][] ary  = new byte[len][];
    for( int i=0; i<len; i++ ) ary[i] = getA1();
    return ary;
  }
  public int[][] getAA4( ) {
    int len = get4();  if( len == -1 ) return null;
    int[][] ary  = new int[len][];
    for( int i=0; i<len; i++ ) ary[i] = getA4();
    return ary;
  }
  public long[][] getAA8( ) {
    int len = get4();  if( len == -1 ) return null;
    long[][] ary  = new long[len][];
    for( int i=0; i<len; i++ ) ary[i] = getA8();
    return ary;
  }
  public double[][] getAA8d( ) {
    int len = get4();  if( len == -1 ) return null;
    double[][] ary  = new double[len][];
    for( int i=0; i<len; i++ ) ary[i] = getA8d();
    return ary;
  }

  public String getStr( ) {
    int len = get2();
    return len == 65535 ? null : new String(getA1(len));
  }

  public AutoBuffer putA1( byte[] ary ) {
    return ary == null ? put4(-1) : put4(ary.length).putA1(ary,ary.length);
  }
  public AutoBuffer putA1( byte[] ary, int length ) {
    int sofar = 0;
    while( sofar < length ) {
      int len = Math.min(length - sofar, _bb.remaining());
      _bb.put(ary, sofar, len);
      sofar += len;
      if( sofar < length ) sendPartial();
    }
    return this;
  }
  public AutoBuffer putA2( short[] ary ) {
    if( ary == null ) return put4(-1);
    put4(ary.length);
    int sofar = 0;
    while( sofar < ary.length ) {
      ShortBuffer sb = _bb.asShortBuffer();
      int len = Math.min(ary.length - sofar, sb.remaining());
      sb.put(ary, sofar, len);
      sofar += len;
      _bb.position(_bb.position() + sb.position()*2);
      if( sofar < ary.length ) sendPartial();
    }
    return this;
  }
  public AutoBuffer putA4( int[] ary ) {
    if( ary == null ) return put4(-1);
    put4(ary.length);
    int sofar = 0;
    while( sofar < ary.length ) {
      IntBuffer sb = _bb.asIntBuffer();
      int len = Math.min(ary.length - sofar, sb.remaining());
      sb.put(ary, sofar, len);
      sofar += len;
      _bb.position(_bb.position() + sb.position()*4);
      if( sofar < ary.length ) sendPartial();
    }
    return this;
  }
  public AutoBuffer putA8( long[] ary ) {
    if( ary == null ) return put4(-1);
    put4(ary.length);
    int sofar = 0;
    while( sofar < ary.length ) {
      LongBuffer sb = _bb.asLongBuffer();
      int len = Math.min(ary.length - sofar, sb.remaining());
      sb.put(ary, sofar, len);
      sofar += len;
      _bb.position(_bb.position() + sb.position()*8);
      if( sofar < ary.length ) sendPartial();
    }
    return this;
  }
  public AutoBuffer putA4f( float[] ary ) {
    if( ary == null ) return put4(-1);
    put4(ary.length);
    int sofar = 0;
    while( sofar < ary.length ) {
      FloatBuffer sb = _bb.asFloatBuffer();
      int len = Math.min(ary.length - sofar, sb.remaining());
      sb.put(ary, sofar, len);
      sofar += len;
      _bb.position(_bb.position() + sb.position()*4);
      if( sofar < ary.length ) sendPartial();
    }
    return this;
  }
  public AutoBuffer putA8d( double[] ary ) {
    if( ary == null ) return put4(-1);
    put4(ary.length);
    int sofar = 0;
    while( sofar < ary.length ) {
      DoubleBuffer sb = _bb.asDoubleBuffer();
      int len = Math.min(ary.length - sofar, sb.remaining());
      sb.put(ary, sofar, len);
      sofar += len;
      _bb.position(_bb.position() + sb.position()*8);
      if( sofar < ary.length ) sendPartial();
    }
    return this;
  }

  public AutoBuffer putAA1( byte[][] ary ) {
    if( ary == null ) return put4(-1);
    put4(ary.length);
    for( int i=0; i<ary.length; i++ ) putA1(ary[i]);
    return this;
  }
  public AutoBuffer putAA4( int[][] ary ) {
    if( ary == null ) return put4(-1);
    put4(ary.length);
    for( int i=0; i<ary.length; i++ ) putA4(ary[i]);
    return this;
  }
  public AutoBuffer putAA8( long[][] ary ) {
    if( ary == null ) return put4(-1);
    put4(ary.length);
    for( int i=0; i<ary.length; i++ ) putA8(ary[i]);
    return this;
  }
  public AutoBuffer putAA8d( double[][] ary ) {
    if( ary == null ) return put4(-1);
    put4(ary.length);
    for( int i=0; i<ary.length; i++ ) putA8d(ary[i]);
    return this;
  }
  // Put a String as 2bytes of length then string bytes (not chars!)
  public AutoBuffer putStr( String s ) {
    if( s==null ) return put2((char)65535);
    byte[] b = s.getBytes();
    return put2((char)b.length).putA1(b,b.length);
  }

  public AutoBuffer putEnum( Enum x ) {
    return put1(x==null ? -1 : x.ordinal());
  }

  public AutoBuffer copyArrayFrom(int offset, AutoBuffer ab, int abOff, int len) {
    byte[] dst = _bb.array();
    offset += _bb.arrayOffset();
    byte[] src = ab._bb.array();
    abOff += ab._bb.arrayOffset();
    System.arraycopy(src, abOff, dst, offset, len);
    _bb.position(_bb.position()+len); // Bump dest buffer offset
    return this;
  }

  public void shift(int source, int target, int length) {
    System.arraycopy(_bb.array(), source, _bb.array(), target, length);
  }
}
