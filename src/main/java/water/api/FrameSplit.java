package water.api;


import java.util.Random;

import water.*;
import water.fvec.*;
import water.util.Log;

import com.google.gson.*;

public class FrameSplit extends Request2 {

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Source frame", required = true, filter = Default.class)
  private Frame source;

  @API(help = "Destination keys, comma separated", required = false, filter = Default.class)
  private String destination_keys; // Key holding final value after job is removed

  @API(help = "Fraction per frame, comma separated", required = true, filter = Default.class)
  private double[] fractions;

  @API(help = "Random number seed (set to 0 to specify a time-based seed)", required = false, filter = Default.class)
  private long seed;

  private static final String KEY_PREFIX = "__FRAME_SPLIT__";
  private static Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }

  @Override protected Response serve() {
    if( source == null )
      return Response.error("source is required");

    Key[] keys = new Key[ fractions.length ];
    if( this.destination_keys == null || "".equals(this.destination_keys) ){
      for( int i=0; i<keys.length; i++ )
        keys[ i ] = makeKey();
    } else {
      String[] skeys = this.destination_keys.split(",");
      if( skeys.length != fractions.length )
        return Response.error("set as many keys as fractions");
      for( int i=0; i<keys.length; i++ )
        keys[i] = Key.make(skeys[i]);
    }

    StringBuilder sb = new StringBuilder();
    sb.append(String.format("framesplit: %s [%d,%d] to ", input("source"), source.numRows(), source.numCols()));
    for( int i=0; i < fractions.length; i++ ){
      sb.append( fractions[ i ] );
      sb.append( " -> ");
      sb.append( keys[ i ] );
      sb.append(";");
    }
    Log.info(sb.toString());

    Frame[] frames = splitFrame(source, fractions, seed);
    for( int f=0; f<frames.length; f++ )
      DKV.put( keys[f], frames[f]);

    Results r = new Results();
    r.fractions = fractions;
    String[] ks = new String[ keys.length ];
    long[] rows = new long[ keys.length ];
    for (int i=0; i<ks.length; i++){
      ks[i] = keys[ i ].toString();
      rows[i] = frames[ i ].numRows();
    }
    r.keys = ks;
    r.numRows = rows;

    Gson gson = new Gson();
    JsonParser parser = new JsonParser();
    JsonObject o = parser.parse( gson.toJson(r) ).getAsJsonObject();

    return Response.done(o);
  }

  /*
  public boolean toHTML( StringBuilder sb, Key[] keys, Frame[] frames, double[] fractions) {
    DocGen.HTML.section(sb,"Frames");
    DocGen.HTML.arrayHead(sb, new String[]{"frame", "fraction", "rows"});
    for( int i=0; i<keys.length; i++ ){
      sb.append( "<tr>");
        sb.append( "<td>" + keys[ i ] + "</td>");
        sb.append( "<td> " + fractions[ i ] + " </td> " );
        sb.append( "<td> " + frames[ i ].numRows() + " </td>" );
      sb.append( "</tr>");
    }

    return true;
  }
  */

  private static class Results {
    String[] keys;
    double[] fractions;
    long[] numRows;
  }


  /**
   * splits frame into desired fractions via a uniform random draw.  <b> DOES NOT </b> promise such a division, and for small numbers of rows,
   * you get what you get
   *
   * @param fractions  must sum to 1.0.  eg {0.8, 0.2} to get an 80/20 train/test split
   *
   * @param seed  random number seed. If set to 0, a time-based seed will be used.
   *
   * @return array of frames
   */
  public Frame[] splitFrame(Frame frame, double[] fractions, long seed){

    double[] splits = new double[fractions.length];
    double cumsum = 0.;
    for( int i=0; i<fractions.length; i++ ) {
      cumsum += fractions[i];
      splits[i] = cumsum;
    }

    splits[ splits.length - 1 ] = 1.01; // force row to be assigned somewhere, even if the fractions passed in are garbage

    FrameSplitter task = new FrameSplitter();
    Frame f = task.initHead(frame, splits, seed);
    task._fr = f;
    task.doAll(f);

    return task.finishHead();
  }

  /**
   * Split frame into desired fractions with a time-based seed (as if 0 was passed in method above).
   * @param frame input frame
   * @param fractions must sum to 1.0.  eg {0.8, 0.2} to get an 80/20 train/test split
   * @return array of frames
   */
  public Frame[] splitFrame(Frame frame, double[] fractions){
    return splitFrame(frame, fractions, 0);
  }


  /**
   * split a frame into multiple frames, with the data split as desired <br>
   * NB: this allocates fvecs; caller is responsible for remove-ing them <br>
   *<br>
   * TODO: allow perfect splitting instead of at-random, particularly for unit tests
   */
  protected static class FrameSplitter extends MRTask2<FrameSplitter> {
    int _num_columns;
    double[] _splits;
    long _seed;

    /**
     * must be called on headnode before doAll to perform setup
     */
    Frame initHead(Frame frame, double[] splits, long seed){
      _num_columns = frame.vecs().length;
      _splits = splits;
      _seed = seed;

      // elh: don't ask why this is necessary but it is
      for(int i=0; i < _num_columns; i++)
        frame.vecs()[i].isInt();

      Vec[] v = new Vec[_num_columns * (1 + _splits.length)];
      for( int i = 0; i < _num_columns; i++ )
        v[i] = frame.vecs()[i];
      Key keys[] = frame.anyVec().group().addVecs(_num_columns * _splits.length);
      for( int i = _num_columns; i < v.length; i++ )
        v[i] = new AppendableVec(keys[i - _num_columns]);

      String[] names = new String[_num_columns * (1 + _splits.length)];
      for( int copy = 0; copy < 1 + _splits.length; copy++ )
        System.arraycopy(frame._names, 0, names, copy * _num_columns, _num_columns);

       return new Frame(names, v);
    }

    /**
     * return the new frames; call on headnode after doAll
     */
    Frame[] finishHead() {
      Frame[] frames = new Frame[_splits.length];
      String[] names = new String[_num_columns];
      System.arraycopy(_fr.names(), 0, names, 0, _num_columns);

      for( int f = 0; f < _splits.length; f++ ) {
        Vec[] vecs = new Vec[_num_columns];
        for( int i = 0; i < _num_columns; i++ )
          vecs[i] = ((AppendableVec) _fr.vecs()[(f + 1) * _num_columns + i]).close(null);

        for( int column = 0; column < _num_columns; column++ )
          if( _fr.vecs()[column].isEnum() )
            vecs[column]._domain = _fr.vecs()[column]._domain;
        frames[f] = new Frame(names, vecs);
      }

      return frames;
    }


    @Override public void map(Chunk[] cs) {
      // Use time-based seed if seed was set to 0
      final Random random = (_seed != 0) ? new Random(_seed) : new Random();
//      Log.info(String.format("Map called with %d chunks operating on %d source cols; offset %d", cs.length, _num_columns, cs[0]._start));

      NewChunk[] new_chunks = new NewChunk[_num_columns * _splits.length];
      for( int i = 0; i < _num_columns * _splits.length; i++ )
        new_chunks[i] = (NewChunk) cs[_num_columns + i];

      Vec[] vecs = _fr.vecs();
//      StringBuilder sb = new StringBuilder();
//      sb.append("chunk " + cs[0]._start + ": ");
//      for(int i=0; i < _num_columns; i++)
//        sb.append(String.format("%d:%s, ", i, vecs[i].isInt()));
//      Log.info(sb.toString());
      for( int chunk_row = 0; chunk_row < cs[0]._len; chunk_row++ ) {
        double draw = random.nextDouble();
        int split = 0;
        while( draw > _splits[split] ) { split++; }

        for( int col = 0; col < _num_columns; col++ ) {
          if( vecs[col].isEnum() ) {
            if( !cs[col].isNA0(chunk_row) )
              new_chunks[split * _num_columns + col].addEnum((int) cs[col].at80(chunk_row));
            else
              new_chunks[split * _num_columns + col].addNA();

          } else if( vecs[col].isInt() ) {
            if( !cs[col].isNA0(chunk_row) )
              new_chunks[split * _num_columns + col].addNum(cs[col].at80(chunk_row), 0);
            else
              new_chunks[split * _num_columns + col].addNA();

          } else { // assume double; NaN == NA so should be able to just assign;
            new_chunks[split * _num_columns + col].addNum(cs[col].at0(chunk_row));
          }
        }
      }

//      Log.info("Map finished with some chunks; offset " + cs[0]._start);
    }
  }



}
