package water.api;


import java.util.ArrayList;
import java.util.Arrays;

import water.*;
import water.fvec.Frame;
import water.util.Log;

import com.google.gson.*;

public class FrameSplit extends Request2 {

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.

  @API(help = "Source frame", required = true, filter = Default.class)
  public Frame source;

  @API(help = "Destination keys, comma separated", required = false, filter = Default.class)
  public String destination_keys; // Key holding final value after job is removed

  @API(help = "fraction per frame, comma separated", required = true, filter = Default.class)
  public String fractions;


  public static final String KEY_PREFIX = "__FRAME_SPLIT__";
  public static final Key makeKey() { return Key.make(KEY_PREFIX + Key.make());  }


  public FrameSplit(){ }


  @Override protected Response serve() {
    //TODO elh: once request2 properly works with string APIs, remove hack
    this.fractions = input("fractions");
    this.destination_keys = input("destination_keys");

    if( source == null || source.equals(""))
      return Response.error("source is required");
    if( fractions == null || fractions.length() == 0 )
      return Response.error("fractions must be set");

    final Frame fr = new Frame(source);

    ArrayList< Double > fs = new ArrayList< Double >();
    for( String s : fractions.split(",") ){
      try {
        fs.add( Double.parseDouble( s ) );
      } catch(NumberFormatException nfe){
        Response.error("invalid number format: " + s);
      }
    }
    double[] fractions = new double[ fs.size() ];
    for( int i=0; i < fractions.length; i++ )
      fractions[i] = fs.get(i);

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
    sb.append("framesplit: ");
    for( int i=0; i < fractions.length; i++ ){
      sb.append( fractions[ i ] + " -> ");
      sb.append( keys[ i ] );
      sb.append(";");
    }
    Log.info(sb.toString());




    Frame[] frames = fr.splitFrame(fractions);
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


}
