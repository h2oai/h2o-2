package water.api;

import dontweave.gson.*;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;
import water.*;
import water.util.Log;
import water.util.TimelineSnapshot;

public class IOStatus extends Request {
  private static final String HISTOGRAM = "histogram";
  public IOStatus() { _requestHelp = "Displays recent I/O activity."; }
  // Delta-time for histogram summaries, in seconds
  private static final int[] dts = new int[]{1,5,60,300};

  @Override public Response serve() {
    JsonObject response = new JsonObject();
    final long[][] snapshot = TimeLine.system_snapshot();
    final H2O cloud = TimeLine.CLOUD;
    final int csz = cloud.size();
    SimpleDateFormat sdf = new SimpleDateFormat("HH:mm:ss:SSS");
    // Histograms!  Per-8-i/o flavors (see Value.persist bits) per read/write per time-window
    long durs[][][][] = new long[csz][8][2][dts.length]; // Duration from open-to-close
    long blks[][][][] = new long[csz][8][2][dts.length]; // Blocked-for-i/o nanos 
    long sizs[][][][] = new long[csz][8][2][dts.length]; // Bytes moved
    int  cnts[][][][] = new int [csz][8][2][dts.length]; // Events in this bucket

    // Process all the timeline events
    JsonArray iops = new JsonArray();
    TimelineSnapshot events = new TimelineSnapshot(cloud, snapshot);
    long now = System.currentTimeMillis(); // Time 'now' just AFTER the snapshot
    for( TimelineSnapshot.Event event : events ) {
      int flavor = event.is_io();
//      if( flavor == -1 ) continue; //0 leads to problems below as nameOfPersist(0) == null
      if( flavor <= 0 ) continue;
      int nidx = event._nodeId;
      int rw = event.send_recv();// 1 for receive or read
      long ctms = event.ms();   // Close-time msec
      long dura = event.ms_io();// Duration in msec open-to-close
      long blkd = event.ns();   // Nano's in blocking i/o calls;
      long size = event.size_io(); // Bytes read/written
      // Collect histograms
      for( int i=0; i<dts.length; i++ ) {
        int dt = dts[i]*1000;   // Duration of this histogram bucket, in ms
        if( ctms+dt >= now ) {  // Ends within the bucket?
          durs[nidx][flavor][rw][i] += dura;
          blks[nidx][flavor][rw][i] += blkd;
          sizs[nidx][flavor][rw][i] += size;
          cnts[nidx][flavor][rw][i] ++;
        }
      }

      // Also dump the raw io ops
      JsonObject iop = new JsonObject();
      iop.addProperty("closeTime", sdf.format(new Date(ctms)));
      iop.addProperty(Constants.NODE,cloud._memary[nidx].toString());
      iop.addProperty("i_o",Value.nameOfPersist(flavor));
      iop.addProperty("r_w",rw==0?"write":"read");
      iop.addProperty("duration"+Constants.Suffixes.MILLIS,dura); // ms from open-to-close
      iop.addProperty("blocked"+Constants.Suffixes.MILLIS,TimeUnit.MILLISECONDS.convert(blkd,TimeUnit.NANOSECONDS)); // ns in blocking i/o calls
      iop.addProperty("size"+Constants.Suffixes.BYTES,size); // bytes read/written
      iops.add(iop);
    }

    // Dump out histograms
    JsonArray histo = new JsonArray();
    for( int n=0; n<csz; n++ ) {
      for( int i=0; i<8; i++ ) {
        for( int j=0; j<2; j++ ) {
          for( int k=0; k<dts.length; k++ ) {
            if( cnts[n][i][j][k] != 0 ) {
              JsonObject sum = new JsonObject();
              sum.addProperty("cloud_node_idx",n);
              sum.addProperty("i_o",Value.nameOfPersist(i));
              sum.addProperty("r_w",j==0?"write":"read");
              sum.addProperty("window",dts[k]);
              double dur = durs[n][i][j][k]/1e3; // Duration
              double blk = blks[n][i][j][k]/1e9; // Blocked
              double siz = sizs[n][i][j][k]*1.0;
              if( dur == 0.0 ) dur = blk;     // Round-off error sometimes; fix div-by-0
              sum.addProperty("effective"+Constants.Suffixes.BYTES_PER_SECOND, siz/dur);
              sum.addProperty("peak"     +Constants.Suffixes.BYTES_PER_SECOND, siz/blk);
              histo.add(sum);
            }
          }
        }
      }      
    }
    response.add(HISTOGRAM,histo);

    response.add("raw_iops",iops);
    Response r = Response.done(response);
    r.setBuilder(HISTOGRAM, new HistogramBuilder());
    return r;
  }

  private static class HistogramBuilder extends Builder {
    @Override public String build(Response response, JsonElement je, String contextName) {
      final H2O cloud = TimeLine.CLOUD;
      final int csz = cloud.size();
      // Painfully reverse the Json to a java array again
      long ebws[][][][] = new long[csz][8][2][dts.length]; // Duration from open-to-close
      long pbws[][][][] = new long[csz][8][2][dts.length]; // Duration from open-to-close
      boolean f[][][]= new boolean[csz][8][2];
      for (JsonElement e : je.getAsJsonArray() ) {
        JsonObject jo = e.getAsJsonObject();
        int nidx = jo.get("cloud_node_idx").getAsInt();
        // Convert flavor string to a flavor index
        String fs = jo.get("i_o").getAsString();
        int flavor;
        for( flavor=0; flavor<8; flavor++ )
          if( fs.equals(Value.nameOfPersist(flavor)) )
            break;
        assert flavor < 8;
        // Convert r/w string to 1/0
        int r_w = jo.get("r_w").getAsString().equals("write") ? 0 : 1;
        // Convert time-window value into time-window index
        int window, widx = jo.get("window").getAsInt();
        for( window=0; window < dts.length; window++ )
          if( dts[window] == widx )
            break;
        ebws[nidx][flavor][r_w][window] = jo.get("effective"+Constants.Suffixes.BYTES_PER_SECOND).getAsLong();
        pbws[nidx][flavor][r_w][window] = jo.get("peak"     +Constants.Suffixes.BYTES_PER_SECOND).getAsLong();
        f   [nidx][flavor][r_w] = true;
      }

      StringBuilder sb = new StringBuilder();
      for( int n=0; n<csz; n++ ) {
        sb.append("<h4>").append(cloud._memary[n]).append("</h4>");
        sb.append("<span style='display: inline-block;'>");
        sb.append("<table class='table table-striped table-bordered'>");
        // Header
        sb.append("<tr>");
        sb.append("<th>i/o</th><th>r/w</th>");
        for( int i=0; i<dts.length; i++ )
          sb.append("<th>").append(dts[i]).append("s </th>");
        sb.append("</tr>");
        // For all I/O flavors
        for( int flavor=0; flavor<8; flavor++ ) {
          if( !f[n][flavor][0] && !f[n][flavor][1] ) continue;
          int rows = 0; // Compute rows for either read or write or both
          if( f[n][flavor][0] ) rows++;
          if( f[n][flavor][1] ) rows++;
          sb.append("<tr>");
          sb.append("<td rowspan=\""+rows+"\"><h4>").append(Value.nameOfPersist(flavor)).append("</h4></td>");
          if( f[n][flavor][1] ) doRow(sb,ebws,pbws,n,flavor,1);
          if( f[n][flavor][0] ) doRow(sb,ebws,pbws,n,flavor,0);
          sb.append("</tr>");
        }
        sb.append("</table></span>");
      }

      return sb.toString();
    }

  }

  // Do a single row, all time-windows
  private static void doRow( StringBuilder sb, long[][][][] ebws, long[][][][] pbws, int nidx, int flavor, int r_w ) {
    sb.append("<td> eff/peak ").append(r_w==0?"write":"read").append("</td>");
    for( int i=0; i<dts.length; i++ ) {
      sb.append("<td>");
      long eff = ebws[nidx][flavor][r_w][i];
      long peak= pbws[nidx][flavor][r_w][i];
      if( eff > 0 || peak > 0 ) {
        int scale = Math.max(PrettyPrint.byteScale(eff),PrettyPrint.byteScale(peak));
        String s1 = PrettyPrint.bytes(eff,scale);
        String s2 = s1.substring(0,s1.length()-3); // Strip units off
        sb.append(s2).append(" / ").append(PrettyPrint.bytes(peak,scale)).append("/S");
      }
      sb.append("</td>");
    }
    sb.append("</tr>");
  }
}
