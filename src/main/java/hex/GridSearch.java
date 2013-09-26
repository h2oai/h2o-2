package hex;

import hex.gbm.GBM.GBMModel;

import java.util.ArrayList;
import java.util.UUID;

import water.*;
import water.api.*;

public class GridSearch extends Job {

  public Job[] jobs;

  public GridSearch() {
    super("", Key.make("__GridSearch_" + UUID.randomUUID().toString()));
  }

  @Override protected void run() {
    UKV.put(destination_key, this);
    for( Job job : jobs )
      job.startFJ();
  }

  @Override public float progress() {
    double d = 0.1;
    for( Job job : jobs )
      d += job.progress();
    return Math.min(1f, (float) (d / jobs.length));
  }

  @Override protected Response redirect() {
    String n = GridSearchProgress.class.getSimpleName();
    return new Response(Response.Status.redirect, this, -1, -1, n, "job", job_key, "dst_key", destination_key);
  }

  public static class GridSearchProgress extends Progress2 {

    @Override protected Response serve() {
      GridSearch grid = UKV.get(Key.make(dst_key.value()));
      if( grid != null ) {
        boolean done = true;
        for( int i = 0; i < grid.jobs.length; i++ )
          if( grid.jobs[i].running() )
            done = false;
        if( done )
          grid.remove();
      }
      return super.serve();
    }

    @Override public boolean toHTML(StringBuilder sb) {
      GridSearch grid = UKV.get(Key.make(dst_key.value()));
      if( grid != null ) {
        DocGen.HTML.arrayHead(sb);
        sb.append("<tr class='warning'>");
        ArrayList<Argument> args = grid.jobs[0].arguments();
        // Filter some keys to simplify UI
        args = (ArrayList<Argument>) args.clone();
        filter(args, "destination_key", "source", "cols", "response", "validation");
        for( int i = 0; i < args.size(); i++ )
          sb.append("<td><b>").append(args.get(i)._name).append("</b></td>");
        sb.append("<td><b>").append("run time (s)").append("</b></td>");
        sb.append("<td><b>").append("model key").append("</b></td>");
        sb.append("<td><b>").append("prediction error %").append("</b></td>");
        sb.append("<td><b>").append("precision & recall").append("</b></td>");
        sb.append("</tr>");

        for( int i = 0; i < grid.jobs.length; i++ ) {
          sb.append("<tr>");
          for( Argument a : args ) {
            try {
              sb.append("<td>").append(a._field.get(grid.jobs[i])).append("</td>");
            } catch( Exception e ) {
              throw new RuntimeException(e);
            }
          }
          sb.append("<td>").append((System.currentTimeMillis() - grid.jobs[i].start_time) / 1000).append("</td>");

          Object value = UKV.get(grid.jobs[i].destination_key);
          Model model = value instanceof Model ? (Model) value : null;
          String link = grid.jobs[i].destination_key.toString();
          if( model instanceof GBMModel )
            link = GBMModelView.link(link, grid.jobs[i].destination_key);
          else
            link = Inspect.link(link, grid.jobs[i].destination_key);
          sb.append("<td>").append(link).append("</td>");

          String pct = "", f1 = "";
          if( model != null ) {
            pct = String.format("%.2f", 100 * model.predictionError()) + "%";
            ConfusionMatrix cm = model.cm();
            if( cm != null && cm._arr.length == 2 )
              f1 = String.format("%.2f", cm.precisionAndRecall());
          }
          sb.append("<td><b>").append(pct).append("</b></td>");
          sb.append("<td><b>").append(f1).append("</b></td>");
          sb.append("</tr>");
        }
        DocGen.HTML.arrayTail(sb);
      }
      return true;
    }

    static void filter(ArrayList<Argument> args, String... names) {
      for( String name : names )
        for( int i = args.size() - 1; i >= 0; i-- )
          if( args.get(i)._name.equals(name) )
            args.remove(i);
    }

    @Override protected Response jobDone(final Job job, final String dst) {
      return new Response(Response.Status.done, this, 0, 0, null);
    }
  }
}
