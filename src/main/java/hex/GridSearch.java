package hex;

import hex.KMeans2.KMeans2Model;
import hex.KMeans2.KMeans2ModelView;
import hex.NeuralNet.NeuralNetModel;
import hex.NeuralNet.NeuralNetProgress;
import hex.gbm.GBM.GBMModel;
import hex.drf.DRF.DRFModel;

import java.util.*;

import water.*;
import water.api.*;
import water.util.Utils;

public class GridSearch extends Job {
  public Job[] jobs;

  @Override protected Status exec() {
    UKV.put(destination_key, this);
    int max = jobs[0].gridParallelism();
    int head = 0, tail = 0;
    while( head < jobs.length && !cancelled() ) {
      if( tail - head < max && tail < jobs.length )
        jobs[tail++].fork();
      else {
        try {
          jobs[head++].get();
        } catch( Exception e ) {
          throw new RuntimeException(e);
        }
      }
    }
    return Status.Done;
  }

  @Override protected void onCancelled() {
    for( Job job : jobs )
      job.cancel();
  }

  @Override public float progress() {
    double d = 0.1;
    for( Job job : jobs )
      if(job.start_time > 0)
        d += job.progress();
    return Math.min(1f, (float) (d / jobs.length));
  }

  @Override public Response redirect() {
    String redirectName = new GridSearchProgress().href();
    return Response.redirect(this, redirectName, "job_key", job_key, "destination_key", destination_key);
  }

  public static class GridSearchProgress extends Progress2 {
    static final int API_WEAVER = 1;
    static public DocGen.FieldDoc[] DOC_FIELDS;

    @API(help = "Jobs")
    public Job[] jobs;

    @Override protected Response serve() {
      Response response = super.serve();
      if( destination_key != null ) {
        GridSearch grid = UKV.get(destination_key);
        if( grid != null )
          jobs = grid.jobs;
      }
      return response;
    }

    @Override public boolean toHTML(StringBuilder sb) {
      if( jobs != null ) {
        DocGen.HTML.arrayHead(sb);
        sb.append("<tr class='warning'>");
        ArrayList<Argument> args = jobs[0].arguments();
        // Filter some keys to simplify UI
        args = (ArrayList<Argument>) args.clone();
        filter(args, "destination_key", "source", "cols", "ignored_cols", "ignored_cols_by_name", //
            "response", "classification", "validation");
        for( int i = 0; i < args.size(); i++ )
          sb.append("<td><b>").append(args.get(i)._name).append("</b></td>");
        sb.append("<td><b>").append("run time").append("</b></td>");
        String perf = jobs[0].speedDescription();
        if( perf != null )
          sb.append("<td><b>").append(perf).append("</b></td>");
        sb.append("<td><b>").append("model key").append("</b></td>");
        sb.append("<td><b>").append("prediction error").append("</b></td>");
        sb.append("<td><b>").append("F1 score").append("</b></td>");
        sb.append("</tr>");

        ArrayList<JobInfo> infos = new ArrayList<JobInfo>();
        for( Job job : jobs ) {
          JobInfo info = new JobInfo();
          info._job = job;
          if(job.destination_key != null){
            Object value = UKV.get(job.destination_key);
            info._model = value instanceof Model ? (Model) value : null;
            if( info._model != null ) {
              info._cm = info._model.cm();
              info._error = info._model.mse();
            }
          }
          if( info._cm != null)
            info._error = info._cm.err();
          infos.add(info);
        }
        Collections.sort(infos, new Comparator<JobInfo>() {
          @Override public int compare(JobInfo a, JobInfo b) {
            return Double.compare(a._error, b._error);
          }
        });

        for( JobInfo info : infos ) {
          sb.append("<tr>");
          for( Argument a : args ) {
            try {
              Object value = a._field.get(info._job);
              String s;
              if( value instanceof int[] )
                s = Utils.sampleToString((int[]) value, 20);
              else
                s = "" + value;
              sb.append("<td>").append(s).append("</td>");
            } catch( Exception e ) {
              throw new RuntimeException(e);
            }
          }
          String runTime = "Pending", speed = "";
          if( info._job.start_time != 0 ) {
            runTime = PrettyPrint.msecs(info._job.runTimeMs(), true);
            speed = perf != null ? PrettyPrint.msecs(info._job.speedValue(), true) : "";
          }
          sb.append("<td>").append(runTime).append("</td>");
          if( perf != null )
            sb.append("<td>").append(speed).append("</td>");
          String link = "";
          if( info._job.start_time != 0 && DKV.get(info._job.destination_key) != null ) {
            link = info._job.destination_key.toString();
            if( info._model instanceof GBMModel )
              link = GBMModelView.link(link, info._job.destination_key);
            else if( info._model instanceof DRFModel )
              link = DRFModelView.link(link, info._job.destination_key);
            else if( info._model instanceof NeuralNetModel )
              link = NeuralNetProgress.link(info._job.self(), info._job.destination_key, link);
            if( info._model instanceof KMeans2Model )
              link = KMeans2ModelView.link(link, info._job.destination_key);
            else
              link = Inspect.link(link, info._job.destination_key);
          }
          sb.append("<td>").append(link).append("</td>");

          String pct = "", f1 = "";
          if( info._cm != null ) {
            pct = String.format("%.2f", 100 * info._error) + "%";
            if( info._cm._arr.length == 2 )
              f1 = String.format("%.2f", info._cm.precisionAndRecall());
          } else pct = String.format("%.2f", info._error) ;
          sb.append("<td><b>").append(pct).append("</b></td>");
          sb.append("<td><b>").append(f1).append("</b></td>");
          sb.append("</tr>");
        }
        DocGen.HTML.arrayTail(sb);
      }
      return true;
    }

    static class JobInfo {
      Job _job;
      Model _model;
      ConfusionMatrix _cm;
      double _error = Double.POSITIVE_INFINITY;
    }

    static void filter(ArrayList<Argument> args, String... names) {
      for( String name : names )
        for( int i = args.size() - 1; i >= 0; i-- )
          if( args.get(i)._name.equals(name) )
            args.remove(i);
    }

    @Override protected Response jobDone(final Job job, final Key dst) {
      return Response.done(this);
    }
  }
}
