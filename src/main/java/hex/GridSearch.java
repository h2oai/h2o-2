package hex;

import hex.KMeans2.KMeans2Model;
import hex.KMeans2.KMeans2ModelView;
import hex.NeuralNet.NeuralNetModel;
import hex.drf.DRF.DRFModel;
import hex.gbm.GBM.GBMModel;
import hex.deeplearning.DeepLearningModel;
import hex.singlenoderf.SpeeDRFModel;
import hex.singlenoderf.SpeeDRFModelView;
import water.*;
import water.api.*;
import water.util.Utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class GridSearch extends Job {
  public Job[] jobs;

  public GridSearch(){

  }
  @Override protected void execImpl() {
    UKV.put(destination_key, this);
    int max = jobs[0].gridParallelism();
    int head = 0, tail = 0;
    while( head < jobs.length && isRunning(self()) ) {
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
    @API(help = "Prediction Errors")
    public double[] prediction_errors;
    @API(help = "State")
    public String[] job_state;

    @Override protected Response serve() {
      Response response = super.serve();
      if( destination_key != null ) {
        GridSearch grid = UKV.get(destination_key);
        if( grid != null )
          jobs = grid.jobs;
        updateErrors(null);
      }
      return response;
    }

    void updateErrors(ArrayList<JobInfo> infos) {
      if (jobs == null) return;
      prediction_errors = new double[jobs.length];
      job_state = new String[jobs.length];
      int i = 0;
      for( Job job : jobs ) {
        JobInfo info = new JobInfo();
        info._job = job;
        if(job.dest() != null){
          Object value = UKV.get(job.dest());
          info._model = value instanceof Model ? (Model) value : null;
          if( info._model != null ) {
            info._cm = info._model.cm();
            info._error = info._model.mse();
          }
        }
        if( info._cm != null && (info._model == null || info._model.isClassifier()))
          info._error = info._cm.err();
        if (infos != null) infos.add(info);
        prediction_errors[i] = info._error;
        job_state[i] = info._job.state.toString();
        i++;
      }
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
        for (Argument arg : args) sb.append("<td><b>").append(arg._name).append("</b></td>");
        sb.append("<td><b>").append("run time").append("</b></td>");
        String perf = jobs[0].speedDescription();
        if( perf != null )
          sb.append("<td><b>").append(perf).append("</b></td>");
        sb.append("<td><b>").append("model key").append("</b></td>");
        sb.append("<td><b>").append("prediction error").append("</b></td>");
        sb.append("<td><b>").append("F1 score").append("</b></td>");
        sb.append("</tr>");

        ArrayList<JobInfo> infos = new ArrayList<JobInfo>();
        updateErrors(infos);
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
              else if( value instanceof double[] )
                s = Utils.sampleToString((double[]) value, 20);
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
          if( info._job.start_time != 0 && DKV.get(info._job.dest()) != null ) {
            link = info._job.dest().toString();
            if( info._model instanceof GBMModel )
              link = GBMModelView.link(link, info._job.dest());
            else if( info._model instanceof DRFModel )
              link = DRFModelView.link(link, info._job.dest());
            else if( info._model instanceof NeuralNetModel )
              link = NeuralNetModelView.link(link, info._job.dest());
            else if( info._model instanceof DeepLearningModel)
              link = DeepLearningModelView.link(link, info._job.dest());
            if( info._model instanceof KMeans2Model )
              link = KMeans2ModelView.link(link, info._job.dest());
            if (info._model instanceof SpeeDRFModel)
              link = SpeeDRFModelView.link(link, info._job.dest());
            else
              link = Inspect2.link(link, info._job.dest());
          }
          sb.append("<td>").append(link).append("</td>");

          String err, f1 = "";
          if( info._cm != null && info._cm._arr != null) {
            err = String.format("%.2f", 100 * info._error) + "%";
            if (info._cm.isBinary()) f1 = String.format("%.4f", info._cm.F1());
          } else err = String.format("%.5f", info._error) ;
          sb.append("<td><b>").append(err).append("</b></td>");
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

    @Override protected Response jobDone(final Key dst) {
      return Response.done(this);
    }
  }
}
