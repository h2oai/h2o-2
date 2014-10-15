package water.api;

import water.*;
import water.H2ONode.TaskInfo;
import water.H2ONode.task_status;

import java.util.Arrays;

/**
 * Created by tomasnykodym on 9/17/14.
 */
public class TaskStatus extends Request2 {

  static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
  static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
  public static class NodeTaskInfo extends Iced {
    static final int API_WEAVER = 1; // This file has auto-gen'd doc & json fields
    static public DocGen.FieldDoc[] DOC_FIELDS; // Initialized from Auto-Gen code.
    @API(help="node name")
    final String _node;
    @API(help="tasks sent here by remote nodes")
    final TaskInfo[][] _remotes;
    @API(help="pending tasks sent by me to remotes")
    final TaskInfo  [] _pending;

    public NodeTaskInfo(TaskInfo [] pending, TaskInfo[][] ts) {
      _node = H2O.SELF.toString();
      _remotes = ts;
      _pending = pending;
    }
  }
  public static class GetTaskInfo extends DRemoteTask<GetTaskInfo>{
    NodeTaskInfo [] _infos;

    @Override
    public void reduce(GetTaskInfo drt) {
      if(_infos == null)
        _infos = drt._infos;
      else {
        for(int i = 0; i < _infos.length; ++i){
          if(_infos[i] == null)
            _infos[i] = drt._infos[i];
        }
      }
    }

    @Override
    public void lcompute() {
      _infos = new NodeTaskInfo[H2O.CLOUD.size()];
      TaskInfo [][] ts = new TaskInfo[H2O.CLOUD.size()][];
      int i = 0;
      for (H2ONode n : H2O.CLOUD._memary)
        ts[i++] = n.currentTasksInfo();
      RPC [] pendingRPCs = UDPTimeOutThread.pendingRPCs();
      TaskInfo [] pending = new TaskInfo[pendingRPCs.length];
      for(int j = 0; j < pendingRPCs.length; ++j)
        pending[j] = new TaskInfo(pendingRPCs[j].task(),pendingRPCs[j].taskNum(),pendingRPCs[j].target().index(), pendingRPCs[j].isDone()? task_status.DONE:task_status.CMP,pendingRPCs[j]._callCnt);
      _infos[H2O.SELF.index()] = new NodeTaskInfo(pending,ts);
      tryComplete();
    }
  }

  @API(help="task infos for each node")
  NodeTaskInfo[] _infos;
  @Override
  protected Response serve() {
    _infos = new GetTaskInfo().invokeOnAllNodes()._infos;
    return Response.done(this);
  }
  public boolean toHTML( StringBuilder sb ) {
    for(NodeTaskInfo x:_infos) {
      sb.append("<div>");
      sb.append("<h3>" + x._node + "</h3>");
      sb.append("<table class='table table-bordered table-condensed'>");
      for(int i = 0; i < H2O.CLOUD.size(); ++i){
        if(H2O.CLOUD._memary[i] == H2O.SELF)
          continue;
        sb.append("<tr>");
        sb.append("<th>Pending[" + H2O.CLOUD._memary[i] + "]</th>");
        sb.append("<td>");
        for(TaskInfo ti:x._pending)
          if(ti.nodeId == i)
            sb.append(" " + ti.toString());
        sb.append("</td>");
        sb.append("</tr>");
      }
      int i = 0;
      for (TaskInfo[] ti : x._remotes) {
        sb.append("<tr>");
        sb.append("<th>Remote[" + H2O.CLOUD._memary[i++] + "]</th>");
        sb.append("<td>");
        sb.append(Arrays.deepToString(ti));
        sb.append("</td>");
        sb.append("</tr>");
      }
      sb.append("</table>");
      sb.append("</div>");
      sb.append("</p>");
    }
    return true;
  }
}
