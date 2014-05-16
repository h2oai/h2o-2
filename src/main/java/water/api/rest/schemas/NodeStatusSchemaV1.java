package water.api.rest.schemas;

import water.Iced;
import water.api.DocGen;
import water.api.Request.API;
import water.api.rest.REST.Versioned;
import water.api.rest.Version;

public class NodeStatusSchemaV1 extends Iced implements Versioned<Version.V1> {
  static final int API_WEAVER = 1;
  static public DocGen.FieldDoc[] DOC_FIELDS;

  @API(help="name")
  public String name;

  @API(help="num_keys")
  public int num_keys;

  @API(help="value_size_bytes")
  public int value_size_bytes;

  @API(help="free_mem_bytes")
  public int free_mem_bytes;

  @API(help="tot_mem_bytes")
  public int tot_mem_bytes;

  @API(help="max_mem_bytes")
  public int max_mem_bytes;

  @API(help="free_disk_bytes")
  public int free_disk_bytes;

  @API(help="max_disk_bytes")
  public int max_disk_bytes;

  @API(help="num_cpus")
  public int num_cpus;

  @API(help="system_load")
  public int system_load;

  @API(help="elapsed_time")
  public int elapsed_time;

  @API(help="node_healthy")
  public boolean node_healthy;

  @API(help="PID")
  public int PID;

  @API(help="rpcs")
  public int rpcs;

  @API(help="tcps_active")
  public int tcps_active;

  @API(help="open_fds")
  public int open_fds;

  @API(help="my_cpu_percent")
  public float my_cpu_percent;

  @API(help="sys_cpu_percent")
  public float sys_cpu_percent;

  @API(help="last_contact")
  public long last_contact;

  @API(help="fj_threads_lo")
  int [] fj_threads_lo;

  @API(help="fj_queue_lo")
  int []fj_queue_lo;

  @API(help="fj_threads_hi")
  int [] fj_threads_hi;

  @API(help="fj_queue_hi")
  int [] fj_queue_hi;

  @Override
  public Version.V1 getVersion() { return Version.v1; }
}
