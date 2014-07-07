
import h2o_cloud, h2o_func


h2o = h2o_cloud.build_cloud(3)

h2o_func.put_file(path="/home/0xdiag/datasets/standard/covtype.data", key='covtype.hex')

