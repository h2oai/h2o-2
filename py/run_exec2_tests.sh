
cd testdir_single_jvm
./n0.doit test_exec2_covtype_cols.py
./n0.doit test_exec2_covtype_rand1.py
./n0.doit test_exec2_env_fail.py
./n0.doit test_exec2_env_pop_fail.py
./n0.doit test_exec2_filter_slice.py
./n0.doit test_exec2_operators2.py
./n0.doit test_exec2_operators.py
./n0.doit test_exec2_result_race.py

cd ../testdir_multi_jvm
./n0.doit test_exec2_2.py
./n0.doit test_exec2_arith_precedence.py
./n0.doit test_exec2_col_scalar.py
./n0.doit test_exec2_covtype_rand2.py
./n0.doit test_exec2_dkv.py
./n0.doit test_exec2_factor.py
./n0.doit test_exec2_int2cat_nested.py
./n0.doit test_exec2_int2cat.py
./n0.doit test_exec2_rotate_inc.py
./n0.doit test_exec2_sum_cols.py

