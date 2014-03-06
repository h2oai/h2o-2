SELECT 
    tr.test_name,
    tr.run_date,
    tr.build_date,
    tr.branch_name,
    tr.train_dataset_name, 
    tr.num_train_rows, 
    tr.num_explan_cols, 
    tr.num_cpus, 
    tr.mem_heap_bytes,
    (tp.phase_end_time - tp.start_time) / 1000 parse_time_s
FROM test_run tr
INNER JOIN test_run_phase_result tp
ON tr.test_run_id = tp.test_run_id
WHERE tp.phase_start_time = 'parse'
ORDER BY tr.run_date;
