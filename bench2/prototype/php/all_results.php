<?php 
  include 'DB.php';

  if(isset($_POST['phase_name'])){
    //TODO: sanitize
    $phase_name = $_POST['phase_name'];
  }

  if(isset($_POST['test_name'])){
    //TODO: sanitize
    $test_name = $_POST['test_name'];
  }

  if(isset($_POST['num_hosts'])) {
    //TODO: sanitize
    $num_hosts = $_POST['num_hosts'];
  }

  $QUERY =<<<EOT
SELECT
  -- Select from test_run --
  FROM_UNIXTIME(test_run.start_epoch_ms, "%Y-%m-%d") AS date,
  test_run.product_name AS product,
  test_run.build_branch AS h2o_build_name,
  test_run.build_version AS h2o_version,
  test_run.dataset_name AS dataset,
  test_run.total_hosts AS total_hosts,
  -- Select from test_run_phase_result --
  test_run_phase_result.start_epoch_ms AS start_time,
  test_run_phase_result.end_epoch_ms AS end_time,
  test_run_phase_result.end_epoch_ms - test_run_phase_result.start_epoch_ms AS elapsed_time
FROM 
  test_run
INNER JOIN 
  test_run_phase_result
ON 
  test_run.test_run_id = test_run_phase_result.test_run_id
WHERE
  test_run_phase_result.phase_name = ?  # phase_name: Default is model building phase
  AND test_run.test_name = ?  #test_name 
  AND test_run.total_hosts = ? #total_hosts
ORDER BY
  product, date;
EOT;

  $result = mysqli_prepare($con, $QUERY);
  mysqli_stmt_bind_param($result, 'sss', $phase_name, $test_name, $num_hosts);
  mysqli_stmt_execute($result);
  $results = fetch_records($result);
  echo json_encode($results);
?>

