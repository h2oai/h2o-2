<?php
  include 'DB2.php';

  $test = $_POST['test_name'];
  $ip   = $_POST['ip'];
  $dt   = $_POST['dt'];

  $query = "SELECT * FROM stats WHERE test_name ='" . $test . "' AND node_ip = '" . $ip . "' AND from_unixtime(ts, '%Y%m%d') = '" . $dt . "' AND test_run_id IN (SELECT MAX(test_run_id) FROM stats WHERE test_name  ='" . $test . "' AND node_ip = '" . $ip . "' AND from_unixtime(ts, '%Y%m%d') = '" . $dt . "');";


  $result = mysqli_prepare($con, $query);
  if ( !$result ) { 
    printf('errno: %d, error: %s', $con->errno, $con->error);
    printf("<br /><br/>");
    printf("\nFriendly reminder: Don't use double quotes!");
    die;
  }

  mysqli_stmt_execute($result);
  $results = fetch_records2($result);
  
  echo json_encode($results);

?>
