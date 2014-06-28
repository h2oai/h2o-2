<?php
  include 'DB2.php';

  $test = $_POST['test_run_id'];
  $ip   = $_POST['ip'];

  $query = "SELECT * FROM stats WHERE test_run_id =" . $test . " AND node_ip = '" . $ip . "';";

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
