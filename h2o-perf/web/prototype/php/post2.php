<?php
  include 'DB2.php';

  $a = array_keys($_POST);
  $quer = htmlspecialchars($_POST[$a[0]]);

  $query = "SELECT * FROM stats WHERE test_run_id =" . $quer;
  
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