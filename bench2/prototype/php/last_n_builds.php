<?php
  include 'DB.php';
  $result = mysqli_prepare($con, "CALL proc1()");
  mysqli_stmt_execute($result);
  $results = fetch_records2($result);
  echo json_encode($results);
?>

