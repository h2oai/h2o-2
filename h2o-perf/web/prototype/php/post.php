<?php
  include 'DB.php';

  $a = array_keys($_POST);
  $query = htmlspecialchars($_POST[$a[0]]);

  $result = mysqli_prepare($con, $query);
  if ( !$result ) {
    printf('errno: %d, error: %s', $con->errno, $con->error);
    printf("<br /><br/>"); 
    printf("\nFriendly reminder: Don't use double quotes!");
    die;
  }

  mysqli_stmt_execute($result);
  $results = fetch_records2($result);
  $_SESSION['results'] = $results;
  header('Location: ../../html/result_page.html');
?>
