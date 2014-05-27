<?php 
  include 'DB.php';

  if(isset($_POST['id'])){
    //TODO: sanitize
    $id = $_POST['id'];
  }

  if(isset($_POST['query'])){
    //TODO: sanitize
    $query = '%'.$_POST['query'].'%';
  } 

  $con = mysqli_connect($host,$user,$pass,$databaseName);

  // Check connection
  if (mysqli_connect_errno())
    {
    echo "Failed to connect to MySQL: " . mysqli_connect_error();
    }

  $result = mysqli_prepare($con, "SELECT DISTINCT " . $id  . " FROM polls_poll where question LIKE ?");
  mysqli_stmt_bind_param($result, 's', $query);
  mysqli_stmt_execute($result);

  $results = array();
  $res2 = $result->get_result();
  while ($row = $res2->fetch_array(MYSQLI_NUM)) {
    foreach ($row as $r) {
      $results[] = $r;
    }
  }

  // and return to typeahead
  echo json_encode($results);
?>
