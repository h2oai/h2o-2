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
  
  $result = mysqli_prepare($con, "SELECT DISTINCT " . $id  . " FROM polls_poll where question LIKE ?");
  mysqli_stmt_bind_param($result, 's', $query);
  mysqli_stmt_execute($result);
  $results = fetch_records($result);

  // and return to typeahead
  echo json_encode($results);
?>
