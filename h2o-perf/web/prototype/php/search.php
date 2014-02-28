<?php 
  include 'DB.php';

  if(isset($_GET['id'])){
    //TODO: sanitize
    $id = $_GET['id'];
  }

  if(isset($_GET['query'])){
    //TODO: sanitize
    $query = '%'.$_GET['query'].'%';
  }


  if(isset($_GET['table'])){
    //TODO: sanitize
    $table = $_GET['table'];
  }

  #echo $id
  #####
  #  PLAN: 
  #  This is the typeahead php handler. Hit the mysql table passed in and get back the appropriate entries.
  #  The test_run_meta table has the test name (this is the $id) and the question for that id
  #  As such test_name is not unique in this table (a single test name can have multiple questions
  #  asked of it).
  #
  #  TODO: Add logic to update the test_run_meta table whenever a new test is added
  #        This can be manual for the time being...
  ######

  $result = mysqli_prepare($con, "SELECT DISTINCT " . $id  . " FROM " . $table . " where " . $id . " LIKE ?");
  
  if ( !$result ) {
      printf('errno: %d, error: %s', $con->errno, $con->error);

      die;
  }

  

  mysqli_stmt_bind_param($result, 's', $query);
  mysqli_stmt_execute($result);
  $results = fetch_records($result);

  // and return to typeahead
  echo json_encode($results);
?>
