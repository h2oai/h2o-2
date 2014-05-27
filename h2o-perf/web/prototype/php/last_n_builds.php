<?php
  include 'DB.php';

  if(isset($_GET['test'])){
    //TODO: sanitize
    $test = $_GET['test'];
  }

  if(isset($_GET['num_builds'])){
    //TODO: sanitize
    $num_builds = (int) $_GET['num_builds'];
  }

  if(isset($_GET['phase_select'])){
    $phase = $_GET['phase_select'];
    if($phase == 'Modeling') {
      $phase = 'model';
    }
    if($phase == 'Import/Parse') {
      $phase = 'parse';
    }
    if($phase == 'Score/Prediction') {
      $phase = 'predict';
    }
  } else {
    $phase = "model";
  }

  #This should get the results for the last N builds of all releases
  #It calls a stored procedure on the back end.
  #Takes the test name, number of bulds, and phase to look at (e.g. parse, modeling, etc)
  #echo $phase,$num_builds,$test_name

  $x = "CALL LastN('".$test."', ".$num_builds.", '".$phase."')";
  #echo $x
  $result = mysqli_prepare($con, $x);
  if ( !$result ) {
    printf('errno: %d, error: %s', $con->errno, $con->error);
    die;
  }

  mysqli_stmt_execute($result);
  $results = fetch_records2($result);
  //echo $results;
  echo json_encode($results);
?>
