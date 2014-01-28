<?php

  //connect to the database (possibly don't use this tableName)

  $host = "localhost:3306";
  $user = "spencer";
  $pass = "spencer";

  $databaseName = "PerfDB";

  $con = mysqli_connect($host,$user,$pass,$databaseName);
 
  if (mysqli_connect_errno())
    {   
    echo "Failed to connect to MySQL: " . mysqli_connect_error();
    }
  
  function fetch_records($records) {
    $arr = array();
    $res = $records->get_result();
    while($row = $res->fetch_array(MYSQLI_NUM)) {
      foreach ($row as $r) {
        $arr[] = $r; 
      }   
    }   
    return $arr;
  }

  function fetch_records2($records) {
    $arr = array('data'=> array());
    $res = $records->get_result();
    while($row = mysqli_fetch_assoc($res)) {
      array_push($arr['data'], $row);
    }
    return $arr;
  }

  #TODO: Make this functional
  function perform_query($q, $bind, $bind_types) {
    $res = mysqli_prepare($con, $q);
    if(count($bind) > 0) {
      mysqli_stmt_bind_param($res, $bind_types, $bind);
    }
    mysqli_stmt_execute($res);
    return fetch_records($res);
  }

?>
