<?php
 session_start();
 ini_set('post_max_size','5M');
 ini_set('display_errors','On');
 error_reporting(E_ALL);
  //connect to the database (possibly don't use this tableName)

  $host = "localhost";
  $user = "spencer";
  $pass = "spencer";

  $databaseName = "Hound";

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

  #TODO: Hook this up...
  function perform_query($q, $bind, $bind_types) {
    $res = mysqli_prepare($con, $q);
    if(count($bind) > 0) {
      mysqli_stmt_bind_param($res, $bind_types, $bind);
    }
    mysqli_stmt_execute($res);
    return fetch_records($res);
  }

?>
