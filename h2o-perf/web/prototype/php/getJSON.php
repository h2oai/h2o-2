<?php
  session_start();
  $r = $_SESSION['results'];
  echo json_encode($r);
?>
