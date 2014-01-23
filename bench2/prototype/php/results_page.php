<?php
  $packed = explode(',',$_POST['query']);
  $arr = array(
      "test"         => $packed[0],
      "machine"      => $packed[1],
      "question"     => $packed[2],
      "num_builds"   => $packed[3],
      "phase_select" => $packed[4],
  );
  header('Location: ../../html/result_page.html?'.http_build_query($arr));
?>
