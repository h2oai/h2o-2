<?php
  $packed = explode(',',$_POST['query']);
  $arr = array(
      "test"         => $packed[0],
#      "machine"      => $packed[1],
      "question"     => $packed[1],
      "num_builds"   => $packed[2],
      "phase_select" => $packed[3],
  );
  header('Location: ../../html/result_page.html?'.http_build_query($arr));
?>
