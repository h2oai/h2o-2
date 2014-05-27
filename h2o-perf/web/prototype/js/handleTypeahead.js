$(document).ready(function() {
  $('#test').typeahead({
   source: function (query, process) {
     $.ajax({
       url: '../prototype/php/search.php',
      type: 'GET',
      dataType: 'JSON',
      data: 'id=test_name' + '&query=' + query + '&table=test_run',
      success: function(data) {
        process(data);
      }
     });
   }
  });
});

/*
$(document).ready(function() {
  $('#machine').typeahead({
    source: function (query, process) {
      $.ajax({
        url: '../prototype/php/search.php',
        type: 'POST',
        dataType: 'JSON',
        data: 'id=machine' + '&query=' + query + '&table=,
        success: function(data) {
          process(data);
        }
      });
    }
  });
});
*/

$(document).ready(function() {
  $('#question').typeahead({
    source: function (query, process) {
      $.ajax({
        url: '../prototype/php/search.php',
        type: 'GET',
        dataType: 'JSON',
        data: 'id=question' + '&query=' + query + '&table=perf_questions',
        success: function(data) {
          process(data);
        }
      });
    }
  });
});

