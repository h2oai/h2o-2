$(document).ready(function() {
  $('#test').typeahead({
   source: function (query, process) {
     $.ajax({
       url: '../prototype/php/search.php',
      type: 'POST',
      dataType: 'JSON',
      data: 'id=test' + '&query=' + query,
      success: function(data) {
        process(data);
      }
     });
   }
  });
});

$(document).ready(function() {
  $('#machine').typeahead({
    source: function (query, process) {
      $.ajax({
        url: '../prototype/php/search.php',
        type: 'POST',
        dataType: 'JSON',
        data: 'id=machine' + '&query=' + query,
        success: function(data) {
          process(data);
        }
      });
    }
  });
});

$(document).ready(function() {
  $('#question').typeahead({
    source: function (query, process) {
      $.ajax({
        url: '../prototype/php/search.php',
        type: 'POST',
        dataType: 'JSON',
        data: 'id=question' + '&query=' + query,
        success: function(data) {
          process(data);
        }
      });
    }
  });
});

