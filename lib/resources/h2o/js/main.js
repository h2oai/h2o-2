// Initialize required javascript after page load
$(function () {
    $.fn.tooltip.defaults.animation = false;
    $('[rel=tooltip]').tooltip();
    $('[rel=popover]').popover();
});

function collectValuesCBox(name) {
  var values = $('input[name="'+name+'"]:not(:checked)').map( function() { return this.value } ).get();
  return values.join(',')
}

function redirectWithCols(e,name) {
  var target = e.href.replace(/&ignored_cols=.*/g,'') + "&ignored_cols="+collectValuesCBox(name);
  console.log(target)
  e.href = target;
  return true;
}

window.displayJavaModel = function() {
  regex = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
  var emailField = $('#emailForJavaModel');
  var email = emailField.val().trim();
  if (regex.test(email)) {
    $('#javaModelSource').toggleClass('hide');
    $('#javaModelWarningBlock').hide();
    ga('send', 'event', 'view-java-model', email);
  } else {
    alert('Please enter a valid email address.');
    emailField.focus();
  }
}
