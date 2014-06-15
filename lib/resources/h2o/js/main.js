// Initialize required javascript after page load
$(function () {
    $.fn.tooltip.defaults.animation = false;
    $('[rel=tooltip]').tooltip();
    $('[rel=popover]').popover();
});

function collectValuesCBox(name) {
  var values = $('input[name="'+name+'"]:not(:checked)').map( function() { return this.value } ).get();
  return values.join(',');
}

function redirectWithCols(e,name) {
  var target = e.href.replace(/&ignored_cols=.*/g,'') + "&ignored_cols="+collectValuesCBox(name);
  console.log(target)
  e.href = target;
  return true;
}

//
// Begin javascript handling for temporary licensing of java models.
//

function writeJavaModelLicenseCookie(days, email) {
  $.cookie('java-model-license', email, { expires: days });
}

function readJavaModelLicenseCookie() {
  return $.cookie('java-model-license');
}

function displayJavaModel(email) {
  $('#javaModelSource').toggleClass('hide');
  $('#javaModelWarningBlock').hide();
  ga('send', 'event', 'view-java-model', 'email', email);
}

function showOrHideJavaModel() {
  var email = readJavaModelLicenseCookie();
  if(email) {
    displayJavaModel(email);
  } 
}

function processJavaModelLicense() {
  regex = /^(([^<>()[\]\\.,;:\s@\"]+(\.[^<>()[\]\\.,;:\s@\"]+)*)|(\".+\"))@((\[[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\.[0-9]{1,3}\])|(([a-zA-Z\-0-9]+\.)+[a-zA-Z]{2,}))$/;
  var emailField = $('#emailForJavaModel');
  var email = emailField.val().trim();
  if (regex.test(email)) {
    writeJavaModelLicenseCookie(1, email);
    displayJavaModel(email);
  } else {
    alert('Please enter a valid email address.');
    emailField.focus();
  }
}
//
// End javascript handling for temporary licensing of java models.
//
