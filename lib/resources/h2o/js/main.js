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
