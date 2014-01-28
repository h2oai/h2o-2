(function() {
  this.JSONApiServerURI = function() {
    var baseURI;
    baseURI = URI(window.location);
    baseURI.path("/Inspect");
    return baseURI.clone();
  };

  this.ENV = 'RELEASE';

}).call(this);
