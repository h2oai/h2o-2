(function(i,s,o,g,r,a,m){i['GoogleAnalyticsObject']=r;i[r]=i[r]||function(){
(i[r].q=i[r].q||[]).push(arguments)},i[r].l=1*new Date();a=s.createElement(o),
m=s.getElementsByTagName(o)[0];a.async=1;a.src=g;m.parentNode.insertBefore(a,m)
})(window,document,'script','//www.google-analytics.com/analytics.js','ga');

try {
  ga('create', 'UA-44624604-1', 'auto');
  ga('send', 'pageview', {
    // Override title (dt) to remove cloud/username.
    title: document.title.replace(/^.+?\-\s*(.+)$/, "$1"),
    // Override page (dp) and location (dl) args to strip out sensitive information.
    page: window.location.pathname,
    location: window.location.pathname 
  });
} catch (error) {
  // non-critical, so swallow
};
