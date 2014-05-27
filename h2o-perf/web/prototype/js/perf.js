/** Performance Harness Javascript

Holds utility and all purpose javascript

*/

//d3 table from json
//The json returned from the php is of the form 
//  [{key:value,...,key:value}, 
//   {key:value,...,key:value},
//   ...,
//   {key:value,...,key:value}]
function makeTable(json, svg) {
   var datas = new Array();
   var header = d3.keys(json.data[0]);
   datas[0] = header;
   for(i = 0; i < json.data.length; i++) {
     datas[i+1] = d3.values(json.data[i]);
   }   
   
   d3.select(svg)
     .append("table")
     .style("border-collapse", "collapse")
     .style("border", "2px black solid")

     .selectAll("tr")
     .data(datas).enter().append("tr")

     .selectAll("td")
     .data(function(d){return d}).enter().append("td")
     .style("border", "1px black solid")
     .style("padding", "5px")
     .on("mouseover", function(){d3.select(this).style("background-color", "aliceblue")})
     .on("mouseout", function(){d3.select(this).style("background-color", "white")})
     .text(function(d){return d;})
     .style("font-size", "12px"); 
}

//show the num_builds input field when the question is appropriate
function showNum() {
  // returns -1 if "last N" is not a substring (and hence does not display the field)
  if ($('#question').val().indexOf("last N") != -1) {
    document.getElementById("num_builds_text").style["display"] = "inline-block";
    document.getElementById("num_builds_div").style["display"] = "inline-block";
  } else {
    document.getElementById("num_builds_text").style["display"] = "none";
    document.getElementById("num_builds_div").style["display"] = "none";
  }
}

//use jquery to handle the num_builds input field
$('*').on('change', '#question', function() {
  showNum();
});



