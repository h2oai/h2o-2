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


function makeGraph(json, svg) {
    var margin = {top: 20, right: 80, bottom: 30, left: 50},
        width = 960 - margin.left - margin.right,
        height = 500 -margin.top -margin.bottom;

    var x = d3.scale.linear().range([0, width]);
    var y = d3.scale.linear().range([height, 0]);
    var color = d3.scale.category10();

    var xAxis = d3.svg.axis().scale(x).orient("bottom").tickFormat(d3.format("d"));
    var yAxis = d3.svg.axis().scale(y).orient("left");
    
    var lineFunction = d3.svg.line().interpolate("basis")
                .x(function(d) {return x(d[0]); })
                .y(function(d) { return y(d[1]); });

    var svg = d3.select(svg).append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
              .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var datas = new Object();
    var test_names = new Array();
    for(i = 0; i < json.data.length; i++) {
      bv = d3.values(json.data[i])[6].split('.');
      bv = bv[bv.length - 1];
      if (bv == "99999") { continue; }
      if (bv == 99999) { continue; }
      test_names[i] = d3.values(json.data[i])[1];
      datas[i] = d3.values(json.data[i]);
    }
    test_names = d3.set(test_names).values();

    var datas2 = new Array();
    var domains = new Array();
    for (i = 0; i < test_names.length; i++) {
      domains[i] = test_names[i];
      color.domain(domains)
      datas2[i] = {
            name: test_names[i],
            color: color(),
            data: new Array()
            }
    }
    for(i = 0; i < json.data.length; i++) {
      build = d3.values(json.data[i])[6].split('.');
      build = build[build.length - 1]
      console.log(build)
      if (build == "99999") { continue; }
      if (build == 99999) { continue; }
      test  = d3.values(json.data[i])[1];
      dat   = d3.values(json.data[i])[2];
      for(j = 0; j < datas2.length; j++) {
        if (datas2[j].name === test) {
            datas2[j].data.push([build, dat])
        }
      }
    }
    console.log(datas2)

    x.domain([
      d3.min(datas2, function(c) { return d3.min(c.data, function(v) { return v[0]; }); }),
      d3.max(datas2, function(c) { return d3.max(c.data, function(v) { return v[0]; }); })
    ]);

    y.domain([
      d3.min(datas2, function(c) { return d3.min(c.data, function(v) {  return +v[1]; }); }),
      d3.max(datas2, function(c) { return d3.max(c.data, function(v) { return +v[1]; }); })
    ]);

    var linesGroup = svg.append("g").attr("class", "line");
  
    for (var i in datas2) {
        linedata = datas2[i]
        linesGroup.append("path")
                  .attr("d", lineFunction(linedata.data))
                  .attr("class", "line")
                  .attr("fill", "none")
                  .attr("stroke", function(d, i) {
                      return linedata.color;
                  });
//        linesGroup.append("text")
//              .attr("x", 750)
//              .attr("dy", ".35em")
//              .text(linedata.name);
     };

//    var tests = color.domain().map(function(name) {
//        return {
//            name: name,
//            values: datas.filter(function(d) { return d.test_name !== name })
//                        .map(function(d) {
//                          build_version = d.build_version.split('.');
//                          build_version = build_version[build_version.length - 1];
//                          return {build: build_version, time: d.time_s};
//                         })
//         };
//    });
//
//    x.domain(d3.extent(datas, function(d) { bv = d.build_version.split('.'); return bv[bv.length - 1]; }));
//    y.domain([
//        d3.min(tests, function(t) { return d3.min(t.values, function(v) {return v.time; }); }),
//        d3.max(tests, function(t) { return d3.max(t.values, function(v) {return v.time; }); })
//    ]);
//
    svg.append("g")
      .attr("class", "x axis")
      .attr("transform", "translate(0," + height + ")")
      .call(xAxis);

    svg.append("g")
      .attr("class", "y axis")
      .call(yAxis)
    .append("text")
      .attr("transform", "rotate(-90)")
      .attr("y", 6)
      .attr("dy", ".71em")
      .style("text-anchor", "end")
      .text("Time (s)");
//    
//    
//    var test = svg.selectAll(".test")
//      .data(tests)
//    .enter().append("g")
//      .attr("class", "test");
//
//
//    test.append("path")
//      .attr("class", "line")
//      .attr("d", function(d) { return line(d.values); })
//      .style("stroke", function(d) { return color(d.name); });
//
//  test.append("text")
//      .datum(function(d) { return {name: d.name, value: d.values[d.values.length - 1]}; })
//      .attr("transform", function(d) { return "translate(" + x(d.value.build) + "," + y(d.value.time) + ")"; })
//      .attr("x", 3)
//      .attr("dy", ".35em")
//      .text(function(d) { return d.name; });

}
