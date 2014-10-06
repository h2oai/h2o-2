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
   header.push("PerfGraphs")
   datas[0] = header;
   for(i = 0; i < json.data.length; i++) {
     datas[i+1] = d3.values(json.data[i]);
     link = "http://172.16.2.171:4040/perflink.html?test_run_id=" + datas[i+1][0] + "&test_name=" + datas[i+1][2] + "&num_hosts=" + datas[i+1][10] + "&rundate=" + datas[i+1][1].split('-').join('');
     datas[i+1].push(link)
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
     .style("cursor", function(d) {
      if ( (d.toString()).indexOf("http") > -1) {
        return "pointer"
      } else {
        return "auto"
       }   
     })
     .on("mouseover", function(){d3.select(this).style("background-color", "aliceblue")})
     .on("mouseout", function(){d3.select(this).style("background-color", "white")})
     .text(function(d){return d;})
       .on("click", function(d, i) {
        if ( (d.toString()).indexOf("http") > -1)    
            window.open(d, "", "height=800, width=800")
       })
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
        width = 2000 - margin.left - margin.right,
        height = 500 -margin.top -margin.bottom;

    var x = d3.scale.linear().range([0, width - 1000]);
    var y = d3.scale.linear().range([height, 0]);
    var color = d3.scale.category10();

    var xAxis = d3.svg.axis().scale(x).orient("bottom").tickFormat(d3.format("d"));
    var yAxis = d3.svg.axis().scale(y).orient("left");
    
    var lineFunction = d3.svg.line().interpolate("ordinal")
                .x(function(d) {return x(d[0]); })
                .y(function(d) { return y(d[1]); });

    var svg = d3.select(svg).append("svg")
                .attr("width", width + margin.left + margin.right)
                .attr("height", height + margin.top + margin.bottom)
              .append("g")
                .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var test_names = new Array();
    for(i = 0; i < json.data.length; i++) {
      bv = d3.values(json.data[i])[7].split('.');
      bv = bv[bv.length - 1];
      if (bv == "99999") { continue; }
      if (bv == 99999) { continue; }
      test_names.push(d3.values(json.data[i])[2]);
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
      build = d3.values(json.data[i])[7].split('.');
      build = build[build.length - 1]
      if (build == "99999") { continue; }
      if (build == 99999) { continue; }
      test  = d3.values(json.data[i])[2];
      dat   = d3.values(json.data[i])[3];
      for(j = 0; j < datas2.length; j++) {
        if (datas2[j].name === test) {
            datas2[j].data.push([build, dat, test])
        }
      }
    }

    x.domain([
      d3.min(datas2, function(c) { return d3.min(c.data, function(v) { return v[0]; }); }),
      d3.max(datas2, function(c) { return d3.max(c.data, function(v) { return v[0]; }); })
    ]);

    y.domain([
      0,
      d3.max(datas2, function(c) { return d3.max(c.data, function(v) { return +v[1]; }); })
    ]);

    var linesGroup = svg.append("g").attr("class", "line");
    var div = d3.select("body").append("div").attr("class", "tooltip").style("opacity", 0);
    var links = []
    var nodes = {}

    var j = 0;
    for (var i in datas2) {
        linedata = datas2[i]
        links.push({source: "source" + j, target: linedata.name, x: x(linedata.data[linedata.data.length - 1][0]), 
                                                             y: y(linedata.data[linedata.data.length - 1][1])})
        j++;
        svg.selectAll(".point").data(linedata.data).enter()
              .append("svg:circle")
              .attr("stroke", "black")
              .attr("fill", "black")
            .on("mouseover", function(d, i) {
                var dd = document.createElement("div");
                dd.style.position = "absolute";
                dd.style.visibility = "hidden";
                dd.style.height = "auto";
                dd.style.width = "auto";
                dd.innerHTML = d[2];
                dd.setAttribute("id", "a")
                document.body.appendChild(dd);
                div.transition().duration(200).style("opacity", .95);
                div.html("build:" + d[0] + "<br /> test_name: " + d[2] + "<br /> time: " + parseFloat(d[1]).toFixed(1) + "(s)")
                .style("left", (d3.event.pageX) + "px")
                .style("top", (d3.event.pageY - 28) + "px")
                .style("height",(dd.clientHeight + 30) + "px")
                .style("width", (dd.clientWidth + 100) + "px");
             })
              .on("mouseout", function(d) {
                  div.transition().duration(400).style("opacity", 0)})
              .attr("cx", function(d, i) { return x(d[0]) })
              .attr("cy", function(d, i) { return y(d[1]) })
              .attr("r", function(d, i) { return 3 }); 
        linesGroup.append("path")
                  .attr("d", lineFunction(linedata.data))
                  .attr("class", "lines")
                  .attr("fill", "none")
                  .attr("stroke", function(d, i) {
                      return linedata.color;
                  });
     };

    links.forEach(function(link, i) {
      link.source = nodes[link.source] || (nodes[link.source] = {name: '', isTarget: false, x: link.x, y: link.y});
      link.target = nodes[link.target] || (nodes[link.target] = {name: link.target, isTarget: true, x: link.x + 50, y: link.y - 20});
    });

    var force = d3.layout.force()
        .nodes(d3.values(nodes))
        .links(links)
        .size([width*2, height/2])
        //.gravity(0.5)
        .linkDistance(10)
        .theta(1)
        .charge(-1200)
        .on("tick", tick);

    for(i = 0; i < force.nodes().length; i++) {
        if (!force.nodes()[i].isTarget) {
          force.nodes()[i].fixed = true;
        }
        if(force.nodes()[i].isTarget && Math.random() < 0.5) {
            force.nodes()[i].y += 250
        }
    }
    force.start();

    var link = svg.selectAll(".link")
        .data(force.links())
      .enter().append("line")
        .attr("class", "link");

    var node = svg.selectAll(".node")
        .data(force.nodes())
      .enter().append("g")
        .attr("class", "node");
        //.on("mouseover", mouseover)
        //.on("mouseout", mouseout);
        //.call(force.drag);

    node.append("circle")
        .attr("r", function(d) { 
            if (d.isTarget) {
                return 20;
            } else { return 0;}
         });

    node.append("text")
        .attr("text-anchor", "right")
        //.attr("x", 4)
        //.attr("dy", ".35em")
        .text(function(d) { return d.name; });

    function tick() {
      link
          .attr("x1", function(d) { return d.source.x; })
          .attr("y1", function(d) { return d.source.y; }) 
          .attr("x2", function(d) { return d.target.x; })
          .attr("y2", function(d) { return d.target.y; });
    
      node.attr("x", function(d) { return d.x = Math.max(20, Math.min(width - 20, d.x)); })
        .attr("y", function(d) { return d.y = Math.max(20, Math.min(height - 20, d.y)); })
        .attr("transform", function(d) { 
              return "translate(" + d.x + "," + d.y + ")";});
    }

    function mouseover() {
      d3.select(this).select("circle").transition()
          .duration(750)
          .attr("r", 16);
    }
    
    function mouseout() {
      d3.select(this).select("circle").transition()
          .duration(750)
          .attr("r", 2);
    }

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
}

function zip(x, y) {
    return $.map(x, function (el, idx) {
        return [[el, y[idx]]];
    });
}

function nodePerfGraph(json, svgCPU, svgRSS) {
    var margin = {top: 20, right: 80, bottom: 30, left: 50},
        width = 2000 - margin.left - margin.right,
        height = 500 -margin.top -margin.bottom;

    var xCPU = d3.scale.linear().range([0, width - 1000]);
    var yCPU = d3.scale.linear().range([height, 0]);

    var xRSS = d3.scale.linear().range([0, width - 1000]);
    var yRSS = d3.scale.linear().range([height, 0]);

    var xAxisCPU = d3.svg.axis().scale(xCPU).orient("bottom").tickFormat(d3.format("d"));
    var yAxisCPU = d3.svg.axis().scale(yCPU).orient("left");
    
    var xAxisRSS = d3.svg.axis().scale(xRSS).orient("bottom").tickFormat(d3.format("d"));
    var yAxisRSS = d3.svg.axis().scale(yRSS).orient("left"); 

  
    var lineFunctionCPU = d3.svg.line().interpolate("ordinal")
        .x(function(d) {
          return xCPU(d[0]); })
        .y(function(d) { 
          return yCPU(d[1]); });

    var lineFunctionRSS = d3.svg.line().interpolate("ordinal")
        .x(function(d) {return xRSS(d[0]); })
        .y(function(d) { return yRSS(d[1]); });


    var svg_cpu = d3.select(svgCPU).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    var svg_rss = d3.select(svgRSS).append("svg")
        .attr("width", width + margin.left + margin.right)
        .attr("height", height + margin.top + margin.bottom)
        .append("g")
        .attr("transform", "translate(" + margin.left + "," + margin.top + ")");

    /**
     * JSON:
     *  {
     *     "test_run_id"
     *     "test_name"
     *     "node_name"
     *     "timevals": []
     *     "sys_cpu": []
     *     "user_cpu": []
     *     "rss": []
     */

    var test_run_id = json.data[0]['test_run_id'];
    var test_name = json.data[0]['test_name'];
    var node_name = json.data[0]['node_ip'];
    var ts_raw = json.data.map(function(d) { return d['ts'] });
    var max_time = d3.max(ts_raw);
    var ts = (ts_raw.map(function(d) { return max_time - d })).sort(function(a,b) {return a - b; });
    var sys_cpu = zip(ts, json.data.map(function(d) { return d['cum_cpu_sys'] }));
    var user_cpu = zip(ts, json.data.map(function(d) { return d['cum_cpu_proc'] }));
    var rss = zip(ts, json.data.map(function(d) { return d['rss'] / (1.0 * 1024 * 1024) }));

    var cpu_min = d3.min( [d3.min(user_cpu.map(function(d) {return d[0];})),  d3.min(sys_cpu.map(function(d) { return d[0]; }))]);
    var cpu_max = d3.max( [d3.max(user_cpu.map(function(d) {return d[0]; })),  d3.max(sys_cpu.map(function(d) { return d[0]; }))])

    xCPU.domain([0, cpu_max]);

    yCPU.domain([
        0,
        100, 
    ]);

    xRSS.domain([
        0,
        d3.max(rss.map(function(d) { return d[0] }))
    ]);

    yRSS.domain([
        0,
        d3.max(rss.map(function(d) {return d[1] }))
    ]);

    var cpu = [];
    for (var pp in sys_cpu) {
        cpu.push({
            name: "sys",
            color: "green",
            data: sys_cpu[pp]
        })
    }
    for (var pp in user_cpu) {
        cpu.push({
            name: "user",
            color: "blue",
            data: user_cpu[pp]
        })
    }

    // Make the CPU svg
    var linesGroup = svg_cpu.append("g").attr("class", "line");
  
    svg_cpu.selectAll(".point").data(cpu).enter()
      .append("svg:circle")
      .attr("stroke", "black")
      .attr("fill", "black")
      .attr("cx", function(d, i) {
        return xCPU(d.data[0])
      })
      .attr("cy", function(d, i) {
        return yCPU(d.data[1])
      })
      .attr("r", 3)
      .attr("stroke", function(d) { return d.color});

      
    linesGroup.append("path")
      .attr("d", lineFunctionCPU(sys_cpu))
      .attr("class", "lines")
      .attr("fill", "none")
      .attr("stroke", "green");

    linesGroup.append("path")
      .attr("d", lineFunctionCPU(user_cpu))
      .attr("class", "lines")
      .attr("fill", "none")
      .attr("stroke", "blue");

    svg_cpu.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxisCPU)
        .append("text")
        .attr("y", 0)
        .attr("x", xCPU(cpu_max +1 ))
        .text("Time (s)");

    svg_cpu.append("g")
        .attr("class", "y axis")
        .call(yAxisCPU)
        .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Cumulative CPU %");

    // Do the RSS graph
    var linesGroup2 = svg_rss.append("g").attr("class", "line");
    
    svg_rss.selectAll(".point").data(rss).enter()
        
    svg_rss.selectAll(".point").data(rss).enter()
      .append("svg:circle")
      .attr("stroke", "black")
      .attr("fill", "black")
      .attr("cx", function(d, i) {
        console.log(d)
        return xRSS(d[0])
      })  
      .attr("cy", function(d, i) {
        return yRSS(d[1])
      })  
      .attr("r", 3)
      .attr("stroke", "black");

    linesGroup2.append("path")
      .attr("d", lineFunctionRSS(rss))
      .attr("class", "lines")
      .attr("fill", "none")
      .attr("stroke", "black");

    svg_rss.append("g")
        .attr("class", "x axis")
        .attr("transform", "translate(0," + height + ")")
        .call(xAxisRSS)
        .append("text")
        .attr("y", 0)
        .attr("x", xRSS(cpu_max +1 ))
        .text("Time (s)");

    svg_rss.append("g")
        .attr("class", "y axis")
        .call(yAxisRSS)
        .append("text")
        .attr("transform", "rotate(-90)")
        .attr("y", 6)
        .attr("dy", ".71em")
        .style("text-anchor", "end")
        .text("Cumulative Memory Used (GB)"); 
}

