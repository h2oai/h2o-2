#!/bin/bash

function doctype {
cat << EOF
<!DOCTYPE HTML>
<html lang="en">
<head>
<meta http-equiv="Content-Type" content="text/html; charset=ISO-8859-1">
<title>H2O Benchmarks</title>

<link rel="stylesheet" href="example.css" TYPE="text/css" MEDIA="screen">

<script type="text/javascript">

/* Optional: Temporarily hide the "tabber" class so it does not "flash"
   on the page as plain HTML. After tabber runs, the class is changed
   to "tabberlive" and it will appear.
*/
document.write('<style type="text/css">.tabber{display:none;}<\/style>');

var tabberOptions = {

  /* Optional: instead of letting tabber run during the onload event,
     we'll start it up manually. This can be useful because the onload
     even runs after all the images have finished loading, and we can
     run tabber at the bottom of our page to start it up faster. See the
     bottom of this page for more info. Note: this variable must be set
     BEFORE you include tabber.js.
  */
  'manualStartup':true,

  /* Optional: code to run after each tabber object has initialized */

  /* Optional: code to run when the user clicks a tab. If this
     function returns boolean false then the tab will not be changed
     (the click is canceled). If you do not return a value or return
     something that is not boolean false, */

  /* Optional: set an ID for each tab navigation link */
  'addLinkId': true

};

</script>

<!-- Load the tabber code -->
<script type="text/javascript" src="tabber.js"></script>
<script type="text/javascript" src="d3.v3.js"></script>

</head>
<body>
<script>

function makeTableNCharts(csvPathname, svgDiv, phase) {

    d3.text(csvPathname, function(datasetText) {
    
    var parsedCSV = d3.csv.parseRows(datasetText);
    
    var sampleHTML = d3.select(svgDiv)
        .append("table")
        .style("border-collapse", "collapse")
        .style("border", "2px black solid")
    
        .selectAll("tr")
        .data(parsedCSV)
        .enter().append("tr")
    
        .selectAll("td")
        .data(function(d){return d;})
        .enter().append("td")
        .style("border", "1px black solid")
        .style("padding", "5px")
        .on("mouseover", function(){d3.select(this).style("background-color", "aliceblue")})
        .on("mouseout", function(){d3.select(this).style("background-color", "white")})
        .text(function(d){return d;})
        .style("font-size", "12px");

    //make charts too
    var data = d3.csv.parse(datasetText);
    var valueLabelWidth = 60; // space reserved for value labels (right)
    var barHeight = 20; // height of one bar
    var barLabelWidth = 200; // space reserved for bar labels
    var barLabelPadding = 5; // padding between bar and bar labels (left)
    var gridLabelHeight = 40; // space reserved for gridline labels
    var gridChartOffset = 3; // space between start of grid and first bar
    var maxBarWidth = 500; // width of the bar with the max value
   
     // accessor functions 
    var barLabel = function(d) { return d['dataset']; };
    var barValue = function(d) { return parseFloat(d[phase]); };
     
    // scales
    var yScale = d3.scale.ordinal().domain(d3.range(0, data.length)).rangeBands([0, data.length * barHeight]);
    var y = function(d, i) { return yScale(i); };
    var yText = function(d, i) { return y(d, i) + yScale.rangeBand() / 2; };
    var x = d3.scale.linear().domain([0, d3.max(data, barValue)]).range([0, maxBarWidth]);
    // svg container element
    var chart = d3.select(svgDiv+'chart').append("svg")
      .attr('width', maxBarWidth + barLabelWidth + valueLabelWidth)
      .attr('height', gridLabelHeight + gridChartOffset + data.length * barHeight);
    // grid line labels
    var gridContainer = chart.append('g')
      .attr('transform', 'translate(' + barLabelWidth + ',' + gridLabelHeight + ')'); 
    gridContainer.selectAll("text").data(x.ticks(10)).enter().append("text")
      .attr("x", x)
      .attr("dy", -3)
      .attr("text-anchor", "middle")
      .text(String);
    // vertical grid lines
    gridContainer.selectAll("line").data(x.ticks(10)).enter().append("line")
      .attr("x1", x)
      .attr("x2", x)
      .attr("y1", 0)
      .attr("y2", yScale.rangeExtent()[1] + gridChartOffset)
      .style("stroke", "#ccc");
    // bar labels
    var labelsContainer = chart.append('g')
      .attr('transform', 'translate(' + (barLabelWidth - barLabelPadding) + ',' + (gridLabelHeight + gridChartOffset) + ')'); 
    labelsContainer.selectAll('text').data(data).enter().append('text')
      .attr('y', yText)
      .attr('stroke', 'none')
      .attr('fill', 'black')
      .attr("dy", ".35em") // vertical-align: middle
      .attr('text-anchor', 'end')
      .text(barLabel);
    // bars
    var barsContainer = chart.append('g')
      .attr('transform', 'translate(' + barLabelWidth + ',' + (gridLabelHeight + gridChartOffset) + ')'); 
    barsContainer.selectAll("rect").data(data).enter().append("rect")
      .attr('y', y)
      .attr('height', yScale.rangeBand())
      .attr('width', function(d) { return x(barValue(d)); })
      .attr('stroke', 'white')
      .attr('fill', 'steelblue');
    // bar value labels
    barsContainer.selectAll("text").data(data).enter().append("text")
      .attr("x", function(d) { return x(barValue(d)); })
      .attr("y", yText)
      .attr("dx", 3) // padding-left
      .attr("dy", ".35em") // vertical-align: middle
      .attr("text-anchor", "start") // text-align: right
      .attr("fill", "black")
      .attr("stroke", "none")
      .text(function(d) { return d3.round(barValue(d), 2); });
    // start line
    barsContainer.append("line")
      .attr("y1", -gridChartOffset)
      .attr("y2", yScale.rangeExtent()[1] + gridChartOffset)
      .style("stroke", "#000");
    });
}
</script>
<h1>Benchmarks</h1>
EOF
}

function makeTail {
cat << EOF

<script type="text/javascript">

/* Since we specified manualStartup=true, tabber will not run after
   the onload event. Instead let's run it now, to prevent any delay
   while images load.
*/

tabberAutomatic(tabberOptions);

</script>

</body>
</html>
EOF
}

function makeTab {
cat << EOF

  <div class="tabbertab" id="$1">
    <h2>$1</h2>
EOF
}

function endDiv {
cat << EOF
</div>
EOF
}

function makeTabber {
cat << EOF

<div class="tabber" id="tab">

EOF
}

function addTable {
cat << EOF
    <h1>$1 benchmarks</h1>
    <div id="$1"></div>
    <div id="$1chart"></div>
    <script type="text/javascript">makeTableNCharts("$2","#$1","$3BuildTime")</script>
EOF
}

function endDiv2 {
cat << EOF
  </div>
EOF
}

doctype
makeTabber

for dir in ./benches/*
do
    dir=${dir%*/}
    d=${dir##*/}
    makeTab $d
    for f in $dir/*
    do
        svg=`echo $f| awk -F'/' '{print $NF}' | awk -F'bench.csv' '{print $1}'`
        addTable ${svg}_${d} $f $svg
    done
    endDiv2
done
endDiv
makeTail
