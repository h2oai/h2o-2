function zip(x, y) {
    return $.map(x, function (el, idx) {
      return [[el, y[idx]]];
    }); 
}
/** Name of element to append SVG, names (y-axis), values (x-axis) */
function g_varimp(divid, names, varimp) { 
  // Create a dataset as an array of tuples
  var dataset = zip(names, varimp);
  var valueLabelWidth = 20; // Space reserved for values on the right
  var barLabelWidth = 80; // Space reserved for col names on the left
  var barHeight = 14; // Height of bar
  var maxBarWidth = 640; // Maximum bar width
  var nfeatures = names.length;
  // Setup size and axis
  var margin = {top: 30, right: 5, bottom: 10, left: 5},
      width = maxBarWidth; // - margin.left - margin.right,
      height = nfeatures*barHeight; // - margin.top - margin.bottom;

  var minVI = d3.min(varimp);
  var maxVI = d3.max(varimp);
  var xScale = d3.scale.linear()
      .range([0, width])
      .domain([minVI > 0 ? 0 : minVI, maxVI < 0 ? 0 : maxVI]).nice();
        
  var yScale = d3.scale.ordinal()
      .rangeRoundBands([0, height], .2)
      .domain(names);

  var xAxis = d3.svg.axis()
      .scale(xScale)
      .orient("top");

  var svg = d3.select("#"+divid).append("svg")
    .attr("width", width + margin.left + margin.right + valueLabelWidth + barLabelWidth)
    .attr("height", height + margin.top + margin.bottom)

  // function returning width of given bar representing a variable
  var fx = function(d) { return xScale(Math.min(0, d[1])); }
  // function returning height of given bar representing a variable
  var fy = function(d) { return yScale(d[0]); }
  // function returning position for text
  var yText = function(d, i) { return fy(d) + yScale.rangeBand() / 2; };
  // function returning bar label (i.e., feature name)
  var barLabel = function(d) { return d[0]; };

  // Container for features labels
  var labelsContainer = svg.append("g").attr("transform", "translate("+(barLabelWidth-margin.left) + "," + margin.top + ")");
  labelsContainer.selectAll("text").data(dataset).enter().append('text')
    .attr('y', yText)
    .attr('stroke', 'none')
    .attr('fill', 'black')
    .attr("dy", ".25em") // vertical-align: middle
    .attr('text-anchor', 'end')
    .attr("font-size", "11px")
    .text(barLabel);

  // Container for colored bars
  var barsContainer = svg.append("g").attr("transform", "translate(" + barLabelWidth + "," + margin.top + ")");
  barsContainer.selectAll(".bar")
      .data(dataset)
    .enter().append("rect")
      .attr("class", function(d) { return d[1] < 0 ? "bar negative" : "bar positive"; })
      .attr("x", fx)
      .attr("y", fy)
      .attr("width", function(d) { return Math.abs(xScale(d[1]) - xScale(0)); })
      .attr("height", yScale.rangeBand());

  // Append values at the end of bar
  var dname = function(d) { return d[0]; };
  var dval  = function(d) { return d[1]; };
  var tval  = function(d) { return d3.round(dval(d), 4); };

  barsContainer.selectAll("text").data(dataset).enter().append("text")
    .attr("x", function(d) { var v=dval(d); return v < 0 ? xScale(0) : xScale(v); })
    .attr("y", yText)
    .attr("dx", 3) // padding-left
    .attr("dy", ".35em") // vertical-align: middle
    .attr("text-anchor", "start") // text-align: right
    .attr("fill", "black")
    .attr("stroke", "none")
    .attr("font-size", "11px")
    .text(function(d) { return tval(d); });

  // Axes into a dedicated container
  var gridContainer = svg.append("g").attr("transform", "translate(" + barLabelWidth + "," + margin.top + ")");
  gridContainer.append("g")
      .attr("class", "x axis")
      .call(xAxis);

  gridContainer.append("g")
      .attr("class", "y axis")
    .append("line")
      .attr("x1", xScale(0))
      .attr("x2", xScale(0))
      .attr("y2", height);

  // Hook for sort button
  $(document).ready(function() {
      var sorted = false;
      var sortIt = function() {
          sorted = !sorted;
          bars = function(l,r) {
              if (sorted) {
                  return l[1]- r[1];
              }
              return r[1]- l[1];
          }
          // The following times can be zero for large number of variables
          fdelay = function(d,i) { return nfeatures > 50 ? 0 : i*40; } // Make domino effect during sorting
          var cduration = nfeatures > 50 ? 0 : 200; // Total time for sort

          barsContainer.selectAll(".bar")
              .sort(bars)
              .transition()
              .delay(fdelay) 
              .duration(cduration)
              .attr("y", function(d,i) {
                  return yScale(names[i]);
              });
          barsContainer.selectAll("text")
              .sort(bars)
              .transition()
              .delay(fdelay)
              .duration(cduration)
              .text(function(d) { return tval(d); })
              .attr("text-anchor", "start") // text-align: right
              //.attr("x", function(d,i) { return xScale(dval(d)); })
              .attr("x", function(d,i) { var v=dval(d); return v < 0 ? xScale(0) : xScale(v); })
              .attr("y", function(d,i) { return yScale(names[i]) + yScale.rangeBand() / 2; } );
          labelsContainer.selectAll("text")
              .sort(bars)
              .transition()
              .delay(fdelay)
              .duration(cduration)
              .text(function(d,i) { return d[0]; })
              .attr("y", function(d,i) { return yScale(names[i]) + yScale.rangeBand() / 2; } );
      };
      d3.select("#sortBars").on("click", sortIt);
  });
}

