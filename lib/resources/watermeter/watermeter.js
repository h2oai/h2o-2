/**
 * Created by tomk on 9/27/14.
 */

var PB_PIXEL_WIDTH_BAR = 6;
var PB_PIXEL_WIDTH_SPACING = 3;
var PB_PIXEL_HEIGHT = 100;

function PerfbarCore(coreIdx) {
    this.pbCoreIdx = coreIdx;
    this.pbUserTicks = 0;
    this.pbSystemTicks = 0;
    this.pbOtherTicks = 0;
    this.pbIdleTicks = 0;
    this.pbX = this.pbCoreIdx * (PB_PIXEL_WIDTH_BAR + PB_PIXEL_WIDTH_SPACING);

    this.updateTicks = function (ctx, userTicks, systemTicks, otherTicks, idleTicks) {
        var x = this.pbX;
        var y;
        var y_height;

        // user (green)
        y = idleTicks + otherTicks + systemTicks + userTicks;
        if (y > 100) {
            y = 100;
        }
        y_height = userTicks;
        if (y - y_height < 0) {
            y_height = y
        }
        ctx.fillStyle = "#00FF00";
        ctx.fillRect(x, y, PB_PIXEL_WIDTH_BAR, y_height);

        // system (red)
        y = idleTicks + otherTicks + systemTicks;
        if (y > 100) {
            y = 100;
        }
        y_height = systemTicks;
        if (y - y_height < 0) {
            y_height = y
        }
        ctx.fillStyle = "#FF0000";
        ctx.fillRect(x, y, PB_PIXEL_WIDTH_BAR, y_height);

        // other (white)
        y = idleTicks + otherTicks;
        if (y > 100) {
            y = 100;
        }
        y_height = otherTicks;
        if (y - y_height < 0) {
            y_height = y
        }
        ctx.fillStyle = "#FFFFFF";
        ctx.fillRect(x, y, PB_PIXEL_WIDTH_BAR, y_height);

        // idle (blue)
        y = idleTicks;
        if (y > 100) {
            y = 100;
        }
        y_height = idleTicks;
        if (y - y_height < 0) {
            y_height = y
        }
        ctx.fillStyle = "#0000FF";
        ctx.fillRect(x, y, PB_PIXEL_WIDTH_BAR, y_height);

        this.pbUserTicks = userTicks;
        this.pbSystemTicks = systemTicks;
        this.pbOtherTicks = otherTicks;
        this.pbIdleTicks = idleTicks;
    };
}

function Perfbar(nodeName, nodeIdx, numCores) {
    this.pbCtx = null;
    this.pbNodeName = nodeName;
    this.pbNodeIdx = nodeIdx;
    this.pbNumCores = numCores;
    this.pbCores = new Array(numCores);

    for (var i = 0; i < numCores; i++) {
        this.pbCores[i] = new PerfbarCore (i);
    }

    this.docWriteCanvas = function() {
        var width = (numCores * PB_PIXEL_WIDTH_BAR) + ((numCores - 1) * PB_PIXEL_WIDTH_SPACING);

        var s = "<canvas id=\"" +
            nodeName + "" +
            "\" width=\"" +
            width +
            "\" height=\"" +
            PB_PIXEL_HEIGHT +
            "\" style=\"background-color: black;\"></canvas>\n";
        console.log("docWriteCanvas: ", s);
        document.write(s);
    };

    this.docGetElement = function() {
        console.log("docGetElement called");
        var c = document.getElementById(this.pbNodeName);
        this.pbCtx = c.getContext("2d");
    };

    this.updateTicks = function(userTicks, systemTicks, otherTicks, idleTicks) {
        console.log("updateTicks called");
        for (var i = 0; i < this.pbNumCores; i++) {
            this.pbCores[i].updateTicks(this.pbCtx, userTicks, systemTicks, otherTicks, idleTicks);
        }
    };
}

var pb0 = new Perfbar("pb0", 0, 8);
pb0.docWriteCanvas();
pb0.docGetElement();



var i = 0;
var timeoutDelayMillis = 50;
var timeout = null;
repaintAndArmTimeout();

function repaintAndArmTimeout() {
    clearTimeout(timeout);

    repaint();

    i = i + 1;
    if (i > 100) {
        return;
    }

    pb0.updateTicks(i, 0, 0, 100-i);

    timeout = setTimeout(function() {
        repaintAndArmTimeout();
    }, timeoutDelayMillis);
}

function repaint() {
    console.log("In repaint, i is", i);
}



//var n = 4, // number of layers
//    m = 58, // number of samples per layer
//    stack = d3.layout.stack(),
//    layers = stack(d3.range(n).map(function() { return bumpLayer(m, .1); })),
//    yGroupMax = d3.max(layers, function(layer) { return d3.max(layer, function(d) { return d.y; }); }),
//    yStackMax = d3.max(layers, function(layer) { return d3.max(layer, function(d) { return d.y0 + d.y; }); });
//
//var margin = {top: 40, right: 10, bottom: 20, left: 10},
//    width = 960 - margin.left - margin.right,
//    height = 500 - margin.top - margin.bottom;
//
//var x = d3.scale.ordinal()
//    .domain(d3.range(m))
//    .rangeRoundBands([0, width], .08);
//
//var y = d3.scale.linear()
//    .domain([0, yStackMax])
//    .range([height, 0]);
//
//var color = d3.scale.linear()
//    .domain([0, n - 1])
//    .range(["#aad", "#556"]);
//
//var xAxis = d3.svg.axis()
//    .scale(x)
//    .tickSize(0)
//    .tickPadding(6)
//    .orient("bottom");
//
//var svg = d3.select("body").append("svg")
//    .attr("width", width + margin.left + margin.right)
//    .attr("height", height + margin.top + margin.bottom)
//    .append("g")
//    .attr("transform", "translate(" + margin.left + "," + margin.top + ")");
//
//var layer = svg.selectAll(".layer")
//    .data(layers)
//    .enter().append("g")
//    .attr("class", "layer")
//    .style("fill", function(d, i) { return color(i); });
//
//var rect = layer.selectAll("rect")
//    .data(function(d) { return d; })
//    .enter().append("rect")
//    .attr("x", function(d) { return x(d.x); })
//    .attr("y", height)
//    .attr("width", x.rangeBand())
//    .attr("height", 0);
//
//rect.transition()
//    .delay(function(d, i) { return i * 10; })
//    .attr("y", function(d) { return y(d.y0 + d.y); })
//    .attr("height", function(d) { return y(d.y0) - y(d.y0 + d.y); });
//
//svg.append("g")
//    .attr("class", "x axis")
//    .attr("transform", "translate(0," + height + ")")
//    .call(xAxis);
//
//var timeout = null;
//timeout = setTimeout(function() {
//    repaintAndArmTimeout()
//}, 2000);
//
//function repaintAndArmTimeout() {
//    clearTimeout(timeout);
//    repaint();
//    timeout = setTimeout(function() {
//        repaintAndArmTimeout()
//    }, 2000);
//}
//
//function repaint() {
//    transitionStacked();
//}
//
//function transitionStacked() {
//    y.domain([0, yStackMax]);
//
//    rect.transition()
//        .duration(500)
//        .delay(function(d, i) { return i * 10; })
//        .attr("y", function(d) { return y(d.y0 + d.y); })
//        .attr("height", function(d) { return y(d.y0) - y(d.y0 + d.y); })
//        .transition()
//        .attr("x", function(d) { return x(d.x); })
//        .attr("width", x.rangeBand());
//}
//
//// Inspired by Lee Byron's test data generator.
//function bumpLayer(n, o) {
//
//    function bump(a) {
//        var x = 1 / (.1 + Math.random()),
//            y = 2 * Math.random() - .5,
//            z = 10 / (.1 + Math.random());
//        for (var i = 0; i < n; i++) {
//            var w = (i / n - y) * z;
//            a[i] += x * Math.exp(-w * w);
//        }
//    }
//
//    var a = [], i;
//    for (i = 0; i < n; ++i) a[i] = o + o * Math.random();
//    for (i = 0; i < 5; ++i) bump(a);
//    return a.map(function(d, i) { return {x: i, y: Math.max(0, d)}; });
//}
