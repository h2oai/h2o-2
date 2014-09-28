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
