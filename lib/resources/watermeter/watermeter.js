/**
 * Created by tomk on 9/27/14.
 */

var PB_PIXEL_WIDTH_BAR = 6;
var PB_PIXEL_WIDTH_SPACING = 3;
var PB_PIXEL_HEIGHT = 100;

function saturate0(value) {
    if (value < 0) {
        return 0;
    }

    return value;
}

function PerfbarCore(coreIdx) {
    this.pbCoreIdx = coreIdx;
    this.pbUserTicks = 0;
    this.pbSystemTicks = 0;
    this.pbOtherTicks = 0;
    this.pbIdleTicks = 0;
    this.pbX = this.pbCoreIdx * (PB_PIXEL_WIDTH_BAR + PB_PIXEL_WIDTH_SPACING);

    this.fill = function (ctx, fillStyle, x, y, x_width, y_height) {
        console.log("fillRect:", fillStyle, x, y, x_width, y_height);
        ctx.fillStyle = fillStyle;
        ctx.fillRect(x, y, x_width, y_height);
    };

    this.updateTicks = function (ctx, userTicks, systemTicks, otherTicks, idleTicks) {
        console.log("updateTics:", this.pbCoreIdx);

        var deltaUserTicks   = saturate0(userTicks - this.pbUserTicks);
        var deltaSystemTicks = saturate0(systemTicks - this.pbSystemTicks);
        var deltaOtherTicks  = saturate0(otherTicks - this.pbOtherTicks);
        var deltaIdleTicks   = saturate0(idleTicks - this.pbIdleTicks);
        var deltaTotalTicks  = deltaUserTicks + deltaSystemTicks + deltaOtherTicks + deltaIdleTicks;

        var userTicksPct;
        var systemTicksPct;
        var otherTicksPct;
        if (deltaTotalTicks > 0) {
            userTicksPct     = (deltaUserTicks / deltaTotalTicks);
            systemTicksPct   = (deltaSystemTicks / deltaTotalTicks);
            otherTicksPct    = (deltaOtherTicks / deltaTotalTicks);
        }
        else {
            userTicksPct     = 0;
            systemTicksPct   = 0;
            otherTicksPct    = 0;
        }

        var userHeight     = PB_PIXEL_HEIGHT * userTicksPct;
        var systemHeight   = PB_PIXEL_HEIGHT * systemTicksPct;
        var otherHeight    = PB_PIXEL_HEIGHT * otherTicksPct;
        var idleHeight     = saturate0(PB_PIXEL_HEIGHT - (userHeight + systemHeight + otherHeight));

        var x = this.pbX;
        var x_width = PB_PIXEL_WIDTH_BAR;
        var y;
        var y_height;

        // user (green)
        y = idleHeight + otherHeight + systemHeight;
        y_height = userHeight;
        this.fill(ctx, "#00FF00", x, y, x_width, y_height);

        // system (red)
        y = idleHeight + otherHeight;
        y_height = systemHeight;
        this.fill(ctx, "#FF0000", x, y, x_width, y_height);

        // other (white)
        y = idleHeight;
        y_height = otherHeight;
        this.fill(ctx, "#FFFFFF", x, y, x_width, y_height);

        // idle (blue)
        y = 0;
        y_height = idleHeight;
        this.fill(ctx, "#0000FF", x, y, x_width, y_height);

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

    console.log("instantiating:", this.pbNodeName, this.pbNodeIdx);

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

var pb0 = new Perfbar("pb0", 0, 1);
pb0.docWriteCanvas();
pb0.docGetElement();

var gticks = new Array(100);
gticks[ 0] = [ 0,  0,  0,   0];
gticks[ 1] = [ 0,  0,  0, 100];
gticks[ 2] = [ 0,  0,  0, 200];
gticks[ 3] = [50,  0,  0, 250];
gticks[ 4] = [50,  0,  0, 350];
gticks[ 5] = [50,  0,  0, 450];
gticks[ 6] = [50, 50,  0, 500];
gticks[ 7] = [50, 50,  0, 600];
gticks[ 8] = [50, 50,  0, 700];
gticks[ 9] = [50, 50, 50, 750];
gticks[10] = [50, 50, 50, 850];
gticks[11] = [50, 50, 50, 950];

var i = 0;
var timeoutDelayMillis = 1000;
var timeout = null;
repaintAndArmTimeout();

function repaintAndArmTimeout() {
    clearTimeout(timeout);

    console.log("updating for i", i);
    pb0.updateTicks(gticks[i][0], gticks[i][1], gticks[i][2], gticks[i][3]);

    i = i + 1;
    if (i > 11) {
        return;
    }

    timeout = setTimeout(function() {
        repaintAndArmTimeout();
    }, timeoutDelayMillis);
}
