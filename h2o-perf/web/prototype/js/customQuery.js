/** customQuery javascript

Holds functions for passing between query/results page

*/

//submit the query and redirect to the results page
function query_submit(test,question,num_builds,phase) {
  var data = new Array();
  data[0] = test;
//  data[1] = machine;
  data[1] = question;
  data[2] = num_builds;
  data[3] = phase;
  document.query.query.value = data;
  document.query.submit();

} 

//redirect back to the query page
function new_query() {
  window.location = '../index.html'
}

//helper function to get the param payload from the url after redirect
function GetUrlValue(){
    var loc = document.URL;
    var params = loc.split('?')[1];
    return params;
}

//Perform the query by accessing the appropriate php script and then
//processing the data returned with d3
function doQuery(phpQueryPage) {
    d3.json(phpQueryPage, function(json) {
    //d3.json(phpQueryPage+'?'+GetUrlValue(), function(json) {
        try {
          makeGraph(json, "#graph_area")
        } catch(err) {
          var graph = document.getElementById("graph_area")
          graph.style.height = "20px"
          console.log("No graph...")
        }
        makeTable(json, '#results_table')
    });
}

function showPerfGraphs(test_name, total_hosts, dt) {
    if (total_hosts == 1) {
        // Just looks at node 192.168.1.164

        $.ajax({
            url: '../prototype/php/post2.php',
            type: 'POST',
            dataType: 'JSON',
            data: 'test_name=' + test_name + '&ip=172.16.2.164&dt='+dt,
            success: function(data) {
              console.log(data)
              if(data.data.length == 0) {
                d3.select("#cpu_svg1").append("text").text("No CPU Data Captured!");
                d3.select("#rss_svg1").append("text").text("No RSS Data Captured!");
              } else {
                  nodePerfGraph(data, '#cpu_svg', '#rss_svg');
              }
            },
            error: function(request, status, error) {
              console.log(request.responseText);
            }
        });
    } else {
        for (var i = 0; i < total_hosts; ++i) {
            $.ajax({
                url: '../prototype/php/post2.php',
                type: 'POST',
                dataType: 'JSON',
                data: 'test_name=' + test_name + '&ip=172.16.2.16' + (i+1) + '&dt='+dt,
                async: false,
                success: function(data) {
                    if(data.data.length == 0) {
                      if (i < 1) {
                        d3.select("#cpu_svg1").append("text").text("No CPU Data Captured!");
                        d3.select("#rss_svg1").append("text").text("No RSS Data Captured!");
                      }
                    } else {
                      nodePerfGraph(data, '#cpu_svg'+(i+1), '#rss_svg'+(i+1));
                    }
                },
                error: function (request, status, error) {
                  console.log(request.responseText);
                }
            });
        }
    }
}


$('#accordion').on('show.bs.collapse', function () {
    $('#accordion .in').collapse('hide');
});
