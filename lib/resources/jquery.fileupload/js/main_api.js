$(function () {
    $('#Fileupload').fileupload({
    	url: 'Upload',
    	type: 'POST',        
        sequentialUploads: true,
        dataType: 'json',
//        maxChunkSize: 1000, // uncomment to enable chunked uploads - supported only in FF&Chrome
        change: function (e, data) {
          resetUploadTable();
          $.each(data.files, function (index, file) {
              $('#filename').text(file.name);
              $('table #Key').val(file.name);
              if (file.size === undefined)
                  $('#filesize').text('N/A');
              else if (file.size < Math.pow(2, 10))
                  $('#filesize').text(file.size +' B');
              else if (file.size < Math.pow(2, 20))
                  $('#filesize').text((file.size/Math.pow(2, 10)).toFixed(1) +' KB');
              else if (file.size < Math.pow(2, 30))
                  $('#filesize').text((file.size/Math.pow(2, 20)).toFixed(1) +' MB');
              else if (file.size < Math.pow(2, 40))
                  $('#filesize').text((file.size/Math.pow(2, 30)).toFixed(2) +' GB');
              else if (file.size < Math.pow(2, 50))
                  $('#filesize').text((file.size/Math.pow(2, 40)).toFixed(3) +' TB');
              else if (file.size < Math.pow(2, 60))
                  $('#filesize').text((file.size/Math.pow(2, 50)).toFixed(3) +' PB');
              else
                  $('#filesize').text(file.size +' B');
          });
          $('#UploadTable').show();
	  $('table #Key').keypress(function (e) {
	    if (e.which == 13)
	      $('#UploadBtn').click()(e);
	  });
        },
        progressall: function (e, data) {
            var progress = parseInt(data.loaded / data.total * 100, 10);
            $('#progress .bar').css('width',progress + '%');          
        },        
        add: function (e, data) {          
          $('#UploadBtn').prop("disabled",false);
          $('#UploadBtn').unbind('click').click(function(e) {    	
              	e.preventDefault();    	
              	$(this).prop("disabled",true);
              	$.getJSON('WWWFileUpload.json', 
                   { File: $('table #filename').text(), key: $("table #Key").val(), RF: $("table #RF").val() },
                      function(result){
                    	  if (result) {
                        	  var uri = document.URL;
                        	  if (uri.lastIndexOf(':') > 4) 
                        		  uri = uri.slice(0,uri.lastIndexOf(':'))
                        	  else 
                        		  uri = uri.slice(0,uri.lastIndexOf('/'))
                        	  uri = uri + ':' + result.port + '/Upload';
                        	  data.url = uri;                      	
                        	  data.submit();
                    	  }
                      });
              });
        },
        done: function (e, data) {        	
        	var result = data.result;
        	if (result.constructor == String) { // IE returns string, however Chrome, Safari do automatic parsing
        		result = $.parseJSON(result);
        	}
        	if ($.isArray(result)) { 
        		if (result[0]) {
                	result = result[0];
                	$('#progress .bar').css('width','100%');
                        $('table #filename').html("<a href='Get?key=" + encodeURIComponent(result.key) +"'>" + result.name + "</a>");
                        $('table #keytd').html("<span class='span3'><a href='Inspect?key=" + encodeURIComponent(result.key) +"'>" + result.key + "</a></span>");
                	$('table #rftd').html("<span class='span2'>" + result.rf + "</html>");
                	$('table #btntd').html("<span class='label label-success'>DONE</span>");
                	window.location.href='Inspect.html?key=' + encodeURIComponent(result.key);
                	return;
        		}
        	} 
        	// ups something is wrong
        	failedUpload("callback done(), but the response is wrong: " + data.response, data);
        },
        fail: function (e, data) {
        	failedUpload("callback fail()", data);
        },        
    });    
            
    function resetUploadTable() {
        $('#Fileupload').css("display","none");
    	$('#UploadTable').empty();
    	$('#UploadTable').append("<tr><td style='border:0px' class='span1' id='btntd'><button type='submit' class='btn btn-primary' id='UploadBtn'>Upload</button></td><td style='border:0px' class='span2' id='filename'></td>"
        + "      <td style='border:0px' class='span2' id='filesize'></td>"
        + "      <td style='border:0px' class='span3' id='keytd'><input type='text' class='input-small span2' placeholder='key (optional)' name='Key' id='Key' maxlength='512'></td>"
        + "      <td style='border:0px' class='span2' id='rftd'><input type='text' class='input-small span2' placeholder='replication (optional)' name='RF' id='RF' maxlength='512'></td>"
        + "      <td style='border:0px' class='span6'><div id='progress' class='progress progress-striped span6'><div class='bar' style='width: 0%;'></div></div></td>"
	+ "   </tr>");
    }
    function failedUpload(msg,data) {
    	$('#UploadBtn').text("Failed!").addClass("btn-danger");
    	console.log("FAILED upload: " + msg);
    	console.log(data.textStatus);
    	console.log(data.errorThrown);    	
    }
    
    $('#UploadTable').hide();
});

