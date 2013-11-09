casper.test.comment("In future this file will load the test dataset and parse it (being a de facto parse test).
    \n There is no point in making those test now, as we are on the verge of making new interface for parse. 
    \n For now we have a couple of assumptions.
    \n Assuming you have a runnign h20 with data loaded.
    \n Assuming you have new UI running on port 8888
    \n Assuming you have a key uploaded and parsed, called test.hex")

testhost   = casper.cli.get "testhost"
screenshot = casper.cli.get "screenfile"

casper.fill = (selector, values, submit = false) ->
    @evaluate (selector, values) ->
        $("#{selector} [name='#{name}']").val(value).trigger('input') for name, value of values
    ,
        selector: selector
        values: values

# Capture screens from all fails
casper.test.on "fail", (failure) ->
  casper.capture(screenshot)
  casper.exit 1

# Capture screens from timeouts from e.g. @waitUntilVisible
# Requires RC3 or higher.
casper.options.onWaitTimeout = ->
  @capture(screenshot)
  @exit 1

# Scan for the word notice|warning|error|exception by default
# Might get useful later, for now we don't have any error messages

# casper.on "step.complete", (page) ->
#   @test.assertEval ->
#     !$('div#content').text().match(/(notice|warning|error|exception)/i)
#   , "no notices, warnings, errors or exceptions on page"


casper.test.done()
