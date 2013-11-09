## Setup
##########################################################################

utils  = require("utils")
x = require('casper').selectXPath

testhost   = casper.cli.get "testhost"
screenshot = casper.cli.get "screenfile"

casper
  .log("Using testhost: #{testhost}", "info")
  .log("Using screenshot: #{screenshot}", "info")

if not testhost or not screenshot or not /\.(png)$/i.test screenshot
  casper
    .echo("Usage: $ casperjs test ./casper --testhost=<testhost> --screenfile=<screenshot.png>")
    .exit(1)

# ## Testcases
# ##########################################################################

casper.start "http://#{testhost}/Inspect.html?key=test.hex", ->
  @test.assertHttpStatus(200, "Inspect page should load 200 OK")

casper.then ->
  @test.assertTextExists('Year', 'Table has "Year" header')
  @test.assertTextExists('DepTime', 'Table has "DepTime" header')

casper.then ->
  @click ".header .menu .logo"
casper.then ->
  @test.assertEval ->
    $('#h2o-menu-container').hasClass('open')
  , "Left menu shows up when logo clicked."

casper.then ->
  @clickLabel "Data", "li"
casper.then ->
  @test.assertVisible(x("//*[@id=\"h2o-menu-container\"]/ul/ul[1]/li[1]/a"), "Dropdown main menu visible after click")

casper.then ->
  @click ".menu.lighter.right"
casper.then ->
  @test.assertEval ->
    $(".menu.lighter.right").hasClass('open')
  , "Models menu shows up when logo clicked."
casper.then ->
  @clickLabel "Score Data", "li"
casper.then ->
  @test.assertVisible(x("//*[@id=\"model-menu-container\"]/ul/ul[1]/li[1]/a"), "Dropdown in Models visible after click")

casper.then ->
  @click ".menu.light.right"
casper.then ->
  @test.assertEval ->
    $(".menu.light.right").hasClass('open')
  , "Columns menu shows up when logo clicked."
casper.then ->
  @test.assertSelectorHasText(x('//*[@id="column-menu-container"]/div[4]/label/span'), "Year", "Year is the first column in columns list.")

# sort by variance
casper.then ->
  @clickLabel "Variance", 'button'
casper.then ->
  @test.assertSelectorHasText(x('/html/body/div[5]/table/thead/tr/th[2]'), "FlightNum", "Sorting by Variance works.")
casper.then ->
  @clickLabel "Default", 'button'
casper.then ->
  @test.assertSelectorHasText(x('/html/body/div[5]/table/thead/tr/th[2]'), "Year", "Sorting by Default works.")

# select all
casper.then ->
  @clickLabel "Deselect All", 'button'
casper.then ->
  @test.assertNotVisible(x('/html/body/div[5]/table/tbody/tr[2]/td[2]'), "At least first column not visible when DeselectAll clicked")
  @test.assertEval ->
    $("#column-menu-container input:checkbox:not(:checked)").length == 31
  , "All checkboxes not checked after DeselectAll"

casper.then ->
  @clickLabel "Select All", 'button'
casper.then ->
  @test.assertVisible(x('/html/body/div[5]/table/tbody/tr[2]/td[2]'), "At least first column visible when SelectAll clicked")
  @test.assertEval ->
    $("#column-menu-container input:checkbox(:checked)").length == 31
  , "All checkboxes checked after SelectAll"

# pages
casper.thenClick 'button.gray.right', ->
  @waitWhileVisible ".spinner-wrapper"
casper.then ->
  @test.assertSelectorHasText(x('/html/body/div[5]/table/tbody/tr[6]/td[1]'), "100", "Turned to second page.")

casper.thenClick 'button.gray.left', ->
  @waitWhileVisible ".spinner-wrapper"
casper.then ->
  @test.assertSelectorHasText(x('/html/body/div[5]/table/tbody/tr[6]/td[1]'), "0", "Turned to first page.")

# check if NAs are marked
casper.then ->
  @test.assertExists(x("//*[contains(@class,'na')][normalize-space()='NA']"), "At least one NA value has na class.")

# basic column filter
casper.then ->
  @fill "form#column-filter-form", 
    'column-filter': 'year'
casper.then ->
  @test.assertSelectorHasText(x('//*[@id="column-menu-container"]/div[4]/label/span'), "Year", "Year is visible after filtering for 'year'")
  @test.assertDoesntExist(x('//*[@id="column-menu-container"]/div[5]/label/span'), "Only one column visible on list after filtering")

# select / deselect with column filtering
casper.then ->
  @clickLabel "Deselect All", 'button'
casper.then ->
  @test.assertSelectorHasText(x('/html/body/div[5]/table/thead/tr/th[3]'), 'Month', "Deselect All respects filter")
  @test.assertVisible(x('/html/body/div[5]/table/thead/tr/th[3]'))

casper.then ->
  @fill "form#column-filter-form", 
    'column-filter': ''
casper.then ->
  @clickLabel "Deselect All", 'button'
  @fill "form#column-filter-form", 
    'column-filter': 'deptime'
  @clickLabel "Select All", 'button'
casper.then ->
  @test.assertSelectorHasText(x('/html/body/div[5]/table/thead/tr/th[6]'), 'DepTime', "Select All respects filter")
  @test.assertVisible(x('/html/body/div[5]/table/thead/tr/th[6]'))
  @test.assertNotVisible(x('/html/body/div[5]/table/thead/tr/th[3]'))


casper.run ->
  @test.done()
