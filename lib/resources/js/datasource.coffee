@HTTP_STATUS =
    "ok": 0
    "loading": 1
    "error": 2

dataSourceApp = angular.module('h2o.datasource', [])

dataSourceApp.directive "statusFromDataService", (InspectDataService) ->
    restrict: "A"
    link: (scope, element, attrs) ->
        jqElement = $(element)
        
        elementClass = () ->
            return "ok" if InspectDataService.status == HTTP_STATUS.ok
            return "loading" if InspectDataService.status == HTTP_STATUS.loading
            return "error" if InspectDataService.status == HTTP_STATUS.error
            ""
        elementText = () ->
            if InspectDataService.status == HTTP_STATUS.ok
                miliseconds = InspectDataService.meta.processingTime
                seconds = miliseconds / 1000.0
                if miliseconds < 5000
                    return "Produced in #{ seconds.toFixed(3) } sec." 
                else
                    return "Produced in #{ moment.duration(miliseconds).humanize() }." 
            if InspectDataService.status == HTTP_STATUS.loading
                return "Loading..." 
            if InspectDataService.status == HTTP_STATUS.error
                return "#{InspectDataService.error}"
            ""

        updateElement = () ->
            jqElement.removeClass('ok').removeClass('loading').removeClass('error')

            jqElement.addClass elementClass()
            jqElement.html "<div class=\"message\">#{elementText()}"

        scope.$watch 'InspectDataService.meta', updateElement
        scope.$watch 'InspectDataService.error', updateElement
        scope.$watch 'InspectDataService.status', updateElement

dataSourceApp.directive "datasourcespinner", (InspectDataService) ->
    restrict: "E"
    template: '<div class="spinner-wrapper"><div class="spinner"><i class="icon-spinner icon-spin icon-large"></i><button class="retry-button">Retry <i class="icon-repeat"></i></button></div></div>'
    replace: true
    link: (scope, element, attrs) ->
        jqElement = $(element)
        jqSpinner = jqElement.find("i.icon-spinner")
        jqRetryButton = jqElement.find("button")

        jqRetryButton.click () ->
            InspectDataService.fetch()

        updateElement = () ->
            if InspectDataService.status == HTTP_STATUS.loading or InspectDataService.status == HTTP_STATUS.error
                jqElement.stop().fadeIn()
            else
                jqElement.stop().fadeOut()

            if InspectDataService.status == HTTP_STATUS.error
                jqSpinner.hide()
                jqRetryButton.show()
            else
                jqSpinner.show()
                jqRetryButton.hide()

        scope.$watch 'InspectDataService.status', updateElement

dataSourceApp.service 'InspectDataService', ($http, $log, $rootScope) ->
    @offset = 0
    @defaultLimit = 100
    @maxLimit = 1000
    @limit = @defaultLimit

    @data = {}
    @columns = []
    @numCols = 0
    @numRows = 0

    @meta = 
        key: ""
        rowSize: 0
        valueSizeBytes: 0
        processingTime: 0
    @error = ""
    @status = HTTP_STATUS.ok
    @isLoading = false

    @validateLimit = () =>
        intLimitValue = parseInt @limit
        isIntLimitValueInvalid = isNaN(intLimitValue) or intLimitValue <= 0

        if isIntLimitValueInvalid
            @limit = @defaultLimit
        else 
            if intLimitValue > @maxLimit
                @limit = @maxLimit
            else
                @limit = intLimitValue

    @setStatus = (status) =>
        @status = status

        if status == HTTP_STATUS.ok
            @isLoading = false
            @error = ""
        if status == HTTP_STATUS.loading
            @isLoading = true
            @error = ""
        if status == HTTP_STATUS.error
            @isLoading = false            

    @apiURI = () =>
        JSONApiServerURI()

    @apiFetchURIString = () =>
        @apiURI().addQuery
            offset: @offset
            view: @limit if @limit?
        .toString()

    @checkColumnValidity = (column, data) =>
        if not column.base?
            $log.log("'base' missing from column", column)
            return false
        if not column.max?
            $log.log("'max' missing from column", column)
            return false
        if not column.min?
            $log.log("'min' missing from column", column)
            return false
        if not column.mean?
            $log.log("'mean' missing from column", column)
            return false
        if not column.num_missing_values?
            $log.log("'num_missing_values' missing from column", column)
            return false            
        if not column.name?
            $log.log("'name' missing from column", column)
            return false
        if not column.variance?
            $log.log("'name' missing from column", column)
            return false
        if not column.type?
            $log.log("'type' missing from column", column)
            return false

        true

    @checkRowValidity = (row, data) =>
        if not row.row?
            $log.log("'row' missing from row", row)
            return false

        true

    @checkResponseValidity = (data) =>
        if not data.key?
            $log.log("'key' missing from response")
            return false
        if not data.row_size?
            $log.log("'row_size' missing from response")
            return false
        if not data.value_size_bytes?
            $log.log("'value_size_bytes' missing from response")
            return false

        if not data.num_rows?
            $log.log("'num_rows' missing from response")
            return false
        if not data.num_cols?
            $log.log("'num_cols' missing from response")
            return false
        if not data.cols?
            $log.log("'cols' missing from response")
            return false
        if not data.rows?
            $log.log("'rows' missing from response")
            return false
        if not data.status == "done"
            $log.log("Wrong status: ", data.status)
            return false

        colsValid = true
        angular.forEach data.cols, (c) =>
            colsValid = false if not @checkColumnValidity c, data

        if not colsValid
            $log.log("Columns not valid: ", data.cols)
            return false

        rowsValid = true
        angular.forEach data.rows, (r) =>
            rowsValid = false if not @checkRowValidity r, data

        if not rowsValid
            $log.log("Rows not valid: ", data.cols)
            return false

        true

    @fetch = (newOffset) =>
        @setStatus HTTP_STATUS.loading

        # $log.log "Fetching:", @offset, newOffset

        $http
            method: "GET"
            url: @apiFetchURIString()
        .success (data, status, headers, config) =>
            if not @checkResponseValidity data
                @error = data.error ? 'Received data is not valid'
                
                @setStatus HTTP_STATUS.error
                
                return

            newColumns = []
            newColumns.push 
                name: "row"
                visual: "Row"
                unfilterable: true
            angular.forEach data.cols, (c) ->
                # $log.log "Found column", c
                c.visual = c.name
                newColumns.push c

            newRows = []
            angular.forEach data.rows, (r) ->
                # $log.log "Found row", r
                newRows.push r

            @numRows = data.num_rows
            @numCols = data.num_cols
            @rows = newRows
            @columns = newColumns
            @limit = if @rows.length > 0 then @rows.length else null

            @meta = 
                key: data.key
                rowSize: data.row_size
                valueSizeBytes: data.value_size_bytes
                processingTime: data.response.time ? 0

            @setStatus HTTP_STATUS.ok

            $log.log('Done', @numRows, @numCols, data)

        .error (data, status, headers, config) =>
            $log.log('Error');
            @error = "Could not communicate with the backend."
            @setStatus HTTP_STATUS.error

dataSourceApp.service 'InspectColumnService', ($http, $log, $rootScope, InspectDataService) ->
    @tableData = []
    @tableHeaders = []

    @filterableTableHeaders = []
    @orderedTableHeaders = []
    @shownTableHeaders = {}
    
    @filteredTableHeaders = []

    @columnsShown = 0

    @defaultOrder = []

    @init = () =>
        prepareDataFunc = (newVal, oldVal, scope) =>
            # $log.log "prepareDataFunc", newVal, oldVal, scope
            return if not newVal?
            @prepareData()

        $scope = $rootScope.$new()

        $scope.$watch (() -> InspectDataService.rows), prepareDataFunc
        $scope.$watch (() -> InspectDataService.columns), prepareDataFunc

    @prepareData = () =>
        newTableData = []
        newDefaultOrder = []
        newTableHeaders = []
        newFilterableTableHeaders = []

        angular.forEach InspectDataService.columns, (c) =>
            newTableHeaders.push c
            newDefaultOrder.push c.name
            newFilterableTableHeaders.push c if not (c.unfilterable ? false)

        angular.forEach InspectDataService.rows, (r) =>
            newTableData.push r

        @defaultOrder = newDefaultOrder
        @tableData = newTableData
        @tableHeaders = newTableHeaders
        @filterableTableHeaders = newFilterableTableHeaders

        # $log.log "Prepared data: ", @filterableTableHeaders

        @refilter()

    @resetFiltering = () =>
        angular.forEach @tableHeaders, (c) =>
            @shownTableHeaders[c.name] = true

        @orderedTableHeaders = @tableHeaders

    @refilter = () =>
        @resetFiltering() if not @orderedTableHeaders? or @orderedTableHeaders.length == 0

        newFilteredTableHeaders = []

        angular.forEach @orderedTableHeaders, (c) =>
            newFilteredTableHeaders.push c
        
        newColumnsShown = 0
        angular.forEach newFilteredTableHeaders, (c) =>
            c.shown = @shownTableHeaders[c.name] ? true
            newColumnsShown += c.shown

        @columnsShown = newColumnsShown
        @filteredTableHeaders = newFilteredTableHeaders

        # $log.log "Refiltered: ", @filterableTableHeaders

    @init()
