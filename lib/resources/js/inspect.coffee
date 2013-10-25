inspectApp = angular.module('h2o.inspectApp', ['h2o.app', 'h2o.datasource', 'h2o.table', 'h2o.utilities', 'h2o.widgets'])

inspectApp.controller 'InspectAppController', ($scope, InspectDataService, H2OMenuService) ->
    $scope.InspectDataService   = InspectDataService
    $scope.H2OMenuService       = H2OMenuService

    @init = () =>
        InspectDataService.fetch()

    @init()

inspectApp.controller 'InspectColumnController', ($scope, $log, InspectDataService, InspectColumnService) ->
    $scope.InspectDataService   = InspectDataService
    $scope.InspectColumnService = InspectColumnService

    @init = () =>
        return

    $scope.sortByDefault = () =>
        $scope.reorder @defaultOrder

    $scope.sortByFunc = (valueFunc, ascending=true) =>
        headers = new Array()

        angular.forEach InspectDataService.columns, (c) =>
            headers.push 
                name: c.name
                value: valueFunc c

        if ascending
            sortFunc = (a,b) -> (a.value - b.value)
        else
            sortFunc = (a,b) -> (b.value - a.value)

        headers.sort sortFunc

        $scope.reorder (c.name for c in headers)
        
    $scope.sortByVariance = () =>
        $scope.sortByFunc ((c) => c.variance), false

    $scope.showAllColumns = () =>
        for c in InspectColumnService.filterableTableHeaders
            InspectColumnService.shownTableHeaders[c.name] = true

        InspectColumnService.refilter()

    $scope.hideMissingColumns = () =>
        for c in  InspectColumnService.filterableTableHeaders
            noValues = c.num_missing_values == InspectDataService.numRows
            InspectColumnService.shownTableHeaders[c.name] = !noValues

        InspectColumnService.refilter()

    @init()

inspectApp.controller 'InspectTableController', ($scope, $log, InspectDataService, InspectColumnService) ->
    $scope.InspectDataService   = InspectDataService
    $scope.InspectColumnService = InspectColumnService

    $scope.tableHeaders = []
    $scope.tableData = []

    @init = () =>
        $scope.$watch 'InspectColumnService.filteredTableHeaders', (newVal, oldVal, scope) =>
            $scope.tableHeaders = newVal ? []
        $scope.$watch 'InspectColumnService.tableData', (newVal, oldVal, scope) =>
            $scope.tableData = newVal ? []

        return

    $scope.reorder = (newOrder) =>
        newOrderedTableHeaders = []
        angular.forEach newOrder, (cName) =>
            newOrderedTableHeaders.push @columnForColumnName cName

        InspectColumnService.orderedTableHeaders = newOrderedTableHeaders

    @columnForColumnName = (cName) ->
        result = null
        angular.forEach InspectColumnService.tableHeaders, (c) =>
            result = c if c.name == cName

        result

    @init()

inspectApp.controller 'InspectPaginationController', ($scope, $log, InspectDataService) ->
    $scope.InspectDataService = InspectDataService

    $scope.offset = 0
    $scope.limit = 0
    $scope.firstRow = 0
    $scope.lastRow = 0

    $scope.pageOffset = 0
    $scope.firstPage = 0
    $scope.lastPage = 0
    $scope.canGoToNextPage = false
    $scope.canGoToPrevPage = false
    $scope.isLoading = false

    @init = () ->
        $scope.$watch 'InspectDataService.numRows', (newVal, oldVal, scope) =>
            $scope.lastRow = newVal if newVal
            @recalculatePages()
        $scope.$watch 'InspectDataService.offset', (newVal, oldVal, scope) =>
            $scope.offset = newVal 
            @recalculatePages()
        $scope.$watch 'InspectDataService.limit', (newVal, oldVal, scope) =>
            $scope.limit = newVal if newVal
            @recalculatePages()
        $scope.$watch 'InspectDataService.status', (newVal, oldVal, scope) =>
            @recalculatePages()
    
    @offsetFromPage = (page) ->
        page * ($scope.limit ? InspectDataService.defaultLimit)

    @recalculatePages = () ->
        if not $scope.limit
            newPageOffset = 0
            newLastPage = 0
        else
            newPageOffset = Math.ceil($scope.offset / $scope.limit)
            newLastPage = Math.floor($scope.lastRow / $scope.limit)
        
        $scope.pageOffset = newPageOffset
        $scope.lastPage = newLastPage

        $scope.isLoading = $scope.InspectDataService.isLoading

        $scope.canGoToNextPage = not $scope.isLoading and $scope.pageOffset < $scope.lastPage
        $scope.canGoToPrevPage = not $scope.isLoading and $scope.pageOffset > 0

    $scope.fetch = () =>
        # $log.log "Fetching:", $scope.pageOffset, $scope.offset

        InspectDataService.offset = @offsetFromPage $scope.pageOffset
        InspectDataService.limit = $scope.limit

        @recalculatePages()

        InspectDataService.fetch()

    $scope.nextPage = () ->
        return if not $scope.canGoToNextPage

        $scope.pageOffset += 1
        $scope.fetch()

    $scope.prevPage = () ->
        return if not $scope.canGoToPrevPage

        $scope.pageOffset -= 1
        $scope.fetch()

    $scope.pageSliderTooltipValue = (pageOffset) =>
        "row #{ @offsetFromPage(pageOffset) }"

    @init()
