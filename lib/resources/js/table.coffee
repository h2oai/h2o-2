# @heatMap = (valuePosition) =>
#     DEFAULT_KLASS = "heat0"

#     if isNaN(valuePosition)
#         return DEFAULT_KLASS

#     klasses = ['heat1', 'heat2', 'heat3', 'heat4', 'heat5', 'heat6', 'heat7', 'heat8', 'heat9']
#     step = 1.0 / klasses.length

#     for i in [0..klasses.length]
#         min = if i == 0 then -Infinity else (i * step)
#         max = if i == (klasses.length - 1) then +Infinity else ((i + 1) * step)

#         return klasses[i] if valuePosition >= min and valuePosition < max

#     return DEFAULT_KLASS

tableApp = angular.module('h2o.table', [])

tableApp.directive "showHeaderOnHover", ($parse) ->
    restrict: 'A'
    link: (scope, element, attrs) ->
        fauxTable       = null
        fauxHoveredRow  = null 
        cachedColumnWidths = []
        cachedColumnOuterWidths = []

        createFauxTable = ($td, $tr, e) ->
            trOffset    = $tr.offset()

            firstContinousVisibleRange = ($elements) ->
                $window         = $(window)
                scrollLeft      = $window.scrollLeft()
                scrollTop       = $window.scrollTop()
                windowWidth     = $window.width()
                windowHeight    = $window.height()

                i       = 0
                start   = 0
                end     = 0

                inRange = false

                for el in $elements
                    $el = $(el)
                    isVisible = $el.isOnScreen scrollTop, scrollLeft, windowWidth, windowHeight

                    if isVisible
                        if inRange
                            end     = i + 1
                        else if $el.is(':visible')
                            start   = i
                            inRange = true
                    else 
                        if inRange
                            end     = i
                            break

                    i += 1


                {start: start, end: end}

            if not fauxTable?
                cachedColumnWidths      = []
                cachedColumnOuterWidths = []
                $tr.children().each (i, e) ->
                    $e = $(e)
                    if $e.is(':visible')
                        cachedColumnWidths.push $e.width()
                        cachedColumnOuterWidths.push $e.outerWidth()
                    else
                        cachedColumnWidths.push 0
                        cachedColumnOuterWidths.push 0

                fauxTable = $('<table></table>')
                fauxTable.addClass 'header-hover-table'
                fauxTable.addClass element.attr('class')
                fauxTable.css
                    position: "absolute"
    
                $("body").append fauxTable

            fauxTable.empty()

            header          = element.find("tr").first()
            headerCells     = $(header).children()
            rowCells        = $tr.children()
            visibleRange    = firstContinousVisibleRange rowCells

            if cachedColumnWidths 
                cellsInRangeWidths              = cachedColumnWidths.slice visibleRange.start, visibleRange.end
                cellsInRangeOuterWidths         = cachedColumnOuterWidths.slice visibleRange.start, visibleRange.end
                cellsToTheLeftOuterWidths       = []

                if visibleRange.start
                    cellsToTheLeftOuterWidths   = cachedColumnOuterWidths.slice 0, visibleRange.start
            else
                cellsInRangeWidths              = []
                cellsInRangeOuterWidths         = []
                cellsToTheLeftOuterWidths       = []

            left    = cellsToTheLeftOuterWidths.sum()
            width   = cellsInRangeOuterWidths.sum()
            
            fauxTable.css
                top: trOffset.top
                left: left
                width: width

            fauxHeader      = $("<tr class=\"faux-header-black\"></tr>")
            fauxHoveredRow  = $("<tr></tr>")

            fauxHeader.append $(headerCells.slice visibleRange.start, visibleRange.end).clone()
            fauxHoveredRow.append $(rowCells.slice visibleRange.start, visibleRange.end).clone()

            $(fauxHeader.children()[1]).addClass 'first'

            i = 0
            for cellWidth in cellsInRangeWidths
                $(fauxHeader.children()[i]).width cellWidth

                # Mark penultimate header cell
                if (i == cellsInRangeWidths.length - 2)
                    $(fauxHeader.children()[i]).addClass 'penultimate' 

                i += 1

            fauxTable.append fauxHeader
            fauxTable.append fauxHoveredRow

        destroyFauxTable = () ->
            fauxTable.detach() if fauxTable
            fauxTable = null          

        element.on 'mouseover', 'td', (e) ->
            $td         = $(@)
            $tr         = $($td.parent())

            if $tr.data("type") == "data"
                createFauxTable($td, $tr, e)
            else
                destroyFauxTable()

        element.bind 'didReload', (e) -> 
            destroyFauxTable()

        return

tableApp.directive "reorderable", ($parse) ->
    restrict: 'A'
    link: (scope, element, attrs) ->
        reorderableModel = $parse(attrs.reorderable)

        element.dragtable
            boundary: 'drag-boundary'
            topOffset: 200
            stop: () ->
                scope.$apply () =>
                    reorderableModel(scope) element.dragtable("order")


tableApp.directive "d3table", ($parse) ->
    restrict: 'A'
    link: (scope, element, attrs) ->
        datasetModel        = $parse attrs.dataset
        headersModel        = $parse attrs.headers
        summariesModel      = $parse attrs.summaries
        headerClassmapModel = $parse attrs.headerClassmap

        vis = d3.select(element[0])

        thead = vis.append("thead")
        tbody = vis.append("tbody")
        thead_tr = thead.append("tr")

        reload = () ->
            return if not headersModel(scope)? or not datasetModel(scope)?

            prepareSummaryFunction = (row) =>
                (column) =>
                    if (column.name == "row")
                        value = row[1]
                    else
                        value = column[row[0]]

                    klass = if column.shown then "" else "hidden"

                    return {
                        klass: klass
                        value: value
                    }

            prepareRowFunction = (row) =>
                (column) =>
                    value = row[column.name]
                    # valuePosition = (value - column.min) / (column.max - column.min)

                    # klass = if column.shown then heatMap(valuePosition) else "hidden"
                    klass = ""
                    if not column.shown
                        klass = "hidden" 
                    else
                        klass = "na" if value == "NA"

                    return {
                        value: value
                        klass: klass
                    }

            update = (items, funcDict) ->
                added   = items.enter()
                removed = items.exit()
                updated = items

                funcDict.update items
                funcDict.update funcDict.style(funcDict.add added)
                funcDict.remove removed

            thFuncs = 
                add:    (item) -> item.append("th")
                remove: (item) -> item.remove()
                update: (item) -> item.html((c) -> c.visual)
                                    .attr("data-header", (c) -> c.name)
                                    .attr("class", (c) -> headerClassmapModel(scope)[c.name] ? "")
                                    .style("display", (c) -> if (c.shown ? true) then "table-cell" else "none")
                style:  (item) -> item

            summaryRowsFuncs = 
                add:    (item) -> item.append("tr")
                remove: (item) -> item.remove()
                update: (item) -> item
                style:  (item) -> item.attr("data-type", "summary")
                                    .attr('class', (c) -> "summary #{c[0]}")
            dataRowsFuncs = 
                add:    (item) -> item.append("tr")
                remove: (item) -> item.remove()
                update: (item) -> item
                style:  (item) -> item.attr("data-type", "data")
                                    .attr('class', (c) -> "row#{c.row}")

            summaryCellsFuncs = 
                add:    (item) -> item.append("td")
                remove: (item) -> item.remove()
                update: (item) -> item.attr('class', (item) -> item.klass)
                                        .html((item) -> item.value)
                style:  (item) -> item

            dataCellsFuncs = 
                add:    (item) -> item.append("td")
                remove: (item) -> item.remove()
                update: (item) -> item.attr('class', (item) -> item.klass)
                                        .html((item) -> item.value)
                style:  (item) -> item

            update thead_tr.selectAll("th").data(headersModel(scope)), thFuncs

            summaryRows = tbody.selectAll("tr[data-type=\"summary\"]").data(summariesModel(scope))
            dataRows = tbody.selectAll("tr[data-type=\"data\"]").data(datasetModel(scope))
            update summaryRows, summaryRowsFuncs
            update dataRows, dataRowsFuncs

            summaryCells = summaryRows.selectAll("td").data((row) => headersModel(scope).map prepareSummaryFunction(row))
            dataCells = dataRows.selectAll("td").data((row) => headersModel(scope).map prepareRowFunction(row))
            update summaryCells, summaryCellsFuncs
            update dataCells, dataCellsFuncs

            element.trigger "didReload"

        scope.$watch headersModel, (newVal, oldVal) ->
            # console.log "NEW HEADERS", newVal
            return if not newVal
            reload()
