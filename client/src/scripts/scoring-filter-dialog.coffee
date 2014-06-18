Steam.ScoringFilterDialog = (_, _filters, go) ->
  createFilterView = (filter) ->
    items = map filter.items, (item) ->
      caption: item.factor.caption
      data: item
      isChecked: node$ item.isSelected
    caption: filter.variable.caption
    data: filter
    items: items

  _filterViews = map _filters, createFilterView

  confirm = -> go 'confirm', map _filterViews, (filterView) ->
    predicate = {}
    for item in filterView.items
      predicate[item.data.factor.value] = item.isChecked()
    predicate

  cancel = -> go 'cancel'

  filters: _filterViews
  confirm: confirm
  cancel: cancel
  template: 'scoring-filter-dialog'


