def pp_rf_result(rf):
    jcm = rf['confusion_matrix']
    header = jcm['header']
    cm = ' '.join(header)
    c = 0
    for line in jcm['scores']:
        lineSum  = sum(line)
        errorSum = lineSum - line[c]
        if (lineSum>0): 
            err = float(errorSum) / lineSum
        else:
            err = 0.0
        cm = "{0}\n {1} {2} {3}".format(cm, header[c], ' '.join(map(str,line)), err)
        c += 1

    return """
 Leaves: {0} / {1} / {2}
  Depth: {3} / {4} / {5}
   mtry: {6}
   Type: {7}
    Err: {8} %
   Time: {9} seconds

   Confusion matrix:
      {10}
""".format(
        rf['trees']['leaves']['min'],
        rf['trees']['leaves']['mean'],
        rf['trees']['leaves']['max'],
        rf['trees']['depth']['min'],
        rf['trees']['depth']['mean'],
        rf['trees']['depth']['max'],
        rf['mtry'], 
        rf['confusion_matrix']['type'],
        rf['confusion_matrix']['classification_error'] *100,
        rf['response']['time'],
        cm)

