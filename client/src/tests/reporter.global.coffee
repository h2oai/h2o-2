if testStatus.fail is 0
  console.log clc.green "\n#{testStatus.pass} tests passed!\n"
else
  console.log clc.red "\n*** FAILURES : #{testStatus.fail} of #{testStatus.total} tests failed. ***\n"
