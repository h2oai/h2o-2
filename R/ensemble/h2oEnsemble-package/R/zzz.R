.onAttach <- function(...) {
  packageStartupMessage('h2oEnsemble (beta)')
  packageStartupMessage('Version: ', utils::packageDescription('h2oEnsemble')$Version)
  packageStartupMessage('Package created on ', utils::packageDescription('h2oEnsemble')$Date, '\n')
}
