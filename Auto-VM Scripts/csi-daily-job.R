# CSI Dashboard Daily Update Job
# 20170531


# check if log directory exists if not then create -------------------------------------------------
  
  setwd('../')
  dir_path <- getwd()

  # check/create log directory
  dir1 <- paste0(dir_path, '/logs')
  dir_exists <- dir.exists(dir1)
  if (!isTRUE(dir_exists)) {
    dir.create(path = dir1)
  }

  # check/create ciipo-weekly directory
  dir2 <- paste0(dir_path, '/logs/csi-daily/')
  dir_exists2 <- dir.exists(dir2)
  if (!isTRUE(dir_exists2)) {
    dir.create(path = dir2)
  }
  
  
# run update function ------------------------------------------------------------------------------
  
  # start log 
  log_name <- paste0(dir2, 'csi-daily-', format(Sys.time(), '%Y-%m-%d_%H%M%S'), '.txt')
  sink(log_name)
  
  tryCatch({
    rIA::update_csi_daily()
  }, error = function(cond){
    cat('CSI daily data is already up-to-date')
  })

  # end log 
  sink()

  