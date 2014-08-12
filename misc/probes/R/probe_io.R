# Reads probe sample (semicolon-separated) into data frame
# read_probe <- function (fname) {
#   RS <- c()
#   Region <- c()
#   Time <- c()
#   for (l in readLines(fname)) {
#     f <- strsplit(l, ";")[[1]]
#     if (length(f) == 3) {
#       RS <- append (RS, f[1])
#       Region <- append (Region, f[2])
#       Time <- append (Time, as.numeric(f[3]))
#     }
#   }
#   
#   df <- data.frame(RS=RS, Region=Region, Time=Time)
#   df
# }

read_probe <- function(fname, semicolon) {
  if (semicolon)
    fd <- pipe(paste("python semi2tabs.py <", fname))
  else
    fd <- fname
  read_probe_tabs(fd)
}

# Read data from tab-separated format of probe
read_probe_tabs <- function (fname) {
  df <- read.csv(fname, header=F, sep='\t', col.names = c("RS", "Region", "Time"))
  df$Time <- as.numeric(df$Time)
  df
}


# deduct date of probe from file name.
# We expect format of file name as year-month-day_hour with possible prefix or postfix
probe_date <- function (fname) {
  name <- basename(fname)
  strptime(name, "%Y-%m-%d_%H")
}


read_probes <- function (names, semicolon=F) {
  first <- T
  for (name in names) {
    df <- merge(data.frame(Date=probe_date(name)), read_probe(name, semicolon))
    if (first)
      res <- df
    else
      res <- rbind(res, df)
    first <- F
  }
  res
}

