Slowest RS dynamics
========================================================

Here we try to find slowest (or bunch of slowest regionservers and look into their response change over time). I haven't used all available data, only two-three days, not more.


```r
source ("probe_io.R")
library(plyr)
```


```r
from_date <- as.POSIXct("2014-07-21")
to_date <- as.POSIXct("2014-08-05")
```

Data range for analisys 2014-07-21 -- 2014-08-05, load it:

```r
  # add one day to upper bound
  to_date <- to_date + 24*60*60
  files <- c()  
  for (file in list.files("data", full.names=T)) {
    d <- probe_date(file)
    if (d >= from_date && d <= to_date) {
      files <- append(files, file)
    }
  }

  str(files)
```

```
##  chr [1:281] "data/2014-07-21_01.txt" "data/2014-07-21_03.txt" ...
```

Get slowest regionservers on first probe (file data/2014-07-21_01.txt)


```r
read_rs_mean_ord <- function(fname) {
  first <- read_probe(fname, semicolon=T)
  first_rs_mean <- ddply(first, .(RS), summarise, time=mean(Time))
  first_rs_mean[order(first_rs_mean$time, decreasing=T),]
}

ord_first <- read_rs_mean_ord(files[1])
head(ord_first, n=20)
```

```
##              RS  time
## 280  prdhdp48.p 27933
## 174 prdhdp315.p  9517
## 30  prdhdp185.p  7720
## 191 prdhdp331.p  7240
## 133 prdhdp279.p  7121
## 154 prdhdp298.p  7064
## 128 prdhdp274.p  6929
## 157   prdhdp2.p  6902
## 224 prdhdp361.p  6767
## 206 prdhdp345.p  6501
## 218 prdhdp356.p  6439
## 298  prdhdp64.p  6419
## 150 prdhdp294.p  6408
## 180 prdhdp320.p  6352
## 170 prdhdp311.p  6168
## 65  prdhdp217.p  6101
## 233  prdhdp36.p  6090
## 255  prdhdp38.p  6059
## 305  prdhdp70.p  5977
## 95  prdhdp244.p  5971
```

```r
plot(ord_first$time, type='l')
```

![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-4.png) 

Get list of slowest regionservers over time

```r
slow_rses_over_time <- NULL
rs_times <- NULL

for (file in files) {
  ord <- read_rs_mean_ord(file)
  date <- probe_date(file)
  slow_rses_over_time <- rbind(slow_rses_over_time,
                               data.frame(date=date, rs=ord[1,]$RS, time=ord[1,]$time))
  rs_times <- rbind(rs_times,
                    cbind(date=date, ord))
}

head(slow_rses_over_time)
```

```
##                  date          rs  time
## 1 2014-07-21 01:00:00  prdhdp48.p 27933
## 2 2014-07-21 03:00:00   prdhdp1.p 13713
## 3 2014-07-21 05:00:00  prdhdp20.p 13659
## 4 2014-07-21 09:00:00   prdhdp8.p 32728
## 5 2014-07-21 15:00:00 prdhdp252.p  8940
## 6 2014-07-21 17:00:00 prdhdp189.p  7659
```

Hmm, in slowest list we have lots of the same regionservers:

```r
rses_count <- ddply(slow_rses_over_time, .(rs), summarise, count=length(rs))
rses_count <- rses_count[order(rses_count$count, decreasing=T),]
head(rses_count)
```

```
##             rs count
## 82  prdhdp61.p    29
## 93  prdhdp80.p    26
## 79  prdhdp57.p    25
## 28 prdhdp210.p     9
## 83  prdhdp62.p     8
## 44 prdhdp252.p     7
```

So, it's clear that slowest RS are not random, let's look on their benchmark over time.


```r
# take first and second most frequent slow rs regionservers
rs_1 <- rses_count$rs[1]
rs_2 <- rses_count$rs[2]
# take one RS which haven't been in slowest
rs_ok <- setdiff(rs_times$RS, rses_count$rs)[2]
plot(rs_times[rs_times$RS == rs_1, c('date', 'time')], type='l', col='red')
lines(rs_times[rs_times$RS == rs_2, c('date', 'time')], type='l', col='blue')
lines(rs_times[rs_times$RS == rs_ok, c('date', 'time')], type='l', col='black')
```

![plot of chunk unnamed-chunk-7](figure/unnamed-chunk-7.png) 

It looks like an effect caused by lack of compaction.
