Test of probe reading
======================
This document is an example and test code for probe reading. Normally, it will read one probe sample, which consists from list of regions with their regionserver and probe time.


```r
source ("probe_io.R")
library(plyr)
```

This code reads probe data into data frame

```r
df <- read_probe("data/2014-08-07_02.txt")
summary(df)
```

```
##            RS       
##  prdhdp214.p:   80  
##  prdhdp233.p:   80  
##  prdhdp251.p:   79  
##  prdhdp258.p:   79  
##  prdhdp185.p:   78  
##  prdhdp238.p:   78  
##  (Other)    :20731  
##                                                                        Region     
##  apps,,1383138270892.014d173583793274dc7f373e6504740f.                    :    1  
##  apps,Eysio2flIJvVkHEcj9nb,1387446006780.a52d391f1653263137cacc0f6098bd4b.:    1  
##  apps,IyEvnfjeXgjx5Z3B3mbP,1387446006780.318e1d864171397b0102f518d0b0b6bc.:    1  
##  apps,k-RPLtI803e9qguRAhGg,1387430705923.2efbb005d96d887b486e0617996fea85.:    1  
##  apps,MwmmcZSDitz2PmqNgCor,1387446017533.ddb220cbe57e8a210dff63d9cd939855.:    1  
##  apps,QvpBPx6ADT6eDeFjXi2W,1387446017533.2c90c29d45b8f21754f82a67c5dcbf9d.:    1  
##  (Other)                                                                  :21199  
##       Time       
##  Min.   :     5  
##  1st Qu.:  2368  
##  Median :  3637  
##  Mean   :  5417  
##  3rd Qu.:  6343  
##  Max.   :285961  
## 
```

```r
str(df)
```

```
## 'data.frame':	21205 obs. of  3 variables:
##  $ RS    : Factor w/ 319 levels "prdhdp10.p","prdhdp11.p",..: 43 167 1 49 146 108 141 175 230 91 ...
##  $ Region: Factor w/ 21205 levels "apps,,1383138270892.014d173583793274dc7f373e6504740f.",..: 1 2 3 5 6 8 4 7 9 10 ...
##  $ Time  : num  3120 1796 2144 1917 1994 ...
```

Group reply time by regionserver and average

```r
d <- ddply(df, .(RS), summarise, time=mean(Time), count=length(Time))
# order by mean time
od <- d[order(d$time, decreasing=T),]
head(od, n=20)
```

```
##              RS  time count
## 278  prdhdp43.p 28733    62
## 246  prdhdp37.p 27319    60
## 13  prdhdp167.p 27296    61
## 308  prdhdp71.p 26466    60
## 305  prdhdp69.p 24096    61
## 48   prdhdp19.p 23861    62
## 277  prdhdp42.p 23730    64
## 37   prdhdp18.p 19045    63
## 181  prdhdp31.p 11349    63
## 22  prdhdp176.p 11096    75
## 2    prdhdp11.p 10226    66
## 61  prdhdp210.p  9991    73
## 225  prdhdp35.p  8901    68
## 45  prdhdp197.p  8774    68
## 60   prdhdp20.p  8543    66
## 10  prdhdp164.p  8542    72
## 63  prdhdp212.p  8390    64
## 126  prdhdp26.p  8347    64
## 56  prdhdp206.p  8303    70
## 64  prdhdp213.p  8104    68
```

Plots of regions in top-10 regionservers

```r
for (i in seq(10)) {
  a <- df[df$RS == od[i, "RS"],]
  plot(a$Time, type='l', main=od[i, "RS"])
}
```

![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-41.png) ![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-42.png) ![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-43.png) ![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-44.png) ![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-45.png) ![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-46.png) ![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-47.png) ![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-48.png) ![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-49.png) ![plot of chunk unnamed-chunk-4](figure/unnamed-chunk-410.png) 


Group reply time by regionserver and max

```r
dm <- ddply(df, .(RS), summarise, time=max(Time), count=length(Time))
odm <- dm[order(dm$time, decreasing=T),]
head(odm, n=20)
```

```
##              RS   time count
## 2    prdhdp11.p 285961    66
## 305  prdhdp69.p 170636    61
## 277  prdhdp42.p 158126    64
## 278  prdhdp43.p 149212    62
## 22  prdhdp176.p 144273    75
## 308  prdhdp71.p 142980    60
## 66  prdhdp215.p 129006    74
## 48   prdhdp19.p 123036    62
## 126  prdhdp26.p 121206    64
## 76  prdhdp224.p 111718    73
## 37   prdhdp18.p 111193    63
## 13  prdhdp167.p 106634    61
## 246  prdhdp37.p 102706    60
## 84  prdhdp231.p  92018    70
## 181  prdhdp31.p  85813    63
## 296  prdhdp60.p  85755    62
## 225  prdhdp35.p  79135    68
## 27  prdhdp180.p  76734    73
## 106 prdhdp251.p  75968    79
## 43  prdhdp195.p  74990    65
```

Top-20 regions by time

```r
odf <- df[order(df$Time, decreasing=T),]
head(odf, n=20)
```

```
##                RS
## 9927   prdhdp11.p
## 20070  prdhdp69.p
## 16100  prdhdp42.p
## 12285  prdhdp43.p
## 7570  prdhdp176.p
## 17777  prdhdp71.p
## 14409  prdhdp71.p
## 2697  prdhdp215.p
## 14269  prdhdp71.p
## 14712  prdhdp19.p
## 874    prdhdp26.p
## 11083 prdhdp224.p
## 15357  prdhdp18.p
## 20101 prdhdp167.p
## 16153  prdhdp37.p
## 15902  prdhdp69.p
## 13797 prdhdp167.p
## 16048  prdhdp19.p
## 16347  prdhdp69.p
## 14329  prdhdp37.p
##                                                                                     Region
## 9927                       webpagesII,dv--,1374142198287.3529f70a06685551599aa540b857f37f.
## 20070                   webpages_hash,ss--,1370244275201.744c8a425c98656ba60335a6013eddc1.
## 16100                   webpages_hash,Ot--,1370244275053.b080b740372a820fab318035679e659c.
## 12285                      webpagesII,wF--,1374142198383.4b0756eba7daff7b9baa419c7fc0ca37.
## 7570                       webpagesII,MY--,1374142198191.edfc02460462b4c4e49b9a54fca4f9ed.
## 17777                   webpages_hash,az--,1370244275117.cf4187c0f63334ba4a5ec96310a97222.
## 14409                   webpages_hash,BjV-,1370244274988.3abcf974511f3fc898d1f49da70b7da8.
## 2697  queries,Y2hpamlrIHBpamlrIHBlc255YQ==,1382743983032.42afccbfb486b47e9769bcd6c82f748f.
## 14269                   webpages_hash,AeV-,1370244274982.e8a756f71490581827890c952e42a6b1.
## 14712                   webpages_hash,E4V-,1370244274999.ed059c761db7a7e5a4f237b97f44b18d.
## 874            images,S-------------------,1370422967881.eec8c93b82592a0ba604a09c437d9899.
## 11083                      webpagesII,mvV-,1374142198334.b0fadbf6811c29679507ded050c80549.
## 15357                   webpages_hash,J4V-,1370244275023.e9a053d9c9d94df99296bc29477b3eb5.
## 20101                   webpages_hash,t6V-,1370244275202.d790411168045db49441f765fcd32a3c.
## 16153                   webpages_hash,PIV-,1370244275055.ea5ae88b5ccad86240099b5bbd317a24.
## 15902                   webpages_hash,NL--,1370244275044.97080b3452853f70065571e84f92e669.
## 13797                   webpages_hash,6yV-,1370244274964.f175b21a022de1314812a2964c45a335.
## 16048                   webpages_hash,OU--,1370244275051.ba88666e6237f758ca5d8a6be2c312e3.
## 16347                   webpages_hash,QoV-,1370244275062.9d3363d45078121af307dbdeba6268b0.
## 14329                   webpages_hash,B7V-,1370244274985.203345d5a8301c2e4626dfd9ec976bcf.
##         Time
## 9927  285961
## 20070 170636
## 16100 158126
## 12285 149212
## 7570  144273
## 17777 142980
## 14409 141830
## 2697  129006
## 14269 125355
## 14712 123036
## 874   121206
## 11083 111718
## 15357 111193
## 20101 106634
## 16153 102706
## 15902 102225
## 13797 101460
## 16048 100156
## 16347  99981
## 14329  98774
```

Optimization
---------------------
Initial implementation of read_probe is not too great.

    system.time(df <- read_probe("data/2014-08-07_02.txt"))
     user  system elapsed 
     3.42    0.00    3.41 

The source of slowdown is the need to parse line-by-line. After switch to tab-separated format:

    system.time(df2 <- read_probe_tabs("data/2014-08-07_02-tabs.txt"))
     user  system elapsed 
     0.276   0.000   0.272
