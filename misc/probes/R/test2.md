Probe series read
========================================================

Here we read series of probes into one large dataframe for later analisys


```r
source("probe_io.R")
```

Deduct probe date from file name

```r
probe_date("data/2014-08-07_02.txt", semicolon=T)
```

```
## Error: unused argument (semicolon = T)
```

Read list of files into one large data frame

```r
  df <- read_probes(c("data/2014-08-07_02.txt"), semicolon=T)
  summary(df)
```

```
##       Date                               RS       
##  Min.   :2014-08-07 02:00:00   prdhdp214.p:   80  
##  1st Qu.:2014-08-07 02:00:00   prdhdp233.p:   80  
##  Median :2014-08-07 02:00:00   prdhdp251.p:   79  
##  Mean   :2014-08-07 02:00:00   prdhdp258.p:   79  
##  3rd Qu.:2014-08-07 02:00:00   prdhdp185.p:   78  
##  Max.   :2014-08-07 02:00:00   prdhdp238.p:   78  
##                                (Other)    :20737  
##                                                                        Region     
##  apps,,1383138270892.014d173583793274dc7f373e6504740f.                    :    1  
##  apps,Eysio2flIJvVkHEcj9nb,1387446006780.a52d391f1653263137cacc0f6098bd4b.:    1  
##  apps,IyEvnfjeXgjx5Z3B3mbP,1387446006780.318e1d864171397b0102f518d0b0b6bc.:    1  
##  apps,k-RPLtI803e9qguRAhGg,1387430705923.2efbb005d96d887b486e0617996fea85.:    1  
##  apps,MwmmcZSDitz2PmqNgCor,1387446017533.ddb220cbe57e8a210dff63d9cd939855.:    1  
##  apps,QvpBPx6ADT6eDeFjXi2W,1387446017533.2c90c29d45b8f21754f82a67c5dcbf9d.:    1  
##  (Other)                                                                  :21205  
##       Time       
##  Min.   :     5  
##  1st Qu.:  2367  
##  Median :  3637  
##  Mean   :  5416  
##  3rd Qu.:  6342  
##  Max.   :285961  
## 
```

Read data for two days:

```r
  system.time(df <- read_probes(c("data/2014-08-06_08.txt", "data/2014-08-06_20.txt", "data/2014-08-07_02.txt",
                                  "data/2014-08-05_01.txt", "data/2014-08-06_02.txt", "data/2014-08-05_11.txt",
                                  "data/2014-08-05_07.txt", "data/2014-08-05_04.txt", "data/2014-08-05_16.txt"),
                                semicolon=T))
```

```
##    user  system elapsed 
##   4.088   0.072   3.760
```

```r
  summary(df)
```

```
##       Date                               RS        
##  Min.   :2014-08-05 01:00:00   prdhdp233.p:   710  
##  1st Qu.:2014-08-05 07:00:00   prdhdp214.p:   706  
##  Median :2014-08-05 16:00:00   prdhdp258.p:   703  
##  Mean   :2014-08-05 21:13:20   prdhdp251.p:   694  
##  3rd Qu.:2014-08-06 08:00:00   prdhdp238.p:   693  
##  Max.   :2014-08-07 02:00:00   prdhdp240.p:   693  
##                                (Other)    :186700  
##                                                                        Region      
##  apps,,1383138270892.014d173583793274dc7f373e6504740f.                    :     9  
##  apps,Eysio2flIJvVkHEcj9nb,1387446006780.a52d391f1653263137cacc0f6098bd4b.:     9  
##  apps,IyEvnfjeXgjx5Z3B3mbP,1387446006780.318e1d864171397b0102f518d0b0b6bc.:     9  
##  apps,k-RPLtI803e9qguRAhGg,1387430705923.2efbb005d96d887b486e0617996fea85.:     9  
##  apps,MwmmcZSDitz2PmqNgCor,1387446017533.ddb220cbe57e8a210dff63d9cd939855.:     9  
##  apps,QvpBPx6ADT6eDeFjXi2W,1387446017533.2c90c29d45b8f21754f82a67c5dcbf9d.:     9  
##  (Other)                                                                  :190845  
##       Time        
##  Min.   :      3  
##  1st Qu.:   2656  
##  Median :   4349  
##  Mean   :   8356  
##  3rd Qu.:   8355  
##  Max.   :1756633  
## 
```


