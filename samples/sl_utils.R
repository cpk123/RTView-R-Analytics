##############################################################################
#   Example R code for "Analytics with RTView Data using R"
##############################################################################

#  getCurrent queries the RTView REST interface for current data from the given cache.
#
#getCurrent("localhost:8068","VmwVirtualMachines",package="hostbase_rtvquery")

getCacheCurrent <- function(rtvServer, cache, package="emsample_rtvquery", fmt="text", cols="") {
	url <- sprintf("http://%s/%s/cache/%s/current?fmt=%s",rtvServer,package,cache,fmt)
	if( cols != "" )  url <- sprintf("%s&cols=%s",url,cols)
	print(url)
	read.delim(url)     # execute REST query; returns an R dataframe
}

###############################################################################
#
#  getCacheHistory - query the RTView REST interface for history data.
#
#       As commonly used, this function pulls "n" days of data from an RTView cache,
#       starting from an offset in days. The use of an offset makes it easy to
#       retrieve data for modeling without the need to do date arithmetic to
#       calculate start and end times for the desired time series. The data is
#       coerced into an R time-series, with the time scaled in terms of days and
#       sample offset into the day. Hence, if the series is plotted, the x axis 
#       will represent time in days since 1970-01-01.
#
#       Note: when RTView time series data is returned, the time span should
#       be chosen to ensure that the sample spacing is constant throughout 
#       the series, as assumed by the R "ts" class. The function computes an
#       average delta-t for the series, and uses this to estimate the time for 
#       the last sample in order to return a valid ts object, so the final sample
#       time may vary slightly from the actual history segment.
#       (TBD) If the first and last delta-time of the series differ by more than
#       one second, the time series should be aggregated so that all samples are
#       spaced the last delta-t apart.
#
#       The first column returned by the query must be a timestamp.
#
#   Examples:
#       # retrieve history data for the EMS server indexed in the cache by 
#       # URL "tcp://192.168.1.116:7222" on the sixth data prior to "today"
#       h6 <- getHistory("192.168.1.101", fval="tcp://192.168.1.116:7222",dayOffset=6)
#
getCacheHistory <- function(rtvServer, cache="EmsQueueTotalsByServer", 
						rtvquery="emsample_rtvquery", fval, fcol="URL", 
						dayOffset=6, secOffset=0, sides=2, ndays=1, tz_offset=8,
						cols="time_stamp", fmt="text", tr=86400) {
    # set up the base URL for the REST query
    base_url <- sprintf("http://%s/%s/cache/%s/history?fmt=%s&cols=%s",rtvServer,rtvquery,cache,fmt,cols)
	# create a column filter to retrieve rows for a specified index value
    emsFilter <- sprintf("fcol=%s&fval=%s",fcol,fval)
    
    # calculate begin time in seconds since 1970
    if(dayOffset == 0 && tr != 86400) {
        # caller wants near real-time data
        #te <- as.integer(unclass(Sys.time())) - tz_offset*3600
        timeFilter <- sprintf("tr=%s",tr)
        #timeFilter <- sprintf("tr=%s&te=%s000",tr,te)
        print("set tr only")
    } else {
        tb <- (unclass(Sys.Date()) - dayOffset)*86400 - secOffset + tz_offset*3600
        trr <- tr * ndays + sides*secOffset
        timeFilter <- sprintf("tr=%s&tb=%s000",trr,tb)
    }

    url <- URLencode(paste(base_url,emsFilter,timeFilter,"sqlex=true",sep="&"))
    #print(paste(">>>fetching URL: ",url))        # debug
    ret <- read.delim(url, sep="\t")     # execute REST query; returns an R dataframe
    num_rows <- nrow(ret)
    if (num_rows != 0) {
        ret$time_stamp <- as.POSIXct(ret$time_stamp,"%b %d, %Y %I:%M:%S %p",tz="GMT")
        delta <- as.numeric(difftime(ret$time_stamp[2],ret$time_stamp[1],units="secs"))
        end_delta <- as.numeric(difftime(ret$time_stamp[num_rows],ret$time_stamp[num_rows-1],units="secs"))
        if (floor(delta/end_delta) > 1) {
            #aggregate samples at lower sample spacing than last pair of samples
            print("*** getCacheHistory: need to aggregate part of return data")
        }
        delta_t <- floor(as.numeric(difftime(ret$time_stamp[num_rows],ret$time_stamp[1],units="secs"))/(num_rows-1))
        this.frequency <- floor(86400/delta_t)  # calculate number of samples per day
        #print(delta_t)
        #print(this.frequency)
        ts(ret[,-1], start=getTsTime(ret$time_stamp[1],delta_t), end=getTsTime(ret$time_stamp[1],delta_t,num_rows),
            frequency=this.frequency, deltat=delta_t)
    }
    else {
        print("*** getCacheHistory: RTView query returned no data")
    }
}

###############################################################################
#
#  getTsTime  - calculate suitable start and end parameters for an R time-series.
#               The given date is converted into a two element list "c(ndays,index)",
#               where ndays is the number of days since 1970-01-01, and index is
#               the sample index (from 1..frequency, where frequency is the expected
#               number of samples per day spaced at intervals of deltat).
#
#   posixctDate - a date-time object of class POSIXct.
#   deltat      - the interval between data samples in seconds for the given time series.
#

getTsTime <- function(posixctDate, deltat, numSamples=1) {
    secSince1970 <- as.numeric(posixctDate + (numSamples-1)*deltat)
    dayPart <- floor(secSince1970/86400)
    sampleOffset <- floor((secSince1970 - dayPart*86400)/deltat) + 1
    c(dayPart, sampleOffset)
}



# get mean and confidence interval
GetCI <- function(x, level=0.95) {
    if(level <= 0 || level >= 1) {
        stop("***The level must be between zero and one!")
    }
    m <- mean(x)
    n <- length(x)
    SE <- sd(x) / sqrt(n)
    upper <- 1-(1-level)/2
    ci <- m + c(-1,1)*qt(upper,n-1)*SE
    return(list(mean=m, se=SE, ci=ci))
}
