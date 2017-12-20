##
##  Forecast high and low alert thresholds
##
# Set the working directory
setwd("C:/Users/Charles/git/RTView-R-Analytics/samples")

# include the "getCacheHistory" function
source("sl_utils.R")

library(forecast)

#########################
#
#  movingAvg calculates a moving average for the input vector x
#           using a window of size "n" (default 5).
#
movingAvg <- function(x,n=5){filter(x,rep(1/n,n), sides=2)}

#########################
#
#  bollingerThresholds executes a simple model to predict the behavior of a
#       given metric over the next 24 hours.
#
#       Given a time series of samples for a given metric on an averaged day,
#       this function smoothes the day with a moving average (default window 
#       is 5 samples, two sided). The Standard Deviation of the detrended data
#       is then added and subtracted to the moving average to create the 
#       upper and lower bounds for a Bollinger Band. 
#
bollingerThresholds <- function(avgDay, ma_window=5) {
    # align the time ranges so that we can average both series
    mavgDay <- na.omit(movingAvg(avgDay, n=ma_window))

    # ndiffs tells us how many differences we need to take in order to remove
    # the trend component from our data
    detrended_avg <- diff(avgDay,differences=ndiffs(avgDay))

    # create upper & lower bounds by biasing the smoothed average up and down
    # relative to the standard deviation of the stationarized smoothed data.
    sdDay <- sd(detrended_avg)
    #print(sdDay)
    upperBound <- mavgDay + 2*sdDay
    lowerBound <- pmax(mavgDay - 2*sdDay,0)     # dont allow series to go negative

    cbind(upperBound,lowerBound,mavgDay)   # return the predictions
}

##################################################################################
# variables for the thresholding forecast.

# query this dataserver for # queued messages in a certain queue
dataserver <- "rtvdemos-163.sl.com"
cacheName <- "EmsQueueTotalsByServer"
filterColumn <- "URL"
filterValue <- "tcp://VMIRIS1023:7222"
columns <- "time_stamp;pendingMessageCount"  # set analysis column
rtvQuery <- "simdata2_rtvquery"
#
#################
#
#    estimateTypicalDay

# To forecast the next day's trend, get same day of the week for the last two weeks.
day1 <- getCacheHistory(dataserver,cacheName, rtvQuery, fcol=filterColumn,fval=filterValue,dayOffset=6,secOffset=2700,ndays=1,cols=columns)
day2 <- getCacheHistory(dataserver,cacheName, rtvQuery, fcol=filterColumn,fval=filterValue,dayOffset=13,secOffset=2700,ndays=1,cols=columns)

# set a common time range (tomorrow) for both series
day1 <- ts(day1, start=start(day1)+c(7,0), end=end(day1)+c(7,0), frequency=frequency(day1))
day2 <- ts(day2, start=start(day1), end=end(day1), frequency=frequency(day1))

# estimate as average of given days
nextDayEstimate <- (day1+day2)/2 #+ runif(length(day1), 10, 5000)

# forecast the alert thresholds for tomorrow
thresholds <- bollingerThresholds(nextDayEstimate, ma_window=7)

# plot the forecasted thresholds
plot(thresholds[,1], ylab="pending messages", main="Expected Threshold Bounds for Day Ahead", ylim=c(min(thresholds[,2]),max(thresholds[,1])))
lines(thresholds[,2])
lines(thresholds[,3],col=3,lty=3)

#################
#

