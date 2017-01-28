library(h2o)
h2o.init()

ngram <- function(inp, window){
    rows <- dim(inp)[1]
    cols <- dim(inp)[2]
    resRows <- rows - window + 1

    res <- c()

    for(idx in 1:resRows) {
        newRow <- inp[idx,]
        for(ii in 1:(window-1)) {
            newRow <- c(newRow, inp[idx+ii,])
        }
        res <- rbind(res,newRow)
    }
    return(res)
}

iotRaw <- read.csv("resources/state_0_loop_0.csv")
iot <- iotRaw[,colSums(iotRaw != 0) > 0]
iot <- iot[,-c(0,1,2,3)]
iot.hex  <- as.h2o(iot)
iot.dl = h2o.deeplearning(training_frame = iot.hex, autoencoder = TRUE, hidden = c(50, 50, 50), epochs = 100,seed=1)

anomalyRaw <- read.csv("resources/state_1_loop_1.csv")
anomaly <- anomalyRaw[,colSums(anomalyRaw != 0) > 0]
anomaly <- anomaly[,-c(0,1,2,3)]
anomaly.hex  <- as.h2o(anomaly)