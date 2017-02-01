library(h2o)
h2o.init()

ngram <- function(inp, window){
    rows <- dim(inp)[1]
    cols <- dim(inp)[2]
    resRows <- rows - window + 1

    res <- matrix(, nrow = resRows, ncol = window*cols)
    for(idx in 1:resRows) {
        if(window-1 > 0) {
            newRow <- c(inp[idx,], t(inp[(idx+1):(idx+window-1),]))
        } else {
            newRow <- inp[idx,]
        }
        if(idx %% 10000 == 0) {
            print(idx)
        }
        res[idx,] <- t(newRow)
    }
    return(res)
}

window <- 1

iotRaw <- read.csv("resources/state_0_loop_0.csv")
iot <- as.matrix(iotRaw[,c("LinAccX..g.","LinAccY..g.","LinAccZ..g.")])
#iot <- iotRaw[,colSums(iotRaw != 0) > 0]
#iot <- as.matrix(iot[,-c(0,1,2,3)])

iot <- ngram(iot, window)
iot.hex  <- as.h2o(iot)

neurons <- 50
iot.dl = h2o.deeplearning(x = 1:(ncol(iot)), training_frame = iot.hex, autoencoder = TRUE, hidden = c(neurons), epochs = 100,
l1 = 1e-5, l2 = 1e-5, max_w2 = 10, activation = "TanhWithDropout", initial_weight_distribution = "UniformAdaptive", adaptive_rate = TRUE)
iot_error <- h2o.anomaly(iot.dl, iot.hex)
avg_iot_error <- sum(iot_error)/nrow(iot_error)
print(nrow(iot_error[iot_error > avg_iot_error])/nrow(iot.hex))

anomalyRaw <- read.csv("resources/state_1_loop_1.csv")
anomaly <- as.matrix(anomalyRaw[,c("LinAccX..g.","LinAccY..g.","LinAccZ..g.")])
#anomaly <- anomalyRaw[,colSums(anomalyRaw != 0) > 0]
#anomaly <- as.matrix(anomaly[,-c(0,1,2,3)])
anomaly <- ngram(anomaly, window)
anomaly.hex  <- as.h2o(anomaly)
anomaly_error <- h2o.anomaly(iot.dl, anomaly.hex)
print(nrow(anomaly_error[anomaly_error > avg_iot_error])/nrow(anomaly.hex))


verifyRaw <- read.csv("resources/verify_0.csv")
verify <- verifyRaw[,colSums(verifyRaw != 0) > 0]
verify <- as.matrix(verify[,-c(0,1,2,3)])
verify <- ngram(verify, window)
verify.hex  <- as.h2o(verify)
verify_error <- h2o.anomaly(iot.dl, verify.hex)
print(nrow(verify_error[verify_error > avg_iot_error])/nrow(verify.hex))