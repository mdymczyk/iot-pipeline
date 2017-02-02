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

#iotRaw <- read.csv("resources/state_0_loop_0.csv")
iotRaw <- read.csv("resources/normal_20170202_2229.csv")
iot <- as.matrix(iotRaw[,c("LinAccX..g.","LinAccY..g.","LinAccZ..g.")])

window <- 1
iot <- ngram(iot, window)
iot.hex  <- as.h2o(iot)

neurons <- 50
iot.dl = h2o.deeplearning(model_id = "iot_dl", x = 1:(ncol(iot)), training_frame = iot.hex, autoencoder = TRUE, hidden = c(neurons), epochs = 100,
l1 = 1e-5, l2 = 1e-5, max_w2 = 10, activation = "TanhWithDropout", initial_weight_distribution = "UniformAdaptive", adaptive_rate = TRUE)
iot_error <- h2o.anomaly(iot.dl, iot.hex)
avg_iot_error <- sum(iot_error)/nrow(iot_error)
print(avg_iot_error)
print(nrow(iot_error[iot_error > avg_iot_error])/nrow(iot.hex))

anomalyRaw <- read.csv("resources/pre-fail_20170202_2234.csv")
#anomalyRaw <- read.csv("resources/state_1_loop_1.csv")
anomaly <- as.matrix(anomalyRaw[,c("LinAccX..g.","LinAccY..g.","LinAccZ..g.")])
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

h2o.download_pojo(iot.dl, path="/Users/mateusz/Dev/code/github/iot-pipeline/predictions/src/main/java/")