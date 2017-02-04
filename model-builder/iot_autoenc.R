library(h2o)
# Starts a local H2O node
h2o.init()

#
# Function which "windows" the input matrix i.e. for matrix:
# 1   2   3
# 4   5   6
# 7   8   9
# 10 11  12
#
# And window 3 the result would be:
# 1  2  3  4  5  6  7   8   9
# 4  5  6  7  8  9  10 11  12
#
# It appends window-1 following rows to each row creating matrix of size dim(rows-window+1, columns*window)
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

# Read training data into memory
iotRaw <- read.csv("resources/normal_20170202_2229.csv")

# Select training columns
iot <- as.matrix(iotRaw[,c("LinAccX..g.","LinAccY..g.","LinAccZ..g.")])

# Set training window and ngram
window <- 200
iot <- ngram(iot, window)

# Send the data to H2O
iot.hex  <- as.h2o(iot)

# Run the deeplearning model in autoencoder mode
neurons <- 50
iot.dl = h2o.deeplearning(model_id = "iot_dl", x = 1:(ncol(iot)), training_frame = iot.hex, autoencoder = TRUE, hidden = c(neurons), epochs = 100,
l1 = 1e-5, l2 = 1e-5, max_w2 = 10, activation = "TanhWithDropout", initial_weight_distribution = "UniformAdaptive", adaptive_rate = TRUE)

# Make predictions for training data
iot_error <- h2o.anomaly(iot.dl, iot.hex)

# Get the prediction threshold as 2*sd -> this should be found empirically on running data
threshold <- sd(iot_error)*2
print(nrow(iot_error[iot_error > threshold])/nrow(iot.hex))

# If required check the model on anomaly data
#anomalyRaw <- read.csv("resources/pre-fail_20170202_2234.csv")
#anomaly <- as.matrix(anomalyRaw[,c("LinAccX..g.","LinAccY..g.","LinAccZ..g.")])
#anomaly <- ngram(anomaly, window)
#anomaly.hex  <- as.h2o(anomaly)
#anomaly_error <- h2o.anomaly(iot.dl, anomaly.hex)
#print(nrow(anomaly_error[anomaly_error > threshold])/nrow(anomaly.hex))

# Check the model on verification data
verifyRaw <- read.csv("resources/verify_20170202_2243.csv")
verify <- as.matrix(verifyRaw[,c("LinAccX..g.","LinAccY..g.","LinAccZ..g.")])
verify <- ngram(verify, window)
verify.hex  <- as.h2o(verify)
verify_error <- h2o.anomaly(iot.dl, verify.hex)
print(nrow(verify_error[verify_error > threshold])/nrow(verify.hex))

# Exports the H2O model as a Java class
exportPojo <- function() {
    h2o.download_pojo(iot.dl, path="../predictions/src/main/java/")
    # Download also downloads a utility class which we don't use for autoencoders
    unlink("../predictions/src/main/java/h2o-genmodel.jar")
    # Write th threshold to a properties file
    cat("threshold=",toString(threshold),file="../predictions/src/main/resources/dl.properties",sep="",append=F)
}

exportPojo()

errors <- which(as.matrix(verify_error) > threshold, arr.ind=T)[,1]
vals <- rep(list(1),length(errors))

# Plot the result of our predictions for verification data
attach(mtcars)
par(mfrow=c(3,1))
plot(verify[-c(errors),1], col="chartreuse4", , xlab="Time", ylab="LinAccX")
points(x=errors,y=verify[errors,1], col="red")
plot(verify[-c(errors),2], col="chartreuse4", xlab="Time", ylab="LinAccY")
points(x=errors,y=verify[errors,2], col="red")
plot(verify[-c(errors),3], col="chartreuse4", xlab="Time", ylab="LinAccZ")
points(x=errors,y=verify[errors,3], col="red")