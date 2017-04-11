import h2o
import pandas as pd

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
def ngram(inp, window):
    rows = inp.shape[0]
    cols = inp.shape[1]
    resRows = rows - window + 1

    res = DataFrame(columns=range(window*cols))
    for idx in range(resRows):
        if window-1 > 0:
            newRow = c(inp[idx,], t(inp[(idx+1):(idx+window-1),]))
        elif:
            newRow = inp[idx,]

        if idx %% 10000 == 0:
            print(idx)
        res[idx,] = t(newRow)
    return(res)

# Read training data into memory
iotRaw = pd.from_csv('resources/normal_20170202_2229.csv')

# Select training columns
iot = iotRaw[["LinAccX..g.","LinAccY..g.","LinAccZ..g."]]

# Set training window and ngram
window = 200
iot = ngram(iot, window)

# Send the data to H2O
iot_hex  = h2o.H2OFrame(iot)

# Run the deeplearning model in autoencoder mode
neurons = 50
iot_dl = h2o.estimators.deeplearning.H2OAutoEncoderEstimator(model_id = "iot_dl",
                                                             autoencoder = TRUE,
                                                             hidden = c(neurons),
                                                             epochs = 100,
                                                             l1 = 1e-5, l2 = 1e-5, max_w2 = 10,
                                                             activation = "TanhWithDropout",
                                                             initial_weight_distribution = "UniformAdaptive",
                                                             adaptive_rate = TRUE)

iot_dl.train(x = 1:(ncol(iot)), training_frame = iot_hex)

# Make predictions for training data
iot_error = h2o.anomaly(iot_dl, iot_hex)

# Get the prediction threshold as 2*sd -> this should be found empirically on running data
threshold = numpy.std(iot_error)*2
print(nrow(iot_error[iot_error > threshold])/nrow(iot.hex))

# Check the model on verification data
verifyRaw = read.csv("resources/verify_20170202_2243.csv")
verify = as.matrix(verifyRaw[,c("LinAccX..g.","LinAccY..g.","LinAccZ..g.")])
verify = ngram(verify, window)
verify.hex  = as.h2o(verify)
verify_error = h2o.anomaly(iot.dl, verify.hex)
print(nrow(verify_error[verify_error > threshold])/nrow(verify.hex))

# Exports the H2O model as a Java class
def exportPojo():
    h2o.download_pojo(iot.dl, path="../predictions/src/main/java/")
    # Write th threshold to a properties file
    print("threshold=" + toString(threshold), file='../predictions/src/main/resources/dl.properties')

exportPojo()