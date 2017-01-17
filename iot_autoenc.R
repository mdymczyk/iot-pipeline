library(h2o)
h2o.init()

iot <- read.csv("resources/iot.csv")
iot.hex  <- as.h2o(iot)
iot.dl = h2o.deeplearning(x = 1:10, training_frame = iot.hex, autoencoder = TRUE,
			                                hidden = c(50, 50, 50), epochs = 100,seed=1)
errors <- h2o.anomaly(iot.dl, iot.hex, per_feature = TRUE)
print(errors)

