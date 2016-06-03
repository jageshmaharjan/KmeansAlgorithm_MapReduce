APP=target/KMeansClustering-1.0.jar 
CLASS=RemoveDuplicates ParseNormalize Clustering
INPUT=/2015280258/input/kddcup.testdata.unlabeled output_1/ output_2/ output_3/
OUTPUT=/user/$USER/wc_output
yarn jar $APP $CLASS $INPUT $OUTPUT
