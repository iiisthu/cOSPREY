NAME=1i27
hadoop fs -rmr /$NAME/output
hadoop jar branchandbound.jar bb.BranchAndBound /$NAME 55

