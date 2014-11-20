del backup.bin
//Because the This test will load 2 groups at a time but due to the delay before the second one is added it will take the order as it has 2 resources it will be expected the order will be 1,2,2,2,2,2,2,1,1
java -jar schedulerTest.jar r2 c p f q m1 s15 m2 m2 m2 m2 m1 m2 m1 m2 
