del backup.bin
//Because the This test will load each group sequentially as it has 1 resources it will be expected the order will be 1,1,1,2,2,2,2,2,2
java -jar schedulerTest.jar r1 c p f q m1 m2 m2 m2 m2 m1 m2 m1 m2 
