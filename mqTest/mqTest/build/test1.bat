//del backup.bin
//Because the This test will load group1 then group2 as it has 2 resources it will be expected the order will be 1,2,2,2,2,2,2,1,1
java -jar schedulerTest.jar r2 c p f q m1 m2 m2 m2 m2 m1 m2 m1 m2 
