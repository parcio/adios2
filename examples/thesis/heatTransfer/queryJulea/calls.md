# Julea-DB

mpirun -n 2 ./bin/thesis_eval_heatTransfer_write_adios2 ../examples/thesis/heatTransfer/heat_julea-db.xml heat_test-julea-db.bp 2 1 10 10 4 10 julea-db

mpirun -n 1 ./bin/thesis_eval_heatTransfer_queryAdios ../examples/thesis/heatTransfer/heat_julea-db.xml heat_test-julea-db.bp 1 1

 mpirun -n 2 ./bin/thesis_eval_heatTransfer_read ../examples/thesis/heatTransfer/heat_julea-db.xml heat_test-julea-db.bp heat_db-2.bp 2 1


# Julea-KV
mpirun -n 2 ./bin/thesis_eval_heatTransfer_write_adios2 ../examples/thesis/heatTransfer/heat_julea-kv.xml heat_test-julea-kv.bp 2 1 10 10 4 10 julea-kv

mpirun -n 1 ./bin/thesis_eval_heatTransfer_queryAdios ../examples/thesis/heatTransfer/heat_julea-kv.xml heat_test-julea-kv.bp 1 1

 mpirun -n 2 ./bin/thesis_eval_heatTransfer_read ../examples/thesis/heatTransfer/heat_julea-kv.xml heat_test-julea-kv.bp heat_kv-2.bp 2 1    :(


# BP4
mpirun -n 2 ./bin/thesis_eval_heatTransfer_write_adios2 ../examples/thesis/heatTransfer/heat_bp4_sync.xml heat_bp4.bp 2 1 10 10 4 10 bp4

mpirun -n 1 ./bin/thesis_eval_heatTransfer_queryAdios ../examples/thesis/heatTransfer/heat_bp4_sync.xml heat_bp4.bp 1 1

 mpirun -n 2 ./bin/thesis_eval_heatTransfer_read ../examples/thesis/heatTransfer/heat_bp4_sync.xml heat_bp4.bp heat_bp4-2.bp 2 1 












# Julea-DB

## Read 	 Compute 	 Analysis
2945 	 2945 	 2945
maxMean: 3.23741e-15
19304 	 19304 	 19304
2714 	 2714 	 2714
blockInfos[j].Max:70.2874
max T > 40 
blockInfos[j].Max:70.2874
max T > 40 
sum: 46696
sum: 46696
befor max sum 
maxSum: 46696
5139 	 5139 	 5139
Total runtime = 0.0380864s

# Read 	 Compute 	 Analysis
50742 	 50742 	 50742
390388 	 390388 	 390388
result1: 26.0796
result100: 30.8967
result1: 25.947
result100: 31.546
maxDiff: 5.59896
49608 	 49608 	 49608
50728 	 50728 	 50728
Total runtime = 0.616336s





# Julea-KV

## Read 	 Compute 	 Analysis
1480 	 1480 	 1480
maxMean: -25.5262
12842 	 12842 	 12842
1457 	 1457 	 1457
1390 	 1390 	 1390
Total runtime = 0.023953s


----- bp4
# Read 	 Compute 	 Analysis
626 	 626 	 626
maxMean: 3.23741e-15
3448 	 3448 	 3448
385 	 385 	 385
blockInfos[j].Max:70.2874
max T > 40 
blockInfos[j].Max:70.2874
max T > 40 
sum: 46696
sum: 46696
befor max sum 
maxSum: 46696
1067 	 1067 	 1067
Total runtime = 0.00865388s

# Read 	 Compute 	 Analysis
4968 	 4968 	 4968
58051 	 58051 	 58051
result1: 26.0796
result100: 30.8967
result1: 25.947
result100: 31.546
maxDiff: 5.59896
4433 	 4433 	 4433
5502 	 5502 	 5502
Total runtime = 0.0841792s



./bin/bpls -lD heat_bp4.bp
  double   P     5*{20, 10} = 29 / 491
        step 0: 
          block 0: [ 0: 9, 0:9] = 29 / 312
          block 1: [10:19, 0:9] = 222 / 268
        step 1: 
          block 0: [ 0: 9, 0:9] = 108 / 351
          block 1: [10:19, 0:9] = 69 / 371
        step 2: 
          block 0: [ 0: 9, 0:9] = 31 / 339
          block 1: [10:19, 0:9] = 108 / 491
        step 3: 
          block 0: [ 0: 9, 0:9] = 72 / 260
          block 1: [10:19, 0:9] = 187 / 382
        step 4: 
          block 0: [ 0: 9, 0:9] = 29 / 324
          block 1: [10:19, 0:9] = 33 / 349
  double   T     5*{20, 10} = -102.612 / 70.2874
        step 0: 
          block 0: [ 0: 9, 0:9] = -59.5579 / 70.2874
          block 1: [10:19, 0:9] = -59.5579 / 70.2874
        step 1: 
          block 0: [ 0: 9, 0:9] = -82.3984 / 26.0796
          block 1: [10:19, 0:9] = -36.6791 / 25.947
        step 2: 
          block 0: [ 0: 9, 0:9] = -94.5501 / 29.3049
          block 1: [10:19, 0:9] = -38.628 / 29.3062
        step 3: 
          block 0: [ 0: 9, 0:9] = -99.827 / 31.538
          block 1: [10:19, 0:9] = -39.6372 / 31.5601
        step 4: 
          block 0: [ 0: 9, 0:9] = -102.612 / 32.5997
          block 1: [10:19, 0:9] = -40.162 / 32.7174


1 kira@MinasTirith ..cuments/Arbeit/Code/julea-adios2/build (git)-[master] % ./bin/bpls -lD heat_bp4.bp
  double   P     5*{20, 10} = 0 / 499
        step 0: 
          block 0: [ 0: 9, 0:9] = 0 / 481
          block 1: [10:19, 0:9] = 1 / 499
        step 1: 
          block 0: [ 0: 9, 0:9] = 1 / 484
          block 1: [10:19, 0:9] = 5 / 497
        step 2: 
          block 0: [ 0: 9, 0:9] = 4 / 494
          block 1: [10:19, 0:9] = 0 / 497
        step 3: 
          block 0: [ 0: 9, 0:9] = 0 / 495
          block 1: [10:19, 0:9] = 8 / 499
        step 4: 
          block 0: [ 0: 9, 0:9] = 1 / 497
          block 1: [10:19, 0:9] = 13 / 493
