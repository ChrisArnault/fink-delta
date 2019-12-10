
delta.sh format=parquet loops=30 batch_size=1 >delta_parquet_bs1.txt
delta.sh format=parquet loops=30 batch_size=10 >delta_parquet_bs10.txt
delta.sh format=parquet loops=30 batch_size=100 >delta_parquet_bs100.txt
delta.sh format=parquet loops=30 batch_size=1000 >delta_parquet_bs1000.txt

delta.sh format=delta loops=30 batch_size=1 >delta_delta_bs1.txt
delta.sh format=delta loops=30 batch_size=10 >delta_delta_bs10.txt
delta.sh format=delta loops=30 batch_size=100 >delta_delta_bs100.txt
delta.sh format=delta loops=30 batch_size=1000 >delta_parquet_bs1000.txt



