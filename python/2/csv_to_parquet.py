import polars as pl
import timeit

csv_file_path = "iris.csv"
df = pl.read_csv(csv_file_path)
parquet_file_path = "iris.parquet"
df.write_parquet(parquet_file_path)

start_time = timeit.default_timer()
df = pl.read_parquet("iris.parquet")
elapsed_time = timeit.default_timer() - start_time
print("Время чтения из файла Parquet:", elapsed_time, "секунд")
start_time = timeit.default_timer()
df.write_parquet("iris.parquet")
elapsed_time = timeit.default_timer() - start_time
print("Время записи в файл Parquet:", elapsed_time, "секунд")
