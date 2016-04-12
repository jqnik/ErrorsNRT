drop table if exists errorsnrt2; create external table errorsNRT_table (code string, counts string, ms string) stored as parquet location '/your/path/ErrorsNRT/errorsNRT_table';
