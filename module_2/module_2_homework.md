## Week 2 Homework

> In case you don't get one option exactly, select the closest one 

For the homework, we'll be working with the _green_ taxi dataset located here:

`https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download`

### Assignment

The goal will be to construct an ETL pipeline that loads the data, performs some transformations, and writes the data to a database (and Google Cloud!).

- Create a new pipeline, call it `green_taxi_etl`
- Add a data loader block and use Pandas to read data for the final quarter of 2020 (months `10`, `11`, `12`).
  - You can use the same datatypes and date parsing methods shown in the course.
  - `BONUS`: load the final three months using a for loop and `pd.concat`
- Add a transformer block and perform the following:
  - Remove rows where the passenger count is equal to 0 _or_ the trip distance is equal to zero.
  - Create a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date.
  - Rename columns in Camel Case to Snake Case, e.g. `VendorID` to `vendor_id`.
  - Add three assertions:
    - `vendor_id` is one of the existing values in the column (currently)
    - `passenger_count` is greater than 0
    - `trip_distance` is greater than 0
- Using a Postgres data exporter (SQL or Python), write the dataset to a table called `green_taxi` in a schema `mage`. Replace the table if it already exists.
- Write your data as Parquet files to a bucket in GCP, partioned by `lpep_pickup_date`. Use the `pyarrow` library!
- Schedule your pipeline to run daily at 5AM UTC.

### Questions

## Question 1. Data Loading

Once the dataset is loaded, what's the shape of the data?

* 266,855 rows x 20 columns
* 544,898 rows x 18 columns
* 544,898 rows x 20 columns
* 133,744 rows x 20 columns

### Solution
Using a for loop to loop through the months to create three individual dataframes, then concatenate them together. 

```python
def load_data_from_csv(*args, **kwargs):
    """
    Loading data from the folder
    """
    months = [10, 11, 12]
    dfs= []

    for month in months:

        url= f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-{month}.csv.gz'

        taxi_dtypes = {
            'VendorID': pd.Int64Dtype(),
            'store_and_fwd_flag': str,
            'RatecodeID': pd.Int64Dtype(),
            'PULocationID': pd.Int64Dtype(),
            'DOLocationID': pd.Int64Dtype(),
            'passenger_count': pd.Int64Dtype(),
            'trip_distance': float,
            'fare_amount': float,
            'extra': float,
            'mta_tax': float,
            'tip_amount': float,
            'tolls_amount': float,
            'ehail_fee': float,
            'improvement_surcharge': float,
            'total_amount': float,
            'payment_type': pd.Int64Dtype(),
            'trip_type': pd.Int64Dtype(),
            'congestion_surcharge': float
        }

        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

        df = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
        dfs.append(df)
    
    final_df = pd.concat(dfs)

    return final_df
```
Answer is 266855 rows x 20 columns

## Question 2. Data Transformation

Upon filtering the dataset where the passenger count is greater than 0 _and_ the trip distance is greater than zero, how many rows are left?

* 544,897 rows
* 266,855 rows
* 139,370 rows
* 266,856 rows

### Solution
Use something like this to calculate
```python
def transform(data, *args, **kwargs):
    condition_1 = data['passenger_count'] > 0
    condition_2 = data['trip_distance'] > 0
    return len(data[condition_1 & condition_2])
```
Answer: 139,370 rows

## Question 3. Data Transformation

Which of the following creates a new column `lpep_pickup_date` by converting `lpep_pickup_datetime` to a date?

* `data = data['lpep_pickup_datetime'].date`
* `data('lpep_pickup_date') = data['lpep_pickup_datetime'].date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date`
* `data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt().date()`

### Solution
Add this to the transformer block:
```python
data['lpep_pickup_date'] = pd.to_datetime(data['lpep_pickup_datetime']).dt.date
```


## Question 4. Data Transformation

What are the existing values of `VendorID` in the dataset?

* 1, 2, or 3
* 1 or 2
* 1, 2, 3, 4
* 1

### Solution

Create another data loader, use

```SQL
query = 'SELECT DISTINCT vendor_id FROM mage.green_taxi'
```
as query to get the answer. 

Answer: 1 or 2.

## Question 5. Data Transformation

How many columns need to be renamed to snake case?

* 3
* 6
* 2
* 4

### Solution
I renamed these columns:
```python
data.rename(columns={"VendorID": "vendor_id", "RatecodeID": "ratecode_id", "PULocationID": "PU_location_id", "DOLocationID": "DO_location_id"}, inplace=True)
```
So there are 4 columns needed to be renamed. 

Answer: 4.

## Question 6. Data Exporting

Once exported, how many partitions (folders) are present in Google Cloud?

* 96
* 56
* 67
* 108

### Solution
I actually got 95 folders after exported. 

## Submitting the solutions

* Form for submitting: https://courses.datatalks.club/de-zoomcamp-2024/homework/hw2
* Check the link above to see the due date
  
## Solution

Will be added after the due date