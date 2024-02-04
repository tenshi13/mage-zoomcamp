import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """""
    Loading data from https://github.com/DataTalksClub/nyc-tlc-data/releases/tag/green/download
    we only need 10/11/12 from 2020
    """""
    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'RatecodeID': pd.Int64Dtype(),
        'store_and_fwd_flag': str,
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'payment_type': pd.Int64Dtype(),
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'congestion_surcharge': float 
    }
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']    

    target_years = [2020]
    target_months = [10,11,12]
    base_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_' # 2020-10.csv.gz

    df_out = pd.DataFrame() # EH: empty dataframe... forum says its bad practice?
    for year in target_years:
        for month in target_months:
            url = f"{base_url}{year}-{month}.csv.gz"
            print(f"Loading {url}")
            df_slice = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=parse_dates)
            df_out = pd.concat([df_out, df_slice])
            print(f"Total rows : {len(df_out.index)}")

    return df_out


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
