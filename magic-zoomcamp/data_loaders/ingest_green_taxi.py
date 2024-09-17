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
    Loading data from https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
    we need 2022 Green Taxi Trip Record Parquet Files
    """""
    year = kwargs.get('year')
    month = kwargs.get('month')

    # parc files we dont need to specify the dtypes
    # taxi_dtypes = {
    #     'VendorID': pd.Int64Dtype(),
    #     'passenger_count': pd.Int64Dtype(),
    #     'trip_distance': float,
    #     'RatecodeID': pd.Int64Dtype(),
    #     'store_and_fwd_flag': str,
    #     'PULocationID': pd.Int64Dtype(),
    #     'DOLocationID': pd.Int64Dtype(),
    #     'payment_type': pd.Int64Dtype(),
    #     'fare_amount': float,
    #     'extra': float,
    #     'mta_tax': float,
    #     'tip_amount': float,
    #     'tolls_amount': float,
    #     'improvement_surcharge': float,
    #     'total_amount': float,
    #     'congestion_surcharge': float 
    # }
    # parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']    

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_{year}-{month}.parquet"
    print(f"Loading file : {url}")

    df_out = pd.read_parquet(url)

    # quick transforms
    rename_cols = {
        'VendorID' : 'vendor_id',
        'RatecodeID' : 'rate_code_id',
        'PULocationID' : 'pu_location_id',
        'DOLocationID' : 'do_location_id'
    }
    df_out.rename(columns=rename_cols, inplace=True)
    
    return df_out


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
