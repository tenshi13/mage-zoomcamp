if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Remove rows where the passenger count is equal to 0 or 
    the trip distance is equal to zero.
    Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    Rename columns in Camel Case to Snake Case, e.g. VendorID to vendor_id.
    """
    print(f"Total rows before : {len(data.index)}")
    print(f"Rows with 0 passenger_count : { data['passenger_count'].fillna(0).isin([0]).sum() }")
    print(f"Rows with 0 trip_distance : { data['trip_distance'].fillna(0).isin([0]).sum() }")

    # filter out unwanted rows
    data = data[ data['passenger_count'].fillna(0) > 0 ]
    data = data[ data['trip_distance'].fillna(0) > 0 ]

    print(f"Total rows after filters : {len(data.index)}")

    # add new date column
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date

    rename_cols = {
        'VendorID' : 'vendor_id',
        'RatecodeID' : 'rate_code_id',
        'PULocationID' : 'pu_location_id',
        'DOLocationID' : 'do_location_id'
    }

    data.rename(columns=rename_cols, inplace=True)

    return data

"""
Add three assertions:
vendor_id is one of the existing values in the column (currently)
passenger_count is greater than 0
trip_distance is greater than 0
"""
@test
def test_vendor_id_isin_output_columns(output, *args) -> None:
    assert 'vendor_id' in output.columns, 'vendor_id is not in output columns'

@test
def test_passenger_count_gt_0(output, *args) -> None:
    # should have 0 rows of passenger_count = 0
    assert output['passenger_count'].fillna(0).isin([0]).sum() == 0, \
            'rows with 0 passenger_count detected'

@test
def test_trip_distance_gt_0(output, *args) -> None:
    # should have 0 rows of trip_distance = 0
    assert output['trip_distance'].fillna(0).isin([0]).sum() == 0, \
            'rows with 0 passenger_count detected'
