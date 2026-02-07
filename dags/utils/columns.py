ODS_COLUMN_MAPPING = {
    'VendorID': 'vendor_id',
    'tpep_pickup_datetime': 'tpep_pickup',
    'tpep_dropoff_datetime': 'tpep_dropoff',
    'RatecodeID': 'ratecode_id',
    'store_and_fwd_flag': 'store_and_forward',
    'PULocationID': 'pu_location_id',
    'DOLocationID': 'do_location_id',
    'fare_amount': 'fare',
    'extra': 'extras',
    'tip_amount': 'tip',
    'tolls_amount': 'tolls',
    'improvement_surcharge': 'improvement',
    'total_amount': 'total',
    'congestion_surcharge': 'congestion',
    'Airport_fee': 'airport_fee'
}


def get_ods_columns():
    """
    TAXI DATA
    :return: List of all column names
    """
    ods_columns = [
        'vendor_id',
        'tpep_pickup',
        'tpep_dropoff',
        'passenger_count',
        'trip_distance',
        'ratecode_id',
        'store_and_forward',
        'pu_location_id',
        'do_location_id',
        'payment_type',
        'fare',
        'extras',
        'mta_tax',
        'tip',
        'tolls',
        'improvement',
        'total',
        'congestion',
        'airport_fee',
        'cbd_congestion_fee'
    ]
    return ods_columns
