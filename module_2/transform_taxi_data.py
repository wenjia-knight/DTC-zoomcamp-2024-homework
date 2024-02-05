if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    condition_1 = data['passenger_count'] != 0
    condition_2 = data['trip_distance'] != 0
    data = data[(condition_1 & condition_2)]
    print(len(data))
    data.rename(columns={"VendorID": "vendor_id", "RatecodeID": "ratecode_id", "PULocationID": "PU_location_id", "DOLocationID": "DO_location_id"}, inplace=True)
    return data

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert "vendor_id" in output.columns, 'There is no column called vender_id'

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert len(output[output["passenger_count"] != 0])!=0, 'There are passenger_count equals zero'

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert len(output[output["trip_distance"] != 0])!=0, 'There are trip distance equals zero'
