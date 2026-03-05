import pandas as pd
import pytest
from breweries_pipeline.quality.checks import check_required_columns, check_not_null, check_unique_ids

def test_quality_checks_ok():
    df = pd.DataFrame([
        {"id":"1","nome":"AA","brewery_type":"micro","country":"BR","estado":"RS"},
        {"id":"2","nome":"BB","brewery_type":"media","country":"BR","estado":"RS"},
    ])
    check_required_columns(df)
    check_not_null(df)
    check_unique_ids(df)

def test_quality_checks_fail_on_duplicates():
    df = pd.DataFrame([
        {"id":"1","nome":"AA","brewery_type":"micro","country":"BR","estado":"RS"},
        {"id":"1","nome":"AA2","brewery_type":"micro","country":"BR","estado":"RS"},
    ])
    with pytest.raises(ValueError):
        check_unique_ids(df)