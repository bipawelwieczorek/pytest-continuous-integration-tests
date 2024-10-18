import os
import pytest
from lib.continuous_data_testing.utils import get_files
from lib.continuous_data_testing.snowflake_test_runner import SnowflakeTestRunner
from lib.continuous_data_testing.diff import *


@pytest.mark.parametrize("test_file", get_files(["*.sql", "*.yml"], file=__file__))
def test_run_sql(test_file, request, metadata):
    
    with SnowflakeTestRunner(metadata=metadata, env=os.environ) as t:
        df = t.run_test(test_file)
        apply_diff_by_column_name(df)
        request.node.stash["result"] = df

    assert df.attrs.get("condition"), df.attrs.get("error_msg")
