from pathlib import Path

import pytest

from transforms.common import (
    stg_national_intensity,
    stg_regional_intensity_generation_mix,
)
from transforms.core import build_fct_half_hourly_intensity

FIXTURES = Path(__file__).parents[2] / "fixtures" / "data" / "staging"


@pytest.mark.explore
def test_explore_fct(spark):
    national = stg_national_intensity(
        spark.read.parquet(str(FIXTURES / "national_intensity" / "2026-01-01.parquet"))
    )
    regional = stg_regional_intensity_generation_mix(
        spark.read.parquet(
            str(FIXTURES / "regional_intensity_generation_mix" / "2026-01-01.parquet")
        )
    )
    build_fct_half_hourly_intensity(national, regional)
