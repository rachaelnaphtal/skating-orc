import os
import tempfile
from datetime import date

import pandas as pd
import pytest

from activityAnalysis.official_birthdate_loader import parse_ages_csv


def test_parse_ages_csv_dedupes_and_parses_dob():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write(
            "Member #,First Name,Last Name,DOB\n"
            "12345,Jane,Doe,03/30/2007\n"
            "12345,Jane,Doe,03/30/2007\n"
            "99999,No,Match,01/15/1980\n"
        )
        path = f.name
    try:
        frame, warnings = parse_ages_csv(path)
        assert len(frame) == 2
        row = frame.set_index("mbr_number").loc["12345"]
        assert row["date_of_birth"] == date(2007, 3, 30)
        assert not warnings
    finally:
        os.unlink(path)


def test_parse_ages_csv_requires_member_and_dob_columns():
    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("Name,Age\n")
        path = f.name
    try:
        with pytest.raises(ValueError, match="Member #"):
            parse_ages_csv(path)
    finally:
        os.unlink(path)
