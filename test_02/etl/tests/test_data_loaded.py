
import sqlalchemy
import pandas as pd
import pytest
from test_02.etl.load import DataLoader
from test_02.etl.fhir import FHIRDataTransformer


class TestDataLoading(object):

    @pytest.fixture(scope="session", autouse=True)
    def setup_once(self):
        # Setup the database
        pass

    def test_data_loaded(self):
        # Given
        ## Setup
        loader = DataLoader()
        transformer = FHIRDataTransformer()

        # When
        loader.load_data()
        patients = transformer.get_patient_resources()
        encounters = transformer.get_encounter_resources()
        print(patients)
        print(encounters)

        # Then
        assert len(patients) == 4
        assert len(encounters) == 4
        names = set()
        for patient in patients:
            for name in patient["name"]:
                names.add((name["given"], name["family"]))

        encounter_ids = set()
        for encounter in encounters:
            for identifier in encounter["identifier"]:
                encounter_ids.add(identifier["value"])
        
        assert ("John", "Doe") in names
        assert 987 and 2345 in encounter_ids
