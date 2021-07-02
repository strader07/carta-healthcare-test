
import json
from datetime import datetime
from sqlalchemy import create_engine


def format_fhir_patient(record):
    """
    Transforms a psql record into FHIR Patient resource format.
    Args:
        record: a psql record
    Return:
        patient: a dict object the transformed Patient profile
    """
    patient = {
        "resourceType": "Patient",
        "identifier": [
            {
                "value": record["MRN"]
            }
        ],
        "name": [
            {
                "given": record["First Name"],
                "family": record["Last Name"]
            }
        ],
        "birthDate": datetime.strptime(record["Birth Date"], "%m/%d/%Y").strftime("%Y-%m-%d")
    }

    return patient


def format_fhir_encounter(record):
    """
    Transforms a psql record into FHIR Encounter resource format.
    Args:
        record: a psql record
    Return:
        encounter: a dict object the transformed Encounter profile
    """
    encounter = {
        "resourceType": "Encounter",
        "identifier": [
            {
                "value": record["Encounter ID"]
            }
        ]
    }
    
    return encounter


class FHIRDataTransformer(object):
    """Transform data in postgres into Patient/Encounter resources"""
    def __init__(self):
        self.psql_host = "localhost"
        self.psql_db = "test_02"
        self.psql_user = ""
        self.psql_pwd = ""

    def get_patient_resources(self):
        ## Query data in postgres, produce array of Patient FHIR resources
        conn = create_engine(f'postgresql+psycopg2://postgres@{self.psql_host}/{self.psql_db}')
        query = "SELECT * from patients;"
        records = conn.execute(query).fetchall()

        patients = []
        for record in records:
            patient = format_fhir_patient(record)
            patients.append(patient)

        return patients

    def get_encounter_resources(self):
        ## Query data in postgres, produce array of Encounter FHIR resources
        conn = create_engine(f'postgresql+psycopg2://postgres@{self.psql_host}/{self.psql_db}')
        query = "SELECT * from patients;"
        records = conn.execute(query).fetchall()

        encounters = []
        for record in records:
            encounter = format_fhir_encounter(record)
            encounters.append(encounter)

        return encounters