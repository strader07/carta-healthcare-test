# Introduction
This is a test for doing some basic ETL on data.

## The Goal
The goal of this task is to take a pair of excel files, load them into SQL, and then transform the dataset into a separate schema (called FHIR). The final deliverable
will be a passing test in test_02/etl/tests/test_data_loaded.py.

### Input data
The input data is an pair of excel files with patient information in it. Each file represents an extract of a data system, each made a month apart. 
Each file includes patients discharged during the last two months, so the data will have intersecting data points, 
but the union of the two is really what you want.  In the boilerplate code below, you can see the file paths and open them up in excel 
to browse it if you'd like. Feel free to ask questions about the file formatting.

# Exercise

## Load into SQL
First, begin by loading the data into a SQL database. We have Postgres running for you already, so use that. There is no username/password on the server, so
a plain connection to localhost 5432 will work.

The data should be a union of the data in both extract files, with an update time corresponding to the newest update date available for each row.

## Transform into FHIR

After the data is loaded into SQL, you'll produce some FHIR resources from the data. In particular, there are two resources you'll make:

1) Patient
2) Encounter

Both of those are defined on the FHIR website, here: http://fhir.org.

## Finish implementing the test

A stub test is implemented in test_data_loaded.py. You can run the test by doing the following:
```
cd test_02
pytest

```

Feel free to implement additional tests or change the API to suit your needs!