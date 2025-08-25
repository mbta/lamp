import polars as pl
from polars import Schema
from polars import (
    String,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    List,
    Struct,
    Boolean,
    Categorical,
    Float64,
)

from lamp_py.tableau.conversions.convert_types import convert_to_tableau_compatible_schema

# narrow scope in here
Categorical = Categorical(String)
### admins ###
spare_raw_input_schema_admins = Schema(
    {
        "id": String,
        "createdAt": Int64,
        "updatedAt": Int64,
        "email": String,
        "firstName": String,
        "lastName": String,
        "lastSeenTs": Int64,
        "roles": List(Categorical),
        "phoneNumber": String,
    }
)


input_df = pl.Schema(schema=input).to_frame()
input_df = input_df.with_columns(pl.col('struct_col').struct.unnest())
convert_to_tableau_compatible_schema()
### appointmentTypes ###
spare_raw_input_schema_appointmentTypes = Schema(
    {
        "id": String,
        "createdAt": Int64,
        "updatedAt": Int64,
        "key": String,
        "nylasConfigId": String,
        "adminIds": List(String),
        "caseTypeId": String,
        "formId": String,
        "status": String,
        "theme": String,
        "title": String,
        "description": String,
        "duration": Int64,
        "availability": String,
        "location": String,
        "emailsDisabled": Boolean,
    }
)
### appointments ###
spare_raw_input_schema_appointments = Schema(
    {
        "id": String,
        "createdAt": Int64,
        "updatedAt": Int64,
        "appointmentTypeId": Categorical,
        "caseId": String,
        "caseFormId": String,
        "adminId": String,
        "bookingId": String,
        "eventId": String,
        "startTs": Int64,
        "endTs": Int64,
        "status": Categorical,
    }
)
### appointments ###
# spare_raw_input_schema_appointments =  Schema({'id': String, 'createdAt': Int64, 'updatedAt': Int64, 'appointmentTypeId': Categorical, 'caseId': String, 'caseFormId': String, 'adminId': String, 'bookingId': String, 'eventId': String, 'startTs': Int64, 'endTs': Int64, 'status': Categorical})
### caseForms ###
spare_raw_input_schema_caseForms = Schema(
    {
        "id": String,
        "createdAt": Int64,
        "updatedAt": Int64,
        "caseId": String,
        "formId": Categorical,
        "metadata": String,
        "smartScanFile.id": String,
        "smartScanFile.size": Int64,
        "smartScanFile.name": String,
        "smartScanFile.format": Categorical,
    }
)
### caseLetters ###
spare_raw_input_schema_caseLetters = Schema(
    {
        "id": String,
        "createdAt": Int64,
        "updatedAt": Int64,
        "caseId": String,
        "letterId": Categorical,
        "file.id": String,
        "file.size": Int64,
        "file.name": String,
        "file.format": Categorical,
        "creatorId": String,
        "status": Categorical,
        "sentStatus": Categorical,
        "sentTs": Int64,
        "sentById": String,
        "mailingAddress": String,
    }
)