# type: ignore
# pylint: skip-file

import os
import polars as pl
import polars as plspringboard_spare_admins
from pyarrow.fs import S3FileSystem
import pyarrow.parquet as pq
import pyarrow.dataset as pd
import pyarrow.compute as pc

from lamp_py.aws.s3 import file_list_from_s3
from lamp_py.runtime_utils.remote_files import S3_ARCHIVE, S3_SPRINGBOARD, S3Location
from lamp_py.runtime_utils.remote_files_spare import SPARE, SPARE_TABLEAU

### --- THIS FILE IS AUTOGEN FROM AUTOGEN_S3_REMOTE.PY --- DO NOT MODIFY --- ###
### --- THIS FILE IS AUTOGEN FROM AUTOGEN_S3_REMOTE.PY --- DO NOT MODIFY --- ###
### --- THIS FILE IS AUTOGEN FROM AUTOGEN_S3_REMOTE.PY --- DO NOT MODIFY --- ###

spare_resources = {}

springboard_spare_admins = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "admins.parquet"))
tableau_spare_admins = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "admins.parquet"))
spare_resources["admins"] = (springboard_spare_admins, tableau_spare_admins)
springboard_spare_appointmentTypes = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "appointmentTypes.parquet")
)
tableau_spare_appointmentTypes = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "appointmentTypes.parquet")
)
spare_resources["appointmentTypes"] = (springboard_spare_appointmentTypes, tableau_spare_appointmentTypes)
springboard_spare_appointments_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "appointments_with_history.parquet")
)
tableau_spare_appointments_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "appointments_with_history.parquet")
)
spare_resources["appointments_with_history"] = (
    springboard_spare_appointments_with_history,
    tableau_spare_appointments_with_history,
)
springboard_spare_caseForms_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "caseForms_with_history.parquet")
)
tableau_spare_caseForms_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "caseForms_with_history.parquet")
)
spare_resources["caseForms_with_history"] = (
    springboard_spare_caseForms_with_history,
    tableau_spare_caseForms_with_history,
)
springboard_spare_caseLetters = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "caseLetters.parquet"))
tableau_spare_caseLetters = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "caseLetters.parquet"))
spare_resources["caseLetters"] = (springboard_spare_caseLetters, tableau_spare_caseLetters)
springboard_spare_caseLetters_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "caseLetters_with_history.parquet")
)
tableau_spare_caseLetters_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "caseLetters_with_history.parquet")
)
spare_resources["caseLetters_with_history"] = (
    springboard_spare_caseLetters_with_history,
    tableau_spare_caseLetters_with_history,
)
springboard_spare_caseStatuses = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "caseStatuses.parquet"))
tableau_spare_caseStatuses = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "caseStatuses.parquet"))
spare_resources["caseStatuses"] = (springboard_spare_caseStatuses, tableau_spare_caseStatuses)
springboard_spare_caseTypes = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "caseTypes.parquet"))
tableau_spare_caseTypes = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "caseTypes.parquet"))
spare_resources["caseTypes"] = (springboard_spare_caseTypes, tableau_spare_caseTypes)
springboard_spare_cases_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "cases_with_history.parquet")
)
tableau_spare_cases_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "cases_with_history.parquet")
)
spare_resources["cases_with_history"] = (springboard_spare_cases_with_history, tableau_spare_cases_with_history)
springboard_spare_charges_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "charges_with_history.parquet")
)
tableau_spare_charges_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "charges_with_history.parquet")
)
spare_resources["charges_with_history"] = (springboard_spare_charges_with_history, tableau_spare_charges_with_history)
springboard_spare_constraintOverrideActions_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "constraintOverrideActions_with_history.parquet")
)
tableau_spare_constraintOverrideActions_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "constraintOverrideActions_with_history.parquet")
)
spare_resources["constraintOverrideActions_with_history"] = (
    springboard_spare_constraintOverrideActions_with_history,
    tableau_spare_constraintOverrideActions_with_history,
)
springboard_spare_drivers = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "drivers.parquet"))
tableau_spare_drivers = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "drivers.parquet"))
spare_resources["drivers"] = (springboard_spare_drivers, tableau_spare_drivers)
springboard_spare_duties_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "duties_with_history.parquet")
)
tableau_spare_duties_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "duties_with_history.parquet")
)
spare_resources["duties_with_history"] = (springboard_spare_duties_with_history, tableau_spare_duties_with_history)
springboard_spare_favoriteLocations = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "favoriteLocations.parquet")
)
tableau_spare_favoriteLocations = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "favoriteLocations.parquet")
)
spare_resources["favoriteLocations"] = (springboard_spare_favoriteLocations, tableau_spare_favoriteLocations)
springboard_spare_favoriteLocations_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "favoriteLocations_with_history.parquet")
)
tableau_spare_favoriteLocations_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "favoriteLocations_with_history.parquet")
)
spare_resources["favoriteLocations_with_history"] = (
    springboard_spare_favoriteLocations_with_history,
    tableau_spare_favoriteLocations_with_history,
)
springboard_spare_fleets = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "fleets.parquet"))
tableau_spare_fleets = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "fleets.parquet"))
spare_resources["fleets"] = (springboard_spare_fleets, tableau_spare_fleets)
springboard_spare_forms = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "forms.parquet"))
tableau_spare_forms = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "forms.parquet"))
spare_resources["forms"] = (springboard_spare_forms, tableau_spare_forms)
springboard_spare_groupConditions = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "groupConditions.parquet")
)
tableau_spare_groupConditions = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "groupConditions.parquet")
)
spare_resources["groupConditions"] = (springboard_spare_groupConditions, tableau_spare_groupConditions)
springboard_spare_groupMemberships = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "groupMemberships.parquet")
)
tableau_spare_groupMemberships = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "groupMemberships.parquet")
)
spare_resources["groupMemberships"] = (springboard_spare_groupMemberships, tableau_spare_groupMemberships)
springboard_spare_groups = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "groups.parquet"))
tableau_spare_groups = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "groups.parquet"))
spare_resources["groups"] = (springboard_spare_groups, tableau_spare_groups)
springboard_spare_letters = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "letters.parquet"))
tableau_spare_letters = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "letters.parquet"))
spare_resources["letters"] = (springboard_spare_letters, tableau_spare_letters)
springboard_spare_paymentMethodTypes = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "paymentMethodTypes.parquet")
)
tableau_spare_paymentMethodTypes = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "paymentMethodTypes.parquet")
)
spare_resources["paymentMethodTypes"] = (springboard_spare_paymentMethodTypes, tableau_spare_paymentMethodTypes)
springboard_spare_paymentMethods = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "paymentMethods.parquet")
)
tableau_spare_paymentMethods = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "paymentMethods.parquet")
)
spare_resources["paymentMethods"] = (springboard_spare_paymentMethods, tableau_spare_paymentMethods)
springboard_spare_quickReplies = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "quickReplies.parquet"))
tableau_spare_quickReplies = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "quickReplies.parquet"))
spare_resources["quickReplies"] = (springboard_spare_quickReplies, tableau_spare_quickReplies)
springboard_spare_requestConstraintOverrides = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "requestConstraintOverrides.parquet")
)
tableau_spare_requestConstraintOverrides = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "requestConstraintOverrides.parquet")
)
spare_resources["requestConstraintOverrides"] = (
    springboard_spare_requestConstraintOverrides,
    tableau_spare_requestConstraintOverrides,
)
springboard_spare_requestConstraintOverrides_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "requestConstraintOverrides_with_history.parquet")
)
tableau_spare_requestConstraintOverrides_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "requestConstraintOverrides_with_history.parquet")
)
spare_resources["requestConstraintOverrides_with_history"] = (
    springboard_spare_requestConstraintOverrides_with_history,
    tableau_spare_requestConstraintOverrides_with_history,
)
springboard_spare_requestRecurrences = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "requestRecurrences.parquet")
)
tableau_spare_requestRecurrences = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "requestRecurrences.parquet")
)
spare_resources["requestRecurrences"] = (springboard_spare_requestRecurrences, tableau_spare_requestRecurrences)
springboard_spare_requests = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "requests.parquet"))
tableau_spare_requests = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "requests.parquet"))
spare_resources["requests"] = (springboard_spare_requests, tableau_spare_requests)
springboard_spare_requests_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "requests_with_history.parquet")
)
tableau_spare_requests_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "requests_with_history.parquet")
)
spare_resources["requests_with_history"] = (
    springboard_spare_requests_with_history,
    tableau_spare_requests_with_history,
)
springboard_spare_riders = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "riders.parquet"))
tableau_spare_riders = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "riders.parquet"))
spare_resources["riders"] = (springboard_spare_riders, tableau_spare_riders)
springboard_spare_services = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "services.parquet"))
tableau_spare_services = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "services.parquet"))
spare_resources["services"] = (springboard_spare_services, tableau_spare_services)
springboard_spare_stops = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "stops.parquet"))
tableau_spare_stops = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "stops.parquet"))
spare_resources["stops"] = (springboard_spare_stops, tableau_spare_stops)
springboard_spare_timeRules = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "timeRules.parquet"))
tableau_spare_timeRules = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "timeRules.parquet"))
spare_resources["timeRules"] = (springboard_spare_timeRules, tableau_spare_timeRules)
springboard_spare_userBans = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "userBans.parquet"))
tableau_spare_userBans = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "userBans.parquet"))
spare_resources["userBans"] = (springboard_spare_userBans, tableau_spare_userBans)
springboard_spare_userFleetAgreements = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "userFleetAgreements.parquet")
)
tableau_spare_userFleetAgreements = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "userFleetAgreements.parquet")
)
spare_resources["userFleetAgreements"] = (springboard_spare_userFleetAgreements, tableau_spare_userFleetAgreements)
springboard_spare_vehicleTypes = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "vehicleTypes.parquet"))
tableau_spare_vehicleTypes = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "vehicleTypes.parquet"))
spare_resources["vehicleTypes"] = (springboard_spare_vehicleTypes, tableau_spare_vehicleTypes)
springboard_spare_vehicles = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "vehicles.parquet"))
tableau_spare_vehicles = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "vehicles.parquet"))
spare_resources["vehicles"] = (springboard_spare_vehicles, tableau_spare_vehicles)
springboard_spare_walletTransactions = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "walletTransactions.parquet")
)
tableau_spare_walletTransactions = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "walletTransactions.parquet")
)
spare_resources["walletTransactions"] = (springboard_spare_walletTransactions, tableau_spare_walletTransactions)
springboard_spare_walletTransactions_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "walletTransactions_with_history.parquet")
)
tableau_spare_walletTransactions_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "walletTransactions_with_history.parquet")
)
spare_resources["walletTransactions_with_history"] = (
    springboard_spare_walletTransactions_with_history,
    tableau_spare_walletTransactions_with_history,
)
springboard_spare_zones = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "zones.parquet"))
tableau_spare_zones = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "zones.parquet"))
spare_resources["zones"] = (springboard_spare_zones, tableau_spare_zones)
springboard_spare_appointments = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "appointments/"))
tableau_spare_appointments = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "appointments.parquet"))
spare_resources["appointments"] = (springboard_spare_appointments, tableau_spare_appointments)
springboard_spare_caseForms = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "caseForms/"))
tableau_spare_caseForms = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "caseForms.parquet"))
spare_resources["caseForms"] = (springboard_spare_caseForms, tableau_spare_caseForms)
springboard_spare_caseLetters = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "caseLetters/"))
tableau_spare_caseLetters = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "caseLetters.parquet"))
spare_resources["caseLetters"] = (springboard_spare_caseLetters, tableau_spare_caseLetters)
springboard_spare_cases = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "cases/"))
tableau_spare_cases = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "cases.parquet"))
spare_resources["cases"] = (springboard_spare_cases, tableau_spare_cases)
springboard_spare_charges = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "charges/"))
tableau_spare_charges = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "charges.parquet"))
spare_resources["charges"] = (springboard_spare_charges, tableau_spare_charges)
springboard_spare_constraintOverrideActions = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "constraintOverrideActions/")
)
tableau_spare_constraintOverrideActions = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "constraintOverrideActions.parquet")
)
spare_resources["constraintOverrideActions"] = (
    springboard_spare_constraintOverrideActions,
    tableau_spare_constraintOverrideActions,
)
springboard_spare_duties = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "duties/"))
tableau_spare_duties = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "duties.parquet"))
spare_resources["duties"] = (springboard_spare_duties, tableau_spare_duties)
springboard_spare_fallback = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "fallback/"))
tableau_spare_fallback = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "fallback.parquet"))
spare_resources["fallback"] = (springboard_spare_fallback, tableau_spare_fallback)
springboard_spare_favoriteLocations = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "favoriteLocations/")
)
tableau_spare_favoriteLocations = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "favoriteLocations.parquet")
)
spare_resources["favoriteLocations"] = (springboard_spare_favoriteLocations, tableau_spare_favoriteLocations)
springboard_spare_groupMemberships = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "groupMemberships/"))
tableau_spare_groupMemberships = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "groupMemberships.parquet")
)
spare_resources["groupMemberships"] = (springboard_spare_groupMemberships, tableau_spare_groupMemberships)
springboard_spare_paymentMethods = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "paymentMethods/"))
tableau_spare_paymentMethods = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "paymentMethods.parquet")
)
spare_resources["paymentMethods"] = (springboard_spare_paymentMethods, tableau_spare_paymentMethods)
springboard_spare_requestConstraintOverrides = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "requestConstraintOverrides/")
)
tableau_spare_requestConstraintOverrides = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "requestConstraintOverrides.parquet")
)
spare_resources["requestConstraintOverrides"] = (
    springboard_spare_requestConstraintOverrides,
    tableau_spare_requestConstraintOverrides,
)
springboard_spare_requestRecurrences = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "requestRecurrences/")
)
tableau_spare_requestRecurrences = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "requestRecurrences.parquet")
)
spare_resources["requestRecurrences"] = (springboard_spare_requestRecurrences, tableau_spare_requestRecurrences)
springboard_spare_requests = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "requests/"))
tableau_spare_requests = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "requests.parquet"))
spare_resources["requests"] = (springboard_spare_requests, tableau_spare_requests)
springboard_spare_riders = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "riders/"))
tableau_spare_riders = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "riders.parquet"))
spare_resources["riders"] = (springboard_spare_riders, tableau_spare_riders)
springboard_spare_userFleetAgreements = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "userFleetAgreements/")
)
tableau_spare_userFleetAgreements = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "userFleetAgreements.parquet")
)
spare_resources["userFleetAgreements"] = (springboard_spare_userFleetAgreements, tableau_spare_userFleetAgreements)
springboard_spare_vehicleLocation = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "vehicleLocation/"))
tableau_spare_vehicleLocation = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "vehicleLocation.parquet")
)
spare_resources["vehicleLocation"] = (springboard_spare_vehicleLocation, tableau_spare_vehicleLocation)
springboard_spare_vehicleLocations = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "vehicleLocations/"))
tableau_spare_vehicleLocations = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "vehicleLocations.parquet")
)
spare_resources["vehicleLocations"] = (springboard_spare_vehicleLocations, tableau_spare_vehicleLocations)
springboard_spare_walletTransactions = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "walletTransactions/")
)
tableau_spare_walletTransactions = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "walletTransactions.parquet")
)
spare_resources["walletTransactions"] = (springboard_spare_walletTransactions, tableau_spare_walletTransactions)

#     ff.write("""
# from polars import Schema
# from polars import String, Int16, Int32, Int64, UInt8, UInt16, UInt32, UInt64, List, Struct, Boolean, Categorical, Float64
# """)

#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_admins.bucket, file_prefix=springboard_spare_admins.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### admins ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_admins = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"admins FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_appointmentTypes.bucket, file_prefix=springboard_spare_appointmentTypes.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### appointmentTypes ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_appointmentTypes = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"appointmentTypes FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_appointments_with_history.bucket, file_prefix=springboard_spare_appointments_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### appointments_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_appointments_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"appointments_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_caseForms_with_history.bucket, file_prefix=springboard_spare_caseForms_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### caseForms_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_caseForms_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"caseForms_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_caseLetters.bucket, file_prefix=springboard_spare_caseLetters.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### caseLetters ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_caseLetters = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"caseLetters FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_caseLetters_with_history.bucket, file_prefix=springboard_spare_caseLetters_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### caseLetters_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_caseLetters_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"caseLetters_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_caseStatuses.bucket, file_prefix=springboard_spare_caseStatuses.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### caseStatuses ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_caseStatuses = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"caseStatuses FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_caseTypes.bucket, file_prefix=springboard_spare_caseTypes.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### caseTypes ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_caseTypes = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"caseTypes FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_cases_with_history.bucket, file_prefix=springboard_spare_cases_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### cases_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_cases_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"cases_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_charges_with_history.bucket, file_prefix=springboard_spare_charges_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### charges_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_charges_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"charges_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_constraintOverrideActions_with_history.bucket, file_prefix=springboard_spare_constraintOverrideActions_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### constraintOverrideActions_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_constraintOverrideActions_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"constraintOverrideActions_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_drivers.bucket, file_prefix=springboard_spare_drivers.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### drivers ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_drivers = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"drivers FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_duties_with_history.bucket, file_prefix=springboard_spare_duties_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### duties_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_duties_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"duties_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_favoriteLocations.bucket, file_prefix=springboard_spare_favoriteLocations.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### favoriteLocations ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_favoriteLocations = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"favoriteLocations FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_favoriteLocations_with_history.bucket, file_prefix=springboard_spare_favoriteLocations_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### favoriteLocations_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_favoriteLocations_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"favoriteLocations_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_fleets.bucket, file_prefix=springboard_spare_fleets.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### fleets ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_fleets = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"fleets FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_forms.bucket, file_prefix=springboard_spare_forms.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### forms ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_forms = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"forms FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_groupConditions.bucket, file_prefix=springboard_spare_groupConditions.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### groupConditions ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_groupConditions = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"groupConditions FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_groupMemberships.bucket, file_prefix=springboard_spare_groupMemberships.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### groupMemberships ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_groupMemberships = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"groupMemberships FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_groups.bucket, file_prefix=springboard_spare_groups.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### groups ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_groups = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"groups FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_letters.bucket, file_prefix=springboard_spare_letters.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### letters ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_letters = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"letters FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_paymentMethodTypes.bucket, file_prefix=springboard_spare_paymentMethodTypes.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### paymentMethodTypes ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_paymentMethodTypes = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"paymentMethodTypes FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_paymentMethods.bucket, file_prefix=springboard_spare_paymentMethods.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### paymentMethods ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_paymentMethods = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"paymentMethods FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_quickReplies.bucket, file_prefix=springboard_spare_quickReplies.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### quickReplies ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_quickReplies = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"quickReplies FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_requestConstraintOverrides.bucket, file_prefix=springboard_spare_requestConstraintOverrides.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### requestConstraintOverrides ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_requestConstraintOverrides = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"requestConstraintOverrides FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_requestConstraintOverrides_with_history.bucket, file_prefix=springboard_spare_requestConstraintOverrides_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### requestConstraintOverrides_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_requestConstraintOverrides_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"requestConstraintOverrides_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_requestRecurrences.bucket, file_prefix=springboard_spare_requestRecurrences.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### requestRecurrences ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_requestRecurrences = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"requestRecurrences FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_requests.bucket, file_prefix=springboard_spare_requests.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### requests ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_requests = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"requests FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_requests_with_history.bucket, file_prefix=springboard_spare_requests_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### requests_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_requests_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"requests_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_riders.bucket, file_prefix=springboard_spare_riders.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### riders ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_riders = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"riders FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_services.bucket, file_prefix=springboard_spare_services.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### services ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_services = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"services FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_stops.bucket, file_prefix=springboard_spare_stops.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### stops ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_stops = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"stops FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_timeRules.bucket, file_prefix=springboard_spare_timeRules.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### timeRules ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_timeRules = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"timeRules FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_userBans.bucket, file_prefix=springboard_spare_userBans.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### userBans ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_userBans = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"userBans FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_userFleetAgreements.bucket, file_prefix=springboard_spare_userFleetAgreements.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### userFleetAgreements ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_userFleetAgreements = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"userFleetAgreements FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_vehicleTypes.bucket, file_prefix=springboard_spare_vehicleTypes.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### vehicleTypes ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_vehicleTypes = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"vehicleTypes FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_vehicles.bucket, file_prefix=springboard_spare_vehicles.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### vehicles ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_vehicles = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"vehicles FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_walletTransactions.bucket, file_prefix=springboard_spare_walletTransactions.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### walletTransactions ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_walletTransactions = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"walletTransactions FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_walletTransactions_with_history.bucket, file_prefix=springboard_spare_walletTransactions_with_history.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### walletTransactions_with_history ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_walletTransactions_with_history = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"walletTransactions_with_history FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_zones.bucket, file_prefix=springboard_spare_zones.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### zones ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_zones = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"zones FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_appointments.bucket, file_prefix=springboard_spare_appointments.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### appointments ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_appointments = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"appointments FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_caseForms.bucket, file_prefix=springboard_spare_caseForms.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### caseForms ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_caseForms = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"caseForms FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_caseLetters.bucket, file_prefix=springboard_spare_caseLetters.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### caseLetters ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_caseLetters = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"caseLetters FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_cases.bucket, file_prefix=springboard_spare_cases.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### cases ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_cases = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"cases FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_charges.bucket, file_prefix=springboard_spare_charges.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### charges ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_charges = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"charges FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_constraintOverrideActions.bucket, file_prefix=springboard_spare_constraintOverrideActions.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### constraintOverrideActions ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_constraintOverrideActions = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"constraintOverrideActions FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_duties.bucket, file_prefix=springboard_spare_duties.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### duties ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_duties = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"duties FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_fallback.bucket, file_prefix=springboard_spare_fallback.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### fallback ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_fallback = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"fallback FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_favoriteLocations.bucket, file_prefix=springboard_spare_favoriteLocations.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### favoriteLocations ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_favoriteLocations = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"favoriteLocations FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_groupMemberships.bucket, file_prefix=springboard_spare_groupMemberships.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### groupMemberships ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_groupMemberships = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"groupMemberships FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_paymentMethods.bucket, file_prefix=springboard_spare_paymentMethods.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### paymentMethods ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_paymentMethods = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"paymentMethods FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_requestConstraintOverrides.bucket, file_prefix=springboard_spare_requestConstraintOverrides.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### requestConstraintOverrides ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_requestConstraintOverrides = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"requestConstraintOverrides FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_requestRecurrences.bucket, file_prefix=springboard_spare_requestRecurrences.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### requestRecurrences ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_requestRecurrences = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"requestRecurrences FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_requests.bucket, file_prefix=springboard_spare_requests.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### requests ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_requests = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"requests FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_riders.bucket, file_prefix=springboard_spare_riders.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### riders ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_riders = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"riders FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_userFleetAgreements.bucket, file_prefix=springboard_spare_userFleetAgreements.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### userFleetAgreements ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_userFleetAgreements = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"userFleetAgreements FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_vehicleLocation.bucket, file_prefix=springboard_spare_vehicleLocation.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### vehicleLocation ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_vehicleLocation = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"vehicleLocation FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_vehicleLocations.bucket, file_prefix=springboard_spare_vehicleLocations.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### vehicleLocations ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_vehicleLocations = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"vehicleLocations FAILED!")


#     try:
#         s3_uris = file_list_from_s3(bucket_name=springboard_spare_walletTransactions.bucket, file_prefix=springboard_spare_walletTransactions.prefix)
#         ds_paths = [s.replace("s3://", "") for s in s3_uris]

#         ds = pd.dataset(
#             ds_paths,
#             format="parquet",
#             filesystem=S3FileSystem(),
#         )


#         for batch in ds.to_batches(batch_size=500_000, filter=None):
#             df = pl.from_arrow(batch)
#             print("### walletTransactions ###", file=ff)
#             #print(df.head(1), file=ff)
#             print("spare_raw_input_schema_walletTransactions = ", df.collect_schema(), file=ff)
#             break
#     except:
#         print(f"walletTransactions FAILED!")

# ff.close()
