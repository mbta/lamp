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
springboard_spare_appointments = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "appointments.parquet"))
tableau_spare_appointments = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "appointments.parquet"))
spare_resources["appointments"] = (springboard_spare_appointments, tableau_spare_appointments)
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
springboard_spare_caseForms = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "caseForms.parquet"))
tableau_spare_caseForms = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "caseForms.parquet"))
spare_resources["caseForms"] = (springboard_spare_caseForms, tableau_spare_caseForms)
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
springboard_spare_cases = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "cases.parquet"))
tableau_spare_cases = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "cases.parquet"))
spare_resources["cases"] = (springboard_spare_cases, tableau_spare_cases)
springboard_spare_cases_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "cases_with_history.parquet")
)
tableau_spare_cases_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "cases_with_history.parquet")
)
spare_resources["cases_with_history"] = (springboard_spare_cases_with_history, tableau_spare_cases_with_history)
springboard_spare_charges = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "charges.parquet"))
tableau_spare_charges = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "charges.parquet"))
spare_resources["charges"] = (springboard_spare_charges, tableau_spare_charges)
springboard_spare_charges_with_history = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "charges_with_history.parquet")
)
tableau_spare_charges_with_history = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "charges_with_history.parquet")
)
spare_resources["charges_with_history"] = (springboard_spare_charges_with_history, tableau_spare_charges_with_history)
springboard_spare_constraintOverrideActions = S3Location(
    bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "constraintOverrideActions.parquet")
)
tableau_spare_constraintOverrideActions = S3Location(
    bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "constraintOverrideActions.parquet")
)
spare_resources["constraintOverrideActions"] = (
    springboard_spare_constraintOverrideActions,
    tableau_spare_constraintOverrideActions,
)
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
springboard_spare_duties = S3Location(bucket=S3_SPRINGBOARD, prefix=os.path.join(SPARE, "duties.parquet"))
tableau_spare_duties = S3Location(bucket=S3_ARCHIVE, prefix=os.path.join(SPARE_TABLEAU, "duties.parquet"))
spare_resources["duties"] = (springboard_spare_duties, tableau_spare_duties)
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
