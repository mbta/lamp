from typing import Optional
import pyarrow


def convert_to_tableau_compatible_schema(
    input_schema: pyarrow.schema, overrides: Optional[dict] = None, exclude: Optional[list] = None
) -> pyarrow.schema:
    """
    generic converter for known types (dates, timezones, etc)
    overrides to correct the rest
    overrides_exclude -
    """
    # Create a map to store the modified fields that can't be auto-inferred
    auto_schema = []
    # Loop through the schema fields and apply changes
    for field in input_schema:
        print(field.type)

        if exclude is not None and field.name in exclude:
            continue
        if overrides is not None and field.name in overrides.keys():
            new_field = field.with_type(overrides[field.name])
            auto_schema.append(new_field)
            continue
        if isinstance(field.type, pyarrow.TimestampType):
            if field.type.tz is not None:
                # strip tz
                new_field = field.with_type(pyarrow.timestamp(unit=field.type.unit))
                auto_schema.append(new_field)
                continue

        # default - no changes, add the field again
        auto_schema.append(field)

    return pyarrow.schema(auto_schema)
