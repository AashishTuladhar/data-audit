import asyncio
import collections
import services
import re
import logging


async def validate_primary_key(file_path, file_name, field, separator, constraint):
    print(f'Primary Key check started for {field.field_name}...')
    duplicates = await get_duplicates(file_path, file_name, field, separator, constraint)
    nulls = await get_nulls(file_path, file_name, field, separator, constraint)
    print('Primary Key check complete')

    return duplicates | nulls


async def validate_nulls(file_path, file_name, field, separator, constraint):
    print(f'Null check started for {field.field_name}...')
    nulls = await get_nulls(file_path, file_name, field, separator, constraint)
    print('Null check complete')

    return nulls


async def validate_data_types(file_path, file_name, field, separator, constraint):
    print(f'Data type check started for {field.field_name}...')
    type_mismatches = await check_data_types(file_path, file_name, field, separator, constraint)
    print(f'Data type check complete')

    return type_mismatches


async def get_duplicates(file_path, file_name, field, separator, constraint):
    result = []

    file = await asyncio.to_thread(services.read_data_file, file_path, file_name)

    rows = ({'item': item, 'count': count}
            for item, count in collections.Counter(map(lambda x: x.split(separator)[field.field_index], file)).items()
            if count > 1)

    for row in rows:
        result.append(f'Found {row["count"]} duplicates for {field.field_name} {row["item"]}')

    return {'constraint': constraint, 'duplicates': result}


async def get_nulls(file_path, file_name, field, separator, constraint):
    result = []

    file = await asyncio.to_thread(services.read_data_file, file_path, file_name)

    rows = filter(lambda x:
                  (x.split(separator)[field.field_index] == "''" if field.field_type != "TEXT" else False)
                  or x.split(separator)[field.field_index] == '', file)

    for row in rows:
        result.append(f'Found null/empty value for {field.field_name} (Index: {field.field_index}): {row}')

    return {'constraint': constraint, 'nulls': result}


async def check_data_types(file_path, file_name, field, separator, constraint):
    result = []

    file = await asyncio.to_thread(services.read_data_file, file_path, file_name)

    if field.field_type == "TEXT":
        rows = filter(lambda x:
                      x.split(separator)[field.field_index].startswith("'") is False
                      or x.split(separator)[field.field_index].endswith("'") is False, file)
    elif field.field_type == "INTEGER":
        rows = filter(lambda x:
                      x.split(separator)[field.field_index].isdigit() is False, file)
    else:
        rows = filter(lambda x:
                      re.match(r'^[-+]?\d*\.?\d+$', x.split(separator)[field.field_index]) is None, file)

    for row in rows:
        result.append(f'Incorrect data type for {field.field_name} (Index: {field.field_index}): {row}')

    return {'constraint': constraint, 'type_mismatch': result}
