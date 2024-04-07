import asyncio
import configuration
import validators
import global_constants as constants
import logging
import database
from time import perf_counter


def start():
    app = configuration.setup()

    name = (input("Enter data file name: ")
            or 'Products.txt')
    path = (input("Enter file path: ")
            or r'F:\Data audit Project\data-audit\Input Files')

    asyncio.run(main(path, name, app))


async def main(path, name, app):
    start_time = perf_counter()
    print(f'\nAudit started for file {name} at {path}...\n')

    audit_errors = 0

    results = await asyncio.gather(
        validators.validate_primary_key(
            path, name, app.fields.primary_key_field, app.separator, constants.Constraints.PRIMARY_KEY),
        *[validators.validate_nulls(
            path, name, item, app.separator, constants.Constraints.NOT_NULL)
            for item in app.fields.not_null_fields],
        *[validators.validate_data_types(
            path, name, item, app.separator, constants.Constraints.TYPE_MISMATCH)
            for item in app.fields.get_all_fields()],
    )

    for result in results:
        if result['constraint'] == constants.Constraints.PRIMARY_KEY:
            for duplicate in result['duplicates']:
                audit_errors += 1
                logging.info(duplicate)

            for null in result['nulls']:
                audit_errors += 1
                logging.info(null)

        elif result['constraint'] == constants.Constraints.NOT_NULL:
            for null in result['nulls']:
                audit_errors += 1
                logging.info(null)

        elif result['constraint'] == constants.Constraints.TYPE_MISMATCH:
            for type_mismatch in result['type_mismatch']:
                audit_errors += 1
                logging.info(type_mismatch)

    end_time = perf_counter()
    print(f'\nAudit completed in {end_time - start_time:0.4f}s')

    if audit_errors > 0:
        print(f'\nFound {audit_errors} inconsistencies in the file. Please check the logs for more details.')
    else:
        print('No data inconsistencies found.')
        input_data = input('\nDo you want to import this data? (Y/N) ')

        if input_data.lower() == 'y':
            print('Starting data import...')
        
            database.create_and_insert_data(r'F:\Data audit Project\data-audit\Validation Files\V_Products.txt', r'F:\Data audit Project\data-audit\Input Files\Products.txt')
        
            print('Data import completed.')
        else:
            print('Canceled data import')


if __name__ == "__main__":
    start()
