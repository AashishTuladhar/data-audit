import asyncio
import configuration
import validators
import global_constants as constants
import logging
import database
import pathlib
from time import perf_counter

# start the application
def start():
    # setup the basic configuration for the app
    app = configuration.setup()

    # get the input files
    name = (input("Enter data file name: ")
            or 'Products.txt')
    path = (input("Enter file path: ")
            or str(pathlib.Path().resolve()) + r"\Input Files")

    # run the application
    asyncio.run(main(path, name, app))


async def main(path, name, app):
    # timer for start
    start_time = perf_counter()
    print(f'\nAudit started for file {name} at {path}...\n')

    # count the audit errors
    audit_errors = 0

    # perform all the required validations asynchronously
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

    # get the result for each validation perfromed
    for result in results:
        # check for each audit result and increment the error count
        if result['constraint'] == constants.Constraints.PRIMARY_KEY:
            for duplicate in result['duplicates']:
                audit_errors += 1
                logging.info(duplicate)

            for null in result['nulls']:
                audit_errors += 1
                logging.info(null)

        # check for each audit result and increment the error count
        elif result['constraint'] == constants.Constraints.NOT_NULL:
            for null in result['nulls']:
                audit_errors += 1
                logging.info(null)

        # check for each audit result and increment the error count
        elif result['constraint'] == constants.Constraints.TYPE_MISMATCH:
            for type_mismatch in result['type_mismatch']:
                audit_errors += 1
                logging.info(type_mismatch)

    # stop timer
    end_time = perf_counter()
    print(f'\nAudit completed in {end_time - start_time:0.4f}s')

    # if no audit errors, give choice to enter into database
    if audit_errors > 0:
        print(f'\nFound {audit_errors} inconsistencies in the file. Please check the logs for more details.')
    else:
        print('No data inconsistencies found.')
        input_data = input('\nDo you want to import this data? (Y/N) ')

        # create and insert the records into the database
        if input_data.lower() == 'y':
            print('Starting data import...')
        
            database.create_and_insert_data(f'{app.validation_file_path}\\{app.validation_file_name}',
                                            f'{path}\\{name}')
        
            print('Data import completed.')
        else:
            print('Canceled data import')


if __name__ == "__main__":
    start()
