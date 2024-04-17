import asyncio 
import configuration 
import validators  
import global_constants as constants 
import logging
import database  
import pathlib  
from time import perf_counter  


def start():
    """
    Start the application.
    """
    # Set up the basic configuration for the app
    app = configuration.setup()

    # Get the input files
    name = (input("Enter data file name: ")  # Prompt user for data file name
            or 'Students.txt')
    path = (input("Enter file path: ")  # Prompt user for file path
            or str(pathlib.Path().resolve()) + r"\Input Files")

    # Run the application
    asyncio.run(main(path, name, app))


async def main(path, name, app):
    """
    Main function for performing data audit asynchronously.

    Args:
        path (str): File path.
        name (str): File name.
        app: Configuration object for the application.

    Returns:
        None
    """
    # Timer for start
    start_time = perf_counter()
    print(f'\nAudit started for file {name} at {path}...\n')

    # Count the audit errors
    audit_errors = []

    # Perform all the required validations asynchronously
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

    # Get the result for each validation performed
    for result in results:
        # Check for each audit result and increment the error count
        if result['constraint'] == constants.Constraints.PRIMARY_KEY:
            for duplicate in result['duplicates']:
                audit_errors.append(duplicate)

            for null in result['nulls']:
                audit_errors.append(null)

        # Check for each audit result and increment the error count
        elif result['constraint'] == constants.Constraints.NOT_NULL:
            for null in result['nulls']:
                audit_errors.append(null)

        # Check for each audit result and increment the error count
        elif result['constraint'] == constants.Constraints.TYPE_MISMATCH:
            for type_mismatch in result['type_mismatch']:
                audit_errors.append(type_mismatch)

    # Stop timer
    end_time = perf_counter()
    print(f'\nAudit completed in {end_time - start_time:0.4f}s')

    # If no audit errors, give choice to enter into database
    if len(audit_errors) > 0:
        configuration.configure_logger() # configure the logger

        for error in audit_errors:
            logging.info(error)

        print(f'\nFound {len(audit_errors)} inconsistencies in the file. Please check the logs for more details.')
    else:
        print('No data inconsistencies found.')
        input_data = input('\nDo you want to import this data? (Y/N) ')

        # Create and insert the records into the database
        if input_data.lower() == 'y':
            print('Starting data import...')
        
            database.create_and_insert_data(app,
                                            f'{path}\\{name}')
        else:
            print('Canceled data import')


if __name__ == "__main__":
    start()
