def read_data_file(path, filename):
    """
    Generator function to read data from a file line by line.

    Args:
        path (str): The path to the file.
        filename (str): The name of the file.

    Yields:
        str: Each line of the file, stripped of leading and trailing whitespace.
    """
    with open(f'{path}\\{filename}', 'r') as file:
        for line in file:
            yield line.rstrip()


def read_validation_file(path, filename):
    """
    Read the content of a validation file.

    Args:
        path (str): The path to the file.
        filename (str): The name of the file.

    Returns:
        list: A list containing all lines of the file, including newline characters.
    """
    with open(f'{path}\\{filename}', 'r') as file:
        return file.readlines()