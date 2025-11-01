def read_sql(file_path:str) -> str:
    """
    Returns the entire contents of a SQL file.

    Parameters
    ----------
    - file_path (str): The path to a SQL file
    """

    try:
        with open(file_path, 'r') as file:
            file_contents = file.read()
        return file_contents
    
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None