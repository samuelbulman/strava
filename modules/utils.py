# Standard imports
import time

# Third-part imports
from colorama import Fore, Style


def log_prefix(log_type: str) -> str:
    """Returns a prefix for adding timne and type details to the scripts logs"""

    formatted_log_time = time.strftime('%Y-%m-%d %H:%M:%S')

    if log_type.upper() in ("START", "END"):
        keyword_color = Fore.GREEN
        dots_after_keyword = 5 if log_type.upper() == "START" else 7

    elif log_type.upper() == 'INFO':
        keyword_color = Fore.YELLOW
        dots_after_keyword = 6

    elif log_type.upper() == 'ERROR':
        keyword_color = Fore.RED
        dots_after_keyword = 5

    else:
        keyword_color = Fore.WHITE
        dots_after_keyword = 5

    dots_post_keyword = "." * dots_after_keyword

    reset = Style.RESET_ALL

    formatted_log_descriptor = f"[{keyword_color}{log_type.upper()}{reset}] {dots_post_keyword}" if log_type else ""

    log_prefix = f"{formatted_log_time} {formatted_log_descriptor}"

    return log_prefix

def read_sql(file_path:str) -> str:
    """
    Returns the entire contents of a SQL file.

    Parameters
    ----------
    file_path (str):
    - The path to a SQL file
    """

    try:
        with open(file_path, 'r') as file:
            file_contents = file.read()
        return file_contents
    
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}")
        return None