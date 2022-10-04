from datetime import datetime
import logging
import os
from pytz import timezone


def timetz(*args):
    """Convert time to given timezone."""
    return datetime.now(tz).timetuple()


def set_logging_configuration(log_folder_path, timezone_name="America/New_York"):
    """Set the configuration of the logging function.

    Key arguments:
        log_folder_path (str) -- folder where the logs should be stored.
        timezone (str) -- timezone where the project team is located. It defaults to America/New York.

    Returns:
        log_path (str) -- complete path where the logs are saved.
    """
    # Calculate time in given timezone.
    global tz
    tz = timezone(timezone_name)
    logging.Formatter.converter = timetz

    # Declare complete path where to save the logs.
    log_path = (
        log_folder_path
        + datetime.now(tz).strftime("%Y-%m-%d_%H:%M:%S")
        + "_"
        + os.getlogin()
        + ".log"
    )

    # Set the logging configuration.
    logging.basicConfig(
        # Declare the format to log messages.
        format="%(asctime)s %(levelname)-8s (%(module)s : %(lineno)d) %(message)s",
        # Define logging level.
        level=logging.DEBUG,
        # Define datetime format.
        datefmt="%Y-%m-%d %H:%M:%S",
        # Declare handlers: save logs to log_path.
        handlers=[logging.FileHandler(log_path)],
    )

    # Define a handler which writes warning messages or higher to the sys.stderr.
    console = logging.StreamHandler()
    console.setLevel(logging.WARNING)

    # Add the handler to the root logger.
    logging.getLogger("").addHandler(console)

    # Log the specified timezone.
    logging.info("Timezone: " + str(tz) + ".")

    return log_path
