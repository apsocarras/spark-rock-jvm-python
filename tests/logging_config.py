import datetime
import logging
import os
from types import ModuleType

logging.getLogger("spark_rock_jvm_python").setLevel(logging.DEBUG)
logging.getLogger("faker").setLevel(logging.CRITICAL)
logging.getLogger("pyspark").setLevel(logging.CRITICAL)
logging.getLogger("py4j").setLevel(logging.CRITICAL)

# Configure logging.
# `export DEBUG=1` to see debug output.
# `mkdir logs` to write to files too.
# Create loggers with `import logging; logger = logging.getLogger(__name__)`

module_logger = logging.getLogger(__name__)
module_logger.setLevel(logging.DEBUG)
module_logger_str = f"({module_logger.name}: {module_logger.level})"


main_logger = logging.getLogger(
    "__main__"
)  # Logger to use ony when module is invoked from CLI (??)
main_logger.setLevel(logging.DEBUG)
main_logger_str = f"({main_logger.name}: {main_logger.level})"

console_handler = logging.StreamHandler()
console_handler.setLevel(logging.DEBUG)

try:
    import importlib

    colorlog: ModuleType = importlib.import_module(
        "colorlog"
    )  # `pip install colorlog` for extra goodies.

    stream_formatter = colorlog.ColoredFormatter(
        "%(log_color)s%(asctime)s %(levelname)-6s %(cyan)s%(name)-10s %(white)s%(message)s",
        "%H:%M:%S",
        log_colors={
            "DEBUG": "blue",
            "INFO": "green",
            "WARNING": "yellow",
            "ERROR": "red",
            "CRITICAL": "bold_red",
            "EXCEPTION": "bold_red",
        },
    )
except ImportError:
    stream_formatter = logging.Formatter(
        "%(levelname)s %(name)s %(module)s %(message)s %(pathname)s %(funcName)s"
    )
console_handler.setFormatter(stream_formatter)

package_timestamp = datetime.datetime.now().strftime("%Y%m%dT%H%M%S")
if os.path.isdir("logs"):
    file_handler = logging.FileHandler(
        os.path.join("logs", f"{__name__}_{package_timestamp}.log"), mode="a"
    )
    file_handler.setLevel(
        logging.INFO if not os.environ.get("DEBUG") else logging.DEBUG
    )
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    )
    module_logger.addHandler(file_handler)
    main_logger.addHandler(file_handler)

module_logger.addHandler(console_handler)
main_logger.addHandler(console_handler)


if __name__ == "__main__":
    module_logger.info(module_logger_str)
    main_logger.info(main_logger_str)
