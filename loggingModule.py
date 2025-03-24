import logging

# Configure Logging
logging.basicConfig(
    level=logging.INFO,  # Logging level
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log", mode="a"),  # Append logs to file
        logging.StreamHandler()  # Print logs to console
    ]
)

# Create a logger instance
logger = logging.getLogger(__name__)

# Example logs
logger.info("This is an info message.")
logger.warning("This is a warning message.")
logger.error("This is an error message.")
