import logging

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("app.log", mode="a"),  
        logging.StreamHandler() 
    ]
)
logger = logging.getLogger(__name__)
logger.info("This is an info message.")
logger.warning("This is a warning message.")
logger.error("This is an error message.")
