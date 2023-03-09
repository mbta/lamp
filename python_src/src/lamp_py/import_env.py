import os
import logging


def load_environment() -> None:
    """
    Load environment variables from .env file if it exists
    """
    try:
        if int(os.environ.get("BOOTSTRAPPED", 0)) == 1:
            return

        here = os.path.dirname(os.path.abspath(__file__))
        env_file = os.path.join(here, "..", "..", "..", ".env")
        env_file = os.path.abspath(env_file)
        logging.info("bootstrapping with env file %s", env_file)

        with open(env_file, "r", encoding="utf8") as reader:
            for line in reader.readlines():
                line = line.rstrip("\n")
                line.replace('"', "")
                if line.startswith("#") or line == "":
                    continue
                key, value = line.split("=")
                logging.info("setting %s to %s", key, value)
                os.environ[key] = value

    except FileNotFoundError as fnfe:
        logging.warning("Unable to find .env file %s", fnfe)
    except Exception as exception:
        logging.exception("Error while trying to bootstrap")
        raise exception
