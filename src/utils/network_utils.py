import logging

from requests import Session, Response, status_codes
from requests.exceptions import (
    Timeout,
    ConnectionError,
    RequestException,
    HTTPError,
    TooManyRedirects
)

def get_request(url: str, session: Session) -> Response | None:
    """
    Perform a GET request to the specified URL using the provided session.
    Returns the response if successful, or None if an error occurs.
    """
    try:
        response = session.get(url, timeout=5)

        if response.status_code == status_codes.codes.ok:
            return response
        else:
            logging.error(f"Request to {url} failed with status code {response.status_code}.")
    except Timeout:
        logging.error(f"Request to {url} timed out.")
    except ConnectionError:
        logging.error(f"Connection error occurred while trying to reach {url}.")
    except HTTPError as http_err:
        logging.error(f"HTTP error occurred: {http_err} - Status Code: {http_err.response.status_code}")
    except TooManyRedirects:
        logging.error(f"Too many redirects for URL: {url}.")
    except RequestException as req_err:
        logging.error(f"An error occurred: {req_err}")

    return None
