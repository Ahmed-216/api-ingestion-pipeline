import requests
import pandas as pd
import os
import time
from typing import List, Dict, Optional
from requests.exceptions import HTTPError, ConnectionError, ReadTimeout
import logging
import api_ingestion_pipeline.config as cfg


class ApiIngestion:
    """
    Utility class to ingest data from public APIs and save as CSV in a specified directory.
    """

    def __init__(
        self,
        output_dir: str,
        headers: Optional[Dict] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.session = requests.Session()
        self.session.headers.update(headers or {})
        self.output_dir = output_dir
        self.logger = logger or logging.getLogger(__name__)

        # Ensure output directory exists if provided
        if self.output_dir:
            os.makedirs(self.output_dir, exist_ok=True)

    def fetch_paginated_data(
        self,
        base_url: str,
        params: Optional[Dict] = None,
        data_key: Optional[str] = None,
        page_param: str = "page",
        per_page_param: str = "per_page",
        per_page: int = 100,
        total_pages_param: Optional[str] = None,
        max_retries: int = 5,
        delay: float = 2.0,
        api_description: Optional[str] = None,
    ) -> List[Dict]:
        """
        Fetch all paginated data from an API endpoint.
        Args:
            base_url: The API endpoint URL
            params: Query parameters (dict)
            data_key: Key to extract data from response (for APIs that wrap data)
            page_param: Name of the page parameter
            per_page_param: Name of the per-page parameter
            per_page: Number of records per page
            total_pages_param: Name of the total pages parameter
            max_retries: Max retries for rate limiting
            delay: Delay between retries (seconds)
        Returns:
            List of all records
        """
        self.logger.info(
            "Fetching paginated data from %s",
            base_url,
        )

        # Extract per_page from params or use default
        if params and per_page_param in params:
            per_page = params[per_page_param]

        # Initialize variables
        all_data = []
        page = 1
        retries = 0
        total_pages = None
        params = params.copy() if params else {}
        params[per_page_param] = per_page

        # Loop until all pages are fetched
        while True:
            # If retries exceed max, raise an error
            if retries >= max_retries:
                self.logger.error(
                    "Max retries reached for %s",
                    base_url,
                )
                raise Exception("Max retries exceeded")

            params[page_param] = page

            try:
                # Log the progress
                page_info = f"{page}/{total_pages}" if total_pages else str(page)
                api_desc = api_description or base_url
                self.logger.info(
                    "Requesting page %s for %s",
                    page_info,
                    api_desc,
                )

                # Get the API response
                response = self.session.get(
                    base_url,
                    params=params,
                    timeout=60,
                )
                response.raise_for_status()
                self.logger.debug(
                    "Page %s response status: %s",
                    page,
                    response.status_code,
                )

                # Parse the response as JSON
                try:
                    data = response.json()
                    self.logger.debug(
                        "Page %s JSON parsed successfully",
                        page,
                    )
                except Exception as json_error:
                    self.logger.error(
                        "JSON decode error for %s: %s...",
                        base_url,
                        response.text[:200],
                    )
                    raise json_error

                # If data_key is provided, use it to extract the data from the
                # response
                if data_key:
                    page_data = data[data_key]
                # World Bank API: data is in data[1], metadata in data[0]
                elif (
                    isinstance(data, list)
                    and len(data) > 1
                    and isinstance(data[1], list)
                ):
                    page_data = data[1]
                else:
                    page_data = data

                # Stop condition 1: Break when reaching empty page
                if not page_data:
                    self.logger.info(
                        "Page %s is empty, stopping pagination",
                        page,
                    )
                    break

                self.logger.info(
                    "Page %s: Retrieved %s records",
                    page,
                    len(page_data),
                )
                all_data.extend(page_data)

                # Stop condition 2: Check if we've reached the last page

                # World Bank: check if we've reached the last page
                if (
                    isinstance(data, list)
                    and len(data) > 0
                    and isinstance(data[0], dict)
                ):
                    # Get total pages from API response (only on first page)
                    if total_pages is None:
                        total_pages = int(data[0].get("pages", 1))

                    if page >= total_pages:
                        self.logger.info(
                            "Reached last page (%s/%s)",
                            page,
                            total_pages,
                        )
                        break

                # Generic API: check total pages from specified parameter
                elif total_pages_param and total_pages is None:
                    # Search for the key anywhere in the response
                    total_pages_value = self._find_key_recursive(
                        data, total_pages_param
                    )
                    if total_pages_value is not None:
                        total_pages = int(total_pages_value)
                        self.logger.info(
                            "Found total_pages: %s",
                            total_pages,
                        )

                # Check if we've reached the last page for generic APIs
                if total_pages is not None and page >= total_pages:
                    self.logger.info(
                        "Reached last page (%s/%s)",
                        page,
                        total_pages,
                    )
                    break

                # Stop condition 3: Partial page indicates last page
                if len(page_data) < per_page:
                    self.logger.info(
                        "Partial page (%s < %s), stopping",
                        len(page_data),
                        per_page,
                    )
                    break

                page += 1
                # Reset retries counter after successful request
                retries = 0

            except HTTPError as e:
                # If rate limited, retry after the Retry-After header
                if e.response is not None and e.response.status_code == 429:
                    retry_after = float(
                        e.response.headers.get(
                            "Retry-After",
                            delay,
                        )
                    )
                    self.logger.warning(
                        "Rate limited. Retrying after %ss...",
                        retry_after,
                    )
                    time.sleep(retry_after)
                    retries += 1
                    continue
                self.logger.error(f"API error: {e}")
                raise
            except (ConnectionError, ReadTimeout):
                self.logger.warning(
                    "Network/timeout error. Retrying after %ss... (%s/%s)",
                    delay,
                    retries + 1,
                    max_retries,
                )
                time.sleep(delay)
                retries += 1
                continue
            except Exception as e:
                self.logger.exception("Unexpected error: %s", e)
                raise
        self.logger.info(
            "Fetched %s total records from %s",
            len(all_data),
            base_url,
        )
        return all_data

    def _find_key_recursive(self, data, target_key):
        """Find a key anywhere in nested dictionary structure."""
        if isinstance(data, dict):
            # Check if key exists at current level
            if target_key in data:
                return data[target_key]
            # Search deeper in nested dictionaries
            for value in data.values():
                result = self._find_key_recursive(value, target_key)
                if result is not None:
                    return result
        return None

    def save_to_csv(self, data: List[Dict], filename: str, output_dir: str):
        """
        Save the data to a CSV file in the specified directory.
        Args:
            data: List of dictionaries containing the data to save
            output_dir: Directory to save the CSV file
            filename: Name of the CSV file
        """
        self.logger.info(
            "Saving %s records to %s",
            len(data),
            filename,
        )
        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, filename)
        df = pd.DataFrame(data)
        df.to_csv(file_path, index=False)
        self.logger.info("✅ Successfully saved data to %s", file_path)

    def ingest_and_save(
        self,
        base_url: str,
        filename: str,
        output_dir: str,
        params: Optional[Dict] = None,
        data_key: Optional[str] = None,
        page_param: str = "page",
        per_page_param: str = "per_page",
        per_page: int = 100,
        total_pages_param: Optional[str] = None,
        api_description: Optional[str] = None,
    ):
        """
        Fetches all paginated data from the API and saves it as a CSV file.
        """
        # Extract per_page from params or use default
        if params and per_page_param in params:
            per_page = params[per_page_param]

        data = self.fetch_paginated_data(
            base_url=base_url,
            params=params,
            data_key=data_key,
            page_param=page_param,
            per_page_param=per_page_param,
            per_page=per_page,
            total_pages_param=total_pages_param,
            api_description=api_description,
        )

        self.save_to_csv(data, filename, output_dir)

    def cleanup_existing_files(self, data_sources: List[str]):
        """
        Remove existing raw CSV files before downloading new ones.

        Args:
            data_sources: List of data sources with filenames
        """
        self.logger.info("🧹 Cleaning up existing CSV files...")

        files_removed = 0
        for source_name in data_sources:
            file_path = os.path.join(self.output_dir, source_name + ".csv")

            if os.path.exists(file_path):
                try:
                    os.remove(file_path)
                    self.logger.info(f"Removed: {source_name}")
                    files_removed += 1
                except Exception as e:
                    self.logger.warning(f"⚠️  Could not remove {source_name}: {str(e)}")

        if files_removed > 0:
            self.logger.info(f"✅ Cleanup complete: {files_removed} files removed")
        else:
            self.logger.info("No existing files to clean up")

    def clear_log_file(self, script_name: str):
        """
        Clear the log file for a given script name.

        Args:
            script_name: Name of the script (used for log filename)
        """
        logs_dir = os.path.join(cfg.ROOT_DIR, "logs")
        log_filename = f"{script_name}.log"
        log_filepath = os.path.join(logs_dir, log_filename)

        # Clear the log file at the start of each run
        with open(log_filepath, "w", encoding="utf-8") as f:
            f.write("")  # Clear the file
