import json
import logging
from typing import List

import requests

from app.entities.processed_agent_data import ProcessedAgentData
from app.interfaces.store_gateway import StoreGateway


class StoreApiAdapter(StoreGateway):
    def __init__(self, api_base_url):
        self.api_base_url = api_base_url

    def save_data(self, processed_agent_data_batch: List[ProcessedAgentData]):
        """
        Save the processed road data to the Store API.
        Parameters:
            processed_agent_data_batch (dict): Processed road data to be saved.
        Returns:
            bool: True if the data is successfully saved, False otherwise.
        """
        # Implement it
        data = [json.loads(item.json()) for item in processed_agent_data_batch]

        try:
            response = requests.post(f"{self.api_base_url}/processed_agent_data/", json=data)
            response.raise_for_status()  # Raise exception for non-2xx status codes
            if response.status_code == requests.codes.ok:
                logging.info("Data successfully saved to the Store API.")
                return True
            else:
                logging.error(f"Failed to save data. Status code: {response.status_code}")
                return False
        except requests.RequestException as e:
            logging.error(f"Error while saving data: {e}")
            return False
