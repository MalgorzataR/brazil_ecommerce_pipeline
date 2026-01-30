from typing import Optional, Any
import backoff
import requests

from airflow.sensors.base import BaseSensorOperator


class BrazilSensorOperator(BaseSensorOperator):
    """
    Sensor that checks for the existence of a dataset at a Kaggle website.

    :param base_url: The base URL to check.
    :param dataset: Dataset name to append to the base URL.
    """
    def __init__(self, base_url: str, dataset: Optional[str] = None, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.dataset = dataset
        self.base_url = base_url
        self.search_url = base_url if dataset is None else f"{base_url}/{dataset}"

    def poke(self) -> bool:
        if self.dataset:
            self.search_url = f"{self.base_url}/{self.dataset}"
        else:
            self.search_url = self.base_url
        self.log.info(f"Poking for dataset URL: {self.search_url}")
        if self._check_response():
            self.log.info(f"Dataset found at URL: {self.search_url}")
            return True
        else:
            self.log.info(f"Dataset not found at URL: {self.search_url}")
            return False
    
    @backoff.on_exception(backoff.expo, requests.exceptions.RequestException, max_tries=5)
    def _check_response(self) -> bool:
        response = requests.get(self.search_url, timeout=10)
        self.log.debug(f"Response status_code: {response.status_code}")
        self.log.info(f"Response status_code: {response.status_code}")
        if response.status_code == 200:
            return True
        return False
