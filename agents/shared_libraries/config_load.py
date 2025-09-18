import logging
from typing import Tuple, Type, List, Dict, Optional
import google.oauth2.id_token
import time
from pydantic import Field, ValidationError
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)
import os


def get_yaml_file():
    """
    Return the path to the lab.yaml file and validate its existence.
    """
    if "CONFIG_PATH" in os.environ:
        yaml_path = os.getenv("CONFIG_PATH")
    else:
        raise Exception(
            "CONFIG_PATH not found in the environment variables. Please"
            " set CONFIG_PATH environment variable following format:"
            " export CONFIG_PATH=lab"
        )
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(f"The configuration file {yaml_path} does not exist.")

    return yaml_path


class SecureToken:
    def __init__(self, token: str):
        self._token = token

    def __str__(self):
        return "<SecureToken: hidden>"

    def __repr__(self):
        return "<SecureToken: hidden>"

    def get(self) -> str:
        return self._token


class TokenCache:
    def __init__(self):
        self._token = None
        self._expiry = 0

    def get_token(self, audience: str) -> SecureToken:
        if self._token is None or time.time() > self._expiry:
            self._token = google.oauth2.id_token.fetch_id_token(
                google.auth.transport.requests.Request(), audience
            )
            self._expiry = time.time() + 300  # Cache for 5 minutes
        return SecureToken(self._token)


class Settings(BaseSettings):
    """
    Class for wrapping all env variables. This way, there is no need to use
    os.getenv() in the app and the variables can be accessed using this class.

    Also, this helps with the validation of the variables. If one variable is
    missing, it will print a message with the variables that are not configured
    in the env file.
    """

    @staticmethod
    def load_configs():
        """Initialize a settings object to get all the defined variables"""
        try:
            settings = Settings()
            return settings
        except ValidationError as e:
            logging.error("Missing env variables in .yaml file:")
            for error in e.errors():
                logging.error("- %s: %s", error["loc"][0], error["msg"])
            raise

    # Configure BaseSettings to read variables from yaml file
    model_config = SettingsConfigDict(yaml_file=get_yaml_file(), extra="ignore")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (env_settings, YamlConfigSettingsSource(settings_cls),)


    PROJECT_ID: str = Field(..., env="PROJECT_ID")
    REGION: str = Field(..., env="REGION")
    LLM_MODEL: str = Field(..., env="LLM_MODEL")
    PROJECT_NUMBER: int = Field(..., env="PROJECT_NUMBER")
    # BQ_TABLE_ID: str = Field(..., env="BQ_TABLE_ID")
    # DISABLE_WEB_DRIVER: int = Field(..., env="DISABLE_WEB_DRIVER")
    # LLM_PROXY_ENDPOINT: str = Field(..., env="LLM_PROXY_ENDPOINT")
    # RUN_AGENT_WITH_DEBUG: bool = Field(..., env="RUN_AGENT_WITH_DEBUG")
    # SERVE_WEB_INTERFACE: bool = Field(..., env="SERVE_WEB_INTERFACE")
    # SESSION_DB_URL: str = Field(..., env="SESSION_DB_URL")
    ARTIFACT_GCS_BUCKET: str = Field(..., env="ARTIFACT_GCS_BUCKET")
    RAG_DEFAULT_TOP_K: int = Field(..., env="RAG_DEFAULT_TOP_K")



