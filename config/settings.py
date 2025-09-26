import logging
import os
from typing import Tuple, Type, List, Dict

from pydantic import Field, ValidationError

from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    YamlConfigSettingsSource,
)



def get_yaml_file():
    """
    Get the env file path from the CONFIG_PATH environment variable.
    """

    if "CONFIG_PATH" in os.environ:
        CONFIG_PATH = os.getenv("CONFIG_PATH")
    else:
        raise Exception(
            "CONFIG_PATH not found in the environment variables. Please"
            " set CONFIG_PATH to a .yaml file in the following format:"
            " config/{USECASE_ID}/{ENV}.yaml"
        )

    return CONFIG_PATH


class Settings(BaseSettings):
    """
    Class for wrapping all env varibles. This way, there is no need to use
    os.getenv() in the app and the variables can be accessed using this class.
    Also, this helps with the validation of the variables. If one variable is
    missing, it will print a message with the variables that are not configured
    in the env file.
    """

    @staticmethod
    def get_settings():
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
    model_config = SettingsConfigDict(yaml_file=get_yaml_file(), env_prefix="APP_", extra="ignore")

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: Type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> Tuple[PydanticBaseSettingsSource, ...]:
        return (env_settings, YamlConfigSettingsSource(settings_cls))

    # ---------- VARIABLES FROM ENV.SH FILE ----------
    PROJECT_ID: str = Field("r2d2-00", env="PROJECT_ID")
    PROJECT_NUMBER: int = Field(..., env="PROJECT_NUMBER")
    REGION: str = Field(..., env="REGION")
    RUN_AGENT_WITH_DEBUG: bool = Field(..., env="RUN_AGENT_WITH_DEBUG")
    ARTIFACT_GCS_BUCKET: str = Field(..., env="ARTIFACT_GCS_BUCKET")
    SESSION_DB_URL: str = Field(..., env="SESSION_DB_URL")
    # SERVICE_ACCOUNT: str = Field(..., env="SERVICE_ACCOUNT")
    RAW_SQL_EXTRACTS_DATASET: str = Field("gdm", env="RAW_SQL_EXTRACTS_DATASET")
    RAW_SQL_EXTRACTS_TABLE: str = Field("raw_sql_extracts", env="RAW_SQL_EXTRACTS_TABLE")

    # RAW_DATA_BUCKET: str = Field(..., env="RAW_DATA_BUCKET")
    # DOCUMENTS_FOLDER: str = Field(..., env="DOCUMENTS_FOLDER")
    # CHUNKS_FOLDER: str = Field(..., env="CHUNKS_FOLDER")
    # CHUNK_SIZE: int = Field(..., env="CHUNK_SIZE")
    # CHUNK_OVERLAP: int = Field(..., env="CHUNK_OVERLAP")
    # PROCESSING_DATA_FOLDER: str = Field(..., env="PROCESSING_DATA_FOLDER")
    # ARCHIVE_FOLDER: str = Field(..., env="ARCHIVE_FOLDER")
    # EMBEDDINGS_FOLDER: str = Field(..., env="EMBEDDINGS_FOLDER")

    # BQ_TABLE: str = Field(..., env="BQ_TABLE")

    # VECTORSEARCH_INDEX_NAME: str = Field(..., env="VECTORSEARCH_INDEX_NAME")
    # VECTORSEARCH_INDEX_ENDPOINT_NAME: str = Field(
    #     ..., env="VECTORSEARCH_INDEX_ENDPOINT_NAME"
    # )
    # VECTORSEARCH_INDEX_ENDPOINT_DEPLOYED_NAME: str = Field(
    #     ..., env="VECTORSEARCH_INDEX_ENDPOINT_DEPLOYED_NAME"
    # )
    # OVERWRITE_INDEX: bool = Field(..., env="OVERWRITE_INDEX")

    # EMBEDDINGS_LLM: str = Field(..., env="EMBEDDINGS_LLM")
    LLM_MODEL: str = Field("gemini-2.5-pro", env="LLM_MODEL")
    # MIRROR_PROJECT_ID: str = Field(..., env="MIRROR_PROJECT_ID")
    # PYTHON_INDEX_URL: str = Field(..., env="PYTHON_INDEX_URL")
    # BASE_IMAGE_URI: str = Field(..., env="BASE_IMAGE_URI")
    # TEMPLATES_IMAGE_URI: str = Field(..., env="TEMPLATES_IMAGE_URI")
    # PIPELINE_ROOT: str = Field(..., env="PIPELINE_ROOT")
    # PIPELINE_PACKAGE: str = Field(..., env="PIPELINE_PACKAGE")
    # PIPELINE_DISPLAY_NAME: str = Field(..., env="PIPELINE_DISPLAY_NAME")
    # PIPELINE_NETWORK: str = Field(..., env="PIPELINE_NETWORK")
    # ENDPOINT_NETWORK: str = Field(..., env="ENDPOINT_NETWORK")
    # LLM_PROXY_ENDPOINT: str = Field(..., env="LLM_PROXY_ENDPOINT")
    # ILB_PROXY_ENDPOINT: str = Field(..., env="ILB_PROXY_ENDPOINT")

    # EVAL_PIPELINE_SERVICE_ACCOUNT: str = Field(..., env="EVAL_PIPELINE_SERVICE_ACCOUNT")
    # EVAL_PIPELINE_DISPLAY_NAME: str = Field(..., env="EVAL_PIPELINE_DISPLAY_NAME")
    # EVAL_PIPELINE_PACKAGE: str = Field(..., env="EVAL_PIPELINE_PACKAGE")
    # EVAL_ENDPOINT_URL: str = Field(..., env="EVAL_ENDPOINT_URL")
    # EVAL_ENDPOINT_METHOD: str = Field(..., env="EVAL_ENDPOINT_METHOD")
    # EVAL_DATASET: str = Field(..., env="EVAL_DATASET")
    # EVAL_GOLDEN_DATASET_BQ: str = Field(..., env="EVAL_GOLDEN_DATASET")
    # EVAL_QUERY_COL_KEY: str = Field(..., env="EVAL_QUERY_COL_KEY")
    # EVAL_RESPONSE_KEY: str = Field(..., env="EVAL_RESPONSE_KEY")
    # EVAL_RESPONSE_QUERY_KEY: str = Field(..., env="EVAL_RESPONSE_QUERY_KEY")
    # EVAL_MODEL_METRICS_LIST: List[str] = Field(..., env="EVAL_MODEL_METRICS_LIST")
    # EVAL_COMPUTATION_METRICS_LIST: List[str] = Field(
    #     ..., env="EVAL_COMPUTATION_METRICS_LIST"
    # )
    # EVAL_MODEL_CUSTOM_METRICS_LIST: List[str] = Field(
    #     ..., env="EVAL_MODEL_CUSTOM_METRICS_LIST"
    # )
    # EVAL_SUMMARY_TABLE_BQ: str = Field(..., env="EVAL_SUMMARY_TABLE_BQ")
    # EVAL_DETAILS_TABLE_BQ: str = Field(..., env="EVAL_DETAILS_TABLE_BQ")
    # EVAL_QUOTA_LIMIT: int = Field(..., env="EVAL_QUOTA_LIMIT")

    # CONT_EVAL_USE_CASE_DATASET: str = Field(..., env="CONT_EVAL_USE_CASE_DATASET")
    # CONT_EVAL_USE_CASE_TABLE: str = Field(..., env="CONT_EVAL_USE_CASE_TABLE")
    # CONT_EVAL_SAMPLING_PCT: int = Field(..., env="CONT_EVAL_SAMPLING_PCT")
    # CONT_EVAL_SAMPLING_MODE: str = Field(..., env="CONT_EVAL_SAMPLING_MODE")
    # CONT_EVAL_DATE_COL: str = Field(..., env="CONT_EVAL_DATE_COL")
    # CONT_EVAL_COL_MAPPING: Dict = Field(..., env="CONT_EVAL_COL_MAPPING")
    # CONT_EVAL_FILTERS: Dict = Field(..., env="CONT_EVAL_FILTERS")
    # CONT_EVAL_DISPLAY_NAME: str = Field(..., env="CONT_EVAL_DISPLAY_NAME")
    # CONT_EVAL_PIPELINE_PACKAGE: str = Field(..., env="CONT_EVAL_PIPELINE_PACKAGE")
    # CONT_EVAL_SUMMARY_TABLE: str = Field(..., env="CONT_EVAL_SUMMARY_TABLE")
    # CONT_EVAL_DETAILS_TABLE: str = Field(..., env="CONT_EVAL_DETAILS_TABLE")
    # CONT_EVAL_EXP_NAME: str = Field(..., env="CONT_EVAL_EXP_NAME")
    # RAG_DEFAULT_EMBEDDING_MODEL: str = Field(..., env="RAG_DEFAULT_EMBEDDING_MODEL")
    RAG_DEFAULT_TOP_K: int = Field(..., env="RAG_DEFAULT_TOP_K")
    RAG_DEFAULT_SEARCH_TOP_K: int = Field(..., env="RAG_DEFAULT_SEARCH_TOP_K")
    RAG_DEFAULT_VECTOR_DISTANCE_THRESHOLD: float = Field(..., env="RAG_DEFAULT_VECTOR_DISTANCE_THRESHOLD")
    # GOOGLE_GENAI_USE_VERTEXAI: int = Field(..., env="GOOGLE_GENAI_USE_VERTEXAI")
    # GOOGLE_CLOUD_PROJECT: str = Field(..., env="GOOGLE_CLOUD_PROJECT")
    # GOOGLE_CLOUD_LOCATION: str = Field(..., env="GOOGLE_CLOUD_LOCATION")
    # This must be changed once the endpoint has been created
    # @property
    # def VECTORSEARCH_INDEX_ENDPOINT_ID(self) -> str:
    #     deployed_index_id = get_deployed_vector_index_id(
    #         self.PROJECT_ID,
    #         self.REGION,
    #         self.VECTORSEARCH_INDEX_NAME,
    #     )
    #     return f"projects/{self.PROJECT_NUMBER}/locations/{self.REGION}/indexEndpoints/{deployed_index_id}"  # noqa E501
