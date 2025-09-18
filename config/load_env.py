import yaml

from settings import Settings, get_yaml_file

settings = Settings.get_settings()

if __name__ == "__main__":
    """
    This code is used to load the env variables in the dockerfile
    using the yaml config file.
    """

    config_file_path = get_yaml_file()

    with open(config_file_path, "r", encoding="utf-8") as tmp:
        config = yaml.safe_load(tmp)

    for key, value in config.items():
        print(f'export {key}="{value}"')
