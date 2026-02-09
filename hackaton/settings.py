from pydantic_settings import SettingsConfigDict, BaseSettings

class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file='.env' 
    )

    DATABASE_URL : str = ''
    SECRETY_KEY : str = ''