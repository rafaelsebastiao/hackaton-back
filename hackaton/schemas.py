from datetime import datetime

from pydantic import BaseModel


class UserSchema(BaseModel):
    username:str
    email:str
    role:str
    password:str

class UserPublic(BaseModel):
    username:str
    email:str
    role:str

class AuditResultSchema(BaseModel):
    date:datetime
    
    line:int

    clear_pm: str

    ref_qtd_sum: int

    ref_freq_sum: int

    ref_formal_sum: int

    ref_informal_sum: int

    nc_total_sum: int

    opened_nc_sum: int

    priority: int

    status: bool

    situation: str

    description: str

class Token(BaseModel):
    access_token:str
    refresh_token:str
    token_type:str