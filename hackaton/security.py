from datetime import datetime, timedelta

from pwdlib import PasswordHash

from jwt import encode, decode, PyJWTError

from zoneinfo import ZoneInfo

from fastapi import Depends, HTTPException

from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

from http import HTTPStatus

from hackaton.settings import Settings

pwd_context = PasswordHash.recommended()

def password_hash(password:str)->str:
    return pwd_context.hash(password)

def verify_password(plain_password:str, hashed_password:str)->str:
    return pwd_context.verify(plain_password, hashed_password)

SECRETY_KEY = Settings().SECRETY_KEY
ALGORITHM = 'HS256'
ACCESS_TOKEN_EXPIRE_MINUTES = 30
REFRESH_TOKEN_EXPIRE_DAYS=2


def create_access_token(data:dict)->str:
    to_encode = data.copy()

    expire = datetime.now(tz=ZoneInfo('UTC') ) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)

    to_encode.update({'exp':expire, 'type':'access'})


    token = encode(to_encode, algorithm=ALGORITHM, key=Settings().SECRETY_KEY)

    return token

def create_refresh_token(data:dict)->str:
    to_encode = data.copy()

    expire = datetime.now(tz=ZoneInfo('UTC') ) + timedelta(days=REFRESH_TOKEN_EXPIRE_DAYS)

    to_encode.update({'exp':expire, 'type':'refresh'})


    refresh_token = encode(to_encode, algorithm=ALGORITHM, key=Settings().SECRETY_KEY)

    return refresh_token


security_scheme = HTTPBearer()


def verify_refresh_token(credentials: HTTPAuthorizationCredentials = Depends(security_scheme)) -> dict:
    token = credentials.credentials
    try:
        payload = decode(token, Settings().SECRETY_KEY, algorithms=[ALGORITHM])
        
        token_type = payload.get("type") 
        
        if token_type != "refresh":
            raise HTTPException(
                status_code=HTTPStatus.BAD_REQUEST,
                detail=f"Erro: Você enviou um token do tipo '{token_type}', mas eu preciso de um 'refresh'!"
            )
            
        return payload
    except PyJWTError as e:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail=f"Erro de JWT: {str(e)}"
        )

    except PyJWTError:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail="Invalid or expired refresh token!"
        )


def get_current_admin(credentials:HTTPAuthorizationCredentials = Depends(security_scheme)) -> dict:
    token = credentials.credentials

    try:
        payload = decode(
            token,
            Settings().SECRETY_KEY,
            algorithms=[ALGORITHM]
        )

        role = payload.get("role")

        if role != "admin":
            raise HTTPException(
                status_code = HTTPStatus.FORBIDDEN
            )
    
        return payload
    except PyJWTError:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail='Invalid or expired token!'
        )


def get_current_user(credentials:HTTPAuthorizationCredentials = Depends(security_scheme)) -> dict:
    token = credentials.credentials

    try:
        payload = decode(
            token,
            Settings().SECRETY_KEY,
            algorithms=[ALGORITHM]
        )

        return payload

    except PyJWTError:
        raise HTTPException(
            status_code=HTTPStatus.UNAUTHORIZED,
            detail='Invalid or expired token!'
        )