from fastapi import APIRouter, Depends, HTTPException

from fastapi.security import OAuth2PasswordRequestForm

from http import HTTPStatus

from hackaton.schemas import Token

from hackaton.database import get_session

from hackaton.models import UserModel

from hackaton.security import verify_password, create_access_token, create_refresh_token, verify_refresh_token

from sqlalchemy import select

from sqlalchemy.orm import Session 

router = APIRouter(prefix='/auth', tags=['auth'])


@router.post('/token', status_code=HTTPStatus.CREATED, response_model=Token)
async def create_token(
    form_data : OAuth2PasswordRequestForm = Depends(), session:Session = Depends(get_session),    
    ):
    db_user = session.scalar(select(UserModel).where(UserModel.email == form_data.username))

    if not db_user or not verify_password(form_data.password, db_user.password):
        raise HTTPException(
            status_code = HTTPStatus.UNAUTHORIZED,
            detail='Email or password are incorrect!'
        )
    
    access_token = create_access_token({'sub': form_data.username, 'role': db_user.role.value})

    refresh_token = create_refresh_token({'sub':form_data.username, 'role':db_user.role.value})



    return {
        'access_token':access_token,
        'refresh_token':refresh_token,  
        'token_type': 'Bearer'
        }


@router.post("/refresh", response_model=Token)
def refresh_token(payload: dict = Depends(verify_refresh_token)):
    user_data = {k: v for k, v in payload.items() if k not in ["exp", "type"]}
    
    new_access_token = create_access_token(data=user_data)
    new_refresh_token = create_refresh_token(data=user_data)
    
    return {
        "access_token": new_access_token,
        "refresh_token": new_refresh_token,
        "token_type": "bearer"
    }