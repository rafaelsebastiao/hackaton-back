from http import HTTPStatus

from fastapi import APIRouter, Depends,  HTTPException

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from hackaton.schemas import UserSchema, UserPublic
from hackaton.models import UserModel, ParseUserPermission

from hackaton.database import get_session
from hackaton.security import password_hash, get_current_admin


router = APIRouter(prefix='/users', tags=['users'])


@router.get('/', status_code=HTTPStatus.OK, response_model=list[UserPublic])
async def get_users(
    session = Depends(get_session),
    admin:dict = Depends(get_current_admin)

    ):
    users = session.scalars(select(UserModel) )
    
    return users


@router.get('/{id}',status_code=HTTPStatus.OK, response_model=UserPublic)
async def get_user(
    id : int, 
    
    session: Session = Depends(get_session),
    
    admin:dict = Depends(get_current_admin)

    ):
    
    db_user = session.scalar(
        select(UserModel).where(UserModel.id == id)
    )
    
    if not db_user:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="User not Found"
        )
    
    
    return db_user

@router.post('/', status_code=HTTPStatus.CREATED, response_model=UserPublic)
async def post_user(user: UserSchema,
    admin : dict=Depends(get_current_admin),
    
    session: Session = Depends(get_session)):
    
    db_user = session.scalar(
        select(UserModel).where(UserModel.username == user.username or UserModel.email == user.email)
    )
    
    if db_user:
        raise HTTPException(
            status_code=HTTPStatus.CONFLICT,
            detail='Username or email already exists!'
        )

    db_user = UserModel(
            username=user.username,
            email=user.email,
            password=password_hash(user.password),
            role=ParseUserPermission(user.role).getUserPermission()
        )    
    

    session.add(db_user)
    

    session.commit()
    
    session.refresh(db_user)
    
    
    return db_user

@router.put('/{id}',status_code=HTTPStatus.OK, response_model=UserPublic)
async def put_user(
    id : int, 
    user: UserSchema, 
    session:Session = Depends(get_session),
    admin:dict = Depends(get_current_admin)

    ):
    db_user = session.scalar(
        select(UserModel).where(UserModel.id == id)
    )
    
    
    if not db_user:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="User not Found"
        )
    
    
    session.execute(
        update(UserModel).where(UserModel.id == id).values(
            username=db_user.username,
            email=db_user.email,
            role=db_user.role,
            password=password_hash(db_user.password)
        )
    )
    
    session.commit()
    
    return db_user

@router.delete('/{id}',status_code=HTTPStatus.OK, response_model=UserPublic)
async def delete_user(
    id : int, 
    session: Session = Depends(get_session),
    admin : dict=Depends(get_current_admin),

        
        ):
    db_user = session.scalar(
        select(UserModel).where(UserModel.id == id)
    )
    
    if not db_user:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail="User not Found"
        )

    session.delete(db_user)
    session.commit()
    
    
    return db_user

