import sys
import os

from sqlalchemy import create_engine

from sqlalchemy.orm import Session

from hackaton.models import UserModel, UserPermission


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from hackaton.settings import Settings

from hackaton.security import password_hash


def create_admin_user():
    username_input = input('username: ')
    email_input=input('email: ')
    password_input =input('password: ')



    # Ana.Santos8@br.bosch.com
    
    admin_user = UserModel(
        username=username_input,
        email=email_input,
        password=password_hash(password_input),
        role=UserPermission.ADMIN
        )
    
    engine = create_engine(Settings().DATABASE_URL)

    with Session(engine) as s:
        s.add(admin_user)
        s.commit()
        s.refresh(admin_user)


create_admin_user()



    




