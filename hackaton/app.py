from fastapi import FastAPI

from hackaton.routers import users, audits, auth, files

app = FastAPI(title='Audit')

app.include_router(users.router)

app.include_router(auth.router)

app.include_router(audits.router)

app.include_router(files.router)