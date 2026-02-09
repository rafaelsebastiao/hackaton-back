import shutil

from fastapi import APIRouter, Depends, HTTPException, UploadFile, File

from http import HTTPStatus

from pathlib import Path

from hackaton.security import  get_current_admin


router = APIRouter(prefix='/files', tags=['files'])


# Definição do caminho na pasta raiz do projeto
BASE_DIR = Path(__file__).resolve().parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


@router.post('/', status_code=HTTPStatus.CREATED)

async def upload_file(
    file: UploadFile = File(),
    admin : dict=Depends(get_current_admin)

    ):
    if not file.filename:
        return {"error": "Nome de arquivo inválido"}
    
    
    try:
        file_location = (UPLOAD_DIR / file.filename).resolve()

        # Salvar o arquivo em disco
        with open(str(file_location), "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
        
        return {
            "info": f"Arquivo '{file.filename}' salvo com sucesso",
            "path": str(file_location),
            "type": file.content_type
        }
    
    except Exception as e:
        raise HTTPException(
            status_code=HTTPStatus.INTERNAL_SERVER_ERROR,
            detail=f'Erro ao salvar: {str(e)}'
            )



