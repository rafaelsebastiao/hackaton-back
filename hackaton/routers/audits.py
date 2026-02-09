from datetime import datetime

from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from http import HTTPStatus

from sqlalchemy import select
from sqlalchemy.orm import Session

from hackaton.schemas import AuditResultSchema

from hackaton.models import AuditResultModel, ParseSituationType

from hackaton.security import get_current_admin, get_current_user
from hackaton.database import get_session

router = APIRouter(prefix='/audits', tags=['audits'])


#Lista de Auditorias
@router.get('/', status_code=HTTPStatus.OK, response_model=list[AuditResultSchema])
async def get_audits(
    line:Optional[int] = None,

    status:Optional[str] = None,

    priority:Optional[str] = None,

    date:Optional[datetime] = None,
    
    session : Session = Depends(get_session),
    
    current_user : dict = Depends(get_current_user)

):
   
    query = select(AuditResultModel)

    if line:
        query = query.where(AuditResultModel.line == line)
    
    if status:
        query = query.where(
            AuditResultModel.status == ParseSituationType(status).getSituationType()
            
        )
    
    if priority:
        query = query.where(
            AuditResultModel.priority == priority 
        )

    db_audits = session.scalars(query).all()
   
    return db_audits

@router.get('/{id}', status_code=HTTPStatus.OK, response_model=AuditResultSchema)
async def get_audit_id(
    id:int,
    session : Session = Depends(get_session),
    current_user : dict = Depends(get_current_user)
):
    db_audit = session.scalar(
        select(AuditResultModel).where(AuditResultModel.id == id)
    )

    if not db_audit:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='Audit not Found!'
        )
    
    return db_audit




@router.post('/', status_code=HTTPStatus.CREATED, response_model=AuditResultSchema)
async def post_audit(
    audit_result : AuditResultSchema,
    session : Session = Depends(get_session),
    admin : dict = Depends(get_current_admin)
):
    db_audit_result = AuditResultModel(
        date=audit_result.date,
        line=audit_result.line,
        clear_pm=audit_result.clear_pm,
        ref_qtd_sum=audit_result.ref_qtd_sum,
        ref_freq_sum=audit_result.ref_freq_sum,
        ref_formal_sum= audit_result.ref_formal_sum,
        ref_informal_sum= audit_result.ref_informal_sum,
        nc_total_sum=audit_result.nc_total_sum,
        opened_nc_sum= audit_result.opened_nc_sum,
        priority=audit_result.priority,
        status=audit_result.status,

        situation=ParseSituationType(audit_result.situation).getSituationType(),
        
        description=audit_result.description
    )
    
    session.add(db_audit_result)

    session.commit()

    session.refresh(db_audit_result)

    return db_audit_result



@router.delete('/{id}', status_code=HTTPStatus.OK, response_model=AuditResultSchema)
async def delete_audit(
    id:int,
    session : Session = Depends(get_session),
    admin : dict = Depends(get_current_admin)
):
    db_audit = session.scalar(select(AuditResultModel).where(AuditResultModel.id == id))

    session.delete(db_audit)

    session.commit()
    
    return db_audit