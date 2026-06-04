"""Export endpoints: create job, poll status."""
from __future__ import annotations

import asyncio
import uuid
from typing import Annotated

import structlog
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.database import get_db, AsyncTimescaleSession
from src.core.dependencies import require_role
from src.models.alert import ExportJob
from src.models.user import User
from src.schemas.query import ExportJobResponse, ExportRequest
from src.services import export_service

logger = structlog.get_logger(__name__)
router = APIRouter(prefix="/export", tags=["export"])


@router.post("", response_model=ExportJobResponse, status_code=status.HTTP_202_ACCEPTED)
async def create_export(
    req: ExportRequest,
    user: Annotated[User, Depends(require_role("premium_user", "admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    # Enforce 1 concurrent job per user
    running_result = await db.execute(
        select(ExportJob).where(
            ExportJob.user_id == user.id,
            ExportJob.status.in_(["pending", "running"]),
        )
    )
    if running_result.scalars().first():
        raise HTTPException(
            status_code=status.HTTP_429_TOO_MANY_REQUESTS,
            detail="You already have an export job in progress",
        )

    job = ExportJob(
        user_id=user.id,
        query_params=req.query_params,
        output_format=req.output_format,
        status="pending",
    )
    db.add(job)
    await db.flush()
    job_id = job.id

    async def _run():
        result = await export_service.run_export(
            job_id=job_id,
            user_id=user.id,
            query_params=req.query_params,
            output_format=req.output_format,
            db_session_factory=AsyncTimescaleSession,
        )
        async with AsyncTimescaleSession() as update_db:
            res = await update_db.execute(select(ExportJob).where(ExportJob.id == job_id))
            j = res.scalar_one_or_none()
            if j:
                for k, v in result.items():
                    setattr(j, k, v)
                await update_db.commit()

    asyncio.create_task(_run())
    return job


@router.get("/{job_id}", response_model=ExportJobResponse)
async def get_export(
    job_id: uuid.UUID,
    user: Annotated[User, Depends(require_role("premium_user", "admin"))],
    db: Annotated[AsyncSession, Depends(get_db)],
):
    result = await db.execute(
        select(ExportJob).where(ExportJob.id == job_id, ExportJob.user_id == user.id)
    )
    job = result.scalar_one_or_none()
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Export job not found")
    return job
