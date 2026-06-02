from __future__ import annotations

from datetime import timedelta

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy import or_, select
from sqlalchemy.orm import Session

from backend.api.deps import get_current_user
from backend.common.auth import create_access_token, hash_password, verify_password
from backend.common.config import get_settings
from backend.common.db import get_session
from backend.common.models import User
from backend.common.schemas import TokenOut, UserCreate, UserLogin, UserOut

router = APIRouter(prefix="/api/v1/auth", tags=["auth"])


@router.post("/register", response_model=TokenOut, status_code=status.HTTP_201_CREATED)
def register(payload: UserCreate, session: Session = Depends(get_session)) -> TokenOut:
    conditions = [User.username == payload.username]
    if payload.email is not None:
        conditions.append(User.email == payload.email)
    existing = session.execute(select(User).where(or_(*conditions))).scalar_one_or_none()
    if existing is not None:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="username or email already exists",
        )

    user = User(
        username=payload.username,
        email=payload.email,
        password_hash=hash_password(payload.password),
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return _token_for_user(user)


@router.post("/login", response_model=TokenOut)
def login(payload: UserLogin, session: Session = Depends(get_session)) -> TokenOut:
    user = session.execute(
        select(User).where(or_(User.username == payload.username, User.email == payload.username))
    ).scalar_one_or_none()
    if user is None or not user.is_active or not verify_password(payload.password, user.password_hash):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="invalid username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return _token_for_user(user)


@router.get("/me", response_model=UserOut)
def me(current_user: User = Depends(get_current_user)) -> User:
    return current_user


def _token_for_user(user: User) -> TokenOut:
    settings = get_settings()
    expires = timedelta(minutes=settings.auth_token_expire_minutes)
    token = create_access_token(
        user_id=user.id,
        username=user.username,
        secret_key=settings.auth_secret_key,
        expires_delta=expires,
    )
    return TokenOut(
        access_token=token,
        expires_in=int(expires.total_seconds()),
        user=UserOut.model_validate(user),
    )

