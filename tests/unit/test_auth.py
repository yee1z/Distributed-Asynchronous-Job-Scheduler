from __future__ import annotations

from datetime import timedelta

from backend.common.auth import (
    create_access_token,
    decode_access_token,
    hash_password,
    verify_password,
)


def test_password_hash_verification():
    password_hash = hash_password("correct-horse-battery")

    assert password_hash != "correct-horse-battery"
    assert verify_password("correct-horse-battery", password_hash)
    assert not verify_password("wrong-password", password_hash)


def test_access_token_round_trip():
    token = create_access_token(
        user_id=123,
        username="alice",
        secret_key="test-secret",
        expires_delta=timedelta(minutes=5),
    )

    payload = decode_access_token(token, secret_key="test-secret")

    assert payload is not None
    assert payload["sub"] == "123"
    assert payload["username"] == "alice"


def test_access_token_rejects_wrong_secret():
    token = create_access_token(
        user_id=123,
        username="alice",
        secret_key="test-secret",
        expires_delta=timedelta(minutes=5),
    )

    assert decode_access_token(token, secret_key="other-secret") is None


def test_access_token_rejects_expired_token():
    token = create_access_token(
        user_id=123,
        username="alice",
        secret_key="test-secret",
        expires_delta=timedelta(seconds=-1),
    )

    assert decode_access_token(token, secret_key="test-secret") is None
