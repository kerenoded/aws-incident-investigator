from backend.shared.authz.context_access import extract_cognito_sub, is_owner


def test_extract_cognito_sub_returns_value():
    event = {
        "requestContext": {
            "authorizer": {
                "claims": {
                    "sub": "abc-123",
                }
            }
        }
    }
    assert extract_cognito_sub(event) == "abc-123"


def test_extract_cognito_sub_missing_returns_none():
    assert extract_cognito_sub({}) is None


def test_is_owner_true_when_subs_match():
    assert is_owner(owner_sub="user-a", caller_sub="user-a") is True


def test_is_owner_false_when_subs_do_not_match():
    assert is_owner(owner_sub="user-a", caller_sub="user-b") is False
