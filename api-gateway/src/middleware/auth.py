"""Bearer token extraction helper (used by dependencies)."""
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

bearer_scheme = HTTPBearer(auto_error=False)
