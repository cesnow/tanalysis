from datetime import datetime

from pydantic import BaseModel


class ProductCreate(BaseModel):
    name: str
    description: str | None = None
    jql: str
    enabled: bool = True


class ProductUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    jql: str | None = None
    enabled: bool | None = None


class ProductOut(BaseModel):
    id: int
    name: str
    description: str | None
    jql: str
    enabled: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}
