from pydantic import BaseModel


class SyncTicketsResponse(BaseModel):
    synced: int
    tickets: list[str]


class ListTicketsResponse(BaseModel):
    count: int
    tickets: list[dict]
