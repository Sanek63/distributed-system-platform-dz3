from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from app.raft import RaftNode


class CommandRequest(BaseModel):
    key: str = Field(min_length=1)
    value: str


class RequestVoteRequest(BaseModel):
    term: int
    candidateId: str
    lastLogIndex: int
    lastLogTerm: int


class AppendEntriesRequest(BaseModel):
    term: int
    leaderId: str
    prevLogIndex: int
    prevLogTerm: int
    entries: list[dict]
    leaderCommit: int


node = RaftNode()


@asynccontextmanager
async def lifespan(_: FastAPI):
    await node.start()
    try:
        yield
    finally:
        await node.stop()


app = FastAPI(title="Raft Node", lifespan=lifespan)


@app.get("/health")
async def health() -> dict:
    return await node.status()


@app.get("/state/{key}")
async def get_state(key: str) -> dict:
    return await node.read_value(key)


@app.post("/command")
async def command(request: CommandRequest) -> dict:
    result = await node.handle_command(request.key, request.value)
    if not result.get("accepted"):
        raise HTTPException(status_code=409, detail=result)
    return result


@app.post("/raft/request-vote")
async def request_vote(request: RequestVoteRequest) -> dict:
    return await node.handle_request_vote(request.model_dump())


@app.post("/raft/append-entries")
async def append_entries(request: AppendEntriesRequest) -> dict:
    return await node.handle_append_entries(request.model_dump())
