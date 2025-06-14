from fastapi import FastAPI
from pydantic import BaseModel
import asyncio

app = FastAPI()

class WorkerRequest(BaseModel):
    name: str

workers = {}

@app.post("/start")
async def start_worker(req: WorkerRequest):
    if req.name in workers:
        return {"status": "running"}
    event = asyncio.Event()
    workers[req.name] = event
    return {"status": "started"}

@app.post("/stop")
async def stop_worker(req: WorkerRequest):
    ev = workers.pop(req.name, None)
    if ev:
        ev.set()
    return {"status": "stopped"}

@app.get("/workers")
async def list_workers():
    return list(workers.keys())
