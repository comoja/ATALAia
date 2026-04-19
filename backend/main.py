import os
import sys

rutaRaiz = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if rutaRaiz not in sys.path:
    sys.path.insert(0, rutaRaiz)

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from backend.api.routes import router as correlation_router
from middleware.utils.loggerConfig import setupLogging
import logging

setupLogging(logPara="backend", projectDir=rutaRaiz)
logger = logging.getLogger(__name__)

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing ATALA.ia Pair Correlation Backend...")
    # scheduler.start() # TODO: Lo encederemos después cuando el cronjob esté 100% definido
    logger.info("Scheduler ready (paused).")
    yield
    logger.info("Shutting down engine...")
    scheduler.shutdown()

app = FastAPI(
    title="ATALA.ia Pair Correlation Engine",
    description="Backend engine for correlation models and background price updates.",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(correlation_router, prefix="/api/v1")

@app.get("/")
def read_root():
    return {"status": "ok", "message": "ATALA.ia Engine is running"}

@app.get("/api/v1/ping")
def ping():
    return {"status": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)