from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.asyncio import AsyncIOScheduler
# Importar el router con nuestros endpoints
from backend.api.routes import router as correlation_router
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ATALAia Pair Correlation Engine",
    description="Backend engine for correlation models and background price updates.",
    version="1.0.0"
)

# Set up CORS to allow requests from the Java Frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], # For production, change to the specific Java server IP/Port
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Incluir las rutas de correlación
app.include_router(correlation_router, prefix="/api/v1")

scheduler = AsyncIOScheduler()

@app.on_event("startup")
async def startup_event():
    logger.info("Initializing ATALAia Pair Correlation Backend...")
    # scheduler.start() # TODO: Lo encederemos después cuando el cronjob esté 100% definido
    logger.info("Scheduler ready (paused).")

@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down engine...")
    scheduler.shutdown()

@app.get("/")
def read_root():
    return {"status": "ok", "message": "ATALAia Engine is running"}

@app.get("/api/v1/ping")
def ping():
    return {"status": "pong"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
