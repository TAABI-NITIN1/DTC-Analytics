import asyncio
import logging
import httpx
from pathlib import Path

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from strawberry.fastapi import GraphQLRouter
from pydantic import BaseModel

from src.config import load_env
from src.clickhouse_utils import get_clickhouse_client, ensure_tables
from src.graphql_schema import schema as graphql_schema

# -------------------------------
# Logging
# -------------------------------
logging.basicConfig(level=logging.INFO)

# -------------------------------
# Paths
# -------------------------------
PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_env(PROJECT_ROOT / '.env')

UI_ROOT = PROJECT_ROOT / 'ui'

# -------------------------------
# App
# -------------------------------
app = FastAPI(title='DTC Analytics API')

# -------------------------------
# CORS (production safe)
# -------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://4.224.101.147:5173",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# -------------------------------
# Static
# -------------------------------
if UI_ROOT.exists():
    app.mount('/static', StaticFiles(directory=UI_ROOT), name='static')

# -------------------------------
# GraphQL (keep UI OFF in prod)
# -------------------------------
app.include_router(
    GraphQLRouter(graphql_schema, graphiql=False),
    prefix='/graphql'
)

# -------------------------------
# SAFE startup (non-blocking)
# -------------------------------
def _init_db():
    try:
        client = get_clickhouse_client()
        ensure_tables(client)
        logging.info("✅ ClickHouse ready")
    except Exception as e:
        logging.error(f"❌ DB error: {e}")

@app.on_event("startup")
async def startup():
    logging.info("🚀 Starting app...")
    asyncio.get_event_loop().run_in_executor(None, _init_db)

# -------------------------------
# Models
# -------------------------------
class ChatRequest(BaseModel):
    messages: list[dict] | None = None
    query: str | None = None
    conversation_id: str | None = None
    context: dict | None = None

# -------------------------------
# Dify logging
# -------------------------------
async def _log_to_dify(query, answer, context):
    import os
    key = os.getenv("DIFY_API_KEY", "")
    url = os.getenv("DIFY_API_URL", "")

    if not key:
        return

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            await client.post(
                f"{url}/chat-messages",
                json={
                    "inputs": {"log_query": query, "log_answer": answer[:2000]},
                    "query": f"[LOG] {query}",
                    "response_mode": "blocking",
                    "user": (context or {}).get("customer_name", "dtc-user"),
                },
                headers={"Authorization": f"Bearer {key}"}
            )
    except Exception as e:
        logging.debug(f"Dify log failed: {e}")

# -------------------------------
# AI endpoint
# -------------------------------
@app.post('/api/ai/chat')
async def ai_chat(req: ChatRequest):
    from src.ai_analyst import chat as chat_fn

    query_text = req.query or ""
    if not query_text and req.messages:
        msgs = [m for m in req.messages if m.get("role") == "user"]
        if msgs:
            query_text = msgs[-1]["content"]

    if not query_text:
        return {"text": "No query provided.", "chart": None}

    result = chat_fn(
        messages=[{"role": "user", "content": query_text}],
        context=req.context or {}
    )

    asyncio.create_task(
        _log_to_dify(query_text, result.get("text", ""), req.context)
    )

    return result

# -------------------------------
# Routes
# -------------------------------
@app.get('/')
def index():
    return FileResponse(UI_ROOT / 'index.html')

@app.get('/health')
def health():
    return {"status": "ok"}













# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import FileResponse
# from fastapi.staticfiles import StaticFiles
# from strawberry.fastapi import GraphQLRouter
# from pydantic import BaseModel
# from pathlib import Path
# import asyncio
# import logging
# import httpx

# from src.config import load_env
# from src.clickhouse_utils import get_clickhouse_client, ensure_tables
# from src.graphql_schema import schema as graphql_schema

# # -------------------------------
# # Logging
# # -------------------------------
# logging.basicConfig(
#     level=logging.INFO,
#     format="%(asctime)s - %(levelname)s - %(message)s"
# )

# # -------------------------------
# # Paths & env
# # -------------------------------
# PROJECT_ROOT = Path(__file__).resolve().parents[1]
# ENV_PATH = PROJECT_ROOT / '.env'
# load_env(ENV_PATH)

# UI_ROOT = PROJECT_ROOT / 'ui'

# # -------------------------------
# # App init
# # -------------------------------
# app = FastAPI(title='DTC Analytics API')

# # -------------------------------
# # CORS (tighten later)
# # -------------------------------
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=["*"],  # change later in prod
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )

# # -------------------------------
# # Static files
# # -------------------------------
# if UI_ROOT.exists():
#     app.mount('/static', StaticFiles(directory=UI_ROOT), name='static')
# else:
#     logging.warning("UI folder not found")

# # -------------------------------
# # GraphQL
# # -------------------------------
# app.include_router(
#     GraphQLRouter(graphql_schema, graphiql=True),
#     prefix='/graphql'
# )

# # -------------------------------
# # SAFE async DB init (non-blocking)
# # -------------------------------
# def _init_db_sync():
#     try:
#         client = get_clickhouse_client()
#         ensure_tables(client)
#         logging.info("✅ ClickHouse ready")
#     except Exception as e:
#         logging.error(f"❌ ClickHouse error: {e}")

# @app.on_event('startup')
# async def startup():
#     logging.info("🚀 Starting application...")
    
#     # Run DB init in background thread (DO NOT BLOCK)
#     asyncio.get_event_loop().run_in_executor(None, _init_db_sync)

#     logging.info("✅ Startup complete (non-blocking)")

# # -------------------------------
# # Models
# # -------------------------------
# class ChatRequest(BaseModel):
#     messages: list[dict] | None = None
#     query: str | None = None
#     conversation_id: str | None = None
#     context: dict | None = None

# # -------------------------------
# # Background logging
# # -------------------------------
# async def log_to_dify(query: str, answer: str, context: dict | None):
#     DIFY_API_KEY = ''
#     DIFY_API_URL = ''

#     if not DIFY_API_KEY:
#         return

#     try:
#         async with httpx.AsyncClient(timeout=10.0) as client:
#             await client.post(
#                 f"{DIFY_API_URL}/chat-messages",
#                 json={
#                     "inputs": {"log_query": query, "log_answer": answer[:2000]},
#                     "query": f"[LOG] {query}",
#                     "response_mode": "blocking",
#                     "user": (context or {}).get("customer_name", "dtc-user"),
#                 },
#                 headers={
#                     "Authorization": f"Bearer {DIFY_API_KEY}",
#                     "Content-Type": "application/json",
#                 },
#             )
#     except Exception as e:
#         logging.debug(f"Dify logging failed: {e}")

# # -------------------------------
# # AI endpoint
# # -------------------------------
# @app.post('/api/ai/chat')
# async def ai_chat(req: ChatRequest):
#     try:
#         from src.ai_analyst import chat as chat_fn
#     except ImportError as e:
#         return {"text": f"Missing AI dependencies: {e}", "chart": None}

#     query_text = req.query or ""
#     if not query_text and req.messages:
#         user_msgs = [m for m in req.messages if m.get("role") == "user"]
#         if user_msgs:
#             query_text = user_msgs[-1].get("content", "")

#     if not query_text:
#         return {"text": "No query provided.", "chart": None}

#     logging.info(f"💬 AI Query: {query_text}")

#     result = chat_fn(
#         messages=[{"role": "user", "content": query_text}],
#         context=req.context or {}
#     )

#     asyncio.create_task(
#         log_to_dify(query_text, result.get("text", ""), req.context)
#     )

#     return result

# # -------------------------------
# # Routes
# # -------------------------------
# @app.get('/')
# def index():
#     index_file = UI_ROOT / 'index.html'
#     if index_file.exists():
#         return FileResponse(index_file)
#     return {"message": "Frontend not found"}

# @app.get('/health')
# def health():
#     return {"status": "ok"}

# @app.get('/debug')
# def debug():
#     return {
#         "ui_exists": UI_ROOT.exists(),
#         "graphql": "/graphql",
#         "chat_api": "/api/ai/chat"
#     }



























# from fastapi import FastAPI
# from fastapi.middleware.cors import CORSMiddleware
# from fastapi.responses import FileResponse
# from fastapi.staticfiles import StaticFiles
# from strawberry.fastapi import GraphQLRouter
# from starlette.middleware.base import BaseHTTPMiddleware
# from pydantic import BaseModel
# from pathlib import Path
# from datetime import datetime
# import asyncio
# import logging
# import httpx
 
# from src.config import load_env
# from src.clickhouse_utils import get_clickhouse_client, ensure_tables
# from src.clickhouse_utils_v2 import V2_TABLES
# from src.graphql_schema import schema as graphql_schema

# logging.basicConfig(level=logging.INFO)
 
# PROJECT_ROOT = Path(__file__).resolve().parents[1]
# ENV_PATH = PROJECT_ROOT / '.env'
# load_env(ENV_PATH)
 
# UI_ROOT = PROJECT_ROOT / 'ui'
 
# app = FastAPI(title='DTC Analytics API')
 
# # Enable CORS for production frontend (port 5173) and backend (port 8005)
# allowed_origins = [
#     "http://localhost:5173",
#     "http://127.0.0.1:5173",
#     # "http://4.224.101.147:5173",
# ]
# app.add_middleware(
#     CORSMiddleware,
#     allow_origins=allowed_origins,
#     allow_credentials=True,
#     allow_methods=["*"],
#     allow_headers=["*"],
# )
 
# class _GraphQLBodyLogger(BaseHTTPMiddleware):
#     async def dispatch(self, request, call_next):
#         try:
#             if request.url.path == '/graphql' and request.method.upper() == 'POST':
#                 body = await request.body()
#                 logging.info('Raw GraphQL request body: %s', (body.decode(errors='replace')[:2000]))
#                 request._body = body
#         except Exception as exc:
#             logging.debug('Failed to log GraphQL body: %s', exc)
#         return await call_next(request)
 
# app.add_middleware(_GraphQLBodyLogger)
# app.mount('/static', StaticFiles(directory=UI_ROOT), name='static')
# app.include_router(GraphQLRouter(graphql_schema), prefix='/graphql')
 
# @app.on_event('startup')
# async def _startup():
#     asyncio.create_task(my_background_job())
#     try:
#         client = get_clickhouse_client()
#         ensure_tables(client)
#     except Exception as exc:
#         logging.warning('Failed to ensure ClickHouse tables exist: %s', exc)
 
# class ChatRequest(BaseModel):
#     messages: list[dict] | None = None
#     query: str | None = None
#     conversation_id: str | None = None
#     context: dict | None = None
 
# async def _log_to_dify(query: str, answer: str, context: dict | None):
#     DIFY_API_KEY = ''
#     DIFY_API_URL = ''
#     if not DIFY_API_KEY:
#         return
#     try:
#         headers = {
#             "Authorization": f"Bearer {DIFY_API_KEY}",
#             "Content-Type": "application/json",
#         }
#         payload = {
#             "inputs": {"log_query": query, "log_answer": answer[:2000]},
#             "query": f"[LOG] {query}",
#             "response_mode": "blocking",
#             "user": (context or {}).get("customer_name", "dtc-user"),
#         }
#         async with httpx.AsyncClient(timeout=10.0) as client:
#             await client.post(f"{DIFY_API_URL}/chat-messages", json=payload, headers=headers)
#     except Exception as e:
#         logging.debug("Dify monitoring log failed (non-critical): %s", e)
 
# @app.post('/api/ai/chat')
# async def ai_chat(req: ChatRequest):
#     try:
#         from src.ai_analyst import chat as chat_fn
#     except ImportError as e:
#         return {"text": f"AI Analyst dependencies not installed: {e}. Install langchain-core, langchain-openai, langgraph.", "chart": None}
 
#     if req.query:
#         query_text = req.query
#     elif req.messages:
#         user_msgs = [m for m in req.messages if m.get("role") == "user"]
#         if user_msgs:
#             query_text = user_msgs[-1].get("content", "")
#         else:
#             query_text = ""
#     else:
#         query_text = ""
#     if not query_text:
#         return {"text": "No query provided.", "chart": None}
#     messages = [{"role": "user", "content": query_text}]
#     context = req.context or {}
#     result = chat_fn(messages=messages, context=context)
#     try:
#         asyncio.ensure_future(_log_to_dify(query_text, result.get("text", ""), context))
#     except Exception:
#         pass
#     return result
 
# @app.get('/')
# def index():
#     return FileResponse(UI_ROOT / 'index.html')
 
# @app.get('/api/known-unknown')
# def known_unknown(
#     start_date: str | None = None,
#     end_date: str | None = None,
#     uniqueid: str | None = None,
#     customer_name: str | None = None,
# ):
#     return {"status": "ok"}
 
# @app.get('/health')
# def health():
#     return {"status": "ok"}
 
 