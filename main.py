# bot_fotos.py
#
# Requisitos:
#   pip install -U "python-telegram-bot[job-queue]==21.6" gspread google-auth
#
# PowerShell (PC):
#   cd "C:\Users\Diego_Siancas\Desktop\BOT TuFibra"
#   $env:BOT_TOKEN="TU_TOKEN"
#   $env:SHEET_ID="TU_SHEET_ID"
#   $env:GOOGLE_CREDS_JSON="google_creds.json"
#   $env:BOT_VERSION="1.0.0"
#   python main.py
#
# Notas:
# - Esta versión está adaptada para correr en PC con polling.
# - Se eliminó todo el envío de evidencias/resúmenes a otros grupos.
# - Se eliminó la lógica de ROUTING / PAIRING / CONFIG de vinculación entre grupos.
# - Se conserva el flujo del caso, validaciones, reaperturas, Google Sheets y almacenamiento local SQLite.
# - NUEVA ESTRUCTURA:
#   /inicio crea una RUTA
#   Inicio de Orden crea una ORDEN dentro de la ruta activa
#   Evidencias, validaciones y cambios de estado ahora cuelgan de id_orden
#   Tareas de ruta ahora cuelgan de id_ruta
# - Google Sheets ahora escribe en:
#   RUTA
#   TAREAS_RUTA
#   ORDENES
#   EVIDENCIAS_PASOS
#   EVIDENCIAS_ARCHIVOS
#   CAMBIO_ESTADO
#   VALIDACIONES
# - CAMBIOS IMPLEMENTADOS:
#   * Cuando AUSENTE / REPROGRAMADO / CANCELADO es aprobado por admin, la orden se CIERRA realmente
#   * En ORDENES ahora se llenan fecha_fin_orden, hora_fin_orden, duracion_orden, duracion_orden_min
#   * registrado_en ahora se guarda en formato Perú: dd-mm-YYYY HH:MM
#   * Se mantiene el histórico: si el mismo cod_abonado vuelve a atenderse más tarde, se crea una NUEVA orden
#   * Cuando un CAMBIO DE ESTADO es aprobado (CLIENTE_DESISTE_INSTALACION, INSTALACION_QUEDA_PENDIENTE,
#     SE_REPROGRAMA_MOTIVO_CLIENTE o RECHAZO_POR_FACILIDADES), la orden también se CIERRA realmente

import os
import json
import sqlite3
import logging
import time
import re
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, List, Tuple

import gspread
from google.oauth2.service_account import Credentials

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, Message
from telegram.error import BadRequest
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

# =========================
# CONFIG
# =========================
BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
DB_PATH = os.getenv("DB_PATH", "bot_fotos.sqlite3")

MAX_MEDIA_PER_STEP = int(os.getenv("MAX_MEDIA_PER_STEP", "8"))
STEP_LOCK_TIMEOUT_MINUTES = int(os.getenv("STEP_LOCK_TIMEOUT_MINUTES", "10"))
MEDIA_ACK_WINDOW_SECONDS = float(os.getenv("MEDIA_ACK_WINDOW_SECONDS", "1.8"))

# Perú (UTC-5)
PERU_TZ = timezone(timedelta(hours=-5))

SERVICE_TYPES = ["ALTA NUEVA", "POSTVENTA", "AVERIAS"]
INSTALL_MODES = ["EXTERNA", "INTERNA"]
PACKAGE_TYPES = ["INTERNET", "INTERNET + TV"]
TV_COUNT_OPTIONS = [1, 2, 3, 4, 5]

ROUTE_STATUS_OPEN = "RUTA_ACTIVA"
ROUTE_STATUS_CLOSED = "RUTA_CERRADA"

ORDER_STATUS_OPEN = "OPEN"
ORDER_STATUS_CLOSED = "CLOSED"
ORDER_STATUS_CANCELLED = "CANCELLED"

PHASE_WAIT_TECHNICIAN = "WAIT_TECHNICIAN"
PHASE_ROUTE_SELFIE = "ROUTE_SELFIE"
PHASE_ROUTE_LOCATION = "ROUTE_LOCATION"
PHASE_ROUTE_MENU = "ROUTE_MENU"
PHASE_ROUTE_CLOSED = "ROUTE_CLOSED"

PHASE_WAIT_SERVICE = "WAIT_SERVICE"
PHASE_WAIT_ABONADO = "WAIT_ABONADO"
PHASE_WAIT_LOCATION = "WAIT_LOCATION"
PHASE_WAIT_INSTALL_MODE = "WAIT_INSTALL_MODE"
PHASE_WAIT_PACKAGE = "WAIT_PACKAGE"
PHASE_WAIT_TV_COUNT = "WAIT_TV_COUNT"
PHASE_WAIT_CLIENT_STATUS = "WAIT_CLIENT_STATUS"
PHASE_WAIT_PERSONA_ATIENDE = "WAIT_PERSONA_ATIENDE"
PHASE_WAIT_DOC_TITULAR = "WAIT_DOC_TITULAR"
PHASE_WAIT_DOC_ENCARGADO = "WAIT_DOC_ENCARGADO"
PHASE_WAIT_DOC_CLIENTE = "WAIT_DOC_CLIENTE"
PHASE_DOC_REVIEW = "DOC_REVIEW"
PHASE_MENU_EVID = "MENU_EVID"
PHASE_EVID_ACTION = "EVID_ACTION"
PHASE_AUTH_MODE = "AUTH_MODE"
PHASE_AUTH_TEXT_WAIT = "AUTH_TEXT_WAIT"
PHASE_AUTH_MEDIA = "AUTH_MEDIA"
PHASE_AUTH_REVIEW = "AUTH_REVIEW"
PHASE_STEP_MEDIA = "STEP_MEDIA"
PHASE_STEP_REVIEW = "STEP_REVIEW"
PHASE_ALT_FINAL_REVIEW = "ALT_FINAL_REVIEW"
PHASE_CHANGE_STATE_MENU = "CHANGE_STATE_MENU"
PHASE_CHANGE_STATE_REASON = "CHANGE_STATE_REASON"
PHASE_CHANGE_STATE_REVIEW = "CHANGE_STATE_REVIEW"
PHASE_VALIDATION_NAME = "VALIDATION_NAME"
PHASE_VALIDATION_PHONE = "VALIDATION_PHONE"
PHASE_VALIDATION_RELATIONSHIP = "VALIDATION_RELATIONSHIP"
PHASE_VALIDATION_CONFIRM = "VALIDATION_CONFIRM"
PHASE_VALIDATION_REVIEW = "VALIDATION_REVIEW"
PHASE_ORDER_CLOSED = "ORDER_CLOSED"
PHASE_ORDER_CANCELLED = "ORDER_CANCELLED"

STEP_STATE_PENDIENTE = "PENDING"
STEP_STATE_EN_CARGA = "EN_CARGA"
STEP_STATE_EN_REVISION = "PENDING"
STEP_STATE_APROBADO = "APPROVED"
STEP_STATE_RECHAZADO = "REJECTED"
STEP_STATE_REABIERTO = "REABIERTO"
STEP_STATE_BLOQUEADO = "BLOQUEADO"

TASK_INICIO_ORDEN = "INICIO_ORDEN"
TASK_EN_CAMINO = "EN_CAMINO"
TASK_RECOJO_MATERIALES = "MATERIALES"
TASK_MANTENIMIENTO_VEHICULAR = "VEHICULO"
TASK_CAPACITACION = "CAPACITACION"
TASK_ALMUERZO = "ALMUERZO"
TASK_CERRAR_RUTA = "CERRAR_RUTA"

ROUTE_TASKS: List[Tuple[str, str]] = [
    (TASK_INICIO_ORDEN, "Inicio de Orden"),
    (TASK_EN_CAMINO, "En Camino"),
    (TASK_RECOJO_MATERIALES, "Recojo de Materiales"),
    (TASK_MANTENIMIENTO_VEHICULAR, "Mantenimiento Vehicular"),
    (TASK_CAPACITACION, "Capacitacion"),
    (TASK_ALMUERZO, "Almuerzo"),
]

ROUTE_TIMED_TASKS = {
    TASK_EN_CAMINO,
    TASK_RECOJO_MATERIALES,
    TASK_MANTENIMIENTO_VEHICULAR,
    TASK_CAPACITACION,
    TASK_ALMUERZO,
}

TASK_SESSION_STATUS_OPEN = "OPEN"
TASK_SESSION_STATUS_CLOSED = "CLOSED"
TASK_SESSION_STATUS_CANCELLED = "CANCELLED"

TASK_EVENT_CONFIRM_START = "CONFIRM_START"
TASK_EVENT_START = "START"
TASK_EVENT_CONFIRM_FINISH = "CONFIRM_FINISH"
TASK_EVENT_FINISH = "FINISH"
TASK_EVENT_SIMPLE = "SIMPLE"

CLIENT_STATUS_EN_PROCESO = "EN_PROCESO"
CLIENT_STATUS_AUSENTE = "AUSENTE"
CLIENT_STATUS_REPROGRAMADO = "REPROGRAMADO"
CLIENT_STATUS_CANCELADO = "CANCELADO"

CLIENT_STATUS_OPTIONS = [
    (CLIENT_STATUS_EN_PROCESO, "EN PROCESO"),
    (CLIENT_STATUS_AUSENTE, "AUSENTE"),
    (CLIENT_STATUS_REPROGRAMADO, "REPROGRAMADO"),
    (CLIENT_STATUS_CANCELADO, "CANCELADO"),
]

ALT_CLIENT_STATUSES = {
    CLIENT_STATUS_AUSENTE,
    CLIENT_STATUS_REPROGRAMADO,
    CLIENT_STATUS_CANCELADO,
}

CHANGE_STATE_DESISTE = "CLIENTE_DESISTE_INSTALACION"
CHANGE_STATE_PENDIENTE = "INSTALACION_QUEDA_PENDIENTE"
CHANGE_STATE_REPROG = "SE_REPROGRAMA_MOTIVO_CLIENTE"
CHANGE_STATE_RECHAZO = "RECHAZO_POR_FACILIDADES"

CHANGE_STATE_OPTIONS = [
    (CHANGE_STATE_DESISTE, "CLIENTE DESISTE INSTALACION"),
    (CHANGE_STATE_PENDIENTE, "INSTALACION QUEDA PENDIENTE"),
    (CHANGE_STATE_REPROG, "SE REPROGRAMA MOTIVO CLIENTE"),
    (CHANGE_STATE_RECHAZO, "RECHAZO POR FACILIDADES"),
]

VALIDATION_STATUS_DRAFT = "DRAFT"
VALIDATION_STATUS_PENDING = "PENDING"
VALIDATION_STATUS_APPROVED = "APPROVED"
VALIDATION_STATUS_REJECTED = "REJECTED"

EVIDENCIAS_ESTADO_INCOMPLETAS = "INCOMPLETAS"
EVIDENCIAS_ESTADO_COMPLETAS = "COMPLETAS"

# =========================
# DEFINICION DE PASOS
# =========================
STEP_FACHADA = 5
STEP_CTO = 6
STEP_POTENCIA_CTO = 7
STEP_CINTILLO_ROTULADO = 8
STEP_DROP_DOMICILIO = 9
STEP_ANCLAJE = 10
STEP_ROSETA_POTENCIA = 11
STEP_SPLITTER_PANORAMICA = 12
STEP_MAC_ONT = 13
STEP_ONT = 14
STEP_TV_BASE = 100
STEP_TEST_VELOCIDAD = 200
STEP_ACTA_INSTALACION = 201

STEP_ALT_FACHADA = 300
STEP_ALT_PLACA = 301
STEP_ALT_SUMINISTRO = 302

STEP_MEDIA_DEFS_BASE: Dict[int, Tuple[str, str]] = {
    STEP_FACHADA: (
        "FACHADA",
        "Envía foto de Fachada con placa de dirección y/o suministro eléctrico",
    ),
    STEP_CTO: (
        "CTO",
        "Envía foto panorámica de la CTO o FAT rotulada",
    ),
    STEP_POTENCIA_CTO: (
        "POTENCIA EN CTO",
        "Envía la foto de la medida de potencia del puerto a utilizar",
    ),
    STEP_CINTILLO_ROTULADO: (
        "CINTILLO ROTULADO",
        "Envía la foto del cintillo rotulado identificando al cliente (DNI o CE y nro de puerto)",
    ),
    STEP_DROP_DOMICILIO: (
        "DROP QUE INGRESA AL DOMICILIO",
        "Envía foto del drop que ingresa al domicilio",
    ),
    STEP_ANCLAJE: (
        "ANCLAJE",
        "Envía foto del punto de anclaje de la fibra drop en el domicilio",
    ),
    STEP_ROSETA_POTENCIA: (
        "ROSETA + MEDICION POTENCIA",
        "Envía foto de la roseta abierta y medición de potencia",
    ),
    STEP_SPLITTER_PANORAMICA: (
        "FOTO PANORAMICA SPLITTER",
        "Envía foto panorámica del splitter instalado",
    ),
    STEP_MAC_ONT: (
        "MAC ONT",
        "Envía foto de la MAC (Etiqueta) de la ONT y/o equipos usados",
    ),
    STEP_ONT: (
        "ONT",
        "Envía foto panorámica de la ONT operativa",
    ),
    STEP_TEST_VELOCIDAD: (
        "TEST DE VELOCIDAD",
        "Envía foto del test de velocidad App Speedtest mostrar ID y fecha claramente",
    ),
    STEP_ACTA_INSTALACION: (
        "ACTA DE INSTALACION",
        "Envía foto del acta de instalación completa con la firma de cliente y datos llenos",
    ),
    STEP_ALT_FACHADA: (
        "FACHADA PANORÁMICA",
        "Envía foto panorámica de la fachada",
    ),
    STEP_ALT_PLACA: (
        "PLACA DE DOMICILIO",
        "Envía foto de la placa o numeración del domicilio",
    ),
    STEP_ALT_SUMINISTRO: (
        "SUMINISTRO ELÉCTRICO Y/O DE GAS",
        "Envía foto del suministro eléctrico y/o de gas",
    ),
}

GUIDE_PARAM_MAP = {
    STEP_FACHADA: "GUIA_FACHADA",
    STEP_CTO: "GUIA_CTO",
    STEP_POTENCIA_CTO: "GUIA_POTENCIA_CTO",
    STEP_CINTILLO_ROTULADO: "GUIA_PRECINTO_ROTULADOR",
    STEP_DROP_DOMICILIO: "GUIA_FALSO_TRAMO",
    STEP_ANCLAJE: "GUIA_ANCLAJE",
    STEP_ROSETA_POTENCIA: "GUIA_ROSETA_POTENCIA",
    STEP_SPLITTER_PANORAMICA: "GUIA_SPLITTER_PANORAMICO",
    STEP_MAC_ONT: "GUIA_MAC_ONT",
    STEP_ONT: "GUIA_ONT",
    STEP_TEST_VELOCIDAD: "GUIA_TEST_VELOCIDAD",
    STEP_ACTA_INSTALACION: "GUIA_ACTA_INSTALACION",
}

GUIDE_NOTE_MAP = {
    STEP_FACHADA: "Foto panorámica de la casa, debe verse toda la fachada y si hay placa tomar una segunda foto.",
    STEP_CTO: "Debe verse claramente el rotulado de la CTO.",
    STEP_POTENCIA_CTO: "La pantalla del power meter debe ser legible y verse en patch cord conectado en el puerto.",
    STEP_CINTILLO_ROTULADO: "Debe verse el cintillo y el rotulado correctamente.",
    STEP_DROP_DOMICILIO: "Debe verse el drop que ingresa al domicilio.",
    STEP_ANCLAJE: "Debe verse el anclaje y los templadores correctamente colocados.",
    STEP_ROSETA_POTENCIA: "Debe verse la roseta y la medición de potencia.",
    STEP_SPLITTER_PANORAMICA: "Debe verse panorámicamente el splitter instalado.",
    STEP_MAC_ONT: "La etiqueta MAC de la ONT debe ser legible.",
    STEP_ONT: "Debe verse la ONT instalada y conectada. La foto debe ser panorámica.",
    STEP_TEST_VELOCIDAD: "Debe verse la fecha y el ID del test de velocidad.",
    STEP_ACTA_INSTALACION: "Debe verse el acta completa con firma. No debe taparse ningún dato.",
}

# =========================
# Google Sheets CONFIG
# =========================
SHEET_ID = os.getenv("SHEET_ID", "").strip()
GOOGLE_CREDS_JSON = os.getenv("GOOGLE_CREDS_JSON", "").strip()
GOOGLE_CREDS_JSON_TEXT = os.getenv("GOOGLE_CREDS_JSON_TEXT", "").strip()
BOT_VERSION = os.getenv("BOT_VERSION", "1.0.0").strip()

RUTA_COLUMNS = [
    "id_ruta",
    "tecnico_nombre",
    "tecnico_user_id",
    "chat_id_origen",
    "fecha_inicio",
    "hora_inicio",
    "fecha_cierre",
    "hora_cierre",
    "duracion_ruta",
    "duracion_ruta_min",
    "hora_envio_selfie",
    "selfie_ruta_file_id",
    "hora_envio_ubi_ruta",
    "ubi_ruta_lat",
    "ubi_ruta_lon",
    "maps_ubi_ruta",
    "estado_ruta",
    "version_bot",
    "registrado_en",
]

TAREAS_RUTA_COLUMNS = [
    "id_ruta",
    "tecnico_nombre",
    "tecnico_user_id",
    "chat_id_origen",
    "tipo_tarea",
    "fecha_inicio_tarea",
    "hora_inicio_tarea",
    "fecha_fin_tarea",
    "hora_fin_tarea",
    "duracion_tarea",
    "duracion_tarea_min",
]

ORDENES_COLUMNS = [
    "id_orden",
    "id_ruta",
    "tecnico_nombre",
    "tecnico_user_id",
    "chat_id_origen",
    "fecha_inicio_orden",
    "hora_inicio_orden",
    "fecha_fin_orden",
    "hora_fin_orden",
    "duracion_orden",
    "duracion_orden_min",
    "tipo_servicio",
    "cod_abonado",
    "ubi_orden_lat",
    "ubi_orden_lon",
    "maps_ubi_orden",
    "tipo_instalacion",
    "tipo_paquete",
    "numero_tv",
    "estado_atencion",
    "estado_orden",
    "evidencias_estado",
    "validacion_estado",
    "nombre_validador",
    "numero_validador",
    "parentesco_validador",
    "veces_aprobado",
    "veces_rechazado",
    "version_bot",
    "registrado_en",
]

EVIDENCIAS_PASOS_COLUMNS = [
    "id_orden",
    "id_ruta",
    "cod_abonado",
    "tecnico_nombre",
    "tecnico_user_id",
    "chat_id_origen",
    "fecha_registro_paso",
    "hora_registro_paso",
    "evidencias_numero",
    "evidencias_nombre",
    "attempt",
    "estado_evidencias",
    "revisado_por",
    "fecha_revision",
    "hora_revision",
    "motivo_rechazo",
    "cantidad_fotos",
    "bloqueado",
]

EVIDENCIAS_ARCHIVOS_COLUMNS = [
    "id_orden",
    "id_ruta",
    "cod_abonado",
    "evidencias_numero",
    "attempt",
    "file_id",
    "file_unique_id",
    "mensaje_telegram_id",
    "fecha_carga",
    "hora_carga",
    "tipo_archivo",
    "media_group_id",
]

CAMBIO_ESTADO_COLUMNS = [
    "id_orden",
    "id_ruta",
    "cod_abonado",
    "tecnico_nombre",
    "tecnico_user_id",
    "chat_id_origen",
    "tipo_cambio",
    "descripcion_cambio",
    "motivo",
    "fecha_registro",
    "hora_registro",
    "estado_solicitud",
    "revisado_por",
    "fecha_revision",
    "hora_revision",
    "motivo_revision",
]

VALIDACIONES_COLUMNS = [
    "id_orden",
    "id_ruta",
    "cod_abonado",
    "tecnico_nombre",
    "tecnico_user_id",
    "chat_id_origen",
    "nombre_validador",
    "numero_validador",
    "parentesco_validador",
    "estado_validacion",
    "fecha_registro",
    "hora_registro",
    "revisado_por",
    "fecha_revision",
    "hora_revision",
    "motivo_revision",
]

CONFIG_COLUMNS = ["parametro", "valor"]
TECNICOS_TAB = "TECNICOS"
TECNICOS_COLUMNS = ["nombre", "activo", "orden", "alias", "updated_at", "telegram_user_id"]

TECH_CACHE_TTL_SEC = int(os.getenv("TECH_CACHE_TTL_SEC", "180"))

# =========================
# Logging
# =========================
logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)
log = logging.getLogger("tufibra_bot")

# =========================
# Safe Telegram helpers
# =========================
async def safe_q_answer(q, text: Optional[str] = None, show_alert: bool = False) -> None:
    if q is None:
        return
    try:
        await q.answer(text=text, show_alert=show_alert, cache_time=0)
    except BadRequest as e:
        msg = str(e).lower()
        if "query is too old" in msg or "response timeout expired" in msg or "query id is invalid" in msg:
            return
        if "invalid callback query" in msg:
            return
        log.warning(f"safe_q_answer BadRequest: {e}")
    except Exception as e:
        log.warning(f"safe_q_answer error: {e}")


async def safe_edit_message_text(q, text: str, **kwargs) -> None:
    if q is None:
        return
    try:
        await q.edit_message_text(text=text, **kwargs)
    except BadRequest as e:
        msg = str(e).lower()
        if "message is not modified" in msg:
            return
        if "message to edit not found" in msg:
            return
        if "query is too old" in msg or "response timeout expired" in msg or "query id is invalid" in msg:
            return
        log.warning(f"safe_edit_message_text BadRequest: {e}")
    except Exception as e:
        log.warning(f"safe_edit_message_text error: {e}")


async def safe_delete_message(context: ContextTypes.DEFAULT_TYPE, chat_id: int, message_id: Optional[int]) -> None:
    if not message_id:
        return
    try:
        await context.bot.delete_message(chat_id=chat_id, message_id=message_id)
    except BadRequest as e:
        msg = str(e).lower()
        if "message to delete not found" in msg:
            return
        if "message can't be deleted" in msg:
            return
        log.warning(f"safe_delete_message BadRequest: {e}")
    except Exception as e:
        log.warning(f"safe_delete_message error: {e}")


# =========================
# DB helpers
# =========================
def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_iso(dt_s: str) -> Optional[datetime]:
    if not dt_s:
        return None
    try:
        d = datetime.fromisoformat(dt_s)
        if d.tzinfo is None:
            d = d.replace(tzinfo=timezone.utc)
        return d
    except Exception:
        return None


def fmt_time_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return "-"
    return d.astimezone(PERU_TZ).strftime("%H:%M")


def fmt_date_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return "-"
    return d.astimezone(PERU_TZ).strftime("%Y-%m-%d")


def fmt_datetime_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return "-"
    return d.astimezone(PERU_TZ).strftime("%Y-%m-%d %H:%M")


def fmt_sheet_datetime_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return ""
    return d.astimezone(PERU_TZ).strftime("%d-%m-%Y %H:%M")


def fmt_sheet_date_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return ""
    return d.astimezone(PERU_TZ).strftime("%d-%m-%Y")


def fmt_sheet_time_pe(dt_s: str) -> str:
    d = parse_iso(dt_s)
    if not d:
        return ""
    return d.astimezone(PERU_TZ).strftime("%H:%M")


def _col_exists(conn: sqlite3.Connection, table: str, col: str) -> bool:
    rows = conn.execute(f"PRAGMA table_info({table})").fetchall()
    return any(r["name"] == col for r in rows)


def lock_expires_at_iso(minutes: int = STEP_LOCK_TIMEOUT_MINUTES) -> str:
    return (datetime.now(timezone.utc) + timedelta(minutes=minutes)).isoformat()


def duration_minutes(created_at: str, finished_at: str) -> Optional[int]:
    a = parse_iso(created_at)
    b = parse_iso(finished_at)
    if not a or not b:
        return None
    seconds = int((b - a).total_seconds())
    if seconds < 0:
        return None
    return max(0, seconds // 60)


def duration_seconds(created_at: str, finished_at: str) -> Optional[int]:
    a = parse_iso(created_at)
    b = parse_iso(finished_at)
    if not a or not b:
        return None
    seconds = int((b - a).total_seconds())
    if seconds < 0:
        return None
    return seconds


def human_duration_from_seconds(total_seconds: Optional[int]) -> str:
    if total_seconds is None:
        return "-"

    h = total_seconds // 3600
    m = (total_seconds % 3600) // 60
    s = total_seconds % 60

    return f"{h} h {m} min {s} seg"


def human_duration_from_minutes(total_minutes: Optional[int]) -> str:
    if total_minutes is None:
        return "-"
    h = total_minutes // 60
    m = total_minutes % 60
    if h > 0 and m > 0:
        return f"{h} h {m} min"
    if h > 0:
        return f"{h} h"
    return f"{m} min"


def close_order(id_orden: int, final_status: str, phase: str = PHASE_ORDER_CLOSED, evidencias_estado: Optional[str] = None):
    order_row = get_order(id_orden)
    if not order_row:
        return

    finished_at = order_row["finished_at"] or now_utc()
    fields = {
        "status": ORDER_STATUS_CLOSED,
        "phase": phase,
        "finished_at": finished_at,
        "final_order_status": final_status,
        "pending_step_no": None,
        "current_step_no": None,
        "admin_pending": 0,
    }
    if evidencias_estado is not None:
        fields["evidencias_estado"] = evidencias_estado

    update_order(id_orden, **fields)


def init_db():
    with db() as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS routes (
                id_ruta INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER,
                username TEXT,
                technician_name TEXT,
                technician_user_id INTEGER,
                created_at TEXT NOT NULL,
                closed_at TEXT,
                status TEXT NOT NULL,
                phase TEXT,
                route_selfie_file_id TEXT,
                route_selfie_file_unique_id TEXT,
                route_selfie_message_id INTEGER,
                route_selfie_at TEXT,
                route_location_lat REAL,
                route_location_lon REAL,
                route_location_at TEXT,
                route_menu_enabled INTEGER NOT NULL DEFAULT 0,
                locked_by_user_id INTEGER,
                locked_by_name TEXT,
                locked_at TEXT,
                lock_expires_at TEXT,
                version_bot TEXT
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_routes_open_chat ON routes(chat_id, status);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS orders (
                id_orden INTEGER PRIMARY KEY AUTOINCREMENT,
                id_ruta INTEGER NOT NULL,
                chat_id INTEGER NOT NULL,
                user_id INTEGER,
                username TEXT,
                created_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL,
                phase TEXT,
                step_index INTEGER NOT NULL DEFAULT 0,
                pending_step_no INTEGER,
                current_step_no INTEGER,
                admin_pending INTEGER NOT NULL DEFAULT 0,
                technician_name TEXT,
                technician_user_id INTEGER,
                service_type TEXT,
                abonado_code TEXT,
                evidencias_estado TEXT,
                install_mode TEXT,
                package_type TEXT,
                tv_count INTEGER,
                client_status TEXT,
                final_order_status TEXT,
                location_lat REAL,
                location_lon REAL,
                location_at TEXT,
                change_state_type TEXT,
                change_state_reason TEXT,
                validation_status TEXT,
                validation_name TEXT,
                validation_phone TEXT,
                validation_relationship TEXT,
                validation_created_at TEXT,
                validation_submitted_at TEXT,
                validation_reviewed_by INTEGER,
                validation_reviewed_by_name TEXT,
                validation_reviewed_at TEXT,
                validation_review_reason TEXT,
                approved_count INTEGER NOT NULL DEFAULT 0,
                rejected_count INTEGER NOT NULL DEFAULT 0,
                version_bot TEXT,
                FOREIGN KEY(id_ruta) REFERENCES routes(id_ruta)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_route ON orders(id_ruta, created_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_open_route ON orders(id_ruta, status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_open_chat ON orders(chat_id, status);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_orders_abonado ON orders(abonado_code, created_at);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS chat_config (
                chat_id INTEGER PRIMARY KEY,
                approval_required INTEGER NOT NULL DEFAULT 1,
                updated_at TEXT
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS step_state (
                id_orden INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL DEFAULT 1,
                submitted INTEGER NOT NULL DEFAULT 0,
                approved INTEGER,
                reviewed_by INTEGER,
                reviewed_at TEXT,
                created_at TEXT NOT NULL,
                reject_reason TEXT,
                reject_reason_by INTEGER,
                reject_reason_at TEXT,
                state_name TEXT NOT NULL DEFAULT 'PENDING',
                taken_by_user_id INTEGER,
                taken_by_name TEXT,
                taken_at TEXT,
                reopened_by TEXT,
                reopened_at TEXT,
                reopen_reason TEXT,
                blocked INTEGER NOT NULL DEFAULT 0,
                PRIMARY KEY(id_orden, step_no, attempt),
                FOREIGN KEY(id_orden) REFERENCES orders(id_orden)
            );
            """
        )

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media (
                media_id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_orden INTEGER NOT NULL,
                id_ruta INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                file_type TEXT NOT NULL,
                file_id TEXT NOT NULL,
                file_unique_id TEXT,
                tg_message_id INTEGER NOT NULL,
                media_group_id TEXT,
                meta_json TEXT,
                created_at TEXT NOT NULL,
                FOREIGN KEY(id_orden) REFERENCES orders(id_orden)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_order_step ON media(id_orden, step_no, attempt);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_order_step_msg ON media(id_orden, step_no, attempt, tg_message_id);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS auth_text (
                auth_id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_orden INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                text TEXT NOT NULL,
                tg_message_id INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY(id_orden) REFERENCES orders(id_orden)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_auth_text_order_step ON auth_text(id_orden, step_no, attempt);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS pending_inputs (
                pending_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                kind TEXT NOT NULL,
                id_ruta INTEGER,
                id_orden INTEGER,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                created_at TEXT NOT NULL,
                reply_to_message_id INTEGER,
                tech_user_id INTEGER
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_pending_inputs ON pending_inputs(chat_id, user_id, kind);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sheet_outbox (
                outbox_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sheet_name TEXT NOT NULL,
                op_type TEXT NOT NULL,
                dedupe_key TEXT NOT NULL,
                row_json TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                attempts INTEGER NOT NULL DEFAULT 0,
                last_error TEXT,
                next_retry_at TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_pending ON sheet_outbox(status, next_retry_at);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_outbox_key ON sheet_outbox(sheet_name, dedupe_key);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media_ack_buffer (
                ack_id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id INTEGER NOT NULL,
                id_ruta INTEGER,
                id_orden INTEGER NOT NULL,
                step_no INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                phase TEXT NOT NULL,
                created_by_user_id INTEGER NOT NULL,
                created_by_name TEXT,
                count_media INTEGER NOT NULL DEFAULT 0,
                last_media_at TEXT,
                ack_status TEXT NOT NULL DEFAULT 'PENDING'
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_media_ack_buffer ON media_ack_buffer(chat_id, id_orden, step_no, attempt, phase, ack_status);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS route_events (
                route_event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_ruta INTEGER NOT NULL,
                task_code TEXT NOT NULL,
                task_label TEXT NOT NULL,
                event_at TEXT NOT NULL,
                user_id INTEGER,
                user_name TEXT,
                chat_id INTEGER NOT NULL,
                detail TEXT,
                FOREIGN KEY(id_ruta) REFERENCES routes(id_ruta)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_route_events_route ON route_events(id_ruta, event_at);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS route_task_sessions (
                session_id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_ruta INTEGER NOT NULL,
                task_code TEXT NOT NULL,
                task_label TEXT NOT NULL,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                status TEXT NOT NULL DEFAULT 'OPEN',
                started_by_user_id INTEGER,
                started_by_name TEXT,
                finished_by_user_id INTEGER,
                finished_by_name TEXT,
                start_message_id INTEGER,
                finish_message_id INTEGER,
                start_confirm_message_id INTEGER,
                finish_confirm_message_id INTEGER,
                start_menu_message_id INTEGER,
                finish_menu_message_id INTEGER,
                FOREIGN KEY(id_ruta) REFERENCES routes(id_ruta)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_route_task_sessions_route ON route_task_sessions(id_ruta, task_code, status);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS alt_request_review (
                review_id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_orden INTEGER NOT NULL,
                status_code TEXT NOT NULL,
                status_label TEXT NOT NULL,
                submitted INTEGER NOT NULL DEFAULT 0,
                approved INTEGER,
                reviewed_by INTEGER,
                reviewed_at TEXT,
                reason TEXT,
                created_at TEXT NOT NULL,
                tg_message_id INTEGER,
                FOREIGN KEY(id_orden) REFERENCES orders(id_orden)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_alt_request_review_order ON alt_request_review(id_orden);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS change_state_log (
                change_id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_orden INTEGER NOT NULL,
                id_ruta INTEGER NOT NULL,
                change_type TEXT NOT NULL,
                change_label TEXT NOT NULL,
                reason TEXT NOT NULL,
                created_at TEXT NOT NULL,
                user_id INTEGER,
                user_name TEXT,
                chat_id INTEGER NOT NULL,
                approval_status TEXT NOT NULL DEFAULT 'PENDING',
                reviewed_by INTEGER,
                reviewed_by_name TEXT,
                reviewed_at TEXT,
                review_reason TEXT,
                tg_message_id INTEGER,
                FOREIGN KEY(id_orden) REFERENCES orders(id_orden)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_change_state_log_order ON change_state_log(id_orden, created_at);")

        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS service_validation (
                validation_id INTEGER PRIMARY KEY AUTOINCREMENT,
                id_orden INTEGER NOT NULL,
                id_ruta INTEGER NOT NULL,
                validator_name TEXT,
                validator_phone TEXT,
                validator_relationship TEXT,
                status TEXT NOT NULL DEFAULT 'DRAFT',
                created_by_user_id INTEGER,
                created_by_name TEXT,
                created_at TEXT NOT NULL,
                submitted_at TEXT,
                reviewed_by INTEGER,
                reviewed_by_name TEXT,
                reviewed_at TEXT,
                review_reason TEXT,
                confirm_message_id INTEGER,
                review_message_id INTEGER,
                FOREIGN KEY(id_orden) REFERENCES orders(id_orden)
            );
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_service_validation_order ON service_validation(id_orden);")

        conn.commit()


def set_approval_required(chat_id: int, required: bool):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO chat_config(chat_id, approval_required, updated_at)
            VALUES(?,?,?)
            ON CONFLICT(chat_id) DO UPDATE
              SET approval_required=excluded.approval_required, updated_at=excluded.updated_at
            """,
            (chat_id, 1 if required else 0, now_utc()),
        )
        conn.commit()


def get_approval_required(chat_id: int) -> bool:
    with db() as conn:
        row = conn.execute("SELECT approval_required FROM chat_config WHERE chat_id=?", (chat_id,)).fetchone()
        if row is None:
            conn.execute(
                "INSERT OR IGNORE INTO chat_config(chat_id, approval_required, updated_at) VALUES(?,?,?)",
                (chat_id, 1, now_utc()),
            )
            conn.commit()
            return True
        return bool(row["approval_required"])


def get_open_route(chat_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            "SELECT * FROM routes WHERE chat_id=? AND status=? ORDER BY id_ruta DESC LIMIT 1",
            (chat_id, ROUTE_STATUS_OPEN),
        ).fetchone()


def get_route(id_ruta: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM routes WHERE id_ruta=?", (id_ruta,)).fetchone()


def update_route(id_ruta: int, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    vals = [fields[k] for k in keys]
    sets = ", ".join([f"{k}=?" for k in keys])
    with db() as conn:
        conn.execute(f"UPDATE routes SET {sets} WHERE id_ruta=?", (*vals, id_ruta))
        conn.commit()


def clear_route_lock(id_ruta: int):
    update_route(
        id_ruta,
        locked_by_user_id=None,
        locked_by_name=None,
        locked_at=None,
        lock_expires_at=None,
    )


def lock_route(id_ruta: int, user_id: int, user_name: str):
    update_route(
        id_ruta,
        locked_by_user_id=user_id,
        locked_by_name=user_name,
        locked_at=now_utc(),
        lock_expires_at=lock_expires_at_iso(),
    )


def is_route_lock_expired(route_row: sqlite3.Row) -> bool:
    exp = parse_iso(route_row["lock_expires_at"] or "")
    if not exp:
        return True
    return datetime.now(timezone.utc) > exp


def maybe_release_expired_route_lock(route_row: Optional[sqlite3.Row]) -> Optional[sqlite3.Row]:
    if not route_row:
        return None
    if route_row["locked_by_user_id"] and is_route_lock_expired(route_row):
        clear_route_lock(int(route_row["id_ruta"]))
        return get_route(int(route_row["id_ruta"]))
    return route_row


def create_or_reset_route(chat_id: int, user_id: int, username: str) -> sqlite3.Row:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM routes WHERE chat_id=? AND status=? ORDER BY id_ruta DESC LIMIT 1",
            (chat_id, ROUTE_STATUS_OPEN),
        ).fetchone()

        if row:
            conn.execute(
                """
                UPDATE routes
                SET user_id=?,
                    username=?,
                    technician_name=NULL,
                    technician_user_id=NULL,
                    created_at=?,
                    closed_at=NULL,
                    status=?,
                    phase=?,
                    route_selfie_file_id=NULL,
                    route_selfie_file_unique_id=NULL,
                    route_selfie_message_id=NULL,
                    route_selfie_at=NULL,
                    route_location_lat=NULL,
                    route_location_lon=NULL,
                    route_location_at=NULL,
                    route_menu_enabled=0,
                    locked_by_user_id=NULL,
                    locked_by_name=NULL,
                    locked_at=NULL,
                    lock_expires_at=NULL,
                    version_bot=?
                WHERE id_ruta=?
                """,
                (user_id, username, now_utc(), ROUTE_STATUS_OPEN, PHASE_WAIT_TECHNICIAN, BOT_VERSION, row["id_ruta"]),
            )
            conn.execute("DELETE FROM orders WHERE id_ruta=?", (row["id_ruta"],))
            conn.execute("DELETE FROM route_events WHERE id_ruta=?", (row["id_ruta"],))
            conn.execute("DELETE FROM route_task_sessions WHERE id_ruta=?", (row["id_ruta"],))
            conn.commit()
            return conn.execute("SELECT * FROM routes WHERE id_ruta=?", (row["id_ruta"],)).fetchone()

        conn.execute(
            """
            INSERT INTO routes(
                chat_id, user_id, username, technician_name, technician_user_id,
                created_at, closed_at, status, phase,
                route_selfie_file_id, route_selfie_file_unique_id, route_selfie_message_id, route_selfie_at,
                route_location_lat, route_location_lon, route_location_at,
                route_menu_enabled, locked_by_user_id, locked_by_name, locked_at, lock_expires_at, version_bot
            )
            VALUES(
                ?,?,?,NULL,NULL,
                ?,NULL,?,?,
                NULL,NULL,NULL,NULL,
                NULL,NULL,NULL,
                0,NULL,NULL,NULL,NULL,?
            )
            """,
            (chat_id, user_id, username, now_utc(), ROUTE_STATUS_OPEN, PHASE_WAIT_TECHNICIAN, BOT_VERSION),
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        return conn.execute("SELECT * FROM routes WHERE id_ruta=?", (new_id,)).fetchone()


def get_open_order_for_route(id_ruta: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE id_ruta=? AND status=? ORDER BY id_orden DESC LIMIT 1",
            (id_ruta, ORDER_STATUS_OPEN),
        ).fetchone()


def get_open_order(chat_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            "SELECT * FROM orders WHERE chat_id=? AND status=? ORDER BY id_orden DESC LIMIT 1",
            (chat_id, ORDER_STATUS_OPEN),
        ).fetchone()


def get_order(id_orden: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM orders WHERE id_orden=?", (id_orden,)).fetchone()


def update_order(id_orden: int, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    vals = [fields[k] for k in keys]
    sets = ", ".join([f"{k}=?" for k in keys])
    with db() as conn:
        conn.execute(f"UPDATE orders SET {sets} WHERE id_orden=?", (*vals, id_orden))
        conn.commit()


def create_order_for_route(route_row: sqlite3.Row, user_id: int, username: str) -> sqlite3.Row:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO orders(
                id_ruta, chat_id, user_id, username, created_at, finished_at, status, phase, step_index,
                pending_step_no, current_step_no, admin_pending, technician_name, technician_user_id,
                service_type, abonado_code, evidencias_estado, install_mode, package_type, tv_count,
                client_status, final_order_status, location_lat, location_lon, location_at,
                change_state_type, change_state_reason,
                validation_status, validation_name, validation_phone, validation_relationship, validation_created_at,
                validation_submitted_at, validation_reviewed_by, validation_reviewed_by_name, validation_reviewed_at, validation_review_reason,
                approved_count, rejected_count, version_bot
            )
            VALUES(
                ?, ?, ?, ?, ?, NULL, ?, ?, 1,
                NULL, NULL, 0, ?, ?,
                NULL, NULL, ?, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                NULL, NULL, NULL, NULL, NULL,
                0, 0, ?
            )
            """,
            (
                int(route_row["id_ruta"]),
                int(route_row["chat_id"]),
                user_id,
                username,
                now_utc(),
                ORDER_STATUS_OPEN,
                PHASE_WAIT_SERVICE,
                route_row["technician_name"],
                route_row["technician_user_id"],
                EVIDENCIAS_ESTADO_INCOMPLETAS,
                BOT_VERSION,
            ),
        )
        conn.commit()
        new_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
        return conn.execute("SELECT * FROM orders WHERE id_orden=?", (new_id,)).fetchone()


def reset_order_data(id_orden: int):
    order_row = get_order(id_orden)
    if not order_row:
        return
    with db() as conn:
        conn.execute("DELETE FROM step_state WHERE id_orden=?", (id_orden,))
        conn.execute("DELETE FROM media WHERE id_orden=?", (id_orden,))
        conn.execute("DELETE FROM auth_text WHERE id_orden=?", (id_orden,))
        conn.execute("DELETE FROM media_ack_buffer WHERE id_orden=?", (id_orden,))
        conn.execute("DELETE FROM alt_request_review WHERE id_orden=?", (id_orden,))
        conn.execute("DELETE FROM change_state_log WHERE id_orden=?", (id_orden,))
        conn.execute("DELETE FROM service_validation WHERE id_orden=?", (id_orden,))
        conn.execute(
            """
            UPDATE orders
            SET user_id=?,
                username=?,
                created_at=?,
                finished_at=NULL,
                status=?,
                phase=?,
                step_index=1,
                pending_step_no=NULL,
                current_step_no=NULL,
                admin_pending=0,
                service_type=NULL,
                abonado_code=NULL,
                evidencias_estado=?,
                install_mode=NULL,
                package_type=NULL,
                tv_count=NULL,
                client_status=NULL,
                final_order_status=NULL,
                location_lat=NULL,
                location_lon=NULL,
                location_at=NULL,
                change_state_type=NULL,
                change_state_reason=NULL,
                validation_status=NULL,
                validation_name=NULL,
                validation_phone=NULL,
                validation_relationship=NULL,
                validation_created_at=NULL,
                validation_submitted_at=NULL,
                validation_reviewed_by=NULL,
                validation_reviewed_by_name=NULL,
                validation_reviewed_at=NULL,
                validation_review_reason=NULL,
                approved_count=0,
                rejected_count=0,
                version_bot=?
            WHERE id_orden=?
            """,
            (
                order_row["user_id"],
                order_row["username"],
                now_utc(),
                ORDER_STATUS_OPEN,
                PHASE_WAIT_SERVICE,
                EVIDENCIAS_ESTADO_INCOMPLETAS,
                BOT_VERSION,
                id_orden,
            ),
        )
        conn.commit()


def mark_evidencias_estado(id_orden: int, value: Optional[str]):
    update_order(id_orden, evidencias_estado=value)


def route_is_fully_started(route_row: sqlite3.Row) -> bool:
    return bool(
        (route_row["status"] or "") == ROUTE_STATUS_OPEN
        and int(route_row["route_menu_enabled"] or 0) == 1
    )


def route_started_at_text(route_row: sqlite3.Row) -> str:
    dt_s = route_row["created_at"] or ""
    if not dt_s:
        return "-"
    return f"{fmt_date_pe(dt_s)} {fmt_time_pe(dt_s)}"


def current_route_and_order(chat_id: int) -> Tuple[Optional[sqlite3.Row], Optional[sqlite3.Row]]:
    route_row = maybe_release_expired_route_lock(get_open_route(chat_id))
    order_row = get_open_order(chat_id)
    return route_row, order_row


def can_user_operate_current_route(route_row: sqlite3.Row, user_id: int) -> Tuple[bool, str]:
    route_row = maybe_release_expired_route_lock(route_row)
    if not route_row:
        return False, "No hay una ruta activa."
    lock_user = route_row["locked_by_user_id"]
    if lock_user and int(lock_user) != int(user_id):
        name = route_row["locked_by_name"] or "otro técnico"
        return False, f"🔒 Este paso está siendo trabajado por {name}."
    return True, ""


# =========================
# Dynamic step helpers
# =========================
def dynamic_step_name(step_no: int) -> str:
    if STEP_TV_BASE <= step_no < STEP_TEST_VELOCIDAD:
        idx = step_no - STEP_TV_BASE + 1
        return f"FOTO DE TV OPERATIVA {idx:02d}"
    return STEP_MEDIA_DEFS_BASE.get(step_no, (f"PASO {step_no}", ""))[0]


def dynamic_step_desc(step_no: int) -> str:
    if STEP_TV_BASE <= step_no < STEP_TEST_VELOCIDAD:
        idx = step_no - STEP_TV_BASE + 1
        return f"Envía foto de TV operativa correspondiente al TV {idx:02d}"
    return STEP_MEDIA_DEFS_BASE.get(step_no, (f"PASO {step_no}", "Envía evidencias"))[1]


def step_name(step_no: int) -> str:
    return dynamic_step_name(step_no)


def build_normal_step_template(install_mode: str, package_type: str, tv_count: int) -> List[Tuple[str, int]]:
    if install_mode == "EXTERNA" and package_type == "INTERNET":
        return [
            ("FACHADA", STEP_FACHADA),
            ("CTO", STEP_CTO),
            ("POTENCIA EN CTO", STEP_POTENCIA_CTO),
            ("CINTILLO ROTULADO", STEP_CINTILLO_ROTULADO),
            ("DROP QUE INGRESA AL DOMICILIO", STEP_DROP_DOMICILIO),
            ("ANCLAJE", STEP_ANCLAJE),
            ("ROSETA + MEDICION POTENCIA", STEP_ROSETA_POTENCIA),
            ("MAC ONT", STEP_MAC_ONT),
            ("ONT", STEP_ONT),
            ("TEST DE VELOCIDAD", STEP_TEST_VELOCIDAD),
            ("ACTA DE INSTALACION", STEP_ACTA_INSTALACION),
        ]

    if install_mode == "INTERNA" and package_type == "INTERNET":
        return [
            ("FACHADA", STEP_FACHADA),
            ("CTO", STEP_CTO),
            ("POTENCIA EN CTO", STEP_POTENCIA_CTO),
            ("CINTILLO ROTULADO", STEP_CINTILLO_ROTULADO),
            ("ROSETA + MEDICION POTENCIA", STEP_ROSETA_POTENCIA),
            ("MAC ONT", STEP_MAC_ONT),
            ("ONT", STEP_ONT),
            ("TEST DE VELOCIDAD", STEP_TEST_VELOCIDAD),
            ("ACTA DE INSTALACION", STEP_ACTA_INSTALACION),
        ]

    if install_mode == "EXTERNA" and package_type == "INTERNET + TV":
        rows = [
            ("FACHADA", STEP_FACHADA),
            ("CTO", STEP_CTO),
            ("POTENCIA EN CTO", STEP_POTENCIA_CTO),
            ("CINTILLO ROTULADO", STEP_CINTILLO_ROTULADO),
            ("DROP QUE INGRESA AL DOMICILIO", STEP_DROP_DOMICILIO),
            ("ANCLAJE", STEP_ANCLAJE),
            ("ROSETA + MEDICION POTENCIA", STEP_ROSETA_POTENCIA),
            ("FOTO PANORAMICA SPLITTER", STEP_SPLITTER_PANORAMICA),
            ("MAC ONT", STEP_MAC_ONT),
            ("ONT", STEP_ONT),
        ]
        for i in range(1, max(1, tv_count) + 1):
            rows.append((f"FOTO DE TV OPERATIVA {i:02d}", STEP_TV_BASE + (i - 1)))
        rows.extend([
            ("TEST DE VELOCIDAD", STEP_TEST_VELOCIDAD),
            ("ACTA DE INSTALACION", STEP_ACTA_INSTALACION),
        ])
        return rows

    if install_mode == "INTERNA" and package_type == "INTERNET + TV":
        rows = [
            ("FACHADA", STEP_FACHADA),
            ("CTO", STEP_CTO),
            ("POTENCIA EN CTO", STEP_POTENCIA_CTO),
            ("CINTILLO ROTULADO", STEP_CINTILLO_ROTULADO),
            ("ROSETA + MEDICION POTENCIA", STEP_ROSETA_POTENCIA),
            ("FOTO PANORAMICA SPLITTER", STEP_SPLITTER_PANORAMICA),
            ("MAC ONT", STEP_MAC_ONT),
            ("ONT", STEP_ONT),
        ]
        for i in range(1, max(1, tv_count) + 1):
            rows.append((f"FOTO DE TV OPERATIVA {i:02d}", STEP_TV_BASE + (i - 1)))
        rows.extend([
            ("TEST DE VELOCIDAD", STEP_TEST_VELOCIDAD),
            ("ACTA DE INSTALACION", STEP_ACTA_INSTALACION),
        ])
        return rows

    return []


def build_alt_step_template(client_status: str) -> List[Tuple[str, int]]:
    if client_status not in ALT_CLIENT_STATUSES:
        return []
    return [
        ("FACHADA PANORÁMICA", STEP_ALT_FACHADA),
        ("PLACA DE DOMICILIO", STEP_ALT_PLACA),
        ("SUMINISTRO ELÉCTRICO Y/O DE GAS", STEP_ALT_SUMINISTRO),
    ]


def is_alt_client_status(order_row: sqlite3.Row) -> bool:
    return (order_row["client_status"] or "").strip() in ALT_CLIENT_STATUSES


def get_order_step_items(order_row: sqlite3.Row) -> List[Tuple[int, str, int]]:
    client_status = (order_row["client_status"] or "").strip()
    install_mode = (order_row["install_mode"] or "").strip()
    package_type = (order_row["package_type"] or "").strip()
    tv_count = int(order_row["tv_count"] or 0)

    if client_status in ALT_CLIENT_STATUSES:
        template = build_alt_step_template(client_status)
    else:
        template = build_normal_step_template(install_mode, package_type, tv_count)

    out: List[Tuple[int, str, int]] = []
    idx = 1
    for label, step_no in template:
        out.append((idx, label, step_no))
        idx += 1
    return out


def is_last_step(order_row: sqlite3.Row, step_no: int) -> bool:
    items = get_order_step_items(order_row)
    if not items:
        return False
    return step_no == items[-1][2]


def _max_attempt(id_orden: int, step_no: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT MAX(attempt) AS mx FROM step_state WHERE id_orden=? AND step_no=?",
            (id_orden, step_no),
        ).fetchone()
        mx = row["mx"] if row and row["mx"] is not None else 0
        return int(mx) if mx else 0


def get_latest_step_state(id_orden: int, step_no: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM step_state
            WHERE id_orden=? AND step_no=?
            ORDER BY attempt DESC LIMIT 1
            """,
            (id_orden, step_no),
        ).fetchone()


def get_active_unsubmitted_step_state(id_orden: int, step_no: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM step_state
            WHERE id_orden=? AND step_no=? AND submitted=0
            ORDER BY attempt DESC LIMIT 1
            """,
            (id_orden, step_no),
        ).fetchone()


def ensure_step_state(id_orden: int, step_no: int, *, owner_user_id: Optional[int] = None, owner_name: Optional[str] = None) -> sqlite3.Row:
    with db() as conn:
        row = conn.execute(
            """
            SELECT * FROM step_state
            WHERE id_orden=? AND step_no=? AND submitted=0
            ORDER BY attempt DESC LIMIT 1
            """,
            (id_orden, step_no),
        ).fetchone()
        if row:
            return row

        prev = conn.execute(
            """
            SELECT * FROM step_state
            WHERE id_orden=? AND step_no=?
            ORDER BY attempt DESC LIMIT 1
            """,
            (id_orden, step_no),
        ).fetchone()

        attempt = _max_attempt(id_orden, step_no) + 1
        initial_state = STEP_STATE_REABIERTO if (prev and prev["approved"] is not None and int(prev["approved"]) == 1) else STEP_STATE_EN_CARGA

        conn.execute(
            """
            INSERT INTO step_state(
                id_orden, step_no, attempt, submitted, approved, reviewed_by, reviewed_at, created_at,
                reject_reason, reject_reason_by, reject_reason_at, state_name,
                taken_by_user_id, taken_by_name, taken_at, reopened_by, reopened_at, reopen_reason, blocked
            )
            VALUES(?,?,?,0,NULL,NULL,NULL,?,NULL,NULL,NULL,?,?,?,?,NULL,NULL,NULL,0)
            """,
            (
                id_orden,
                step_no,
                attempt,
                now_utc(),
                initial_state,
                owner_user_id,
                owner_name,
                now_utc() if owner_user_id else None,
            ),
        )
        conn.commit()
        return conn.execute(
            "SELECT * FROM step_state WHERE id_orden=? AND step_no=? AND attempt=?",
            (id_orden, step_no, attempt),
        ).fetchone()


def set_step_owner(id_orden: int, step_no: int, attempt: int, user_id: int, user_name: str):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET taken_by_user_id=?, taken_by_name=?, taken_at=?, state_name=?
            WHERE id_orden=? AND step_no=? AND attempt=?
            """,
            (user_id, user_name, now_utc(), STEP_STATE_EN_CARGA, id_orden, step_no, attempt),
        )
        conn.commit()


def set_step_state_name(id_orden: int, step_no: int, attempt: int, state_name: str):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET state_name=?
            WHERE id_orden=? AND step_no=? AND attempt=?
            """,
            (state_name, id_orden, step_no, attempt),
        )
        conn.commit()


def get_effective_step_status(id_orden: int, step_no: int) -> str:
    row = get_latest_step_state(id_orden, step_no)
    if not row:
        return STEP_STATE_PENDIENTE
    state_name = (row["state_name"] or "").strip().upper() or STEP_STATE_PENDIENTE
    if int(row["blocked"] or 0) == 1:
        return STEP_STATE_BLOQUEADO
    return state_name


def compute_next_required_step(order_row: sqlite3.Row) -> Tuple[int, str, int, str]:
    items = get_order_step_items(order_row)
    if not items:
        return (0, "-", 0, STEP_STATE_PENDIENTE)
    for num, label, step_no in items:
        st = get_effective_step_status(int(order_row["id_orden"]), step_no)
        if st != STEP_STATE_APROBADO:
            return (num, label, step_no, st)
    last_num, last_label, last_step = items[-1]
    return (last_num, last_label, last_step, STEP_STATE_APROBADO)


def media_count(id_orden: int, step_no: int, attempt: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM media WHERE id_orden=? AND step_no=? AND attempt=?",
            (id_orden, step_no, attempt),
        ).fetchone()
        return int(row["c"]) if row else 0


def media_message_ids(id_orden: int, step_no: int, attempt: int) -> List[int]:
    with db() as conn:
        rows = conn.execute(
            "SELECT tg_message_id FROM media WHERE id_orden=? AND step_no=? AND attempt=? ORDER BY media_id ASC",
            (id_orden, step_no, attempt),
        ).fetchall()
        return [int(r["tg_message_id"]) for r in rows] if rows else []


def total_media_for_order(id_orden: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM media WHERE id_orden=? AND step_no > 0",
            (id_orden,),
        ).fetchone()
        return int(row["c"] or 0)


def total_rejects_for_order(id_orden: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM step_state WHERE id_orden=? AND step_no > 0 AND approved=0",
            (id_orden,),
        ).fetchone()
        return int(row["c"] or 0)


def total_approved_steps_for_order(id_orden: int) -> int:
    with db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM step_state WHERE id_orden=? AND step_no > 0 AND approved=1",
            (id_orden,),
        ).fetchone()
        return int(row["c"] or 0)


def all_normal_evidences_approved(order_row: sqlite3.Row) -> bool:
    if not order_row:
        return False
    if is_alt_client_status(order_row):
        return False
    items = get_order_step_items(order_row)
    if not items:
        return False
    id_orden = int(order_row["id_orden"])
    for _, _, step_no in items:
        if get_effective_step_status(id_orden, step_no) != STEP_STATE_APROBADO:
            return False
    return True


def validation_is_ready(order_row: sqlite3.Row) -> bool:
    if not order_row:
        return False
    return all_normal_evidences_approved(order_row)


def add_media(
    id_orden: int,
    id_ruta: int,
    step_no: int,
    attempt: int,
    file_type: str,
    file_id: str,
    file_unique_id: Optional[str],
    tg_message_id: int,
    media_group_id: Optional[str],
    meta: Dict[str, Any],
):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO media(id_orden, id_ruta, step_no, attempt, file_type, file_id, file_unique_id, tg_message_id, media_group_id, meta_json, created_at)
            VALUES(?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                id_orden,
                id_ruta,
                step_no,
                attempt,
                file_type,
                file_id,
                file_unique_id or "",
                tg_message_id,
                media_group_id or "",
                json.dumps(meta, ensure_ascii=False),
                now_utc(),
            ),
        )
        conn.commit()


def mark_submitted(id_orden: int, step_no: int, attempt: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET submitted=1, state_name=?
            WHERE id_orden=? AND step_no=? AND attempt=?
            """,
            (STEP_STATE_EN_REVISION, id_orden, step_no, attempt),
        )
        conn.commit()


def set_review(id_orden: int, step_no: int, attempt: int, approved: int, reviewer_id: int):
    state_name = STEP_STATE_APROBADO if int(approved) == 1 else STEP_STATE_RECHAZADO
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET approved=?, reviewed_by=?, reviewed_at=?, state_name=?
            WHERE id_orden=? AND step_no=? AND attempt=?
            """,
            (approved, reviewer_id, now_utc(), state_name, id_orden, step_no, attempt),
        )
        conn.commit()


def set_reject_reason(id_orden: int, step_no: int, attempt: int, reason: str, reviewer_id: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET reject_reason=?, reject_reason_by=?, reject_reason_at=?, state_name=?
            WHERE id_orden=? AND step_no=? AND attempt=?
            """,
            (reason, reviewer_id, now_utc(), STEP_STATE_RECHAZADO, id_orden, step_no, attempt),
        )
        conn.commit()


def reopen_step(order_row: sqlite3.Row, step_no: int, admin_name: str, reason: str) -> sqlite3.Row:
    id_orden = int(order_row["id_orden"])
    prev = get_latest_step_state(id_orden, step_no)
    if not prev:
        raise RuntimeError("No existe un intento previo para reabrir.")
    if prev["approved"] is None or int(prev["approved"]) != 1:
        raise RuntimeError("Solo se puede reabrir un paso aprobado.")

    with db() as conn:
        attempt = _max_attempt(id_orden, step_no) + 1
        conn.execute(
            """
            INSERT INTO step_state(
                id_orden,
                step_no,
                attempt,
                submitted,
                approved,
                reviewed_by,
                reviewed_at,
                created_at,
                reject_reason,
                reject_reason_by,
                reject_reason_at,
                state_name,
                taken_by_user_id,
                taken_by_name,
                taken_at,
                reopened_by,
                reopened_at,
                reopen_reason,
                blocked
            )
            VALUES(
                ?, ?, ?,
                0, NULL, NULL, NULL,
                ?,
                NULL, NULL, NULL,
                ?,
                NULL, NULL, NULL,
                ?, ?, ?,
                0
            )
            """,
            (
                id_orden,
                step_no,
                attempt,
                now_utc(),
                STEP_STATE_REABIERTO,
                admin_name,
                now_utc(),
                reason,
            ),
        )
        conn.commit()
    return get_latest_step_state(id_orden, step_no)


def save_auth_text(id_orden: int, auth_step_no: int, attempt: int, text: str, tg_message_id: int):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO auth_text(id_orden, step_no, attempt, text, tg_message_id, created_at)
            VALUES(?,?,?,?,?,?)
            """,
            (id_orden, auth_step_no, attempt, text, tg_message_id, now_utc()),
        )
        conn.commit()


def set_pending_input(
    chat_id: int,
    user_id: int,
    kind: str,
    id_ruta: Optional[int],
    id_orden: Optional[int],
    step_no: int,
    attempt: int,
    reply_to_message_id: Optional[int] = None,
    tech_user_id: Optional[int] = None,
):
    with db() as conn:
        conn.execute("DELETE FROM pending_inputs WHERE chat_id=? AND user_id=? AND kind=?", (chat_id, user_id, kind))
        conn.execute(
            """
            INSERT INTO pending_inputs(chat_id, user_id, kind, id_ruta, id_orden, step_no, attempt, created_at, reply_to_message_id, tech_user_id)
            VALUES(?,?,?,?,?,?,?,?,?,?)
            """,
            (chat_id, user_id, kind, id_ruta, id_orden, step_no, attempt, now_utc(), reply_to_message_id, tech_user_id),
        )
        conn.commit()


def pop_pending_input(chat_id: int, user_id: int, kind: str) -> Optional[sqlite3.Row]:
    with db() as conn:
        row = conn.execute(
            "SELECT * FROM pending_inputs WHERE chat_id=? AND user_id=? AND kind=? ORDER BY pending_id DESC LIMIT 1",
            (chat_id, user_id, kind),
        ).fetchone()
        if row:
            conn.execute("DELETE FROM pending_inputs WHERE pending_id=?", (row["pending_id"],))
            conn.commit()
        return row


def sync_order_progress(id_orden: int):
    order_row = get_order(id_orden)
    if not order_row:
        return

    items = get_order_step_items(order_row)
    if not items:
        update_order(id_orden, current_step_no=None, pending_step_no=None, admin_pending=0)
        return

    _, _, next_step_no, next_status = compute_next_required_step(order_row)
    update_fields = {
        "current_step_no": None if next_status == STEP_STATE_APROBADO else next_step_no,
        "pending_step_no": None if next_status == STEP_STATE_APROBADO else next_step_no,
        "admin_pending": 1 if next_status == STEP_STATE_EN_REVISION else 0,
    }
    update_order(id_orden, **update_fields)


def route_event_count(id_ruta: int) -> int:
    with db() as conn:
        row = conn.execute("SELECT COUNT(*) AS c FROM route_events WHERE id_ruta=?", (id_ruta,)).fetchone()
        return int(row["c"] or 0)


def task_label_by_code(task_code: str) -> str:
    for code, label in ROUTE_TASKS:
        if code == task_code:
            return label
    if task_code == TASK_CERRAR_RUTA:
        return "Cerrar ruta"
    return task_code


def task_icon_by_code(task_code: str) -> str:
    icons = {
        TASK_EN_CAMINO: "🚗",
        TASK_RECOJO_MATERIALES: "📦",
        TASK_MANTENIMIENTO_VEHICULAR: "🛠️",
        TASK_ALMUERZO: "🍽️",
        TASK_CAPACITACION: "📚",
    }
    return icons.get(task_code, "🔹")


def task_label_with_icon(task_code: str) -> str:
    return f"{task_icon_by_code(task_code)} {task_label_by_code(task_code)}"


def add_route_event(id_ruta: int, task_code: str, task_label: str, chat_id: int, user_id: int, user_name: str, detail: str = ""):
    with db() as conn:
        conn.execute(
            """
            INSERT INTO route_events(id_ruta, task_code, task_label, event_at, user_id, user_name, chat_id, detail)
            VALUES(?,?,?,?,?,?,?,?)
            """,
            (id_ruta, task_code, task_label, now_utc(), user_id, user_name, chat_id, detail),
        )
        conn.commit()


def get_open_route_task_session(id_ruta: int, task_code: str) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM route_task_sessions
            WHERE id_ruta=? AND task_code=? AND status=?
            ORDER BY session_id DESC LIMIT 1
            """,
            (id_ruta, task_code, TASK_SESSION_STATUS_OPEN),
        ).fetchone()


def get_any_open_route_task_session(id_ruta: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM route_task_sessions
            WHERE id_ruta=? AND status=?
            ORDER BY session_id DESC LIMIT 1
            """,
            (id_ruta, TASK_SESSION_STATUS_OPEN),
        ).fetchone()


def create_route_task_session(
    id_ruta: int,
    task_code: str,
    task_label: str,
    user_id: int,
    user_name: str,
    start_message_id: Optional[int] = None,
    start_confirm_message_id: Optional[int] = None,
    start_menu_message_id: Optional[int] = None,
) -> int:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO route_task_sessions(
                id_ruta,
                task_code,
                task_label,
                started_at,
                finished_at,
                status,
                started_by_user_id,
                started_by_name,
                finished_by_user_id,
                finished_by_name,
                start_message_id,
                finish_message_id,
                start_confirm_message_id,
                finish_confirm_message_id,
                start_menu_message_id,
                finish_menu_message_id
            )
            VALUES(
                ?, ?, ?, ?, NULL, ?, ?, ?, NULL, NULL, ?, NULL, ?, NULL, ?, NULL
            )
            """,
            (
                id_ruta,
                task_code,
                task_label,
                now_utc(),
                TASK_SESSION_STATUS_OPEN,
                user_id,
                user_name,
                start_message_id,
                start_confirm_message_id,
                start_menu_message_id,
            ),
        )
        conn.commit()
        row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
        return int(row["id"])


def update_route_task_session(session_id: int, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    vals = [fields[k] for k in keys]
    sets = ", ".join([f"{k}=?" for k in keys])
    with db() as conn:
        conn.execute(f"UPDATE route_task_sessions SET {sets} WHERE session_id=?", (*vals, session_id))
        conn.commit()


def close_route_task_session(
    session_id: int,
    user_id: int,
    user_name: str,
    finish_message_id: Optional[int] = None,
    finish_confirm_message_id: Optional[int] = None,
    finish_menu_message_id: Optional[int] = None,
):
    with db() as conn:
        conn.execute(
            """
            UPDATE route_task_sessions
            SET finished_at=?,
                status=?,
                finished_by_user_id=?,
                finished_by_name=?,
                finish_message_id=?,
                finish_confirm_message_id=?,
                finish_menu_message_id=?
            WHERE session_id=?
            """,
            (
                now_utc(),
                TASK_SESSION_STATUS_CLOSED,
                user_id,
                user_name,
                finish_message_id,
                finish_confirm_message_id,
                finish_menu_message_id,
                session_id,
            ),
        )
        conn.commit()


def cancel_route_task_session(session_id: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE route_task_sessions
            SET status=?
            WHERE session_id=?
            """,
            (TASK_SESSION_STATUS_CANCELLED, session_id),
        )
        conn.commit()


def any_route_timed_task_open(id_ruta: int) -> Optional[sqlite3.Row]:
    row = get_any_open_route_task_session(id_ruta)
    if not row:
        return None
    if (row["task_code"] or "") not in ROUTE_TIMED_TASKS:
        return None
    return row


def get_alt_request_row(id_orden: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM alt_request_review
            WHERE id_orden=?
            ORDER BY review_id DESC LIMIT 1
            """,
            (id_orden,),
        ).fetchone()


def upsert_alt_request(id_orden: int, status_code: str, status_label: str, tg_message_id: Optional[int] = None):
    with db() as conn:
        row = conn.execute(
            """
            SELECT * FROM alt_request_review
            WHERE id_orden=?
            ORDER BY review_id DESC LIMIT 1
            """,
            (id_orden,),
        ).fetchone()
        if row:
            conn.execute(
                """
                UPDATE alt_request_review
                SET status_code=?, status_label=?, submitted=1, approved=NULL, reviewed_by=NULL, reviewed_at=NULL, reason=NULL, tg_message_id=?
                WHERE review_id=?
                """,
                (status_code, status_label, tg_message_id, int(row["review_id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO alt_request_review(id_orden, status_code, status_label, submitted, approved, reviewed_by, reviewed_at, reason, created_at, tg_message_id)
                VALUES(?,?,?,?,NULL,NULL,NULL,NULL,?,?)
                """,
                (id_orden, status_code, status_label, 1, now_utc(), tg_message_id),
            )
        conn.commit()


def set_alt_request_review(id_orden: int, approved: int, reviewer_id: int, reason: str = ""):
    with db() as conn:
        row = conn.execute(
            """
            SELECT * FROM alt_request_review
            WHERE id_orden=?
            ORDER BY review_id DESC LIMIT 1
            """,
            (id_orden,),
        ).fetchone()
        if not row:
            return
        conn.execute(
            """
            UPDATE alt_request_review
            SET approved=?, reviewed_by=?, reviewed_at=?, reason=?
            WHERE review_id=?
            """,
            (approved, reviewer_id, now_utc(), reason, int(row["review_id"])),
        )
        conn.commit()


def alt_flow_all_steps_loaded(order_row: sqlite3.Row) -> bool:
    id_orden = int(order_row["id_orden"])
    items = get_order_step_items(order_row)
    if not items:
        return False
    for _, _, step_no in items:
        st = get_latest_step_state(id_orden, step_no)
        if not st:
            return False
        if int(st["submitted"] or 0) != 1 or st["approved"] is None or int(st["approved"]) != 1:
            return False
    return True


def get_client_status_label(status_code: str) -> str:
    for code, label in CLIENT_STATUS_OPTIONS:
        if code == status_code:
            return label
    return status_code


def get_latest_change_state_request(id_orden: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM change_state_log
            WHERE id_orden=?
            ORDER BY change_id DESC LIMIT 1
            """,
            (id_orden,),
        ).fetchone()


def create_change_state_request(id_orden: int, id_ruta: int, change_type: str, change_label: str, reason: str, user_id: int, user_name: str, chat_id: int) -> int:
    with db() as conn:
        conn.execute(
            """
            INSERT INTO change_state_log(
                id_orden, id_ruta, change_type, change_label, reason, created_at, user_id, user_name, chat_id,
                approval_status, reviewed_by, reviewed_by_name, reviewed_at, review_reason, tg_message_id
            )
            VALUES(?,?,?,?,?,?,?,?,?, 'PENDING', NULL, NULL, NULL, NULL, NULL)
            """,
            (id_orden, id_ruta, change_type, change_label, reason, now_utc(), user_id, user_name, chat_id),
        )
        conn.commit()
        row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
        return int(row["id"])


def update_change_state_request(request_id: int, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    vals = [fields[k] for k in keys]
    sets = ", ".join([f"{k}=?" for k in keys])
    with db() as conn:
        conn.execute(f"UPDATE change_state_log SET {sets} WHERE change_id=?", (*vals, request_id))
        conn.commit()


def get_change_state_request(request_id: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute("SELECT * FROM change_state_log WHERE change_id=?", (request_id,)).fetchone()


def get_latest_service_validation(id_orden: int) -> Optional[sqlite3.Row]:
    with db() as conn:
        return conn.execute(
            """
            SELECT * FROM service_validation
            WHERE id_orden=?
            ORDER BY validation_id DESC LIMIT 1
            """,
            (id_orden,),
        ).fetchone()


def upsert_service_validation_draft(id_orden: int, id_ruta: int, user_id: int, user_name: str, name: str = "", phone: str = "", relationship: str = "") -> sqlite3.Row:
    with db() as conn:
        row = conn.execute(
            """
            SELECT * FROM service_validation
            WHERE id_orden=?
            ORDER BY validation_id DESC LIMIT 1
            """,
            (id_orden,),
        ).fetchone()

        if row:
            conn.execute(
                """
                UPDATE service_validation
                SET validator_name=?,
                    validator_phone=?,
                    validator_relationship=?,
                    status=?,
                    created_by_user_id=?,
                    created_by_name=?,
                    created_at=?,
                    submitted_at=NULL,
                    reviewed_by=NULL,
                    reviewed_by_name=NULL,
                    reviewed_at=NULL,
                    review_reason=NULL
                WHERE validation_id=?
                """,
                (
                    name,
                    phone,
                    relationship,
                    VALIDATION_STATUS_DRAFT,
                    user_id,
                    user_name,
                    now_utc(),
                    int(row["validation_id"]),
                ),
            )
        else:
            conn.execute(
                """
                INSERT INTO service_validation(
                    id_orden, id_ruta, validator_name, validator_phone, validator_relationship, status,
                    created_by_user_id, created_by_name, created_at, submitted_at,
                    reviewed_by, reviewed_by_name, reviewed_at, review_reason,
                    confirm_message_id, review_message_id
                )
                VALUES(?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL,NULL,NULL,NULL)
                """,
                (
                    id_orden,
                    id_ruta,
                    name,
                    phone,
                    relationship,
                    VALIDATION_STATUS_DRAFT,
                    user_id,
                    user_name,
                    now_utc(),
                ),
            )
        conn.commit()
        return conn.execute(
            """
            SELECT * FROM service_validation
            WHERE id_orden=?
            ORDER BY validation_id DESC LIMIT 1
            """,
            (id_orden,),
        ).fetchone()


def update_service_validation(validation_id: int, **fields):
    if not fields:
        return
    keys = list(fields.keys())
    vals = [fields[k] for k in keys]
    sets = ", ".join([f"{k}=?" for k in keys])
    with db() as conn:
        conn.execute(f"UPDATE service_validation SET {sets} WHERE validation_id=?", (*vals, validation_id))
        conn.commit()


def sync_order_validation_fields(id_orden: int):
    order_row = get_order(id_orden)
    val = get_latest_service_validation(id_orden)
    if not order_row:
        return
    if not val:
        update_order(
            id_orden,
            validation_status=None,
            validation_name=None,
            validation_phone=None,
            validation_relationship=None,
            validation_submitted_at=None,
            validation_reviewed_by=None,
            validation_reviewed_by_name=None,
            validation_reviewed_at=None,
            validation_review_reason=None,
        )
        return

    update_order(
        id_orden,
        validation_status=val["status"],
        validation_name=val["validator_name"],
        validation_phone=val["validator_phone"],
        validation_relationship=val["validator_relationship"],
        validation_created_at=val["created_at"],
        validation_submitted_at=val["submitted_at"],
        validation_reviewed_by=val["reviewed_by"],
        validation_reviewed_by_name=val["reviewed_by_name"],
        validation_reviewed_at=val["reviewed_at"],
        validation_review_reason=val["review_reason"],
    )


def auto_approve_db_step(id_orden: int, db_step_no: int, attempt: int):
    with db() as conn:
        conn.execute(
            """
            UPDATE step_state
            SET submitted=1, approved=1, reviewed_by=?, reviewed_at=?, state_name=?
            WHERE id_orden=? AND step_no=? AND attempt=?
            """,
            (0, now_utc(), STEP_STATE_APROBADO, id_orden, db_step_no, attempt),
        )
        conn.commit()


def increment_order_review_counts(id_orden: int):
    aprob = total_approved_steps_for_order(id_orden)
    rech = total_rejects_for_order(id_orden)
    update_order(id_orden, approved_count=aprob, rejected_count=rech)


# =========================
# Outbox helpers
# =========================
def outbox_enqueue(sheet_name: str, op_type: str, dedupe_key: str, row: Dict[str, Any]):
    now = now_utc()
    row_json = json.dumps(row, ensure_ascii=False)
    with db() as conn:
        existing = conn.execute(
            """
            SELECT outbox_id, status FROM sheet_outbox
            WHERE sheet_name=? AND dedupe_key=? AND status IN ('PENDING','FAILED')
            ORDER BY outbox_id DESC LIMIT 1
            """,
            (sheet_name, dedupe_key),
        ).fetchone()

        if existing:
            conn.execute(
                """
                UPDATE sheet_outbox
                SET row_json=?, op_type=?, status='PENDING', last_error=NULL, next_retry_at=NULL, updated_at=?
                WHERE outbox_id=?
                """,
                (row_json, op_type, now, int(existing["outbox_id"])),
            )
        else:
            conn.execute(
                """
                INSERT INTO sheet_outbox(
                    sheet_name,
                    op_type,
                    dedupe_key,
                    row_json,
                    status,
                    attempts,
                    last_error,
                    next_retry_at,
                    created_at,
                    updated_at
                )
                VALUES(?, ?, ?, ?, 'PENDING', 0, NULL, NULL, ?, NULL)
                """,
                (sheet_name, op_type, dedupe_key, row_json, now),
            )
        conn.commit()


def outbox_fetch_batch(limit: int = 20) -> List[sqlite3.Row]:
    now = now_utc()
    with db() as conn:
        rows = conn.execute(
            """
            SELECT * FROM sheet_outbox
            WHERE status IN ('PENDING','FAILED')
              AND (next_retry_at IS NULL OR next_retry_at <= ?)
            ORDER BY created_at ASC
            LIMIT ?
            """,
            (now, limit),
        ).fetchall()
        return rows


def outbox_mark_sent(outbox_id: int):
    with db() as conn:
        conn.execute(
            "UPDATE sheet_outbox SET status='SENT', updated_at=? WHERE outbox_id=?",
            (now_utc(), outbox_id),
        )
        conn.commit()


def _next_retry_time(attempts: int) -> str:
    minutes = [1, 2, 4, 8, 15, 30, 60, 120]
    idx = min(attempts, len(minutes) - 1)
    dt = datetime.now(timezone.utc) + timedelta(minutes=minutes[idx])
    return dt.isoformat()


def outbox_mark_failed(outbox_id: int, attempts: int, err: str, dead: bool = False):
    status = "DEAD" if dead else "FAILED"
    next_retry_at = None if dead else _next_retry_time(attempts)
    with db() as conn:
        conn.execute(
            """
            UPDATE sheet_outbox
            SET status=?, attempts=?, last_error=?, next_retry_at=?, updated_at=?
            WHERE outbox_id=?
            """,
            (status, attempts, err[:500], next_retry_at, now_utc(), outbox_id),
        )
        conn.commit()


# =========================
# Google Sheets helpers
# =========================
def sheets_client():
    if not SHEET_ID:
        raise RuntimeError("Falta SHEET_ID. Configura la variable SHEET_ID.")

    scopes = ["https://www.googleapis.com/auth/spreadsheets"]

    if GOOGLE_CREDS_JSON_TEXT:
        creds_info = json.loads(GOOGLE_CREDS_JSON_TEXT)
        creds = Credentials.from_service_account_info(creds_info, scopes=scopes)
    else:
        if not GOOGLE_CREDS_JSON:
            raise RuntimeError("Falta GOOGLE_CREDS_JSON o GOOGLE_CREDS_JSON_TEXT.")
        creds = Credentials.from_service_account_file(GOOGLE_CREDS_JSON, scopes=scopes)

    gc = gspread.authorize(creds)
    sh = gc.open_by_key(SHEET_ID)
    return sh


def _ensure_headers(ws, expected_headers: List[str]):
    values = ws.get_all_values()
    if not values:
        ws.append_row(expected_headers, value_input_option="RAW")
        return
    headers = values[0]
    for h in expected_headers:
        if h not in headers:
            raise RuntimeError(f"Falta columna '{h}' en hoja '{ws.title}'. No modifiques headers.")


def build_index(ws, key_cols: List[str]) -> Dict[str, int]:
    values = ws.get_all_values()
    if not values:
        return {}
    headers = values[0]
    col_idx = {h: i for i, h in enumerate(headers)}
    for c in key_cols:
        if c not in col_idx:
            raise RuntimeError(f"Falta columna '{c}' en hoja '{ws.title}'")

    idx: Dict[str, int] = {}
    for r in range(2, len(values) + 1):
        row = values[r - 1]
        parts: List[str] = []
        for c in key_cols:
            i = col_idx[c]
            parts.append(row[i] if i < len(row) else "")
        k = "|".join(parts).strip()
        if k:
            idx[k] = r
    return idx


def row_to_values(row: Dict[str, Any], columns: List[str]) -> List[Any]:
    return [row.get(c, "") for c in columns]


def _col_index_map(ws) -> Dict[str, int]:
    values = ws.get_all_values()
    if not values:
        return {}
    headers = values[0]
    return {h: i + 1 for i, h in enumerate(headers)}


def _a1(col: int, row: int) -> str:
    letters = ""
    n = col
    while n > 0:
        n, rem = divmod(n - 1, 26)
        letters = chr(65 + rem) + letters
    return f"{letters}{row}"


def sheet_upsert(ws, index: Dict[str, int], key: str, row: Dict[str, Any], columns: List[str], key_cols: List[str]):
    _ensure_headers(ws, columns)
    col_map = _col_index_map(ws)

    for kc in key_cols:
        if kc not in col_map:
            raise RuntimeError(f"Falta columna clave '{kc}' en hoja '{ws.title}'")

    values = row_to_values(row, columns)

    if key in index:
        r = index[key]
        start = _a1(1, r)
        end = _a1(len(columns), r)
        ws.update(values=[values], range_name=f"{start}:{end}", value_input_option="RAW")
    else:
        ws.append_row(values, value_input_option="RAW")
        all_vals = ws.get_all_values()
        index[key] = len(all_vals)


def _is_permanent_sheet_error(err: str) -> bool:
    low = err.lower()
    if "not found" in low and "worksheet" in low:
        return True
    if "invalid" in low and "credentials" in low:
        return True
    if "permission" in low or "insufficient" in low:
        return True
    return False


def _safe_str(v: Any) -> str:
    return str(v).strip() if v is not None else ""


def _parse_bool01(v: Any) -> int:
    s = str(v).strip().lower()
    if s in ("1", "true", "si", "sí", "on", "activo", "yes"):
        return 1
    return 0


def _parse_int_or_default(v: Any, default: int) -> int:
    try:
        return int(str(v).strip())
    except Exception:
        return default


def _read_all_records(ws) -> List[Dict[str, Any]]:
    try:
        return ws.get_all_records()
    except Exception:
        values = ws.get_all_values()
        if not values or len(values) < 2:
            return []
        headers = values[0]
        out: List[Dict[str, Any]] = []
        for r in values[1:]:
            d = {}
            for i, h in enumerate(headers):
                d[h] = r[i] if i < len(r) else ""
            out.append(d)
        return out


def get_config_value(app: Application, param: str) -> Optional[str]:
    ws = app.bot_data.get("ws_config")
    if not ws:
        return None

    rows = _read_all_records(ws)
    for r in rows:
        if str(r.get("parametro", "")).strip() == str(param).strip():
            val = str(r.get("valor", "")).strip()
            return val if val else None

    return None


async def send_step_guide(context: ContextTypes.DEFAULT_TYPE, chat_id: int, step_no: int) -> None:
    real_step_no = abs(step_no)
    param = GUIDE_PARAM_MAP.get(real_step_no)
    if not param and STEP_TV_BASE <= real_step_no < STEP_TEST_VELOCIDAD:
        param = "GUIA_TV_OPERATIVA"
    if not param:
        return

    file_id = get_config_value(context.application, param)
    if not file_id:
        return

    note = GUIDE_NOTE_MAP.get(real_step_no, "")
    if STEP_TV_BASE <= real_step_no < STEP_TEST_VELOCIDAD:
        idx = real_step_no - STEP_TV_BASE + 1
        note = f"Debe verse la TV operativa correspondiente al TV {idx:02d}."

    try:
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=file_id,
            caption=f"📷 Ejemplo de foto correcta\n\n⚠️ {note}",
        )
    except Exception as e:
        log.warning(f"No pude enviar foto guía del paso {real_step_no}: {e}")


def load_tecnicos_cache(app: Application) -> None:
    if not app.bot_data.get("sheets_ready"):
        return
    ws = app.bot_data.get("ws_tecnicos")
    if not ws:
        return
    try:
        _ensure_headers(ws, TECNICOS_COLUMNS)
        rows = _read_all_records(ws)
        techs: List[Dict[str, Any]] = []
        for r in rows:
            nombre = _safe_str(r.get("nombre"))
            if not nombre:
                continue
            activo = _parse_bool01(r.get("activo"))
            if activo != 1:
                continue
            orden = _parse_int_or_default(r.get("orden"), 9999)
            telegram_user_id = _safe_str(r.get("telegram_user_id"))
            techs.append(
                {
                    "nombre": nombre,
                    "orden": orden,
                    "telegram_user_id": telegram_user_id,
                }
            )
        techs.sort(key=lambda x: (x.get("orden", 9999), x.get("nombre", "")))
        app.bot_data["tech_cache"] = techs
        app.bot_data["tech_cache_at"] = time.time()
        log.info(f"TECNICOS cache actualizado: {len(techs)} activos.")
    except Exception as e:
        log.warning(f"TECNICOS cache error: {e}")


async def refresh_config_jobs(context: ContextTypes.DEFAULT_TYPE) -> None:
    app = context.application
    if not app.bot_data.get("sheets_ready"):
        return

    now_ts = time.time()
    tech_at = app.bot_data.get("tech_cache_at", 0)

    if now_ts - tech_at >= TECH_CACHE_TTL_SEC:
        load_tecnicos_cache(app)


async def is_admin_of_chat(context: ContextTypes.DEFAULT_TYPE, chat_id: int, user_id: int) -> bool:
    try:
        admins = await context.bot.get_chat_administrators(chat_id)
        return any(a.user and a.user.id == user_id for a in admins)
    except Exception:
        return False


def mention_user_html(user_id: int, label: str = "Técnico") -> str:
    return f'<a href="tg://user?id={user_id}">{label}</a>'


# =========================
# Prompts
# =========================
def prompt_step3() -> str:
    return (
        "PASO 3 - INGRESA CÓDIGO DE ABONADO\n"
        "✅ Envía el código como texto (puede incluir letras, números o caracteres)."
    )


def prompt_step4() -> str:
    return (
        "PASO 4 - REPORTA TU UBICACIÓN\n"
        "📌 En grupos, Telegram no permite solicitar ubicación con botón.\n"
        "✅ Envía tu ubicación así:\n"
        "1) Pulsa el clip 📎\n"
        "2) Ubicación\n"
        "3) Enviar ubicación actual"
    )


def prompt_step5_install_mode() -> str:
    return (
        "PASO 5 - TIPO DE INSTALACIÓN\n"
        "Selecciona una opción:"
    )


def prompt_step6_package() -> str:
    return (
        "PASO 6 - TIPO DE PAQUETE\n"
        "Selecciona una opción:"
    )


def prompt_step7_tv_count() -> str:
    return (
        "PASO 7 - NUMERO DE TV\n"
        "Elegir la cantidad de TV a instalar:"
    )


def prompt_client_status() -> str:
    return (
        "ESTADO DE ATENCION\n"
        "Selecciona una opción:"
    )


def prompt_step_change_state_reason(change_label: str) -> str:
    return (
        "CAMBIO DE ESTADO\n"
        f"Opción: {change_label}\n\n"
        "✍️ Escribe el motivo en un solo mensaje."
    )


def prompt_validation_name() -> str:
    return "NOMBRE DE PERSONA QUE VALIDA:"


def prompt_validation_phone() -> str:
    return "NRO DE LA PERSONA QUE VALIDA:"


def prompt_validation_relationship() -> str:
    return "PARENTESCO DE LA PERSONA QUE VALIDA:"


def prompt_media_step(step_no: int) -> str:
    title = dynamic_step_name(step_no)
    desc = dynamic_step_desc(step_no)
    return (
        f"{title}\n"
        f"{desc}\n"
        f"📸 Carga entre 1 a {MAX_MEDIA_PER_STEP} fotos (solo se acepta fotos)."
    )


def prompt_auth_media_step(step_no: int) -> str:
    title = dynamic_step_name(step_no)
    return (
        f"Autorización multimedia para {title}\n"
        f"📎 Carga entre 1 a {MAX_MEDIA_PER_STEP} archivos.\n"
        f"✅ En este paso (PERMISO) se acepta FOTO o VIDEO."
    )


def prompt_route_location() -> str:
    return (
        "📍 Enviar ubicacion de inicio de ruta\n"
        "✅ Envía tu ubicación así:\n"
        "1) Pulsa el clip 📎\n"
        "2) Ubicación\n"
        "3) Enviar ubicación actual"
    )


def route_task_menu_text() -> str:
    return "LISTA DE TAREAS\nElige que deseas ejecutar"


def route_confirm_start_text(task_code: str) -> str:
    label = task_label_with_icon(task_code)
    return f"¿Seguro que desea Iniciar {label}?"


def route_confirm_finish_text(task_code: str) -> str:
    label = task_label_with_icon(task_code)
    return f"¿Seguro que desea Finalizar {label}?"


def route_task_started_text(task_code: str, started_at: str) -> str:
    label = task_label_with_icon(task_code)
    return (
        f"{label}\n"
        f"__________\n"
        f"Iniciado: {fmt_time_pe(started_at)}"
    )


def route_task_finished_text(task_code: str, started_at: str, finished_at: str) -> str:
    label = task_label_with_icon(task_code)
    secs = duration_seconds(started_at, finished_at)
    return (
        f"{label}\n"
        f"__________\n"
        f"Iniciado: {fmt_time_pe(started_at)}\n"
        f"Finalizado: {fmt_time_pe(finished_at)}\n"
        f"Tiempo Total: {human_duration_from_seconds(secs)}"
    )


def build_validation_summary(name: str, phone: str, relationship: str) -> str:
    return (
        "Plantilla de validacion\n"
        f"NOMBRE DE PERSONA QUE VALIDA: {name}\n"
        f"NRO DE LA PERSONA QUE VALIDA: {phone}\n"
        f"PARENTESCO DE LA PERSONA QUE VALIDA: {relationship}"
    )


# =========================
# Keyboards
# =========================
def kb_technicians_dynamic(app: Application) -> InlineKeyboardMarkup:
    techs = app.bot_data.get("tech_cache") or []
    rows: List[List[InlineKeyboardButton]] = []

    for t in techs:
        nombre = _safe_str(t.get("nombre"))
        if not nombre:
            continue
        rows.append([InlineKeyboardButton(nombre, callback_data=f"TECH|{nombre}")])

    return InlineKeyboardMarkup(rows)


def kb_services() -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(s, callback_data=f"SERV|{s}")] for s in SERVICE_TYPES]
    rows.append([InlineKeyboardButton("↩️ VOLVER AL MENU ANTERIOR", callback_data="BACK|ROUTE_MENU")])
    return InlineKeyboardMarkup(rows)


def kb_install_mode() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("INST EXTERNA", callback_data="MODE|EXTERNA"),
                InlineKeyboardButton("INST INTERNA", callback_data="MODE|INTERNA"),
            ],
            [InlineKeyboardButton("↩️ VOLVER AL MENU ANTERIOR", callback_data="BACK|CLIENT_STATUS_ROOT")],
        ]
    )


def kb_package_types() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("INTERNET", callback_data="PACK|INTERNET")],
            [InlineKeyboardButton("INTERNET + TV", callback_data="PACK|INTERNET + TV")],
            [InlineKeyboardButton("↩️ VOLVER AL MENU ANTERIOR", callback_data="BACK|INSTALL_MODE")],
        ]
    )


def kb_tv_count() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("1", callback_data="TVCOUNT|1"),
                InlineKeyboardButton("2", callback_data="TVCOUNT|2"),
                InlineKeyboardButton("3", callback_data="TVCOUNT|3"),
                InlineKeyboardButton("4", callback_data="TVCOUNT|4"),
                InlineKeyboardButton("5", callback_data="TVCOUNT|5"),
            ],
            [InlineKeyboardButton("↩️ VOLVER AL MENU ANTERIOR", callback_data="BACK|PACKAGE")],
        ]
    )


def kb_client_status() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("EN PROCESO", callback_data=f"CSTATUS|{CLIENT_STATUS_EN_PROCESO}")],
            [InlineKeyboardButton("AUSENTE", callback_data=f"CSTATUS|{CLIENT_STATUS_AUSENTE}")],
            [InlineKeyboardButton("REPROGRAMADO", callback_data=f"CSTATUS|{CLIENT_STATUS_REPROGRAMADO}")],
            [InlineKeyboardButton("CANCELADO", callback_data=f"CSTATUS|{CLIENT_STATUS_CANCELADO}")],
            [InlineKeyboardButton("↩️ VOLVER AL MENU ANTERIOR", callback_data="BACK|CLIENT_STATUS_ROOT")],
        ]
    )


def kb_evidence_menu(order_row: sqlite3.Row) -> InlineKeyboardMarkup:
    id_orden = int(order_row["id_orden"])
    items = get_order_step_items(order_row)
    req_num, req_label, req_step_no, _req_status = compute_next_required_step(order_row)

    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton("↩️ VOLVER AL MENU ANTERIOR", callback_data="BACK|CLIENT_STATUS")])

    for num, label, step_no in items:
        st = get_effective_step_status(id_orden, step_no)

        if st == STEP_STATE_APROBADO:
            prefix = "🟢"
        elif st == STEP_STATE_EN_REVISION:
            prefix = "🟡"
        elif st == STEP_STATE_RECHAZADO:
            prefix = "🔴"
        elif st == STEP_STATE_BLOQUEADO:
            prefix = "⛔"
        elif st == STEP_STATE_REABIERTO:
            prefix = "🟠"
        elif step_no == req_step_no:
            prefix = "➡️"
        else:
            prefix = "🔒"

        rows.append([InlineKeyboardButton(f"{prefix} {num}. {label}", callback_data=f"EVID|{id_orden}|{step_no}")])

    if not is_alt_client_status(order_row):
        rows.append([InlineKeyboardButton("────────────", callback_data="EVID_SEP|1")])
        rows.append([InlineKeyboardButton("CAMBIOS DE ESTADO", callback_data=f"CHANGE_MENU|{id_orden}")])

        if validation_is_ready(order_row):
            rows.append([InlineKeyboardButton("VALIDACION DE SERVICIO", callback_data=f"VALIDATE_SERVICE|{id_orden}")])

    return InlineKeyboardMarkup(rows)


def kb_action_menu(id_orden: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("SOLICITUD DE PERMISO", callback_data=f"ACT|{id_orden}|{step_no}|PERMISO"),
            InlineKeyboardButton("CARGAR FOTO", callback_data=f"ACT|{id_orden}|{step_no}|FOTO"),
        ]]
    )


def kb_auth_mode(id_orden: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("Solo texto", callback_data=f"AUTH_MODE|{id_orden}|{step_no}|TEXT"),
            InlineKeyboardButton("Multimedia", callback_data=f"AUTH_MODE|{id_orden}|{step_no}|MEDIA"),
        ]]
    )


def kb_auth_media_controls(id_orden: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("➕ CARGAR MAS", callback_data=f"AUTH_MORE|{id_orden}|{step_no}"),
            InlineKeyboardButton("✅ EVIDENCIAS COMPLETAS", callback_data=f"AUTH_DONE|{id_orden}|{step_no}"),
        ]]
    )


def kb_auth_review(id_orden: int, step_no: int, attempt: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ AUTORIZADO", callback_data=f"AUT_OK|{id_orden}|{step_no}|{attempt}"),
            InlineKeyboardButton("❌ RECHAZO", callback_data=f"AUT_BAD|{id_orden}|{step_no}|{attempt}"),
        ]]
    )


def kb_media_controls(id_orden: int, step_no: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("➕ CARGAR MAS", callback_data=f"MEDIA_MORE|{id_orden}|{step_no}"),
            InlineKeyboardButton("✅ EVIDENCIAS COMPLETAS", callback_data=f"MEDIA_DONE|{id_orden}|{step_no}"),
        ]]
    )


def kb_review_step(id_orden: int, step_no: int, attempt: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ CONFORME", callback_data=f"REV_OK|{id_orden}|{step_no}|{attempt}"),
            InlineKeyboardButton("❌ RECHAZO", callback_data=f"REV_BAD|{id_orden}|{step_no}|{attempt}"),
        ]]
    )


def kb_alt_final_review(id_orden: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("APROBADO", callback_data=f"ALT_FINAL_OK|{id_orden}"),
            InlineKeyboardButton("RECHAZADO", callback_data=f"ALT_FINAL_BAD|{id_orden}"),
        ]]
    )


def kb_reopen_menu(order_row: sqlite3.Row) -> InlineKeyboardMarkup:
    id_orden = int(order_row["id_orden"])
    items = get_order_step_items(order_row)
    rows: List[List[InlineKeyboardButton]] = []
    for num, label, step_no in items:
        st = get_effective_step_status(id_orden, step_no)
        if st == STEP_STATE_APROBADO:
            rows.append([InlineKeyboardButton(f"🔄 {num}. {label}", callback_data=f"REOPEN|{id_orden}|{step_no}")])
    rows.append([InlineKeyboardButton("❌ Cerrar", callback_data="REOPEN|CLOSE")])
    return InlineKeyboardMarkup(rows)


def kb_change_state_menu(id_orden: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    for code, label in CHANGE_STATE_OPTIONS:
        rows.append([InlineKeyboardButton(label, callback_data=f"CHSTATE|{id_orden}|{code}")])
    rows.append([InlineKeyboardButton("↩️ VOLVER", callback_data=f"CHSTATE_BACK|{id_orden}")])
    return InlineKeyboardMarkup(rows)


def kb_change_state_review(request_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅ APROBADO", callback_data=f"CHSTATE_OK|{request_id}"),
            InlineKeyboardButton("❌ RECHAZADO", callback_data=f"CHSTATE_BAD|{request_id}"),
        ]]
    )


def kb_validation_confirm(id_orden: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("CONFIRMAR", callback_data=f"VAL_CONFIRM|{id_orden}|YES"),
            InlineKeyboardButton("RECHAZAR", callback_data=f"VAL_CONFIRM|{id_orden}|NO"),
        ]]
    )


def kb_validation_review(validation_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("✅VALIDACION APROBADA", callback_data=f"VAL_REVIEW_OK|{validation_id}"),
            InlineKeyboardButton("❌VALIDACION RECHAZADA", callback_data=f"VAL_REVIEW_BAD|{validation_id}"),
        ]]
    )


def kb_route_tasks(id_ruta: int) -> InlineKeyboardMarkup:
    rows: List[List[InlineKeyboardButton]] = []
    rows.append([InlineKeyboardButton("Inicio de Orden", callback_data=f"ROUTE_TASK|{id_ruta}|{TASK_INICIO_ORDEN}")])
    rows.append([InlineKeyboardButton("🚗 En Camino", callback_data=f"ROUTE_TASK|{id_ruta}|{TASK_EN_CAMINO}")])
    rows.append([InlineKeyboardButton("📦 Recojo de Materiales", callback_data=f"ROUTE_TASK|{id_ruta}|{TASK_RECOJO_MATERIALES}")])
    rows.append([InlineKeyboardButton("🛠️ Mantenimiento Vehicular", callback_data=f"ROUTE_TASK|{id_ruta}|{TASK_MANTENIMIENTO_VEHICULAR}")])
    rows.append([InlineKeyboardButton("📚 Capacitacion", callback_data=f"ROUTE_TASK|{id_ruta}|{TASK_CAPACITACION}")])
    rows.append([InlineKeyboardButton("🍽️ Almuerzo", callback_data=f"ROUTE_TASK|{id_ruta}|{TASK_ALMUERZO}")])
    rows.append([InlineKeyboardButton("────────────", callback_data="ROUTE_SEP|1")])
    rows.append([InlineKeyboardButton("🛑 CERRAR RUTA", callback_data=f"ROUTE_TASK|{id_ruta}|{TASK_CERRAR_RUTA}")])
    rows.append([InlineKeyboardButton("────────────", callback_data="ROUTE_SEP|2")])
    return InlineKeyboardMarkup(rows)


def kb_route_confirm_start(id_ruta: int, task_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("SI", callback_data=f"ROUTE_CONFIRM_START|{id_ruta}|{task_code}|YES"),
            InlineKeyboardButton("NO", callback_data=f"ROUTE_CONFIRM_START|{id_ruta}|{task_code}|NO"),
        ]]
    )


def kb_route_confirm_finish(id_ruta: int, task_code: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[
            InlineKeyboardButton("SI", callback_data=f"ROUTE_CONFIRM_FINISH|{id_ruta}|{task_code}|YES"),
            InlineKeyboardButton("NO", callback_data=f"ROUTE_CONFIRM_FINISH|{id_ruta}|{task_code}|NO"),
        ]]
    )


def kb_route_finish_button(id_ruta: int, task_code: str) -> InlineKeyboardMarkup:
    label = task_label_with_icon(task_code)
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(f"Finalizar {label}", callback_data=f"ROUTE_FINISH_BTN|{id_ruta}|{task_code}")]]
    )


def kb_persona_atiende() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪪 TITULAR", callback_data="PERSONA_ATIENDE|TITULAR")],
        [InlineKeyboardButton("🪪 ENCARGADO", callback_data="PERSONA_ATIENDE|ENCARGADO")],
    ])


def kb_doc_review(id_orden: int, doc_tipo: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ APROBADO", callback_data=f"DOC_OK|{id_orden}|{doc_tipo}"),
            InlineKeyboardButton("❌ RECHAZADO", callback_data=f"DOC_BAD|{id_orden}|{doc_tipo}"),
        ]
    ])


# =========================
# Sheets writers
# =========================
def enqueue_ruta_row(id_ruta: int):
    route_row = get_route(id_ruta)
    if not route_row:
        return

    created_at = route_row["created_at"] or ""
    closed_at = route_row["closed_at"] or ""
    dur_min = duration_minutes(created_at, closed_at) if closed_at else None
    dur_txt = human_duration_from_minutes(dur_min) if dur_min is not None else ""

    maps = ""
    if route_row["route_location_lat"] is not None and route_row["route_location_lon"] is not None:
        maps = f"https://maps.google.com/?q={route_row['route_location_lat']},{route_row['route_location_lon']}"

    row = {
        "id_ruta": str(id_ruta),
        "tecnico_nombre": route_row["technician_name"] or "",
        "tecnico_user_id": str(route_row["technician_user_id"] or ""),
        "chat_id_origen": str(route_row["chat_id"] or ""),
        "fecha_inicio": fmt_date_pe(created_at) if created_at else "",
        "hora_inicio": fmt_time_pe(created_at) if created_at else "",
        "fecha_cierre": fmt_date_pe(closed_at) if closed_at else "",
        "hora_cierre": fmt_time_pe(closed_at) if closed_at else "",
        "duracion_ruta": dur_txt,
        "duracion_ruta_min": str(dur_min) if dur_min is not None else "",
        "hora_envio_selfie": fmt_time_pe(route_row["route_selfie_at"]) if route_row["route_selfie_at"] else "",
        "selfie_ruta_file_id": route_row["route_selfie_file_id"] or "",
        "hora_envio_ubi_ruta": fmt_time_pe(route_row["route_location_at"]) if route_row["route_location_at"] else "",
        "ubi_ruta_lat": str(route_row["route_location_lat"]) if route_row["route_location_lat"] is not None else "",
        "ubi_ruta_lon": str(route_row["route_location_lon"]) if route_row["route_location_lon"] is not None else "",
        "maps_ubi_ruta": maps,
        "estado_ruta": route_row["status"] or "",
        "version_bot": route_row["version_bot"] or BOT_VERSION,
        "registrado_en": fmt_sheet_datetime_pe(now_utc()),
    }
    dedupe_key = str(id_ruta)
    outbox_enqueue("RUTA", "UPSERT", dedupe_key, row)


def enqueue_tarea_ruta_row(id_ruta: int, session_row: sqlite3.Row, route_row: sqlite3.Row):
    started_at = session_row["started_at"] or ""
    finished_at = session_row["finished_at"] or ""
    dur_min = duration_minutes(started_at, finished_at) if finished_at else None
    dur_txt = human_duration_from_minutes(dur_min) if dur_min is not None else ""

    row = {
        "id_ruta": str(id_ruta),
        "tecnico_nombre": route_row["technician_name"] or "",
        "tecnico_user_id": str(route_row["technician_user_id"] or ""),
        "chat_id_origen": str(route_row["chat_id"] or ""),
        "tipo_tarea": session_row["task_code"] or "",
        "fecha_inicio_tarea": fmt_date_pe(started_at) if started_at else "",
        "hora_inicio_tarea": fmt_time_pe(started_at) if started_at else "",
        "fecha_fin_tarea": fmt_date_pe(finished_at) if finished_at else "",
        "hora_fin_tarea": fmt_time_pe(finished_at) if finished_at else "",
        "duracion_tarea": dur_txt,
        "duracion_tarea_min": str(dur_min) if dur_min is not None else "",
    }
    dedupe_key = f"{id_ruta}|{session_row['task_code']}|{started_at}"
    outbox_enqueue("TAREAS_RUTA", "UPSERT", dedupe_key, row)


def enqueue_orden_row(id_orden: int):
    order_row = get_order(id_orden)
    if not order_row:
        return

    created_at = order_row["created_at"] or ""
    finished_at = order_row["finished_at"] or ""
    dur_min = duration_minutes(created_at, finished_at) if finished_at else None
    dur_txt = human_duration_from_minutes(dur_min) if dur_min is not None else ""

    maps = ""
    if order_row["location_lat"] is not None and order_row["location_lon"] is not None:
        maps = f"https://maps.google.com/?q={order_row['location_lat']},{order_row['location_lon']}"

    increment_order_review_counts(id_orden)
    order_row = get_order(id_orden)

    row = {
        "id_orden": str(id_orden),
        "id_ruta": str(order_row["id_ruta"] or ""),
        "tecnico_nombre": order_row["technician_name"] or "",
        "tecnico_user_id": str(order_row["technician_user_id"] or ""),
        "chat_id_origen": str(order_row["chat_id"] or ""),
        "fecha_inicio_orden": fmt_date_pe(created_at) if created_at else "",
        "hora_inicio_orden": fmt_time_pe(created_at) if created_at else "",
        "fecha_fin_orden": fmt_date_pe(finished_at) if finished_at else "",
        "hora_fin_orden": fmt_time_pe(finished_at) if finished_at else "",
        "duracion_orden": dur_txt,
        "duracion_orden_min": str(dur_min) if dur_min is not None else "",
        "tipo_servicio": order_row["service_type"] or "",
        "cod_abonado": order_row["abonado_code"] or "",
        "ubi_orden_lat": str(order_row["location_lat"]) if order_row["location_lat"] is not None else "",
        "ubi_orden_lon": str(order_row["location_lon"]) if order_row["location_lon"] is not None else "",
        "maps_ubi_orden": maps,
        "tipo_instalacion": order_row["install_mode"] or "",
        "tipo_paquete": order_row["package_type"] or "",
        "numero_tv": str(order_row["tv_count"] or ""),
        "estado_atencion": order_row["client_status"] or "",
        "estado_orden": order_row["final_order_status"] or "",
        "evidencias_estado": order_row["evidencias_estado"] or "",
        "validacion_estado": order_row["validation_status"] or "",
        "nombre_validador": order_row["validation_name"] or "",
        "numero_validador": order_row["validation_phone"] or "",
        "parentesco_validador": order_row["validation_relationship"] or "",
        "veces_aprobado": str(order_row["approved_count"] or 0),
        "veces_rechazado": str(order_row["rejected_count"] or 0),
        "version_bot": order_row["version_bot"] or BOT_VERSION,
        "registrado_en": fmt_sheet_datetime_pe(now_utc()),
    }
    dedupe_key = str(id_orden)
    outbox_enqueue("ORDENES", "UPSERT", dedupe_key, row)


def enqueue_evidencia_paso_row(
    id_orden: int,
    step_no_sheet: int,
    attempt: int,
    estado_paso: str,
    reviewer_name: str,
    motivo: str,
    kind: str = "EVID",
    bloqueado: int = 0,
):
    order_row = get_order(id_orden)
    if not order_row:
        return

    reviewed_at = now_utc()
    dt = parse_iso(reviewed_at)
    fecha = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
    hora = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""

    db_step_no = -step_no_sheet if kind == "PERM" else step_no_sheet
    evid_name = dynamic_step_name(step_no_sheet)
    if kind == "PERM":
        evid_name = f"PERMISO - {evid_name}"

    row = {
        "id_orden": str(id_orden),
        "id_ruta": str(order_row["id_ruta"] or ""),
        "cod_abonado": order_row["abonado_code"] or "",
        "tecnico_nombre": order_row["technician_name"] or "",
        "tecnico_user_id": str(order_row["technician_user_id"] or ""),
        "chat_id_origen": str(order_row["chat_id"] or ""),
        "fecha_registro_paso": fecha,
        "hora_registro_paso": hora,
        "evidencias_numero": str(step_no_sheet),
        "evidencias_nombre": evid_name,
        "attempt": str(attempt),
        "estado_evidencias": estado_paso,
        "revisado_por": reviewer_name or "",
        "fecha_revision": fecha,
        "hora_revision": hora,
        "motivo_rechazo": motivo or "",
        "cantidad_fotos": str(media_count(id_orden, db_step_no, attempt)),
        "bloqueado": "1" if int(bloqueado or 0) == 1 else "0",
    }
    dedupe_key = f"{id_orden}|{step_no_sheet}|{attempt}|{kind}"
    outbox_enqueue("EVIDENCIAS_PASOS", "UPSERT", dedupe_key, row)


def enqueue_evidencia_archivo_row(order_row: sqlite3.Row, step_no_sheet: int, attempt: int, file_id: str, file_unique_id: str, tg_message_id: int, file_type: str, media_group_id: Optional[str]):
    created_at = now_utc()
    dt = parse_iso(created_at)
    fecha = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
    hora = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""

    row = {
        "id_orden": str(order_row["id_orden"]),
        "id_ruta": str(order_row["id_ruta"] or ""),
        "cod_abonado": order_row["abonado_code"] or "",
        "evidencias_numero": str(step_no_sheet),
        "attempt": str(attempt),
        "file_id": file_id,
        "file_unique_id": file_unique_id or "",
        "mensaje_telegram_id": str(tg_message_id),
        "fecha_carga": fecha,
        "hora_carga": hora,
        "tipo_archivo": file_type,
        "media_group_id": media_group_id or "",
    }
    dedupe_key = f"{order_row['id_orden']}|{step_no_sheet}|{attempt}|{tg_message_id}"
    outbox_enqueue("EVIDENCIAS_ARCHIVOS", "UPSERT", dedupe_key, row)


def enqueue_change_state_row(
    id_orden: int,
    change_type: str,
    change_label: str,
    reason: str,
    user_id: int,
    user_name: str,
    chat_id: int,
    approval_status: str = "PENDING",
    revisado_por: str = "",
    motivo_revision: str = "",
):
    order_row = get_order(id_orden)
    if not order_row:
        return

    dt_s = now_utc()
    dt = parse_iso(dt_s)
    fecha = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
    hora = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""

    row = {
        "id_orden": str(id_orden),
        "id_ruta": str(order_row["id_ruta"] or ""),
        "cod_abonado": order_row["abonado_code"] or "",
        "tecnico_nombre": order_row["technician_name"] or "",
        "tecnico_user_id": str(order_row["technician_user_id"] or ""),
        "chat_id_origen": str(chat_id),
        "tipo_cambio": change_type,
        "descripcion_cambio": change_label,
        "motivo": reason,
        "fecha_registro": fecha,
        "hora_registro": hora,
        "estado_solicitud": approval_status,
        "revisado_por": revisado_por or "",
        "fecha_revision": fecha if revisado_por else "",
        "hora_revision": hora if revisado_por else "",
        "motivo_revision": motivo_revision or "",
    }
    dedupe_key = f"{id_orden}|{change_type}|{fecha}|{hora}|{user_id}"
    outbox_enqueue("CAMBIO_ESTADO", "UPSERT", dedupe_key, row)


def enqueue_validacion_row(
    id_orden: int,
    nombre_validador: str,
    numero_validador: str,
    parentesco_validador: str,
    estado_validacion: str,
    revisado_por: str = "",
    motivo_revision: str = "",
):
    order_row = get_order(id_orden)
    if not order_row:
        return

    dt_s = now_utc()
    dt = parse_iso(dt_s)
    fecha = dt.astimezone(PERU_TZ).strftime("%Y-%m-%d") if dt else ""
    hora = dt.astimezone(PERU_TZ).strftime("%H:%M") if dt else ""

    row = {
        "id_orden": str(id_orden),
        "id_ruta": str(order_row["id_ruta"] or ""),
        "cod_abonado": order_row["abonado_code"] or "",
        "tecnico_nombre": order_row["technician_name"] or "",
        "tecnico_user_id": str(order_row["technician_user_id"] or ""),
        "chat_id_origen": str(order_row["chat_id"] or ""),
        "nombre_validador": nombre_validador or "",
        "numero_validador": numero_validador or "",
        "parentesco_validador": parentesco_validador or "",
        "estado_validacion": estado_validacion,
        "fecha_registro": fecha,
        "hora_registro": hora,
        "revisado_por": revisado_por or "",
        "fecha_revision": fecha if revisado_por else "",
        "hora_revision": hora if revisado_por else "",
        "motivo_revision": motivo_revision or "",
    }
    dedupe_key = f"{id_orden}|VALIDACION|{estado_validacion}|{fecha}|{hora}"
    outbox_enqueue("VALIDACIONES", "UPSERT", dedupe_key, row)


# =========================
# UI helpers
# =========================
async def send_order_status_summary(chat_id: int, context: ContextTypes.DEFAULT_TYPE, route_row: sqlite3.Row, order_row: Optional[sqlite3.Row]):
    approval_required = get_approval_required(chat_id)
    approval_txt = "ON ✅" if approval_required else "OFF ⚠️ (auto)"
    route_status = route_row["status"] or "-"
    route_ok = "SI" if route_is_fully_started(route_row) else "NO"

    open_task = any_route_timed_task_open(int(route_row["id_ruta"]))
    open_task_txt = "-"
    if open_task:
        open_task_txt = f"{open_task['task_label']} desde {fmt_time_pe(open_task['started_at'])}"

    if order_row:
        items = get_order_step_items(order_row)
        step_actual_txt = "-"
        if items:
            _, label, step_no, state = compute_next_required_step(order_row)
            step_actual_txt = f"{label} ({step_no}) - {state}"
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "📌 ESTADO ACTUAL\n"
                f"• Ruta ID: {route_row['id_ruta']}\n"
                f"• Aprobación: {approval_txt}\n"
                f"• Ruta estado: {route_status}\n"
                f"• Ruta iniciada correctamente: {route_ok}\n"
                f"• Inicio ruta: {route_started_at_text(route_row)}\n"
                f"• Eventos ruta: {route_event_count(int(route_row['id_ruta']))}\n"
                f"• Tarea temporizada abierta: {open_task_txt}\n"
                f"• Orden ID: {order_row['id_orden']}\n"
                f"• Phase: {order_row['phase']}\n"
                f"• Paso actual: {step_actual_txt}\n"
                f"• Técnico: {order_row['technician_name'] or '(pendiente)'}\n"
                f"• Servicio: {order_row['service_type'] or '(pendiente)'}\n"
                f"• Abonado: {order_row['abonado_code'] or '(pendiente)'}\n"
                f"• Evidencias estado: {order_row['evidencias_estado'] or '(pendiente)'}\n"
                f"• Instalación: {order_row['install_mode'] or '(pendiente)'}\n"
                f"• Paquete: {order_row['package_type'] or '(pendiente)'}\n"
                f"• TVs: {order_row['tv_count'] or '(pendiente)'}\n"
                f"• Estado atención: {order_row['client_status'] or '(pendiente)'}\n"
                f"• Estado orden: {order_row['final_order_status'] or '-'}\n"
                f"• Validación estado: {order_row['validation_status'] or '-'}\n"
                f"• Validador: {order_row['validation_name'] or '-'}"
            ),
        )
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text=(
            "📌 ESTADO ACTUAL\n"
            f"• Ruta ID: {route_row['id_ruta']}\n"
            f"• Aprobación: {approval_txt}\n"
            f"• Ruta estado: {route_status}\n"
            f"• Ruta iniciada correctamente: {route_ok}\n"
            f"• Inicio ruta: {route_started_at_text(route_row)}\n"
            f"• Eventos ruta: {route_event_count(int(route_row['id_ruta']))}\n"
            f"• Tarea temporizada abierta: {open_task_txt}\n"
            "• Orden activa: ninguna"
        ),
    )


async def show_package_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=chat_id, text=prompt_step6_package(), reply_markup=kb_package_types())


async def show_tv_count_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=chat_id, text=prompt_step7_tv_count(), reply_markup=kb_tv_count())


async def show_client_status_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE):
    await context.bot.send_message(chat_id=chat_id, text=prompt_client_status(), reply_markup=kb_client_status())


async def show_validation_button(chat_id: int, context: ContextTypes.DEFAULT_TYPE, order_row: sqlite3.Row):
    await context.bot.send_message(
        chat_id=chat_id,
        text="✅ Se completaron todas las evidencias. Ya puedes validar tu servicio",
        reply_markup=InlineKeyboardMarkup(
            [[InlineKeyboardButton("VALIDACION DE SERVICIO", callback_data=f"VALIDATE_SERVICE|{int(order_row['id_orden'])}")]]
        ),
    )


async def show_evidence_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE, order_row: sqlite3.Row):
    sync_order_progress(int(order_row["id_orden"]))
    order_row = get_order(int(order_row["id_orden"]))

    items = get_order_step_items(order_row)
    if not items:
        await context.bot.send_message(chat_id=chat_id, text="⚠️ No se pudo construir el flujo de evidencias.")
        return

    await context.bot.send_message(
        chat_id=chat_id,
        text="📌 Selecciona la evidencia a cargar:",
        reply_markup=kb_evidence_menu(order_row),
    )


async def show_route_tasks_menu(chat_id: int, context: ContextTypes.DEFAULT_TYPE, route_row: sqlite3.Row):
    await context.bot.send_message(chat_id=chat_id, text=route_task_menu_text(), reply_markup=kb_route_tasks(int(route_row["id_ruta"])))


# =========================
# Commands
# =========================
async def start_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    await context.bot.send_message(
        chat_id=msg.chat_id,
        text=(
            "Comandos:\n"
            "• /inicio  → iniciar ruta\n"
            "• /estado  → ver estado\n"
            "• /cancelar → cancelar orden activa o ruta si no hay orden\n"
            "• /id → ver chat_id del grupo\n"
            "• /aprobacion on|off → activar/desactivar validaciones (solo admins)\n"
            "• /reabrir → menú de reapertura (solo admins)\n"
            "• /reload_sheet → recargar técnicos desde Google Sheets\n"
        ),
    )


async def id_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if not msg:
        return
    title = msg.chat.title if msg.chat else "-"
    await context.bot.send_message(chat_id=msg.chat_id, text=f"Chat ID: {msg.chat_id}\nTitle: {title}")


async def reload_sheet_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo Administradores del grupo pueden usar /reload_sheet.")
        return

    if not context.application.bot_data.get("sheets_ready"):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Google Sheets no está disponible en este momento.")
        return

    try:
        load_tecnicos_cache(context.application)
        techs = context.application.bot_data.get("tech_cache") or []
        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=f"✅ Hoja TECNICOS recargada correctamente.\nTécnicos activos cargados: {len(techs)}"
        )
    except Exception as e:
        await context.bot.send_message(chat_id=msg.chat_id, text=f"⚠️ No se pudo recargar la hoja TECNICOS: {e}")


async def inicio_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    chat_id = msg.chat_id
    user_id = msg.from_user.id
    username = msg.from_user.username or msg.from_user.full_name

    existing_route = maybe_release_expired_route_lock(get_open_route(chat_id))
    existing_order = get_open_order(chat_id)

    if existing_route and existing_order:
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ Ya existe una ruta activa con una orden en proceso. Termínala o cancélala antes de iniciar otra.",
        )
        return

    if existing_route and not existing_order and route_is_fully_started(existing_route):
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ Ya existe una ruta activa. Usa el menú de tareas para continuar.",
        )
        await show_route_tasks_menu(chat_id, context, existing_route)
        return

    route_row = create_or_reset_route(chat_id, user_id, username)

    approval_required = get_approval_required(chat_id)
    extra = "✅ Aprobación: ON (requiere admin)" if approval_required else "⚠️ Aprobación: OFF (auto-aprobación)"

    app = context.application
    load_tecnicos_cache(app)

    tech_cache = app.bot_data.get("tech_cache") or []
    if not tech_cache:
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "⚠️ No hay técnicos activos configurados en la hoja TECNICOS.\n"
                "Admin: agrega técnicos activos en Google Sheets y vuelve a intentar."
            ),
        )
        return

    enqueue_ruta_row(int(route_row["id_ruta"]))
    await context.bot.send_message(chat_id=chat_id, text=f"✅ Ruta iniciada.\n{extra}")
    await context.bot.send_message(chat_id=chat_id, text="PASO 1 - NOMBRE DEL TECNICO", reply_markup=kb_technicians_dynamic(app))


async def cancelar_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    route_row, order_row = current_route_and_order(msg.chat_id)
    if not route_row:
        await context.bot.send_message(chat_id=msg.chat_id, text="No hay una ruta abierta en este grupo.")
        return

    if order_row:
        update_order(
            int(order_row["id_orden"]),
            status=ORDER_STATUS_CANCELLED,
            phase=PHASE_ORDER_CANCELLED,
            finished_at=now_utc(),
            current_step_no=None,
            pending_step_no=None,
            admin_pending=0,
            final_order_status="CANCELADA",
            evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
        )
        enqueue_orden_row(int(order_row["id_orden"]))
        await context.bot.send_message(chat_id=msg.chat_id, text="🧾 Orden cancelada. La ruta sigue activa.")
        await show_route_tasks_menu(msg.chat_id, context, route_row)
        return

    open_task = get_any_open_route_task_session(int(route_row["id_ruta"]))
    if open_task:
        cancel_route_task_session(int(open_task["session_id"]))

    update_route(
        int(route_row["id_ruta"]),
        status=ROUTE_STATUS_CLOSED,
        phase=PHASE_ROUTE_CLOSED,
        closed_at=now_utc(),
        route_menu_enabled=0,
    )
    clear_route_lock(int(route_row["id_ruta"]))
    enqueue_ruta_row(int(route_row["id_ruta"]))
    await context.bot.send_message(chat_id=msg.chat_id, text="🧾 Ruta cancelada. Puedes iniciar otra con /inicio.")


async def estado_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None:
        return

    route_row, order_row = current_route_and_order(msg.chat_id)
    if not route_row:
        await context.bot.send_message(chat_id=msg.chat_id, text="No hay una ruta abierta. Usa /inicio.")
        return

    await send_order_status_summary(msg.chat_id, context, route_row, order_row)


async def aprobacion_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo Administradores del grupo pueden usar /aprobacion on|off.")
        return

    args = context.args or []
    if not args:
        state = "ON ✅" if get_approval_required(msg.chat_id) else "OFF ⚠️ (auto)"
        await context.bot.send_message(chat_id=msg.chat_id, text=f"Estado de aprobación: {state}")
        return

    val = args[0].strip().lower()
    if val in ("on", "1", "true", "si", "sí", "activar"):
        set_approval_required(msg.chat_id, True)
        await context.bot.send_message(chat_id=msg.chat_id, text="✅ Aprobación ENCENDIDA. Se requiere validación de admins.")
    elif val in ("off", "0", "false", "no", "desactivar"):
        set_approval_required(msg.chat_id, False)
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Aprobación APAGADA. Los pasos se auto-aprobarán (APROBACION OFF).")
    else:
        await context.bot.send_message(chat_id=msg.chat_id, text="Uso: /aprobacion on  o  /aprobacion off")


async def reabrir_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    if msg is None or msg.from_user is None:
        return

    if not await is_admin_of_chat(context, msg.chat_id, msg.from_user.id):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Solo Administradores del grupo pueden usar /reabrir.")
        return

    route_row, order_row = current_route_and_order(msg.chat_id)
    if not route_row or not order_row:
        await context.bot.send_message(chat_id=msg.chat_id, text="No hay una orden abierta en este grupo.")
        return

    items = get_order_step_items(order_row)
    if not items:
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ La orden aún no tiene flujo de evidencias definido.")
        return

    await context.bot.send_message(
        chat_id=msg.chat_id,
        text="🔄 Selecciona el paso aprobado que deseas reabrir:",
        reply_markup=kb_reopen_menu(order_row),
    )


# =========================
# Workers
# =========================
async def sheets_worker(context: ContextTypes.DEFAULT_TYPE):
    if not context.application.bot_data.get("sheets_ready"):
        return

    ws_ruta = context.application.bot_data["ws_ruta"]
    ws_tareas_ruta = context.application.bot_data["ws_tareas_ruta"]
    ws_ordenes = context.application.bot_data["ws_ordenes"]
    ws_evid_pasos = context.application.bot_data["ws_evid_pasos"]
    ws_evid_arch = context.application.bot_data["ws_evid_arch"]
    ws_change = context.application.bot_data["ws_change"]
    ws_valid = context.application.bot_data["ws_valid"]

    idx_ruta = context.application.bot_data["idx_ruta"]
    idx_tareas_ruta = context.application.bot_data["idx_tareas_ruta"]
    idx_ordenes = context.application.bot_data["idx_ordenes"]
    idx_evid_pasos = context.application.bot_data["idx_evid_pasos"]
    idx_evid_arch = context.application.bot_data["idx_evid_arch"]
    idx_change = context.application.bot_data["idx_change"]
    idx_valid = context.application.bot_data["idx_valid"]

    batch = outbox_fetch_batch(limit=20)
    if not batch:
        return

    for item in batch:
        outbox_id = int(item["outbox_id"])
        sheet_name = item["sheet_name"]
        dedupe_key = item["dedupe_key"]
        attempts = int(item["attempts"]) + 1
        row_json = item["row_json"]

        try:
            row = json.loads(row_json)
            if sheet_name == "RUTA":
                sheet_upsert(ws_ruta, idx_ruta, dedupe_key, row, RUTA_COLUMNS, ["id_ruta"])
            elif sheet_name == "TAREAS_RUTA":
                sheet_upsert(
                    ws_tareas_ruta,
                    idx_tareas_ruta,
                    dedupe_key,
                    row,
                    TAREAS_RUTA_COLUMNS,
                    ["id_ruta", "tipo_tarea", "fecha_inicio_tarea", "hora_inicio_tarea"],
                )
            elif sheet_name == "ORDENES":
                sheet_upsert(ws_ordenes, idx_ordenes, dedupe_key, row, ORDENES_COLUMNS, ["id_orden"])
            elif sheet_name == "EVIDENCIAS_PASOS":
                sheet_upsert(
                    ws_evid_pasos,
                    idx_evid_pasos,
                    dedupe_key,
                    row,
                    EVIDENCIAS_PASOS_COLUMNS,
                    ["id_orden", "evidencias_numero", "attempt"],
                )
            elif sheet_name == "EVIDENCIAS_ARCHIVOS":
                sheet_upsert(
                    ws_evid_arch,
                    idx_evid_arch,
                    dedupe_key,
                    row,
                    EVIDENCIAS_ARCHIVOS_COLUMNS,
                    ["id_orden", "evidencias_numero", "attempt", "mensaje_telegram_id"],
                )
            elif sheet_name == "CAMBIO_ESTADO":
                sheet_upsert(
                    ws_change,
                    idx_change,
                    dedupe_key,
                    row,
                    CAMBIO_ESTADO_COLUMNS,
                    ["id_orden", "tipo_cambio", "fecha_registro", "hora_registro"],
                )
            elif sheet_name == "VALIDACIONES":
                sheet_upsert(
                    ws_valid,
                    idx_valid,
                    dedupe_key,
                    row,
                    VALIDACIONES_COLUMNS,
                    ["id_orden", "estado_validacion", "fecha_registro", "hora_registro"],
                )
            else:
                raise RuntimeError(f"Hoja desconocida: {sheet_name}")
            outbox_mark_sent(outbox_id)
        except Exception as e:
            err = str(e)
            dead = _is_permanent_sheet_error(err) or attempts >= 8
            outbox_mark_failed(outbox_id, attempts, err, dead=dead)
            log.warning(f"Sheets worker error outbox_id={outbox_id} sheet={sheet_name} attempts={attempts}: {err}")
            await asyncio.sleep(0.2)


# =========================
# Callbacks
# =========================
async def on_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE):
    q = update.callback_query
    if q is None or q.message is None or q.from_user is None:
        return

    chat_id = q.message.chat_id
    user_id = q.from_user.id
    user_name = q.from_user.full_name
    data = (q.data or "").strip()

    log.info(f"CALLBACK data={data} chat_id={chat_id} user_id={user_id}")

    if data.startswith("ROUTE_SEP|") or data.startswith("EVID_SEP|"):
        await safe_q_answer(q, " ", show_alert=False)
        return

    if data == "REOPEN|CLOSE":
        await safe_q_answer(q, "Cerrado", show_alert=False)
        await safe_edit_message_text(q, "✅ Menú de reapertura cerrado.")
        return

    if data.startswith("BACK|"):
        target = data.split("|", 1)[1]
        route_row, order_row = current_route_and_order(chat_id)
        if not route_row:
            await safe_q_answer(q, "No hay una ruta abierta.", show_alert=True)
            return

        if target == "ROUTE_MENU":
            if not order_row:
                await safe_q_answer(q, "Volviendo…", show_alert=False)
                await safe_edit_message_text(q, "✅ Volviendo al menú de tareas.")
                await show_route_tasks_menu(chat_id, context, route_row)
                return

            update_order(
                int(order_row["id_orden"]),
                phase=PHASE_ROUTE_MENU,
                step_index=0,
                service_type=None,
                abonado_code=None,
                evidencias_estado=None,
                install_mode=None,
                package_type=None,
                tv_count=None,
                client_status=None,
                location_lat=None,
                location_lon=None,
                location_at=None,
                pending_step_no=None,
                current_step_no=None,
                admin_pending=0,
            )
            clear_route_lock(int(route_row["id_ruta"]))
            await safe_q_answer(q, "Volviendo…", show_alert=False)
            await safe_edit_message_text(q, "✅ Volviendo al menú de tareas.")
            await show_route_tasks_menu(chat_id, context, route_row)
            return

        if not order_row:
            await safe_q_answer(q, "No hay una orden abierta.", show_alert=True)
            return

        if target == "INSTALL_MODE":
            update_order(int(order_row["id_orden"]), phase=PHASE_WAIT_INSTALL_MODE)
            await safe_q_answer(q, "Volviendo…", show_alert=False)
            await safe_edit_message_text(q, "✅ Volviendo al paso anterior.")
            await context.bot.send_message(chat_id=chat_id, text=prompt_step5_install_mode(), reply_markup=kb_install_mode())
            return

        if target == "PACKAGE":
            update_order(int(order_row["id_orden"]), phase=PHASE_WAIT_PACKAGE, tv_count=None)
            await safe_q_answer(q, "Volviendo…", show_alert=False)
            await safe_edit_message_text(q, "✅ Volviendo al paso anterior.")
            await show_package_menu(chat_id, context)
            return

        if target == "CLIENT_STATUS_ROOT":
            if (order_row["package_type"] or "").strip() == "INTERNET + TV":
                update_order(int(order_row["id_orden"]), phase=PHASE_WAIT_TV_COUNT, client_status=None)
                await safe_q_answer(q, "Volviendo…", show_alert=False)
                await safe_edit_message_text(q, "✅ Volviendo al paso anterior.")
                await show_tv_count_menu(chat_id, context)
            elif (order_row["install_mode"] or "").strip():
                update_order(int(order_row["id_orden"]), phase=PHASE_WAIT_PACKAGE, client_status=None)
                await safe_q_answer(q, "Volviendo…", show_alert=False)
                await safe_edit_message_text(q, "✅ Volviendo al paso anterior.")
                await show_package_menu(chat_id, context)
            else:
                update_order(int(order_row["id_orden"]), phase=PHASE_WAIT_INSTALL_MODE, client_status=None)
                await safe_q_answer(q, "Volviendo…", show_alert=False)
                await safe_edit_message_text(q, "✅ Volviendo al paso anterior.")
                await context.bot.send_message(chat_id=chat_id, text=prompt_step5_install_mode(), reply_markup=kb_install_mode())
            return

        if target == "CLIENT_STATUS":
            update_order(int(order_row["id_orden"]), phase=PHASE_WAIT_CLIENT_STATUS, pending_step_no=None, current_step_no=None, admin_pending=0)
            clear_route_lock(int(route_row["id_ruta"]))
            await safe_q_answer(q, "Volviendo…", show_alert=False)
            await safe_edit_message_text(q, "✅ Volviendo al menú anterior.")
            await show_client_status_menu(chat_id, context)
            return

    if data.startswith("REOPEN|"):
        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "⚠️ Solo administradores.", show_alert=True)
            return
        try:
            _, id_orden_s, step_no_s = data.split("|", 2)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido.", show_alert=True)
            return

        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden no válida o cerrada.", show_alert=True)
            return

        set_pending_input(
            chat_id=chat_id,
            user_id=user_id,
            kind="REOPEN_REASON",
            id_ruta=int(order_row["id_ruta"]),
            id_orden=id_orden,
            step_no=step_no,
            attempt=0,
            reply_to_message_id=q.message.message_id,
            tech_user_id=None,
        )
        await safe_q_answer(q, "Escribe el motivo", show_alert=False)
        await context.bot.send_message(chat_id=chat_id, text=f"🔄 Reapertura de paso - {step_name(step_no)}\n✍️ Admin: escribe el motivo de reapertura (un solo mensaje).")
        return

    if data.startswith("CHSTATE_BACK|"):
        try:
            _, id_orden_s = data.split("|", 1)
            id_orden = int(id_orden_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido.", show_alert=True)
            return

        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden inválida.", show_alert=True)
            return

        await safe_q_answer(q, "Volviendo…", show_alert=False)
        await safe_edit_message_text(q, "✅ Volviendo al menú de evidencias.")
        await show_evidence_menu(chat_id, context, order_row)
        return

    if data.startswith("CHANGE_MENU|"):
        try:
            _, id_orden_s = data.split("|", 1)
            id_orden = int(id_orden_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido.", show_alert=True)
            return

        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden inválida.", show_alert=True)
            return

        update_order(id_orden, phase=PHASE_CHANGE_STATE_MENU)
        await safe_q_answer(q, "Menú de cambios de estado", show_alert=False)
        await safe_edit_message_text(q, "CAMBIOS DE ESTADO\nSelecciona una opción:")
        await context.bot.send_message(chat_id=chat_id, text="CAMBIOS DE ESTADO\nSelecciona una opción:", reply_markup=kb_change_state_menu(id_orden))
        return

    if data.startswith("CHSTATE|"):
        try:
            _, id_orden_s, change_code = data.split("|", 2)
            id_orden = int(id_orden_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido.", show_alert=True)
            return

        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden inválida.", show_alert=True)
            return

        change_label = next((label for code, label in CHANGE_STATE_OPTIONS if code == change_code), change_code)
        update_order(id_orden, phase=PHASE_CHANGE_STATE_REASON, change_state_type=change_code)
        set_pending_input(
            chat_id=chat_id,
            user_id=user_id,
            kind="CHANGE_STATE_REASON",
            id_ruta=int(order_row["id_ruta"]),
            id_orden=id_orden,
            step_no=0,
            attempt=0,
            reply_to_message_id=q.message.message_id,
            tech_user_id=None,
        )
        await safe_q_answer(q, "Escribe el motivo", show_alert=False)
        await safe_edit_message_text(q, f"✅ Opción seleccionada: {change_label}")
        await context.bot.send_message(chat_id=chat_id, text=prompt_step_change_state_reason(change_label))
        return

    if data.startswith("CHSTATE_OK|") or data.startswith("CHSTATE_BAD|"):
        try:
            action, request_id_s = data.split("|", 1)
            request_id = int(request_id_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido.", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "Solo Administradores del grupo pueden validar", show_alert=True)
            return

        req = get_change_state_request(request_id)
        if not req:
            await safe_q_answer(q, "No encontré la solicitud.", show_alert=True)
            return
        if (req["approval_status"] or "") != "PENDING":
            await safe_q_answer(q, "Esta solicitud ya fue revisada.", show_alert=True)
            return

        id_orden = int(req["id_orden"])
        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden inválida o cerrada.", show_alert=True)
            return

        admin_name = q.from_user.full_name
        now_dt = now_utc()

        if action == "CHSTATE_OK":
            update_change_state_request(
                request_id,
                approval_status="APPROVED",
                reviewed_by=user_id,
                reviewed_by_name=admin_name,
                reviewed_at=now_dt,
                review_reason="",
                tg_message_id=q.message.message_id,
            )

            update_order(
                id_orden,
                change_state_type=req["change_type"],
                change_state_reason=req["reason"],
            )

            close_order(
                id_orden,
                final_status=req["change_type"],
                phase=PHASE_ORDER_CLOSED,
                evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
            )

            enqueue_change_state_row(
                id_orden,
                req["change_type"],
                req["change_label"],
                req["reason"],
                int(req["user_id"] or 0),
                req["user_name"] or "",
                int(req["chat_id"] or chat_id),
                approval_status="APPROVED",
                revisado_por=admin_name,
                motivo_revision="",
            )
            enqueue_orden_row(id_orden)

            await safe_q_answer(q, "✅ Aprobado", show_alert=False)
            await safe_edit_message_text(q, f"✅ Cambio de estado aprobado.\n• Tipo: {req['change_label']}\n• Motivo: {req['reason']}")
            route_row = get_route(int(req["id_ruta"]))
            if route_row:
                await show_route_tasks_menu(chat_id, context, route_row)
            return

        update_change_state_request(
            request_id,
            approval_status="REJECTED",
            reviewed_by=user_id,
            reviewed_by_name=admin_name,
            reviewed_at=now_dt,
            review_reason="Solicitud rechazada por admin",
            tg_message_id=q.message.message_id,
        )
        update_order(
            id_orden,
            admin_pending=0,
            phase=PHASE_MENU_EVID,
            pending_step_no=None,
            current_step_no=None,
        )
        enqueue_change_state_row(
            id_orden,
            req["change_type"],
            req["change_label"],
            req["reason"],
            int(req["user_id"] or 0),
            req["user_name"] or "",
            int(req["chat_id"] or chat_id),
            approval_status="REJECTED",
            revisado_por=admin_name,
            motivo_revision="Solicitud rechazada por admin",
        )

        await safe_q_answer(q, "❌ Rechazado", show_alert=False)
        await safe_edit_message_text(q, f"❌ Cambio de estado rechazado.\n• Tipo: {req['change_label']}")
        await show_evidence_menu(chat_id, context, get_order(id_orden))
        return

    if data.startswith("VALIDATE_SERVICE|"):
        try:
            _, id_orden_s = data.split("|", 1)
            id_orden = int(id_orden_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden inválida.", show_alert=True)
            return
        if not validation_is_ready(order_row):
            await safe_q_answer(q, "Aún no están todas las evidencias aprobadas.", show_alert=True)
            return

        upsert_service_validation_draft(id_orden, int(order_row["id_ruta"]), user_id, user_name, "", "", "")
        sync_order_validation_fields(id_orden)
        update_order(id_orden, phase=PHASE_VALIDATION_NAME)
        await safe_q_answer(q, "Iniciando validación…", show_alert=False)
        await safe_edit_message_text(q, "✅ Validación de servicio iniciada.")
        await context.bot.send_message(chat_id=chat_id, text=prompt_validation_name())
        return

    if data.startswith("VAL_CONFIRM|"):
        try:
            _, id_orden_s, answer = data.split("|", 2)
            id_orden = int(id_orden_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden inválida.", show_alert=True)
            return

        val = get_latest_service_validation(id_orden)
        if not val:
            await safe_q_answer(q, "No encontré la validación.", show_alert=True)
            return

        if answer == "NO":
            update_order(id_orden, phase=PHASE_VALIDATION_NAME)
            await safe_q_answer(q, "Rechazado por técnico", show_alert=False)
            await safe_edit_message_text(q, "❌ Validación cancelada por el técnico. Vuelve a ingresar los datos.")
            await context.bot.send_message(chat_id=chat_id, text=prompt_validation_name())
            return

        update_service_validation(
            int(val["validation_id"]),
            status=VALIDATION_STATUS_PENDING,
            submitted_at=now_utc(),
            confirm_message_id=q.message.message_id,
        )
        sync_order_validation_fields(id_orden)
        update_order(
            id_orden,
            phase=PHASE_VALIDATION_REVIEW,
            admin_pending=1,
        )
        enqueue_validacion_row(
            id_orden,
            val["validator_name"] or "",
            val["validator_phone"] or "",
            val["validator_relationship"] or "",
            VALIDATION_STATUS_PENDING,
        )

        await safe_q_answer(q, "Enviado a revisión", show_alert=False)
        await context.bot.send_message(
            chat_id=chat_id,
            text="En proceso de validacion, esperando confirmacion del validador",
            reply_markup=kb_validation_review(int(val["validation_id"]))
        )
        return

    if data.startswith("VAL_REVIEW_OK|") or data.startswith("VAL_REVIEW_BAD|"):
        try:
            action, validation_id_s = data.split("|", 1)
            validation_id = int(validation_id_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "Solo Administradores del grupo pueden validar", show_alert=True)
            return

        with db() as conn:
            val = conn.execute("SELECT * FROM service_validation WHERE validation_id=?", (validation_id,)).fetchone()

        if not val:
            await safe_q_answer(q, "No encontré la validación.", show_alert=True)
            return
        if (val["status"] or "") not in (VALIDATION_STATUS_PENDING,):
            await safe_q_answer(q, "Esta validación ya fue revisada.", show_alert=True)
            return

        id_orden = int(val["id_orden"])
        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden inválida o cerrada.", show_alert=True)
            return

        admin_name = q.from_user.full_name
        now_dt = now_utc()

        if action == "VAL_REVIEW_OK":
            update_service_validation(
                validation_id,
                status=VALIDATION_STATUS_APPROVED,
                reviewed_by=user_id,
                reviewed_by_name=admin_name,
                reviewed_at=now_dt,
                review_reason="",
                review_message_id=q.message.message_id,
            )
            sync_order_validation_fields(id_orden)
            close_order(
                id_orden,
                final_status=order_row["client_status"] or "EN_PROCESO",
                phase=PHASE_ORDER_CLOSED,
                evidencias_estado=EVIDENCIAS_ESTADO_COMPLETAS,
            )
            enqueue_validacion_row(
                id_orden,
                val["validator_name"] or "",
                val["validator_phone"] or "",
                val["validator_relationship"] or "",
                VALIDATION_STATUS_APPROVED,
                revisado_por=admin_name,
                motivo_revision="",
            )
            enqueue_orden_row(id_orden)

            await safe_q_answer(q, "✅ Validación aprobada", show_alert=False)
            await safe_edit_message_text(q, "✅ Validación aprobada")

            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "✅CASO COMPLETADO\n"
                    f"• Codigo: {order_row['abonado_code'] or '-'}\n"
                    "• Evidencias: Completas\n"
                    "• Validacion: Correcta"
                ),
            )
            route_row = get_route(int(order_row["id_ruta"]))
            if route_row:
                await show_route_tasks_menu(chat_id, context, route_row)
            return

        update_service_validation(
            validation_id,
            status=VALIDATION_STATUS_REJECTED,
            reviewed_by=user_id,
            reviewed_by_name=admin_name,
            reviewed_at=now_dt,
            review_reason="cliente no da conformidad",
            review_message_id=q.message.message_id,
        )
        sync_order_validation_fields(id_orden)
        update_order(
            id_orden,
            phase=PHASE_MENU_EVID,
            admin_pending=0,
            evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
        )
        enqueue_validacion_row(
            id_orden,
            val["validator_name"] or "",
            val["validator_phone"] or "",
            val["validator_relationship"] or "",
            VALIDATION_STATUS_REJECTED,
            revisado_por=admin_name,
            motivo_revision="cliente no da conformidad",
        )
        enqueue_orden_row(id_orden)

        await safe_q_answer(q, "❌ Validación rechazada", show_alert=False)
        await safe_edit_message_text(q, "❌VALIDACION RECHAZADA cliente no da conformidad")
        await show_validation_button(chat_id, context, get_order(id_orden))
        return

    if data.startswith("ROUTE_CONFIRM_START|"):
        try:
            _, id_ruta_s, task_code, answer = data.split("|", 3)
            id_ruta = int(id_ruta_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        route_row = maybe_release_expired_route_lock(get_route(id_ruta))
        if not route_row or route_row["status"] != ROUTE_STATUS_OPEN:
            await safe_q_answer(q, "Ruta no válida o cerrada.", show_alert=True)
            return

        task_label = task_label_by_code(task_code)

        if answer == "NO":
            await safe_q_answer(q, "Cancelado", show_alert=False)
            await safe_edit_message_text(q, f"❎ No se inició {task_label}.")
            await show_route_tasks_menu(chat_id, context, route_row)
            return

        if task_code not in ROUTE_TIMED_TASKS:
            await safe_q_answer(q, "Tarea no válida.", show_alert=True)
            return

        opened = any_route_timed_task_open(id_ruta)
        if opened and (opened["task_code"] or "") != task_code:
            await safe_q_answer(q, f"⚠️ Ya existe una tarea abierta: {opened['task_label']}. Finalízala primero.", show_alert=True)
            return

        current_same = get_open_route_task_session(id_ruta, task_code)
        if current_same:
            await safe_q_answer(q, "⚠️ Esta tarea ya está iniciada.", show_alert=True)
            return

        await safe_q_answer(q, "Iniciando…", show_alert=False)
        await safe_edit_message_text(q, f"✅ Confirmado inicio de {task_label}.")
        start_msg = await context.bot.send_message(
            chat_id=chat_id,
            text=route_task_started_text(task_code, now_utc()),
            reply_markup=kb_route_finish_button(id_ruta, task_code),
        )

        create_route_task_session(
            id_ruta=id_ruta,
            task_code=task_code,
            task_label=task_label,
            user_id=user_id,
            user_name=user_name,
            start_message_id=start_msg.message_id if start_msg else None,
            start_confirm_message_id=q.message.message_id,
            start_menu_message_id=None,
        )

        add_route_event(id_ruta, task_code, task_label, chat_id, user_id, user_name, f"{TASK_EVENT_START}: {user_name}")
        return

    if data.startswith("ROUTE_FINISH_BTN|"):
        try:
            _, id_ruta_s, task_code = data.split("|", 2)
            id_ruta = int(id_ruta_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        route_row = maybe_release_expired_route_lock(get_route(id_ruta))
        if not route_row or route_row["status"] != ROUTE_STATUS_OPEN:
            await safe_q_answer(q, "Ruta no válida o cerrada.", show_alert=True)
            return

        session = get_open_route_task_session(id_ruta, task_code)
        if not session:
            await safe_q_answer(q, "⚠️ No hay una sesión abierta para esta tarea.", show_alert=True)
            return

        await safe_q_answer(q, "Confirmar cierre…", show_alert=False)
        await safe_edit_message_text(q, route_confirm_finish_text(task_code), reply_markup=kb_route_confirm_finish(id_ruta, task_code))
        update_route_task_session(int(session["session_id"]), finish_confirm_message_id=q.message.message_id)
        return

    if data.startswith("ROUTE_CONFIRM_FINISH|"):
        try:
            _, id_ruta_s, task_code, answer = data.split("|", 3)
            id_ruta = int(id_ruta_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        route_row = maybe_release_expired_route_lock(get_route(id_ruta))
        if not route_row or route_row["status"] != ROUTE_STATUS_OPEN:
            await safe_q_answer(q, "Ruta no válida o cerrada.", show_alert=True)
            return

        session = get_open_route_task_session(id_ruta, task_code)
        task_label = task_label_by_code(task_code)

        if not session:
            await safe_q_answer(q, "⚠️ No hay una sesión abierta para esta tarea.", show_alert=True)
            return

        if answer == "NO":
            await safe_q_answer(q, "Cancelado", show_alert=False)
            await safe_edit_message_text(q, route_task_started_text(task_code, session["started_at"]), reply_markup=kb_route_finish_button(id_ruta, task_code))
            return

        await safe_q_answer(q, "Finalizando…", show_alert=False)
        close_route_task_session(
            int(session["session_id"]),
            user_id=user_id,
            user_name=user_name,
            finish_message_id=q.message.message_id,
            finish_confirm_message_id=q.message.message_id,
        )

        with db() as conn:
            closed_session = conn.execute("SELECT * FROM route_task_sessions WHERE session_id=?", (int(session["session_id"]),)).fetchone()

        await safe_edit_message_text(q, route_task_finished_text(task_code, closed_session["started_at"], closed_session["finished_at"]))
        add_route_event(id_ruta, task_code, task_label, chat_id, user_id, user_name, f"{TASK_EVENT_FINISH}: {user_name}")
        enqueue_tarea_ruta_row(id_ruta, closed_session, route_row)
        await show_route_tasks_menu(chat_id, context, route_row)
        return

    if data.startswith("ROUTE_TASK|"):
        try:
            _, id_ruta_s, task_code = data.split("|", 2)
            id_ruta = int(id_ruta_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        route_row = maybe_release_expired_route_lock(get_route(id_ruta))
        if not route_row or route_row["status"] != ROUTE_STATUS_OPEN:
            await safe_q_answer(q, "Ruta no válida o cerrada.", show_alert=True)
            return

        if int(route_row["chat_id"]) != int(chat_id):
            await safe_q_answer(q, "Esta ruta no pertenece a este grupo.", show_alert=True)
            return

        if not route_is_fully_started(route_row):
            await safe_q_answer(q, "⚠️ Primero completa selfie y ubicación de inicio.", show_alert=True)
            return

        task_label = task_label_by_code(task_code)

        if task_code == TASK_CERRAR_RUTA:
            opened = any_route_timed_task_open(id_ruta)
            if opened:
                await safe_q_answer(q, f"⚠️ Debes finalizar primero la tarea abierta: {opened['task_label']}.", show_alert=True)
                return

            open_order = get_open_order_for_route(id_ruta)
            if open_order:
                await safe_q_answer(q, "⚠️ Debes cerrar o cancelar primero la orden activa.", show_alert=True)
                return

            finish_at = now_utc()
            start_at = route_row["created_at"] or finish_at

            update_route(
                id_ruta,
                status=ROUTE_STATUS_CLOSED,
                phase=PHASE_ROUTE_CLOSED,
                closed_at=finish_at,
                route_menu_enabled=0,
            )
            clear_route_lock(id_ruta)

            add_route_event(id_ruta, TASK_CERRAR_RUTA, task_label, chat_id, user_id, user_name, "Ruta cerrada desde menú")
            enqueue_ruta_row(id_ruta)

            await safe_q_answer(q, "🛑 Ruta cerrada", show_alert=False)
            await safe_delete_message(context, chat_id, q.message.message_id)

            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    "🛑 Ruta cerrada correctamente.\n"
                    f"Técnico: {route_row['technician_name'] or '-'}\n"
                    f"Inicio de ruta: {fmt_date_pe(start_at)} {fmt_time_pe(start_at)}\n"
                    f"Cierre de ruta: {fmt_date_pe(finish_at)} {fmt_time_pe(finish_at)}"
                ),
            )
            return

        if task_code == TASK_INICIO_ORDEN:
            open_order = get_open_order_for_route(id_ruta)
            if open_order:
                await safe_q_answer(q, "⚠️ Ya existe una orden activa. Debes terminarla o cancelarla.", show_alert=True)
                return

            order_row = create_order_for_route(route_row, user_id, user_name)
            enqueue_orden_row(int(order_row["id_orden"]))

            await safe_q_answer(q, "✅ Registrado", show_alert=False)
            await safe_edit_message_text(q, f"✅ {task_label} registrado.")

            add_route_event(id_ruta, task_code, task_label, chat_id, user_id, user_name, f"{TASK_EVENT_SIMPLE}: {user_name}")

            await context.bot.send_message(
                chat_id=chat_id,
                text=f"✅ Tarea registrada\n• {task_label}\n• Técnico: {route_row['technician_name'] or '-'}\n• Hora: {fmt_time_pe(now_utc())}"
            )

            await context.bot.send_message(chat_id=chat_id, text="PASO 2 - TIPO DE SERVICIO\nSelecciona una opción:", reply_markup=kb_services())
            return

        if task_code in ROUTE_TIMED_TASKS:
            opened = any_route_timed_task_open(id_ruta)
            if opened and (opened["task_code"] or "") != task_code:
                await safe_q_answer(q, f"⚠️ Ya existe una tarea abierta: {opened['task_label']}. Finalízala primero.", show_alert=True)
                return

            if get_open_route_task_session(id_ruta, task_code):
                await safe_q_answer(q, "⚠️ Esta tarea ya está iniciada.", show_alert=True)
                return

            await safe_q_answer(q, "Confirmar inicio…", show_alert=False)
            await safe_edit_message_text(q, route_confirm_start_text(task_code), reply_markup=kb_route_confirm_start(id_ruta, task_code))
            add_route_event(id_ruta, task_code, task_label, chat_id, user_id, user_name, f"{TASK_EVENT_CONFIRM_START}: {user_name}")
            return

        add_route_event(id_ruta, task_code, task_label, chat_id, user_id, user_name, f"Tarea ejecutada por {user_name}")

        await safe_q_answer(q, "✅ Registrado", show_alert=False)
        await safe_edit_message_text(q, f"✅ {task_label} registrado.")
        await context.bot.send_message(chat_id=chat_id, text=f"✅ Tarea registrada\n• {task_label}\n• Técnico: {route_row['technician_name'] or '-'}\n• Hora: {fmt_time_pe(now_utc())}")
        await show_route_tasks_menu(chat_id, context, route_row)
        return

    if data.startswith("TECH|"):
        route_row = maybe_release_expired_route_lock(get_open_route(chat_id))
        if not route_row:
            await safe_q_answer(q, "No hay una ruta abierta. Usa /inicio.", show_alert=True)
            return
        if (route_row["phase"] or "") != PHASE_WAIT_TECHNICIAN:
            await safe_q_answer(q, "Este paso ya fue atendido.", show_alert=False)
            return

        name = data.split("|", 1)[1]
        id_ruta = int(route_row["id_ruta"])

        update_route(
            id_ruta,
            technician_name=name,
            technician_user_id=user_id,
            user_id=user_id,
            username=user_name,
            route_menu_enabled=1,
            phase=PHASE_ROUTE_MENU,
            status=ROUTE_STATUS_OPEN,
        )

        enqueue_ruta_row(id_ruta)

        await safe_q_answer(q, "✅ Técnico registrado", show_alert=False)
        await safe_edit_message_text(q, text=f"✅ Técnico seleccionado: {name}")
        await context.bot.send_message(chat_id=chat_id, text=f"BIENVENIDO {name}")
        await context.bot.send_message(chat_id=chat_id, text="✅ Ruta iniciada correctamente.")
        await show_route_tasks_menu(chat_id, context, get_route(id_ruta))
        return

    if data.startswith("SERV|"):
        route_row = maybe_release_expired_route_lock(get_open_route(chat_id))
        order_row = get_open_order(chat_id)
        if not route_row or not order_row:
            await safe_q_answer(q, "No hay orden abierta. Usa Inicio de Orden.", show_alert=True)
            return
        if (order_row["phase"] or "") != PHASE_WAIT_SERVICE:
            await safe_q_answer(q, "Este paso ya fue atendido.", show_alert=False)
            return

        service = data.split("|", 1)[1]
        if service != "ALTA NUEVA":
            await safe_q_answer(q, "PROCESO AUN NO GENERADO", show_alert=True)
            return

        update_order(
            int(order_row["id_orden"]),
            service_type=service,
            step_index=2,
            phase=PHASE_WAIT_ABONADO,
            evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
        )
        enqueue_orden_row(int(order_row["id_orden"]))
        await safe_q_answer(q, "✅ Servicio registrado", show_alert=False)
        await safe_edit_message_text(q, f"✅ Tipo de servicio seleccionado: {service}")
        await context.bot.send_message(chat_id=chat_id, text=prompt_step3())
        return

    if data.startswith("MODE|"):
        order_row = get_open_order(chat_id)
        if not order_row:
            await safe_q_answer(q, "No hay orden abierta. Usa Inicio de Orden.", show_alert=True)
            return
        if int(order_row["step_index"] or 0) != 4:
            await safe_q_answer(q, "Aún no llegas a este paso. Completa pasos previos.", show_alert=True)
            return

        mode = data.split("|", 1)[1]
        if mode not in ("EXTERNA", "INTERNA"):
            await safe_q_answer(q, "Modo inválido.", show_alert=True)
            return

        update_order(int(order_row["id_orden"]), install_mode=mode, phase=PHASE_WAIT_PACKAGE, pending_step_no=None, current_step_no=None)
        enqueue_orden_row(int(order_row["id_orden"]))
        await safe_q_answer(q, f"✅ {mode}", show_alert=False)
        await safe_edit_message_text(q, f"✅ Tipo de instalación seleccionado: {mode}")
        await show_package_menu(chat_id, context)
        return

    if data.startswith("PACK|"):
        order_row = get_open_order(chat_id)
        if not order_row:
            await safe_q_answer(q, "No hay orden abierta.", show_alert=True)
            return

        package_type = data.split("|", 1)[1]
        if package_type not in PACKAGE_TYPES:
            await safe_q_answer(q, "Paquete inválido.", show_alert=True)
            return

        if (order_row["phase"] or "") != PHASE_WAIT_PACKAGE:
            await safe_q_answer(q, "Este paso no está disponible ahora.", show_alert=True)
            return

        if package_type == "INTERNET":
            update_order(int(order_row["id_orden"]), package_type=package_type, tv_count=0, phase=PHASE_WAIT_CLIENT_STATUS)
            enqueue_orden_row(int(order_row["id_orden"]))
            await safe_q_answer(q, "✅ Paquete registrado", show_alert=False)
            await safe_edit_message_text(q, f"✅ Tipo de paquete seleccionado: {package_type}")
            await show_client_status_menu(chat_id, context)
            return

        update_order(int(order_row["id_orden"]), package_type=package_type, phase=PHASE_WAIT_TV_COUNT)
        enqueue_orden_row(int(order_row["id_orden"]))
        await safe_q_answer(q, "✅ Paquete registrado", show_alert=False)
        await safe_edit_message_text(q, f"✅ Tipo de paquete seleccionado: {package_type}")
        await show_tv_count_menu(chat_id, context)
        return

    if data.startswith("TVCOUNT|"):
        order_row = get_open_order(chat_id)
        if not order_row:
            await safe_q_answer(q, "No hay orden abierta.", show_alert=True)
            return

        if (order_row["phase"] or "") != PHASE_WAIT_TV_COUNT:
            await safe_q_answer(q, "Este paso no está disponible ahora.", show_alert=True)
            return

        try:
            tv_count = int(data.split("|", 1)[1])
        except Exception:
            await safe_q_answer(q, "Cantidad inválida.", show_alert=True)
            return

        if tv_count not in TV_COUNT_OPTIONS:
            await safe_q_answer(q, "Cantidad inválida.", show_alert=True)
            return

        update_order(int(order_row["id_orden"]), tv_count=tv_count, phase=PHASE_WAIT_CLIENT_STATUS)
        enqueue_orden_row(int(order_row["id_orden"]))
        await safe_q_answer(q, "✅ Número de TV registrado", show_alert=False)
        await safe_edit_message_text(q, f"✅ Número de TV seleccionado: {tv_count}")
        await show_client_status_menu(chat_id, context)
        return

    if data.startswith("CSTATUS|"):
        order_row = get_open_order(chat_id)
        if not order_row:
            await safe_q_answer(q, "No hay orden abierta.", show_alert=True)
            return

        client_status = data.split("|", 1)[1]

        if client_status == CLIENT_STATUS_EN_PROCESO:
            update_order(
                int(order_row["id_orden"]),
                client_status=client_status,
                phase=PHASE_WAIT_PERSONA_ATIENDE,
                pending_step_no=None,
                current_step_no=None,
                admin_pending=0,
                evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
            )
            enqueue_orden_row(int(order_row["id_orden"]))
            await safe_q_answer(q, "✅ Estado registrado", show_alert=False)
            await safe_edit_message_text(q, f"✅ Estado seleccionado: {client_status.replace('_', ' ')}")
            await context.bot.send_message(
                chat_id=chat_id,
                text="PERSONA QUE ATIENDE\nSelecciona una opción:",
                reply_markup=kb_persona_atiende(),
            )
            return

        update_order(
            int(order_row["id_orden"]),
            client_status=client_status,
            phase=PHASE_MENU_EVID,
            pending_step_no=None,
            current_step_no=None,
            admin_pending=0,
            evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
        )
        enqueue_orden_row(int(order_row["id_orden"]))
        await safe_q_answer(q, "✅ Estado registrado", show_alert=False)
        await safe_edit_message_text(q, f"✅ Estado seleccionado: {client_status.replace('_', ' ')}")

        order_row2 = get_order(int(order_row["id_orden"]))
        await show_evidence_menu(chat_id, context, order_row2)
        return

    if data.startswith("PERSONA_ATIENDE|"):
        order_row = get_open_order(chat_id)
        if not order_row:
            await safe_q_answer(q, "No hay orden abierta.", show_alert=True)
            return

        if (order_row["phase"] or "") != PHASE_WAIT_PERSONA_ATIENDE:
            await safe_q_answer(q, "Este paso no está disponible ahora.", show_alert=True)
            return

        persona = data.split("|", 1)[1]

        if persona == "TITULAR":
            update_order(
                int(order_row["id_orden"]),
                phase=PHASE_WAIT_DOC_TITULAR,
                admin_pending=0,
            )
            await safe_q_answer(q, "✅ Titular seleccionado", show_alert=False)
            await safe_edit_message_text(q, "✅ Persona que atiende: TITULAR")
            await context.bot.send_message(
                chat_id=chat_id,
                text="CARGAR DOCUMENTO DEL CLIENTE\nEnvía la foto del documento del cliente."
            )
            return

        if persona == "ENCARGADO":
            update_order(
                int(order_row["id_orden"]),
                phase=PHASE_WAIT_DOC_ENCARGADO,
                admin_pending=0,
            )
            await safe_q_answer(q, "✅ Encargado seleccionado", show_alert=False)
            await safe_edit_message_text(q, "✅ Persona que atiende: ENCARGADO")
            await context.bot.send_message(
                chat_id=chat_id,
                text="CARGAR DOCUMENTO DEL ENCARGADO\nEnvía la foto del documento del encargado."
            )
            return

        await safe_q_answer(q, "Opción inválida.", show_alert=True)
        return

    if data.startswith("EVID|"):
        try:
            _, id_orden_s, step_no_s = data.split("|", 2)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        order_row = get_order(id_orden)
        route_row = get_route(int(order_row["id_ruta"])) if order_row else None
        if not order_row or not route_row:
            await safe_q_answer(q, "No hay una orden abierta. Usa /inicio.", show_alert=True)
            return

        ok, why = can_user_operate_current_route(route_row, user_id)
        if not ok:
            await safe_q_answer(q, why, show_alert=True)
            return

        req_num, req_label, req_step_no, req_status = compute_next_required_step(order_row)
        if req_status == STEP_STATE_APROBADO:
            await safe_q_answer(q, "✅ Orden ya completada.", show_alert=True)
            return

        if step_no != req_step_no:
            st = get_effective_step_status(id_orden, step_no)
            if st == STEP_STATE_APROBADO:
                await safe_q_answer(q, "✅ Este paso ya está conforme.", show_alert=True)
                return
            if st == STEP_STATE_EN_REVISION:
                await safe_q_answer(q, "⏳ Este paso está en revisión de admin.", show_alert=True)
                return
            if st == STEP_STATE_BLOQUEADO:
                await safe_q_answer(q, "⛔ Este paso está bloqueado por corrección de un paso anterior.", show_alert=True)
                return
            await safe_q_answer(q, f"⚠️ Debes completar primero: {req_num}. {req_label}", show_alert=True)
            return

        if is_alt_client_status(order_row):
            st = ensure_step_state(id_orden, step_no, owner_user_id=user_id, owner_name=user_name)
            set_step_owner(id_orden, int(st["step_no"]), int(st["attempt"]), user_id, user_name)
            update_order(id_orden, phase=PHASE_STEP_MEDIA, pending_step_no=step_no, current_step_no=step_no, user_id=user_id, username=user_name)
            lock_route(int(route_row["id_ruta"]), user_id, user_name)
            await safe_q_answer(q, "Cargar foto…", show_alert=False)
            await safe_edit_message_text(q, f"📌 {req_num}. {step_name(step_no)}\nSelección realizada.")
            await send_step_guide(context, chat_id, step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        update_order(id_orden, phase=PHASE_EVID_ACTION, pending_step_no=step_no, current_step_no=step_no)
        clear_route_lock(int(route_row["id_ruta"]))
        await safe_q_answer(q, "Continuar…", show_alert=False)
        label = step_name(step_no)
        await safe_edit_message_text(q, f"📌 {req_num}. {label}\nElige una opción:")
        await context.bot.send_message(chat_id=chat_id, text=f"📌 {req_num}. {label}\nElige una opción:", reply_markup=kb_action_menu(id_orden, step_no))
        return

    if data.startswith("ACT|"):
        try:
            _, id_orden_s, step_no_s, action = data.split("|", 3)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        order_row = get_order(id_orden)
        route_row = get_route(int(order_row["id_ruta"])) if order_row else None
        if not order_row or not route_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden no válida o cerrada.", show_alert=True)
            return
        if int(order_row["chat_id"]) != int(chat_id):
            await safe_q_answer(q, "Esta orden no pertenece a este grupo.", show_alert=True)
            return

        req_num, req_label, req_step_no, _ = compute_next_required_step(order_row)
        if int(step_no) != int(req_step_no):
            await safe_q_answer(q, f"⚠️ Paso no vigente. Debes trabajar: {req_num}. {req_label}", show_alert=True)
            return

        ok, why = can_user_operate_current_route(route_row, user_id)
        if not ok:
            await safe_q_answer(q, why, show_alert=True)
            return

        latest = get_latest_step_state(id_orden, step_no)
        if latest and (latest["state_name"] or "") == STEP_STATE_APROBADO:
            await safe_q_answer(q, "✅ Este paso ya fue aprobado y está cerrado.", show_alert=True)
            return
        if latest and int(latest["blocked"] or 0) == 1:
            await safe_q_answer(q, "⛔ Este paso está bloqueado.", show_alert=True)
            return

        lock_route(int(route_row["id_ruta"]), user_id, user_name)

        if action == "PERMISO":
            update_order(id_orden, phase=PHASE_AUTH_MODE, pending_step_no=step_no, current_step_no=step_no, user_id=user_id, username=user_name)
            await safe_q_answer(q, "Permiso…", show_alert=False)
            await safe_edit_message_text(q, "✅ Opción seleccionada: SOLICITUD DE PERMISO")
            await context.bot.send_message(chat_id=chat_id, text="Autorización: elige el tipo", reply_markup=kb_auth_mode(id_orden, step_no))
            return

        if action == "FOTO":
            st = ensure_step_state(id_orden, step_no, owner_user_id=user_id, owner_name=user_name)
            set_step_owner(id_orden, step_no, int(st["attempt"]), user_id, user_name)
            update_order(id_orden, phase=PHASE_STEP_MEDIA, pending_step_no=step_no, current_step_no=step_no, user_id=user_id, username=user_name)
            await safe_q_answer(q, "Cargar foto…", show_alert=False)
            await safe_edit_message_text(q, "✅ Opción seleccionada: CARGAR FOTO")
            await send_step_guide(context, chat_id, step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        await safe_q_answer(q, "Acción inválida.", show_alert=True)
        return

    if data.startswith("AUTH_MODE|"):
        try:
            _, id_orden_s, step_no_s, mode = data.split("|", 3)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        order_row = get_order(id_orden)
        route_row = get_route(int(order_row["id_ruta"])) if order_row else None
        if not order_row or not route_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden no válida o cerrada.", show_alert=True)
            return

        ok, why = can_user_operate_current_route(route_row, user_id)
        if not ok:
            await safe_q_answer(q, why, show_alert=True)
            return

        if mode == "TEXT":
            st = ensure_step_state(id_orden, -step_no, owner_user_id=user_id, owner_name=user_name)
            set_step_owner(id_orden, -step_no, int(st["attempt"]), user_id, user_name)
            update_order(id_orden, phase=PHASE_AUTH_TEXT_WAIT, pending_step_no=step_no, current_step_no=step_no, user_id=user_id, username=user_name)
            await safe_q_answer(q, "Envía el texto…", show_alert=False)
            await safe_edit_message_text(q, "✅ Tipo de autorización: Solo texto")
            await context.bot.send_message(chat_id=chat_id, text="Envía el texto de la autorización (en un solo mensaje).")
            return

        if mode == "MEDIA":
            st = ensure_step_state(id_orden, -step_no, owner_user_id=user_id, owner_name=user_name)
            set_step_owner(id_orden, -step_no, int(st["attempt"]), user_id, user_name)
            update_order(id_orden, phase=PHASE_AUTH_MEDIA, pending_step_no=step_no, current_step_no=step_no, user_id=user_id, username=user_name)
            await safe_q_answer(q, "Carga evidencias…", show_alert=False)
            await safe_edit_message_text(q, "✅ Tipo de autorización: Multimedia")
            await context.bot.send_message(chat_id=chat_id, text=prompt_auth_media_step(step_no), reply_markup=kb_auth_media_controls(id_orden, step_no))
            return

        await safe_q_answer(q, "Modo inválido", show_alert=True)
        return

    if data.startswith("AUTH_MORE|"):
        try:
            _, id_orden_s, step_no_s = data.split("|", 2)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return
        order_row = get_order(id_orden)
        if not order_row:
            await safe_q_answer(q, "Orden no válida.", show_alert=True)
            return
        if int(order_row["admin_pending"] or 0) == 1:
            await safe_q_answer(q, "⏳ Ya fue enviado a revisión. No puedes cargar más.", show_alert=True)
            return
        await safe_q_answer(q, "Puedes seguir cargando.", show_alert=False)
        return

    if data.startswith("AUTH_DONE|"):
        try:
            _, id_orden_s, step_no_s = data.split("|", 2)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        order_row = get_order(id_orden)
        route_row = get_route(int(order_row["id_ruta"])) if order_row else None
        if not order_row or not route_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden no válida o cerrada.", show_alert=True)
            return

        auth_step_no = -step_no
        st = ensure_step_state(id_orden, auth_step_no, owner_user_id=user_id, owner_name=user_name)
        attempt = int(st["attempt"])

        if int(st["submitted"]) == 1 and st["approved"] is None:
            await safe_q_answer(q, "Esta autorización ya fue enviada a revisión.", show_alert=True)
            return
        if st["approved"] is not None and int(st["approved"]) == 1:
            await safe_q_answer(q, "✅ Esta autorización ya está aprobada.", show_alert=True)
            return

        count = media_count(id_orden, auth_step_no, attempt)
        if count <= 0:
            await safe_q_answer(q, "⚠️ Debes cargar al menos 1 archivo.", show_alert=True)
            return

        approval_required = get_approval_required(int(order_row["chat_id"]))

        if not approval_required:
            auto_approve_db_step(id_orden, auth_step_no, attempt)
            enqueue_evidencia_paso_row(id_orden, step_no, attempt, STEP_STATE_APROBADO, "APROBACION OFF", "", kind="PERM")
            update_order(id_orden, phase=PHASE_STEP_MEDIA, pending_step_no=step_no, current_step_no=step_no, admin_pending=0)
            clear_route_lock(int(route_row["id_ruta"]))

            await safe_q_answer(q, "✅ Autorización aprobada (OFF)", show_alert=False)
            await safe_edit_message_text(q, "✅ Autorización aprobada automáticamente (APROBACION OFF). Continuando a CARGAR FOTO…")
            await send_step_guide(context, chat_id, step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        mark_submitted(id_orden, auth_step_no, attempt)
        update_order(id_orden, phase=PHASE_AUTH_REVIEW, pending_step_no=step_no, current_step_no=step_no, admin_pending=1)
        clear_route_lock(int(route_row["id_ruta"]))
        await safe_q_answer(q, "📨 Enviado a revisión", show_alert=False)
        await safe_edit_message_text(q, "📨 Autorización enviada a revisión.")
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"🔐 **Revisión de AUTORIZACIÓN (multimedia)**\n"
                f"Para: {step_name(step_no)}\n"
                f"Intento: {attempt}\n"
                f"Técnico: {order_row['technician_name'] or '-'}\n"
                f"Servicio: {order_row['service_type'] or '-'}\n"
                f"Abonado: {order_row['abonado_code'] or '-'}\n"
                f"Archivos: {count}\n\n"
                "Admins: validar con ✅/❌"
            ),
            parse_mode="Markdown",
            reply_markup=kb_auth_review(id_orden, step_no, attempt),
        )
        return

    if data.startswith("AUT_OK|") or data.startswith("AUT_BAD|"):
        try:
            action, id_orden_s, step_no_s, attempt_s = data.split("|", 3)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
            attempt = int(attempt_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "Solo Administradores del grupo pueden validar", show_alert=True)
            return

        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden no válida o cerrada.", show_alert=True)
            return

        auth_step_no = -step_no
        with db() as conn:
            row = conn.execute("SELECT approved FROM step_state WHERE id_orden=? AND step_no=? AND attempt=?", (id_orden, auth_step_no, attempt)).fetchone()
        if not row:
            await safe_q_answer(q, "No encontré la autorización para revisar.", show_alert=True)
            return
        if row["approved"] is not None:
            await safe_q_answer(q, "Esta autorización ya fue revisada.", show_alert=True)
            return

        tech_id = int(order_row["technician_user_id"] or 0)
        admin_name = q.from_user.full_name

        if action == "AUT_OK":
            set_review(id_orden, auth_step_no, attempt, approved=1, reviewer_id=user_id)
            enqueue_evidencia_paso_row(id_orden, step_no, attempt, STEP_STATE_APROBADO, admin_name, "", kind="PERM")
            await safe_q_answer(q, "✅ Autorizado", show_alert=False)
            await safe_edit_message_text(q, "✅ Autorizado. Continuando a CARGAR FOTO…")
            await context.bot.send_message(
                chat_id=chat_id,
                text=f"🔐 {mention_user_html(tech_id)}: ✅ Autorización aprobada para <b>{step_name(step_no)}</b> (Intento {attempt}) por <b>{admin_name}</b>.",
                parse_mode="HTML",
            )
            update_order(id_orden, phase=PHASE_STEP_MEDIA, pending_step_no=step_no, current_step_no=step_no, admin_pending=0)
            await send_step_guide(context, chat_id, step_no)
            await context.bot.send_message(chat_id=chat_id, text=prompt_media_step(step_no))
            return

        await safe_q_answer(q, "Escribe el motivo del rechazo.", show_alert=False)
        set_pending_input(
            chat_id=chat_id,
            user_id=user_id,
            kind="AUTH_REJECT_REASON",
            id_ruta=int(order_row["id_ruta"]),
            id_orden=id_orden,
            step_no=step_no,
            attempt=attempt,
            reply_to_message_id=q.message.message_id,
            tech_user_id=tech_id,
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                "❌ Rechazo de autorización.\n"
                "✍️ Admin: escribe el *motivo del rechazo* (un solo mensaje).\n\n"
                f"Paso: {step_name(step_no)}\n"
                f"Intento: {attempt}\n"
                f"Técnico: {order_row['technician_name'] or '-'}"
            ),
            parse_mode="Markdown",
        )
        return

    if data.startswith("MEDIA_MORE|"):
        try:
            _, id_orden_s, step_no_s = data.split("|", 2)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return
        order_row = get_order(id_orden)
        if not order_row:
            await safe_q_answer(q, "Orden no válida.", show_alert=True)
            return
        latest = get_latest_step_state(id_orden, step_no)
        if latest and int(latest["submitted"] or 0) == 1 and latest["approved"] is None:
            await safe_q_answer(q, "⏳ Este paso ya fue enviado a revisión. No puedes cargar más.", show_alert=True)
            return
        await safe_q_answer(q, "Puedes seguir cargando evidencias.", show_alert=False)
        return

    if data.startswith("MEDIA_DONE|"):
        try:
            _, id_orden_s, step_no_s = data.split("|", 2)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        order_row = get_order(id_orden)
        route_row = get_route(int(order_row["id_ruta"])) if order_row else None
        if not order_row or not route_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden no válida o cerrada.", show_alert=True)
            return

        latest = get_latest_step_state(id_orden, step_no)
        if latest:
            latest_state = (latest["state_name"] or "").strip().upper()
            if latest_state == STEP_STATE_EN_REVISION or (int(latest["submitted"] or 0) == 1 and latest["approved"] is None):
                await safe_q_answer(q, "⏳ Este paso ya fue enviado a revisión.", show_alert=True)
                return
            if latest_state == STEP_STATE_APROBADO or (latest["approved"] is not None and int(latest["approved"]) == 1):
                await safe_q_answer(q, "✅ Este paso ya está aprobado.", show_alert=True)
                return

        st = get_active_unsubmitted_step_state(id_orden, step_no)
        if not st:
            st = ensure_step_state(id_orden, step_no, owner_user_id=user_id, owner_name=user_name)

        attempt = int(st["attempt"])

        if int(st["submitted"] or 0) == 1 and st["approved"] is None:
            await safe_q_answer(q, "⏳ Este paso ya fue enviado a revisión.", show_alert=True)
            return
        if st["approved"] is not None and int(st["approved"]) == 1:
            await safe_q_answer(q, "✅ Este paso ya está aprobado.", show_alert=True)
            return

        count = media_count(id_orden, step_no, attempt)
        if count <= 0:
            await safe_q_answer(q, "⚠️ Debes cargar al menos 1 foto.", show_alert=True)
            return

        title = step_name(step_no)
        approval_required = get_approval_required(int(order_row["chat_id"]))
        tech_id = int(order_row["technician_user_id"] or 0)

        if is_alt_client_status(order_row):
            auto_approve_db_step(id_orden, step_no, attempt)
            enqueue_evidencia_paso_row(id_orden, step_no, attempt, STEP_STATE_APROBADO, "CARGA DIRECTA", "", kind="EVID")
            mark_evidencias_estado(id_orden, EVIDENCIAS_ESTADO_INCOMPLETAS)

            await safe_q_answer(q, "✅ Evidencia completada", show_alert=False)
            await safe_edit_message_text(q, "✅ Evidencia completada.")

            if is_last_step(order_row, step_no):
                status_code = (order_row["client_status"] or "").strip()
                status_label = get_client_status_label(status_code)
                upsert_alt_request(id_orden, status_code, status_label, q.message.message_id)
                update_order(id_orden, phase=PHASE_ALT_FINAL_REVIEW, pending_step_no=None, current_step_no=None, admin_pending=1)
                clear_route_lock(int(route_row["id_ruta"]))

                await context.bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"Se aprueba solicitud de {status_label}\n\n"
                        "Validar solicitud:"
                    ),
                    reply_markup=kb_alt_final_review(id_orden),
                )
                return

            sync_order_progress(id_orden)
            update_order(id_orden, phase=PHASE_MENU_EVID, pending_step_no=None, admin_pending=0)
            clear_route_lock(int(route_row["id_ruta"]))
            await context.bot.send_message(chat_id=chat_id, text="➡️ Continúa con la siguiente evidencia.")
            await show_evidence_menu(chat_id, context, get_order(id_orden))
            return

        if not approval_required:
            auto_approve_db_step(id_orden, step_no, attempt)
            enqueue_evidencia_paso_row(
                id_orden,
                step_no,
                attempt,
                STEP_STATE_APROBADO,
                "CARGA DIRECTA",
                "",
                kind="EVID"
            )
            mark_evidencias_estado(id_orden, EVIDENCIAS_ESTADO_INCOMPLETAS)

            await safe_q_answer(q, "✅ Paso completado", show_alert=False)
            await safe_edit_message_text(q, "✅ Evidencias completadas.")

            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ <b>PASO COMPLETADO</b>\n"
                    f"• Evidencia: <b>{title}</b>\n"
                    f"• Intento: <b>{attempt}</b>\n"
                    f"• Evidencias: <b>{count}</b>"
                ),
                parse_mode="HTML",
            )

            clear_route_lock(int(route_row["id_ruta"]))
            update_order(id_orden, admin_pending=0)

            if is_last_step(order_row, step_no):
                sync_order_progress(id_orden)
                update_order(
                    id_orden,
                    phase=PHASE_MENU_EVID,
                    pending_step_no=None,
                    current_step_no=None,
                )
                order_row2 = get_order(id_orden)
                enqueue_orden_row(id_orden)
                await show_validation_button(chat_id, context, order_row2)
                return

            sync_order_progress(id_orden)
            update_order(
                id_orden,
                phase=PHASE_MENU_EVID,
                pending_step_no=None,
            )
            order_row2 = get_order(id_orden)

            await context.bot.send_message(
                chat_id=chat_id,
                text="➡️ Continúa con el siguiente paso."
            )
            await show_evidence_menu(chat_id, context, order_row2)
            return

    if data.startswith("ALT_FINAL_OK|") or data.startswith("ALT_FINAL_BAD|"):
        try:
            action, id_orden_s = data.split("|", 1)
            id_orden = int(id_orden_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "Solo Administradores del grupo pueden validar", show_alert=True)
            return

        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden inválida o cerrada.", show_alert=True)
            return

        review_row = get_alt_request_row(id_orden)
        if not review_row:
            await safe_q_answer(q, "No encontré la solicitud final.", show_alert=True)
            return
        if review_row["approved"] is not None:
            await safe_q_answer(q, "Esta solicitud ya fue revisada.", show_alert=True)
            return

        if action == "ALT_FINAL_OK":
            set_alt_request_review(id_orden, approved=1, reviewer_id=user_id)
            close_order(
                id_orden,
                final_status=review_row["status_code"],
                phase=PHASE_ORDER_CLOSED,
                evidencias_estado=EVIDENCIAS_ESTADO_COMPLETAS,
            )
            route_row = get_route(int(order_row["id_ruta"]))
            if route_row:
                clear_route_lock(int(route_row["id_ruta"]))
            enqueue_orden_row(id_orden)

            await safe_q_answer(q, "✅ Aprobado", show_alert=False)
            await safe_edit_message_text(q, f"✅ Solicitud de {review_row['status_label']} aprobada.")
            route_row = get_route(int(order_row["id_ruta"]))
            if route_row:
                await show_route_tasks_menu(chat_id, context, route_row)
            return

        set_alt_request_review(id_orden, approved=0, reviewer_id=user_id)
        update_order(
            id_orden,
            admin_pending=0,
            phase=PHASE_WAIT_CLIENT_STATUS,
            pending_step_no=None,
            current_step_no=None,
            client_status=None,
            evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
            final_order_status=None,
        )
        route_row = get_route(int(order_row["id_ruta"]))
        if route_row:
            clear_route_lock(int(route_row["id_ruta"]))
        with db() as conn:
            conn.execute("DELETE FROM step_state WHERE id_orden=? AND step_no IN (?,?,?)", (id_orden, STEP_ALT_FACHADA, STEP_ALT_PLACA, STEP_ALT_SUMINISTRO))
            conn.execute("DELETE FROM media WHERE id_orden=? AND step_no IN (?,?,?)", (id_orden, STEP_ALT_FACHADA, STEP_ALT_PLACA, STEP_ALT_SUMINISTRO))
            conn.commit()

        await safe_q_answer(q, "❌ Rechazado", show_alert=False)
        await safe_edit_message_text(q, f"❌ Solicitud de {review_row['status_label']} rechazada.")
        await show_client_status_menu(chat_id, context)
        return

    if data.startswith("REV_OK|") or data.startswith("REV_BAD|"):
        try:
            action, id_orden_s, step_no_s, attempt_s = data.split("|", 3)
            id_orden = int(id_orden_s)
            step_no = int(step_no_s)
            attempt = int(attempt_s)
        except Exception:
            await safe_q_answer(q, "Callback inválido", show_alert=True)
            return

        if not await is_admin_of_chat(context, chat_id, user_id):
            await safe_q_answer(q, "Solo Administradores del grupo pueden validar", show_alert=True)
            return

        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await safe_q_answer(q, "Orden no válida o cerrada.", show_alert=True)
            return

        with db() as conn:
            row = conn.execute("SELECT approved FROM step_state WHERE id_orden=? AND step_no=? AND attempt=?", (id_orden, step_no, attempt)).fetchone()
        if not row:
            await safe_q_answer(q, "No encontré el paso para revisar.", show_alert=True)
            return
        if row["approved"] is not None:
            await safe_q_answer(q, "Este paso ya fue revisado.", show_alert=True)
            return

        tech_id = int(order_row["technician_user_id"] or 0)
        admin_name = q.from_user.full_name
        title = step_name(step_no)

        if action == "REV_OK":
            set_review(id_orden, step_no, attempt, approved=1, reviewer_id=user_id)
            enqueue_evidencia_paso_row(id_orden, step_no, attempt, STEP_STATE_APROBADO, admin_name, "", kind="EVID")
            increment_order_review_counts(id_orden)
            mark_evidencias_estado(id_orden, EVIDENCIAS_ESTADO_INCOMPLETAS)

            await safe_q_answer(q, "✅ Conforme", show_alert=False)
            await safe_edit_message_text(q, "✅ Conforme.")

            evids = media_count(id_orden, step_no, attempt)
            await context.bot.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ <b>PASO COMPLETADO</b>\n"
                    f"• Evidencia: <b>{title}</b>\n"
                    f"• Intento: <b>{attempt}</b>\n"
                    f"• Evidencias: <b>{evids}</b>\n"
                    f"• Aprobado por: <b>{admin_name}</b>\n"
                    f"• Técnico: {mention_user_html(tech_id)}"
                ),
                parse_mode="HTML",
            )

            update_order(id_orden, admin_pending=0)
            order_row_after = get_order(id_orden)

            if is_last_step(order_row_after, step_no):
                sync_order_progress(id_orden)
                update_order(id_orden, phase=PHASE_MENU_EVID, pending_step_no=None, current_step_no=None)
                order_row2 = get_order(id_orden)
                enqueue_orden_row(id_orden)
                await show_validation_button(chat_id, context, order_row2)
                return

            sync_order_progress(id_orden)
            update_order(id_orden, phase=PHASE_MENU_EVID, pending_step_no=None)
            order_row2 = get_order(id_orden)
            await context.bot.send_message(chat_id=chat_id, text="➡️ Continúa con el siguiente paso.")
            await show_evidence_menu(chat_id, context, order_row2)
            return

        await safe_q_answer(q, "Escribe el motivo del rechazo.", show_alert=False)
        set_pending_input(
            chat_id=chat_id,
            user_id=user_id,
            kind="EVID_REJECT_REASON",
            id_ruta=int(order_row["id_ruta"]),
            id_orden=id_orden,
            step_no=step_no,
            attempt=attempt,
            reply_to_message_id=q.message.message_id,
            tech_user_id=tech_id,
        )
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"❌ Rechazo de evidencia - {title}\n"
                f"Intento: {attempt}\n"
                "✍️ Admin: escribe el *motivo del rechazo* (un solo mensaje)."
            ),
            parse_mode="Markdown",
        )
        return

    await safe_q_answer(q, "Acción no válida.", show_alert=True)


# =========================
# Text handler
# =========================
async def on_text(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

    pending_reopen = pop_pending_input(msg.chat_id, msg.from_user.id, "REOPEN_REASON")
    if pending_reopen:
        reason = (msg.text or "").strip()
        if not reason:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía un texto válido como motivo.")
            set_pending_input(
                chat_id=msg.chat_id,
                user_id=msg.from_user.id,
                kind="REOPEN_REASON",
                id_ruta=int(pending_reopen["id_ruta"]) if pending_reopen["id_ruta"] is not None else None,
                id_orden=int(pending_reopen["id_orden"]) if pending_reopen["id_orden"] is not None else None,
                step_no=int(pending_reopen["step_no"]),
                attempt=0,
                reply_to_message_id=int(pending_reopen["reply_to_message_id"]) if pending_reopen["reply_to_message_id"] is not None else None,
                tech_user_id=None,
            )
            return

        id_orden = int(pending_reopen["id_orden"])
        step_no = int(pending_reopen["step_no"])
        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Orden no válida o ya cerrada.")
            return

        try:
            reopened = reopen_step(order_row, step_no, msg.from_user.full_name, reason)
            enqueue_evidencia_paso_row(
                id_orden,
                step_no,
                int(reopened["attempt"]),
                STEP_STATE_REABIERTO,
                msg.from_user.full_name,
                "",
                kind="EVID",
                bloqueado=0,
            )
            update_order(
                id_orden,
                phase=PHASE_MENU_EVID,
                current_step_no=step_no,
                pending_step_no=None,
                admin_pending=0,
                evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
            )
            route_row = get_route(int(order_row["id_ruta"]))
            if route_row:
                clear_route_lock(int(route_row["id_ruta"]))
            await context.bot.send_message(chat_id=msg.chat_id, text=f"🔄 Paso reabierto por administrador\nPaso: {step_name(step_no)}\nMotivo: {reason}")
            await show_evidence_menu(msg.chat_id, context, get_order(id_orden))
        except Exception as e:
            await context.bot.send_message(chat_id=msg.chat_id, text=f"⚠️ No pude reabrir el paso: {e}")
        return

    pending_change = pop_pending_input(msg.chat_id, msg.from_user.id, "CHANGE_STATE_REASON")
    if pending_change:
        reason = (msg.text or "").strip()
        if not reason:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía un texto válido como motivo.")
            set_pending_input(
                chat_id=msg.chat_id,
                user_id=msg.from_user.id,
                kind="CHANGE_STATE_REASON",
                id_ruta=int(pending_change["id_ruta"]) if pending_change["id_ruta"] is not None else None,
                id_orden=int(pending_change["id_orden"]) if pending_change["id_orden"] is not None else None,
                step_no=0,
                attempt=0,
                reply_to_message_id=int(pending_change["reply_to_message_id"]) if pending_change["reply_to_message_id"] is not None else None,
                tech_user_id=None,
            )
            return

        id_orden = int(pending_change["id_orden"])
        order_row = get_order(id_orden)
        if not order_row or order_row["status"] != ORDER_STATUS_OPEN:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Orden no válida o ya cerrada.")
            return

        change_type = (order_row["change_state_type"] or "").strip()
        if not change_type:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ No se encontró el tipo de cambio de estado.")
            return

        change_label = next((label for code, label in CHANGE_STATE_OPTIONS if code == change_type), change_type)
        request_id = create_change_state_request(id_orden, int(order_row["id_ruta"]), change_type, change_label, reason, msg.from_user.id, msg.from_user.full_name, msg.chat_id)

        update_order(
            id_orden,
            change_state_reason=reason,
            admin_pending=1,
            phase=PHASE_CHANGE_STATE_REVIEW,
            pending_step_no=None,
            current_step_no=None,
        )

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                "Solicitud de cambio de estado enviada.\n"
                f"• Tipo: {change_label}\n"
                f"• Motivo: {reason}\n\n"
                "Admins: validar con ✅ APROBADO o ❌ RECHAZADO"
            ),
            reply_markup=kb_change_state_review(request_id),
        )
        return

    pending_auth = pop_pending_input(msg.chat_id, msg.from_user.id, "AUTH_REJECT_REASON")
    if pending_auth:
        reason = (msg.text or "").strip()
        if not reason:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía un texto válido como motivo.")
            set_pending_input(
                chat_id=msg.chat_id,
                user_id=msg.from_user.id,
                kind="AUTH_REJECT_REASON",
                id_ruta=int(pending_auth["id_ruta"]) if pending_auth["id_ruta"] is not None else None,
                id_orden=int(pending_auth["id_orden"]) if pending_auth["id_orden"] is not None else None,
                step_no=int(pending_auth["step_no"]),
                attempt=int(pending_auth["attempt"]),
                reply_to_message_id=int(pending_auth["reply_to_message_id"]) if pending_auth["reply_to_message_id"] is not None else None,
                tech_user_id=int(pending_auth["tech_user_id"]) if pending_auth["tech_user_id"] is not None else None,
            )
            return

        id_orden = int(pending_auth["id_orden"])
        step_no = int(pending_auth["step_no"])
        attempt = int(pending_auth["attempt"])
        auth_step_no = -step_no

        order_db = get_order(id_orden)
        if not order_db or order_db["status"] != ORDER_STATUS_OPEN:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Orden no válida o ya cerrada.")
            return

        set_review(id_orden, auth_step_no, attempt, approved=0, reviewer_id=msg.from_user.id)
        set_reject_reason(id_orden, auth_step_no, attempt, reason, msg.from_user.id)
        enqueue_evidencia_paso_row(id_orden, step_no, attempt, STEP_STATE_RECHAZADO, msg.from_user.full_name, reason, kind="PERM")

        tech_id = int(pending_auth["tech_user_id"]) if pending_auth["tech_user_id"] is not None else None
        reply_to = int(pending_auth["reply_to_message_id"]) if pending_auth["reply_to_message_id"] is not None else None
        title = step_name(step_no)
        mention = mention_user_html(tech_id) if tech_id else "Técnico"

        update_order(
            id_orden,
            phase=PHASE_EVID_ACTION,
            pending_step_no=step_no,
            current_step_no=step_no,
            admin_pending=0,
            evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
        )
        route_row = get_route(int(order_db["id_ruta"]))
        if route_row:
            clear_route_lock(int(route_row["id_ruta"]))

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"❌ Autorización rechazada ({mention}).\n"
                f"📌 Paso: <b>{title}</b> (Intento {attempt})\n"
                f"📝 Motivo: {reason}\n\n"
                "El técnico puede volver a solicitar permiso o cargar foto."
            ),
            parse_mode="HTML",
            reply_to_message_id=reply_to if reply_to else None,
        )
        await context.bot.send_message(chat_id=msg.chat_id, text="Elige una opción:", reply_markup=kb_action_menu(id_orden, step_no))
        return

    pending_evid = pop_pending_input(msg.chat_id, msg.from_user.id, "EVID_REJECT_REASON")
    if pending_evid:
        reason = (msg.text or "").strip()
        if not reason:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía un texto válido como motivo.")
            set_pending_input(
                chat_id=msg.chat_id,
                user_id=msg.from_user.id,
                kind="EVID_REJECT_REASON",
                id_ruta=int(pending_evid["id_ruta"]) if pending_evid["id_ruta"] is not None else None,
                id_orden=int(pending_evid["id_orden"]) if pending_evid["id_orden"] is not None else None,
                step_no=int(pending_evid["step_no"]),
                attempt=int(pending_evid["attempt"]),
                reply_to_message_id=int(pending_evid["reply_to_message_id"]) if pending_evid["reply_to_message_id"] is not None else None,
                tech_user_id=int(pending_evid["tech_user_id"]) if pending_evid["tech_user_id"] is not None else None,
            )
            return

        id_orden = int(pending_evid["id_orden"])
        step_no = int(pending_evid["step_no"])
        attempt = int(pending_evid["attempt"])

        order_db = get_order(id_orden)
        if not order_db or order_db["status"] != ORDER_STATUS_OPEN:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Orden no válida o ya cerrada.")
            return

        set_review(id_orden, step_no, attempt, approved=0, reviewer_id=msg.from_user.id)
        set_reject_reason(id_orden, step_no, attempt, reason, msg.from_user.id)

        tech_id = int(pending_evid["tech_user_id"]) if pending_evid["tech_user_id"] is not None else None
        reply_to = int(pending_evid["reply_to_message_id"]) if pending_evid["reply_to_message_id"] is not None else None
        title = step_name(step_no)
        mention = mention_user_html(tech_id) if tech_id else "Técnico"

        enqueue_evidencia_paso_row(id_orden, step_no, attempt, STEP_STATE_RECHAZADO, msg.from_user.full_name, reason, kind="EVID")
        increment_order_review_counts(id_orden)
        update_order(
            id_orden,
            phase=PHASE_EVID_ACTION,
            pending_step_no=step_no,
            current_step_no=step_no,
            admin_pending=0,
            evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
        )
        route_row = get_route(int(order_db["id_ruta"]))
        if route_row:
            clear_route_lock(int(route_row["id_ruta"]))

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"❌ Evidencia rechazada - <b>{title}</b> ({mention}).\n"
                f"Intento: <b>{attempt}</b>\n"
                f"📝 Motivo: {reason}\n\n"
                "El técnico debe reenviar este paso."
            ),
            parse_mode="HTML",
            reply_to_message_id=reply_to if reply_to else None,
        )
        await context.bot.send_message(chat_id=msg.chat_id, text="Elige una opción:", reply_markup=kb_action_menu(id_orden, step_no))
        return

    route_row, order_row = current_route_and_order(msg.chat_id)
    if not route_row:
        return

    route_phase = (route_row["phase"] or "")

    if route_phase == PHASE_ROUTE_SELFIE:
        if not msg.photo:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Debes enviar una selfie en foto para continuar la ruta.")
            return

        ph = msg.photo[-1]
        update_route(
            int(route_row["id_ruta"]),
            route_selfie_file_id=ph.file_id,
            route_selfie_file_unique_id=ph.file_unique_id,
            route_selfie_message_id=msg.message_id,
            route_selfie_at=now_utc(),
            phase=PHASE_ROUTE_LOCATION,
        )
        enqueue_ruta_row(int(route_row["id_ruta"]))
        await context.bot.send_message(chat_id=msg.chat_id, text=prompt_route_location())
        return

    if not order_row:
        return

    phase = (order_row["phase"] or "")

    if phase == PHASE_VALIDATION_NAME:
        text = (msg.text or "").strip()
        if not text:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Ingresa un nombre válido.")
            return
        upsert_service_validation_draft(int(order_row["id_orden"]), int(order_row["id_ruta"]), msg.from_user.id, msg.from_user.full_name, name=text)
        sync_order_validation_fields(int(order_row["id_orden"]))
        update_order(int(order_row["id_orden"]), phase=PHASE_VALIDATION_PHONE)
        await context.bot.send_message(chat_id=msg.chat_id, text=prompt_validation_phone())
        return

    if phase == PHASE_VALIDATION_PHONE:
        text = (msg.text or "").strip()
        if not re.fullmatch(r"\d{9}", text):
            await context.bot.send_message(
                chat_id=msg.chat_id,
                text="⚠️ El número de la persona que valida debe tener exactamente 9 dígitos."
            )
            return

        val = get_latest_service_validation(int(order_row["id_orden"]))
        if not val:
            val = upsert_service_validation_draft(int(order_row["id_orden"]), int(order_row["id_ruta"]), msg.from_user.id, msg.from_user.full_name)
        update_service_validation(int(val["validation_id"]), validator_phone=text)
        sync_order_validation_fields(int(order_row["id_orden"]))
        update_order(int(order_row["id_orden"]), phase=PHASE_VALIDATION_RELATIONSHIP)
        await context.bot.send_message(chat_id=msg.chat_id, text=prompt_validation_relationship())
        return

    if phase == PHASE_VALIDATION_RELATIONSHIP:
        text = (msg.text or "").strip()
        if not text:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Ingresa un parentesco válido.")
            return
        val = get_latest_service_validation(int(order_row["id_orden"]))
        if not val:
            val = upsert_service_validation_draft(int(order_row["id_orden"]), int(order_row["id_ruta"]), msg.from_user.id, msg.from_user.full_name)
        update_service_validation(int(val["validation_id"]), validator_relationship=text)
        sync_order_validation_fields(int(order_row["id_orden"]))
        update_order(int(order_row["id_orden"]), phase=PHASE_VALIDATION_CONFIRM)

        val2 = get_latest_service_validation(int(order_row["id_orden"]))
        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=build_validation_summary(
                val2["validator_name"] or "",
                val2["validator_phone"] or "",
                val2["validator_relationship"] or "",
            ),
            reply_markup=kb_validation_confirm(int(order_row["id_orden"])),
        )
        return

    if phase in (PHASE_STEP_MEDIA, PHASE_AUTH_MEDIA):
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En este paso no se acepta texto. Envía el archivo según corresponda.")
        return

    if phase == PHASE_AUTH_TEXT_WAIT:
        step_no = int(order_row["pending_step_no"] or 0)
        if step_no <= 0:
            return

        ok, why = can_user_operate_current_route(route_row, msg.from_user.id)
        if not ok:
            await context.bot.send_message(chat_id=msg.chat_id, text=why)
            return

        text = (msg.text or "").strip()
        if not text:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía el texto de autorización.")
            return

        id_orden = int(order_row["id_orden"])
        auth_step_no = -step_no
        st = ensure_step_state(id_orden, auth_step_no, owner_user_id=msg.from_user.id, owner_name=msg.from_user.full_name)
        attempt = int(st["attempt"])
        set_step_owner(id_orden, auth_step_no, attempt, msg.from_user.id, msg.from_user.full_name)
        save_auth_text(id_orden, auth_step_no, attempt, text, msg.message_id)

        approval_required = get_approval_required(int(order_row["chat_id"]))
        if not approval_required:
            auto_approve_db_step(id_orden, auth_step_no, attempt)
            enqueue_evidencia_paso_row(id_orden, step_no, attempt, STEP_STATE_APROBADO, "APROBACION OFF", "", kind="PERM")
            update_order(id_orden, phase=PHASE_STEP_MEDIA, pending_step_no=step_no, current_step_no=step_no, admin_pending=0)
            clear_route_lock(int(route_row["id_ruta"]))
            await context.bot.send_message(chat_id=msg.chat_id, text="✅ Autorización aprobada automáticamente (APROBACION OFF).\n➡️ Continúa con la carga de foto del paso.")
            await context.bot.send_message(chat_id=msg.chat_id, text=prompt_media_step(step_no))
            return

        mark_submitted(id_orden, auth_step_no, attempt)
        update_order(id_orden, phase=PHASE_AUTH_REVIEW, pending_step_no=step_no, current_step_no=step_no, admin_pending=1)
        clear_route_lock(int(route_row["id_ruta"]))
        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"🔐 **Revisión de AUTORIZACIÓN (solo texto)**\n"
                f"Para: {step_name(step_no)}\n"
                f"Intento: {attempt}\n"
                f"Técnico: {order_row['technician_name'] or '-'}\n"
                f"Servicio: {order_row['service_type'] or '-'}\n"
                f"Abonado: {order_row['abonado_code'] or '-'}\n\n"
                f"Texto:\n{text}\n\n"
                "Admins: validar con ✅/❌"
            ),
            parse_mode="Markdown",
            reply_markup=kb_auth_review(id_orden, step_no, attempt),
        )
        return

    if int(order_row["step_index"] or 0) != 2:
        return

    text = (msg.text or "").strip()
    if not text:
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía el código de abonado como texto.")
        return

    update_order(
        int(order_row["id_orden"]),
        abonado_code=text,
        evidencias_estado=EVIDENCIAS_ESTADO_INCOMPLETAS,
        step_index=3,
        phase=PHASE_WAIT_LOCATION,
    )
    enqueue_orden_row(int(order_row["id_orden"]))
    await context.bot.send_message(chat_id=msg.chat_id, text=f"✅ Código de abonado registrado: {text}\n\n{prompt_step4()}")


# =========================
# Ubicación
# =========================
async def on_location(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

    route_row, order_row = current_route_and_order(msg.chat_id)
    if not route_row:
        return

    route_phase = (route_row["phase"] or "")

    if route_phase == PHASE_ROUTE_LOCATION:
        if not msg.location:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía tu ubicación usando 📎 → Ubicación → ubicación actual.")
            return

        id_ruta = int(route_row["id_ruta"])
        update_route(
            id_ruta,
            route_location_lat=msg.location.latitude,
            route_location_lon=msg.location.longitude,
            route_location_at=now_utc(),
            route_menu_enabled=1,
            phase=PHASE_ROUTE_MENU,
            status=ROUTE_STATUS_OPEN,
        )

        enqueue_ruta_row(id_ruta)
        await context.bot.send_message(chat_id=msg.chat_id, text="✅ Ruta iniciada correctamente.")
        await show_route_tasks_menu(msg.chat_id, context, get_route(id_ruta))
        return

    if not order_row:
        return

    if int(order_row["step_index"] or 0) != 3:
        return

    if not msg.location:
        await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Envía tu ubicación usando 📎 → Ubicación → ubicación actual.")
        return

    update_order(
        int(order_row["id_orden"]),
        location_lat=msg.location.latitude,
        location_lon=msg.location.longitude,
        location_at=now_utc(),
        step_index=4,
        phase=PHASE_WAIT_INSTALL_MODE,
        pending_step_no=None,
    )
    enqueue_orden_row(int(order_row["id_orden"]))

    await context.bot.send_message(chat_id=msg.chat_id, text=prompt_step5_install_mode(), reply_markup=kb_install_mode())


# =========================
# Carga de media
# =========================
async def on_media(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg: Message = update.effective_message
    if msg is None or msg.from_user is None:
        return

    route_row, order_row = current_route_and_order(msg.chat_id)
    if not route_row:
        return

    route_phase = (route_row["phase"] or "")

    if route_phase == PHASE_ROUTE_SELFIE:
        if not msg.photo:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ Debes enviar una selfie en foto para continuar la ruta.")
            return

        ph = msg.photo[-1]
        update_route(
            int(route_row["id_ruta"]),
            route_selfie_file_id=ph.file_id,
            route_selfie_file_unique_id=ph.file_unique_id,
            route_selfie_message_id=msg.message_id,
            route_selfie_at=now_utc(),
            phase=PHASE_ROUTE_LOCATION,
        )
        enqueue_ruta_row(int(route_row["id_ruta"]))
        await context.bot.send_message(chat_id=msg.chat_id, text=prompt_route_location())
        return

    if not order_row:
        if route_row:
            await context.bot.send_message(chat_id=msg.chat_id, text="ℹ️ Usa el menú para elegir Inicio de Orden antes de enviar evidencias.")
        return

    phase = (order_row["phase"] or "")
    id_orden = int(order_row["id_orden"])
    id_ruta = int(order_row["id_ruta"])
    pending_step_no = int(order_row["pending_step_no"] or 0)

    if phase in (PHASE_WAIT_DOC_TITULAR, PHASE_WAIT_DOC_ENCARGADO, PHASE_WAIT_DOC_CLIENTE):
        if not msg.photo:
            await context.bot.send_message(
                chat_id=msg.chat_id,
                text="⚠️ Solo se acepta foto del documento."
            )
            return

        if phase == PHASE_WAIT_DOC_TITULAR:
            doc_tipo = "CLIENTE"
        elif phase == PHASE_WAIT_DOC_ENCARGADO:
            doc_tipo = "ENCARGADO"
        else:
            doc_tipo = "CLIENTE"

        update_order(
            id_orden,
            phase=PHASE_DOC_REVIEW,
            admin_pending=1,
        )

        await context.bot.send_message(
            chat_id=msg.chat_id,
            text=(
                f"🔎 Revisión requerida - DOCUMENTO {doc_tipo}\n"
                f"Técnico: {order_row['technician_name'] or '-'}\n"
                f"Servicio: {order_row['service_type'] or '-'}\n"
                f"Abonado: {order_row['abonado_code'] or '-'}\n\n"
                "Admins: validar con ✅ APROBADO o ❌ RECHAZADO"
            ),
            reply_markup=kb_doc_review(id_orden, doc_tipo),
        )
        return

    if phase not in (PHASE_AUTH_MEDIA, PHASE_STEP_MEDIA):
        if int(order_row["step_index"] or 0) >= 4:
            await context.bot.send_message(chat_id=msg.chat_id, text="ℹ️ Usa el menú para elegir el paso antes de enviar archivos.")
        return

    ok, why = can_user_operate_current_route(route_row, msg.from_user.id)
    if not ok and "revisión" not in why.lower():
        await context.bot.send_message(chat_id=msg.chat_id, text=why)
        return

    if pending_step_no <= 0:
        return

    if phase == PHASE_STEP_MEDIA:
        if not msg.photo:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En este paso solo se aceptan FOTOS.")
            return
        file_type = "photo"
    else:
        if msg.photo:
            file_type = "photo"
        elif msg.video:
            file_type = "video"
        else:
            await context.bot.send_message(chat_id=msg.chat_id, text="⚠️ En PERMISO multimedia se aceptan FOTO o VIDEO.")
            return

    if phase == PHASE_AUTH_MEDIA:
        step_no_to_store = -pending_step_no
    else:
        step_no_to_store = pending_step_no

    st = ensure_step_state(id_orden, step_no_to_store, owner_user_id=msg.from_user.id, owner_name=msg.from_user.full_name)
    attempt = int(st["attempt"])
    set_step_owner(id_orden, step_no_to_store, attempt, msg.from_user.id, msg.from_user.full_name)
    lock_route(id_ruta, msg.from_user.id, msg.from_user.full_name)
    mark_evidencias_estado(id_orden, EVIDENCIAS_ESTADO_INCOMPLETAS)

    if int(st["submitted"] or 0) == 1 and st["approved"] is None:
        await context.bot.send_message(chat_id=msg.chat_id, text="⏳ Ya está en revisión. Espera validación del administrador.")
        return
    if st["approved"] is not None and int(st["approved"]) == 1:
        await context.bot.send_message(chat_id=msg.chat_id, text="✅ Ya está aprobado. Continúa con el menú.")
        return

    current = media_count(id_orden, step_no_to_store, attempt)
    if current >= MAX_MEDIA_PER_STEP:
        await context.bot.send_message(chat_id=msg.chat_id, text=f"⚠️ Ya llegaste al máximo de {MAX_MEDIA_PER_STEP}. Presiona ✅ EVIDENCIAS COMPLETAS.")
        return

    if file_type == "photo":
        ph = msg.photo[-1]
        file_id = ph.file_id
        file_unique_id = ph.file_unique_id
    else:
        vd = msg.video
        file_id = vd.file_id if vd else ""
        file_unique_id = vd.file_unique_id if vd else ""

    meta = {
        "from_user_id": msg.from_user.id,
        "from_username": msg.from_user.username,
        "from_name": msg.from_user.full_name,
        "date": msg.date.isoformat() if msg.date else None,
        "caption": msg.caption,
        "phase": phase,
        "step_pending": pending_step_no,
        "attempt": attempt,
        "file_type": file_type,
        "media_group_id": msg.media_group_id,
    }

    add_media(
        id_orden=id_orden,
        id_ruta=id_ruta,
        step_no=step_no_to_store,
        attempt=attempt,
        file_type=file_type,
        file_id=file_id,
        file_unique_id=file_unique_id,
        tg_message_id=msg.message_id,
        media_group_id=msg.media_group_id,
        meta=meta,
    )

    if phase != PHASE_AUTH_MEDIA:
        enqueue_evidencia_archivo_row(order_row, pending_step_no, attempt, file_id, file_unique_id, msg.message_id, file_type, msg.media_group_id)

    # Si Telegram envía varias fotos como álbum, cada foto llega como mensaje separado.
    # Para no responder una vez por cada foto, solo responde al último mensaje recibido.
    if msg.media_group_id:
        await asyncio.sleep(MEDIA_ACK_WINDOW_SECONDS)

        with db() as conn:
            row = conn.execute(
                """
                SELECT MAX(tg_message_id) AS last_msg_id
                FROM media
                WHERE id_orden=?
                    AND step_no=?
                    AND attempt=?
                    AND media_group_id=?
                """,
                (id_orden, step_no_to_store, attempt, msg.media_group_id),
            ).fetchone()

        last_msg_id = int(row["last_msg_id"] or 0) if row else 0

        if int(msg.message_id) != last_msg_id:
            return

    await asyncio.sleep(MEDIA_ACK_WINDOW_SECONDS)

    total = media_count(id_orden, step_no_to_store, attempt)
    remaining = max(0, MAX_MEDIA_PER_STEP - total)

    with db() as conn:
        row = conn.execute(
            """
            SELECT MAX(tg_message_id) AS last_msg_id
            FROM media
            WHERE id_orden=?
                AND step_no=?
                AND attempt=?
            """,
            (id_orden, step_no_to_store, attempt),
        ).fetchone()

    last_msg_id = int(row["last_msg_id"] or 0) if row else 0

    if int(msg.message_id) != last_msg_id:
        return

    if phase == PHASE_AUTH_MEDIA:
        controls_kb = kb_auth_media_controls(id_orden, abs(pending_step_no))
    else:
        controls_kb = kb_media_controls(id_orden, pending_step_no)

    if remaining <= 0:
        text = f"✅ Guardado ({total}/{MAX_MEDIA_PER_STEP}). Ya alcanzaste el máximo. Presiona ✅ EVIDENCIAS COMPLETAS."
    else:
        text = f"✅ Guardado ({total}/{MAX_MEDIA_PER_STEP}). Te quedan {remaining}."

    # Eliminar mensaje anterior de confirmación para dejar solo el último
    ack_key = f"media_ack_msg_{id_orden}_{step_no_to_store}_{attempt}"

    old_ack_msg_id = context.chat_data.get(ack_key)
    if old_ack_msg_id:
        await safe_delete_message(context, msg.chat_id, old_ack_msg_id)

    sent = await context.bot.send_message(
        chat_id=msg.chat_id,
        text=text,
        reply_markup=controls_kb
    )

    context.chat_data[ack_key] = sent.message_id


# =========================
# Error handler
# =========================
async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    log.exception("Error no manejado:", exc_info=context.error)


# =========================
# Main
# =========================
def main():
    if not BOT_TOKEN:
        raise RuntimeError("Falta BOT_TOKEN. Configura la variable BOT_TOKEN con el token de BotFather.")

    init_db()

    request = HTTPXRequest(connect_timeout=10, read_timeout=25, write_timeout=25, pool_timeout=10)
    app = Application.builder().token(BOT_TOKEN).request(request).build()

    app.add_handler(CommandHandler("start", start_cmd))
    app.add_handler(CommandHandler("id", id_cmd))
    app.add_handler(CommandHandler("inicio", inicio_cmd))
    app.add_handler(CommandHandler("cancelar", cancelar_cmd))
    app.add_handler(CommandHandler("estado", estado_cmd))
    app.add_handler(CommandHandler("aprobacion", aprobacion_cmd))
    app.add_handler(CommandHandler("reabrir", reabrir_cmd))
    app.add_handler(CommandHandler("reload_sheet", reload_sheet_cmd))

    app.add_handler(CallbackQueryHandler(on_callbacks))
    app.add_handler(MessageHandler(filters.LOCATION, on_location))
    app.add_handler(MessageHandler(filters.PHOTO | filters.VIDEO, on_media))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, on_text))
    app.add_error_handler(error_handler)

    try:
        sh = sheets_client()

        ws_ruta = sh.worksheet("RUTA")
        ws_tareas_ruta = sh.worksheet("TAREAS_RUTA")
        ws_ordenes = sh.worksheet("ORDENES")
        ws_evid_pasos = sh.worksheet("EVIDENCIAS_PASOS")
        ws_evid_arch = sh.worksheet("EVIDENCIAS_ARCHIVOS")
        ws_config = sh.worksheet("CONFIG")
        ws_tecnicos = sh.worksheet(TECNICOS_TAB)

        try:
            ws_change = sh.worksheet("CAMBIO_ESTADO")
        except Exception:
            ws_change = sh.add_worksheet(title="CAMBIO_ESTADO", rows=2000, cols=20)

        try:
            ws_valid = sh.worksheet("VALIDACIONES")
        except Exception:
            ws_valid = sh.add_worksheet(title="VALIDACIONES", rows=2000, cols=20)

        _ensure_headers(ws_ruta, RUTA_COLUMNS)
        _ensure_headers(ws_tareas_ruta, TAREAS_RUTA_COLUMNS)
        _ensure_headers(ws_ordenes, ORDENES_COLUMNS)
        _ensure_headers(ws_evid_pasos, EVIDENCIAS_PASOS_COLUMNS)
        _ensure_headers(ws_evid_arch, EVIDENCIAS_ARCHIVOS_COLUMNS)
        _ensure_headers(ws_config, CONFIG_COLUMNS)
        _ensure_headers(ws_tecnicos, TECNICOS_COLUMNS)
        _ensure_headers(ws_change, CAMBIO_ESTADO_COLUMNS)
        _ensure_headers(ws_valid, VALIDACIONES_COLUMNS)

        idx_ruta = build_index(ws_ruta, ["id_ruta"])
        idx_tareas_ruta = build_index(ws_tareas_ruta, ["id_ruta", "tipo_tarea", "fecha_inicio_tarea", "hora_inicio_tarea"])
        idx_ordenes = build_index(ws_ordenes, ["id_orden"])
        idx_evid_pasos = build_index(ws_evid_pasos, ["id_orden", "evidencias_numero", "attempt"])
        idx_evid_arch = build_index(ws_evid_arch, ["id_orden", "evidencias_numero", "attempt", "mensaje_telegram_id"])
        idx_change = build_index(ws_change, ["id_orden", "tipo_cambio", "fecha_registro", "hora_registro"])
        idx_valid = build_index(ws_valid, ["id_orden", "estado_validacion", "fecha_registro", "hora_registro"])

        app.bot_data["sheets_ready"] = True
        app.bot_data["sh"] = sh
        app.bot_data["ws_ruta"] = ws_ruta
        app.bot_data["ws_tareas_ruta"] = ws_tareas_ruta
        app.bot_data["ws_ordenes"] = ws_ordenes
        app.bot_data["ws_evid_pasos"] = ws_evid_pasos
        app.bot_data["ws_evid_arch"] = ws_evid_arch
        app.bot_data["ws_config"] = ws_config
        app.bot_data["ws_tecnicos"] = ws_tecnicos
        app.bot_data["ws_change"] = ws_change
        app.bot_data["ws_valid"] = ws_valid

        app.bot_data["idx_ruta"] = idx_ruta
        app.bot_data["idx_tareas_ruta"] = idx_tareas_ruta
        app.bot_data["idx_ordenes"] = idx_ordenes
        app.bot_data["idx_evid_pasos"] = idx_evid_pasos
        app.bot_data["idx_evid_arch"] = idx_evid_arch
        app.bot_data["idx_change"] = idx_change
        app.bot_data["idx_valid"] = idx_valid

        load_tecnicos_cache(app)

        if app.job_queue:
            app.job_queue.run_repeating(sheets_worker, interval=60, first=10)
            app.job_queue.run_repeating(refresh_config_jobs, interval=60, first=15)

        log.info("Sheets: conectado. Worker iniciado. Cache TECNICOS habilitado.")
    except Exception as e:
        app.bot_data["sheets_ready"] = False
        log.warning(f"Sheets deshabilitado: {e}")

    log.info("Bot corriendo en PC...")
    app.run_polling(close_loop=False)


if __name__ == "__main__":
    main()
