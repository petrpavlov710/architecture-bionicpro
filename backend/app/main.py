import os
from typing import List, Optional
from datetime import date, datetime

import httpx
from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt
from clickhouse_connect import get_client
from clickhouse_connect.driver.exceptions import DatabaseError, OperationalError
from fastapi.middleware.cors import CORSMiddleware

from .shemas import ReportRow


def get_env(name: str, default: Optional[str] = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f'Missing required env var: {name}')
    return value


CLICKHOUSE_HOST = get_env('CLICKHOUSE_HOST', 'localhost')
CLICKHOUSE_PORT = int(get_env('CLICKHOUSE_PORT', '8123'))
CLICKHOUSE_USER = get_env('CLICKHOUSE_USER', 'default')
CLICKHOUSE_PASSWORD = get_env('CLICKHOUSE_PASSWORD', '')
CLICKHOUSE_DATABASE = get_env('CLICKHOUSE_DATABASE', 'telemetry')


app = FastAPI()


def get_clickhouse_client():
    return get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE,
    )


def get_clickhouse_watermark_date() -> Optional[date]:
    try:
        ch = get_clickhouse_client()
        res = ch.query("""
            SELECT coalesce(
                (SELECT last_processed_date FROM analytics.processed_watermark WHERE id = 1),
                (SELECT coalesce(max(usage_date), toDate('1970-01-01')) FROM analytics.fact_daily_telemetry)
            ) AS last_processed_date
        """)
        return res.result_rows[0][0] if res.result_rows else None
    except (DatabaseError, OperationalError):
        return None


frontend_origin = os.getenv('FRONTEND_ORIGIN', 'http://localhost:3000')
app.add_middleware(
    CORSMiddleware,
    allow_origins=[frontend_origin],
    allow_credentials=True,
    allow_methods=['*'],
    allow_headers=['Authorization', 'Content-Type'],
)


security = HTTPBearer(auto_error=True)
_jwks_cache: dict | None = None
_jwks_kid_to_key: dict[str, dict] | None = None


def get_jwks() -> dict:
    global _jwks_cache, _jwks_kid_to_key
    if _jwks_cache is not None and _jwks_kid_to_key is not None:
        return _jwks_cache
    jwks_url = get_env('KEYCLOAK_JWKS_URL')
    with httpx.Client(timeout=10) as client:
        resp = client.get(jwks_url)
        resp.raise_for_status()
        _jwks_cache = resp.json()
        _jwks_kid_to_key = {k['kid']: k for k in _jwks_cache.get('keys', [])}
        return _jwks_cache


def verify_jwt_and_get_claims(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    token = credentials.credentials
    jwks = get_jwks()
    kid_to_key = {k['kid']: k for k in jwks.get('keys', [])}
    unverified_header = jwt.get_unverified_header(token)
    kid = unverified_header.get('kid')
    if not kid or kid not in kid_to_key:
        raise HTTPException(status_code=401, detail='Invalid token header')
    key = kid_to_key[kid]
    issuer = get_env('KEYCLOAK_ISSUER')
    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=[key.get('alg', 'RS256')],
            audience=None,
            issuer=issuer,
            options={'verify_aud': False}
        )
        return claims
    except Exception as exc:
        raise HTTPException(status_code=401, detail=f'Token verification failed: {exc}')


@app.get('/health')
def health() -> dict:
    return {'status': 'ok'}


@app.get('/reports', response_model=List[ReportRow])
def get_reports(
    customer_id: Optional[int] = Query(default=None),
    device_id: Optional[str] = Query(default=None),
    start_date: Optional[str] = Query(default=None, description='YYYY-MM-DD'),
    end_date: Optional[str] = Query(default=None, description='YYYY-MM-DD'),
    limit: int = Query(default=100, ge=1, le=1000),
    claims: dict = Depends(verify_jwt_and_get_claims),
) -> List[ReportRow]:
    email_claim = claims.get('email') or claims.get('preferred_username')
    if not email_claim:
        raise HTTPException(status_code=403, detail='Email claim is required')

    filters = []
    params = {}
    filters.append('email = {email:String}')
    params['email'] = email_claim
    if customer_id is not None:
        filters.append('customer_id = {customer_id:Int32}')
        params['customer_id'] = int(customer_id)
    if device_id is not None:
        filters.append('device_id = {device_id:String}')
        params['device_id'] = device_id

    parsed_start: Optional[date] = None
    parsed_end: Optional[date] = None
    if start_date:
        try:
            parsed_start = datetime.strptime(start_date, '%Y-%m-%d').date()
        except ValueError:
            raise HTTPException(status_code=422, detail='Invalid start_date format. Use YYYY-MM-DD')
    if end_date:
        try:
            parsed_end = datetime.strptime(end_date, '%Y-%m-%d').date()
        except ValueError:
            raise HTTPException(status_code=422, detail='Invalid end_date format. Use YYYY-MM-DD')

    today = date.today()
    if parsed_end is None:
        parsed_end = today
    if parsed_start is None:
        parsed_start = parsed_end.replace(day=1) if parsed_end.day > 1 else parsed_end

    if parsed_start > parsed_end:
        raise HTTPException(status_code=422, detail='start_date cannot be after end_date')

    last_processed = get_clickhouse_watermark_date()
    if last_processed is None:
        return []

    if parsed_start > last_processed:
        return []

    if parsed_end > last_processed:
        parsed_end = last_processed

    if parsed_start > parsed_end:
        return []


    filters.append('usage_date BETWEEN {start_date:Date} AND {end_date:Date}')
    params['start_date'] = parsed_start
    params['end_date'] = parsed_end

    where_clause = ' AND '.join(filters)

    ch = get_clickhouse_client()
    query = f"""
        SELECT customer_id, device_id, email, first_name, last_name,
               usage_date, steps_sum, avg_battery, load_avg
        FROM analytics.v_customer_daily_report
        WHERE {where_clause}
        ORDER BY usage_date DESC
        LIMIT {{limit:UInt32}}
    """
    params['limit'] = int(limit)
    try:
        data = ch.query(query, parameters=params)
        rows = data.result_rows
    except (DatabaseError, OperationalError):
        raise HTTPException(status_code=409, detail='Reports are not available yet. Please try later.')

    return [
        ReportRow(
            customer_id=r[0],
            device_id=r[1],
            email=r[2],
            first_name=r[3],
            last_name=r[4],
            usage_date=str(r[5]) if r[5] is not None else "",
            steps_sum=r[6],
            avg_battery=float(r[7]) if r[7] is not None else None,
            load_avg=float(r[8]) if r[8] is not None else None,
        )
        for r in rows
    ]


@app.get('/reports/clickhouse')
def get_clickhouse_raw(
    device_id: Optional[str] = Query(default=None),
    start_ts: Optional[str] = Query(default=None, description='YYYY-MM-DD'),
    end_ts: Optional[str] = Query(default=None, description='YYYY-MM-DD'),
    limit: int = Query(default=1000, ge=1, le=10000),
    claims: dict = Depends(verify_jwt_and_get_claims),
):
    email_claim = claims.get('email') or claims.get('preferred_username')
    if not email_claim:
        raise HTTPException(status_code=403, detail='Email claim is required')

    parsed_start: Optional[date] = None
    parsed_end: Optional[date] = None
    if start_ts:
        try:
            parsed_start = datetime.strptime(start_ts, '%Y-%m-%d').date()
        except ValueError:
            raise HTTPException(status_code=422, detail='Invalid start_ts format. Use YYYY-MM-DD')
    if end_ts:
        try:
            parsed_end = datetime.strptime(end_ts, '%Y-%m-%d').date()
        except ValueError:
            raise HTTPException(status_code=422, detail='Invalid end_ts format. Use YYYY-MM-DD')

    today = date.today()
    if parsed_end is None:
        parsed_end = today
    if parsed_start is None:
        parsed_start = parsed_end.replace(day=1) if parsed_end.day > 1 else parsed_end
    if parsed_start > parsed_end:
        raise HTTPException(status_code=422, detail='start_ts cannot be after end_ts')

    last_processed = get_clickhouse_watermark_date()
    if last_processed is not None and parsed_start > last_processed:
        raise HTTPException(
            status_code=422,
            detail=f'Requested start_ts exceeds processed data ({last_processed}).'
        )
    if last_processed is not None and parsed_end > last_processed:
        parsed_end = last_processed

    ch = get_clickhouse_client()

    conditions = [
        'email = {email:String}',
        'event_ts >= toDateTime({start_dt:DateTime})',
        'event_ts < toDateTime({end_dt:DateTime}) + INTERVAL 1 DAY',
    ]
    params = {
        'email': email_claim,
        'start_dt': f'{parsed_start.strftime("%Y-%m-%d")} 00:00:00',
        'end_dt': f'{parsed_end.strftime("%Y-%m-%d")} 23:59:59',
    }
    if device_id:
        conditions.append('device_id = {device_id:String}')
        params['device_id'] = device_id

    where = ' AND '.join(conditions)
    query = f"""
        SELECT
          event_ts,
          device_id,
          email,
          steps,
          battery_level,
          load_kg
        FROM sensor_events
        WHERE {where}
        ORDER BY event_ts DESC
        LIMIT {{limit:UInt32}}
    """
    params['limit'] = int(limit)

    try:
        data = ch.query(query, parameters=params)
        columns = data.column_names
        rows = [dict(zip(columns, row)) for row in data.result_rows]
        return rows
    except (DatabaseError, OperationalError):
        raise HTTPException(status_code=409, detail='Raw telemetry is not available yet. Please try later.')
