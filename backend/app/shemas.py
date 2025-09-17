from pydantic import BaseModel


class ReportRow(BaseModel):
    customer_id: int
    device_id: str
    email: str
    first_name: str
    last_name: str
    usage_date: str
    steps_sum: int | None = None
    avg_battery: float | None = None
    load_avg: float | None = None
