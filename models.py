from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    Boolean,
    Float,
    TIMESTAMP,
    JSON,
)
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Record(Base):
    __tablename__ = "records"

    # Use Integer primary key for compatibility with SQLite (ROWID)
    id = Column(Integer, primary_key=True, autoincrement=True)

    phone = Column(String(20), nullable=False, unique=True)
    e164_phone = Column(String(20))

    # use Integer for timestamp_ms for portability with sqlite during local testing
    timestamp_ms = Column(Integer)
    batch_id = Column(Text)
    uid = Column(Text)

    db1_success = Column(Boolean)
    db1_confidence = Column(Float)
    db1_latency_ms = Column(Integer)

    name = Column(Text)
    operator = Column(Text)
    phone_type = Column(Text)
    website = Column(Text)

    address = Column(Text)
    result_loc = Column(Text)
    belong_area = Column(Text)

    city = Column(Text)
    state = Column(Text)
    country = Column(Text)

    is_pune = Column(Boolean, default=False)
    city_category = Column(Text)

    raw_json = Column(JSON)

    created_at = Column(TIMESTAMP)
