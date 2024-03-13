from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
import json

from app import redis_client
from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate) -> models.Sensor:
    db_sensor = models.Sensor(name=sensor.name, latitude=sensor.latitude, longitude=sensor.longitude)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    return db_sensor

def record_data(redis: redis_client, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    sensorData = json.dumps(data.dict())
    return redis.set(sensor_id, sensorData)

def get_data(redis: redis_client.RedisClient, sensor_id: int, db:Session) -> schemas.Sensor:
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    sensorDataDB = redis.get(sensor_id)
    sensor_data = json.loads(sensorDataDB.decode())
    sensor_data['id'] = db_sensor.id
    sensor_data['name'] = db_sensor.name
    return sensor_data

def delete_sensor(db: Session, sensor_id: int):
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor