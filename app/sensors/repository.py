from fastapi import HTTPException
from sqlalchemy.orm import Session
from typing import Any, Dict, List, Optional
import json

from app import redis_client
from app.mongodb_client import MongoDBClient
from . import models, schemas

def get_sensor(db: Session, sensor_id: int) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()

def get_sensor_by_name(db: Session, name: str) -> Optional[models.Sensor]:
    return db.query(models.Sensor).filter(models.Sensor.name == name).first()

def get_sensors(db: Session, skip: int = 0, limit: int = 100) -> List[models.Sensor]:
    return db.query(models.Sensor).offset(skip).limit(limit).all()

def create_sensor(db: Session, sensor: schemas.SensorCreate, mongodb_client: MongoDBClient) -> models.Sensor:
    """
    Create a new sensor dividing attriutes in both PostgreSQL and MongoDB.

    Args:
        db (Session): Database session.
        sensor (schemas.SensorCreate): Data of the sensor to be created.
        mongodb_client (MongoDBClient): MongoDB client instance.

    Returns:
        models.Sensor: The created sensor object.
    """
    # SQL
    db_sensor = models.Sensor(name=sensor.name)
    db.add(db_sensor)
    db.commit()
    db.refresh(db_sensor)
    
    #Mongo
    document_sensor_data = {
        "id_sensor": db_sensor.id,
        "type": sensor.type,
        "mac_address": sensor.mac_address,
        "manufacturer": sensor.manufacturer,
        "model": sensor.model,
        "serie_number": sensor.serie_number,
        "firmware_version": sensor.firmware_version,
        # referenced from MongoDB geospatial queries documentation
        "location": {
            "type": "Point",
            "coordinates": [sensor.longitude, sensor.latitude]
        }
    }

    mongodb_client.getDatabase("MongoDB_")
    mongodb_collection = mongodb_client.getCollection("sensors")
    mongodb_collection.insert_one(document_sensor_data)

    return db_sensor

def record_data(redis: redis_client, sensor_id: int, data: schemas.SensorData) -> schemas.Sensor:
    """
    Record data for a sensor in Redis.

    Args:
        redis (RedisClient): Redis client instance.
        sensor_id (int): ID of the sensor.
        data (schemas.SensorData): Data to be recorded for the sensor.

    Returns:
        schemas.Sensor: The recorded sensor data.
    """
    sensorData = json.dumps(data.dict())
    return redis.set(f"sensor:{sensor_id}:data", sensorData)

def get_data(redis: redis_client.RedisClient, sensor_id: int, db:Session) -> schemas.Sensor:
    """
    Retrieve data for a sensor from Redis.

    Args:
        redis (RedisClient): Redis client instance.
        sensor_id (int): ID of the sensor.
        db (Session): Database session.

    Returns:
        schemas.Sensor: The retrieved sensor data.
    """
    db_sensor = get_sensor(db, sensor_id)
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    sensorDataDB = redis.get( f"sensor:{sensor_id}:data")
    sensor_data = json.loads(sensorDataDB.decode())
    sensor_data['id'] = db_sensor.id
    sensor_data['name'] = db_sensor.name
    return sensor_data

def get_sensors_near(mongodb_client: MongoDBClient,  db:Session, redis:redis_client.RedisClient,  latitude: float, longitude: float, radius: int):

    """
    Get a list of sensors near a given location within a specified radius.

    Args:
        mongodb_client (MongoDBClient): MongoDB client instance.
        db (Session): Database session.
        redis (RedisClient): Redis client instance.
        latitude (float): Latitude of the location.
        longitude (float): Longitude of the location.
        radius (int): Radius in meters.

    Returns:
        List[Dict[str, Any]]: A list of sensors near the specified location.
    """
    mongodb_client.getDatabase("MongoDB_")
    collection = mongodb_client.getCollection("sensors")
    #enable geospatial queries
    collection.create_index([("location", "2dsphere")])
    nearby_sensors = list(collection.find(
        {
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [longitude, latitude]
                },
                "$maxDistance": radius
            }
        }
    }
    ))
    #print("Nearby are: ", nearby_sensors)
    sensors = []
    for doc in nearby_sensors:
        doc["_id"] = str(doc["_id"])
        sensor = get_sensor(db=db, sensor_id=doc["id_sensor"]).__dict__
        sensor_redis = get_data(redis=redis, sensor_id=doc["id_sensor"], db=db)
        if sensor is not None:
            sensor = {**sensor, **sensor_redis} 
            sensors.append(sensor)
    return sensors if sensors else []

def delete_sensor(db: Session, sensor_id: int, redis: redis_client, mongodb_client: MongoDBClient):
    """
    Delete a sensor from postgreSQL, Redis, and MongoDB.
    """
    # delete from redis
    redis.delete(f"sensor:{sensor_id}:data")
    # delete from 
    mongodb_client.getDatabase('MongoDB_')
    mongodb_client.getCollection('sensors')
    mongodb_client.collection.delete_one({"id_sensor": sensor_id})
    # delete from posgreSQL
    db_sensor = db.query(models.Sensor).filter(models.Sensor.id == sensor_id).first()
    if db_sensor is None:
        raise HTTPException(status_code=404, detail="Sensor not found")
    db.delete(db_sensor)
    db.commit()
    return db_sensor
