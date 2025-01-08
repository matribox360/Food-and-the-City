from flask_sqlalchemy import SQLAlchemy
import os

db = SQLAlchemy()

# Configurations for Flask
#DATABASE_URI = "postgresql+psycopg2://root:root@localhost:5432/food-and-the-city"
# Use environment variables for the database connection string
DATABASE_URI = os.getenv("DATABASE_URL", "postgresql+psycopg2://root:root@db:5432/food-and-the-city")


def init_app(app):
    app.config["SQLALCHEMY_DATABASE_URI"] = DATABASE_URI
    app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
    db.init_app(app)
