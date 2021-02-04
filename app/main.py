import os
from flask import Flask, jsonify, Response, abort
from flask_sqlalchemy import SQLAlchemy
from google.cloud import secretmanager


PORT = os.environ.get("PORT", 8080)
PROJECT_ID = os.environ['PROJECT_ID']
SECRET_ID = os.environ['SECRET_ID']
VERSION_ID = os.environ['VERSION_ID']
TABLE_SCHEMA = 'public'
TABLE_NAME = 'performance'


DB_URI = (secretmanager
    .SecretManagerServiceClient()
    .access_secret_version(
        request={
            "name": f"projects/{PROJECT_ID}/secrets/{SECRET_ID}/versions/{VERSION_ID}"
        }
    ).payload.data.decode("UTF-8")
)

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = DB_URI
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db = SQLAlchemy(app)

@app.errorhandler(404)
def not_found(e):
    return jsonify(error=str(e)), 404

@app.errorhandler(400)
def bad_request(e):
    return jsonify(error=str(e)), 400

@app.errorhandler(500)
def internal_server_error(e):
    return jsonify(error=str(e)), 500


class Performance(db.Model):
    __tablename__ = TABLE_NAME
    __table_args__ = {"schema": TABLE_SCHEMA}
    lookup = db.Column(db.Text, primary_key=True)
    response = db.Column(db.Text)

@app.route("/api/v1/vehicles/<string:lookup>")
def get_performance(lookup):
    if len(lookup) not in (20, 6):
        abort(400, description='Provide a valid dott vehicle ID (20 characters) or QR-code (6 characters).')
    performance = db.session.query(Performance.response).filter(Performance.lookup == lookup).scalar()
    if performance:
        return Response(performance, mimetype='application/json')
    else:
        abort(404, description='No data available on this vehicle.')


if __name__ == "__main__":
    app.run(
        debug=True,
        host="0.0.0.0",
        port=int(PORT))
