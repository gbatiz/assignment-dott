import os
from flask import Flask, jsonify, Response, abort, redirect
from flask_sqlalchemy import SQLAlchemy
from google.cloud import secretmanager


PORT = os.environ.get("PORT", 8080)
PROJECT_ID = os.environ['PROJECT_ID']
SECRET_ID = os.environ['SECRET_ID']
VERSION_ID = os.environ['VERSION_ID']

TABLE_SCHEMA_PERFORMANCE = 'public'
TABLE_NAME_PERFORMANCE = 'performance'

TABLE_SCHEMA_QRCODE = 'public'
TABLE_NAME_QRCODE = 'qr_code'

SUPERSET_REDIRECT_URL = os.environ['SUPERSET_REDIRECT_URL']


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


class Performance(db.Model):
    __tablename__ = TABLE_NAME_PERFORMANCE
    __table_args__ = {"schema": TABLE_SCHEMA_PERFORMANCE}
    lookup = db.Column(db.Text, primary_key=True)
    response = db.Column(db.Text)

class QRCode(db.Model):
    __tablename__ = TABLE_NAME_QRCODE
    __table_args__ = {"schema": TABLE_SCHEMA_QRCODE}
    qr_code = db.Column(db.Text, primary_key=True)
    vehicle_id = db.Column(db.Text)


@app.route("/api/v1/vehicles/<string:lookup>")
def get_performance(lookup):
    if len(lookup) not in (20, 6):
        return jsonify({'error': '400 Bad Request: Provide a valid dott vehicle ID (20 characters) or QR-code (6 characters).'}), 400
    performance = db.session.query(Performance.response).filter(Performance.lookup == lookup).scalar()
    if performance:
        return Response(performance, mimetype='application/json')
    else:
        return jsonify({'error': '404 Not Found: No data available on this vehicle.'}), 404

@app.route("/report/v1/vehicles/<string:lookup>")
def redirect_to_report(lookup):
    if len(lookup) == 20:
        vehicle_id = db.session.query(QRCode.vehicle_id).filter(QRCode.vehicle_id == lookup).scalar()
    elif len(lookup) == 6:
        vehicle_id = db.session.query(QRCode.vehicle_id).filter(QRCode.qr_code == lookup).scalar()
    else:
        abort(400, description='Provide a valid dott vehicle ID (20 characters) or QR-code (6 characters).')
    if vehicle_id:
        return redirect(
            location=SUPERSET_REDIRECT_URL+'?standalone=true&preselect_filters={"2": {"vehicle_id": "'+vehicle_id+'"}}',
            code=308
        )
    else:
        abort(404, description='No data available on this vehicle.')


if __name__ == "__main__":
    app.run(
        debug=True,
        host="0.0.0.0",
        port=int(PORT))
