from flask_socketio import SocketIO

socketio = SocketIO(
    cors_allowed_origins="*",
    logger=True,
    engineio_logger=True,
    ping_timeout=60
)

def emit_university_update(university_id: str, update_type: str, data: dict):
    try:
        socketio.emit('university_update', {
            'id': university_id,
            'type': update_type,
            **data
        })
    except Exception as e:
        print(f"Socket emission error: {str(e)}")