web: gunicorn --worker-class eventlet -w 1 app:app
worker: celery -A app.celery worker --loglevel=info