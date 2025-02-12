from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
import click
from datetime import datetime
import json

def setup_indexes(db):
    # Users collection
    db.users.create_index([('email', ASCENDING)], unique=True)
    db.users.create_index([('is_premium', ASCENDING)])
    db.users.create_index([('is_admin', ASCENDING)])
    
    # Universities collection
    db.universities.create_index([('url', ASCENDING)], unique=True)
    db.universities.create_index([('programs', ASCENDING)])
    db.universities.create_index([('metadata.name', TEXT)])
    
    # User data collection
    db.user_data.create_index([
        ('user_email', ASCENDING),
        ('university_url', ASCENDING)
    ], unique=True)

def cleanup_expired_subscriptions(db):
    now = datetime.utcnow()
    result = db.users.update_many(
        {
            'is_premium': True,
            'subscription.expiry': {'$lt': now}
        },
        {
            '$set': {
                'is_premium': False,
                'subscription.status': 'expired'
            }
        }
    )
    return result.modified_count

@click.group()
def cli():
    pass

@cli.command()
@click.option('--uri', default='mongodb://localhost:27017/', help='MongoDB URI')
@click.option('--db-name', default='university_tracker', help='Database name')
def init(uri, db_name):
    """Initialize database with required indexes"""
    client = MongoClient(uri)
    db = client[db_name]
    setup_indexes(db)
    click.echo('Indexes created successfully')

@cli.command()
@click.option('--uri', default='mongodb://localhost:27017/', help='MongoDB URI')
@click.option('--db-name', default='university_tracker', help='Database name')
def cleanup(uri, db_name):
    """Cleanup expired subscriptions"""
    client = MongoClient(uri)
    db = client[db_name]
    count = cleanup_expired_subscriptions(db)
    click.echo(f'Cleaned up {count} expired subscriptions')

@cli.command()
@click.option('--uri', default='mongodb://localhost:27017/', help='MongoDB URI')
@click.option('--db-name', default='university_tracker', help='Database name')
@click.argument('backup_file')
def backup(uri, db_name, backup_file):
    """Backup database to a file"""
    client = MongoClient(uri)
    db = client[db_name]
    
    backup_data = {
        'users': list(db.users.find({}, {'_id': False})),
        'universities': list(db.universities.find({}, {'_id': False})),
        'user_data': list(db.user_data.find({}, {'_id': False}))
    }
    
    with open(backup_file, 'w') as f:
        json.dump(backup_data, f)
    
    click.echo(f'Backup saved to {backup_file}')

@cli.command()
@click.option('--uri', default='mongodb://localhost:27017/', help='MongoDB URI')
@click.option('--db-name', default='university_tracker', help='Database name')
@click.argument('backup_file')
def restore(uri, db_name, backup_file):
    """Restore database from a backup file"""
    client = MongoClient(uri)
    db = client[db_name]
    
    with open(backup_file, 'r') as f:
        backup_data = json.load(f)
    
    for collection, data in backup_data.items():
        if data:
            db[collection].delete_many({})
            db[collection].insert_many(data)
    
    click.echo('Database restored successfully')

if __name__ == '__main__':
    cli()