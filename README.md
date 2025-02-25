# Deploying Your MVP on Railway.app

This guide will help you deploy your University Tracker MVP on Railway.app quickly and efficiently.

## Prerequisites

1. Create a Railway.app account at https://railway.app
2. Install the Railway CLI (optional):
   ```
   npm i -g @railway/cli
   ```
3. Make sure your code is in a GitHub repository

## Deployment Steps

### 1. Create the Required Files

Make sure you have all the updated files in your repository:

- `Procfile` - Defines processes for Railway to run 
- `railway.toml` - Railway-specific configuration
- `requirements.txt` - All Python dependencies

### 2. Deploy from the Railway Dashboard

1. Log in to Railway.app
2. Click "New Project"
3. Select "Deploy from GitHub repo"
4. Connect your GitHub account if not already connected
5. Select your repository
6. Railway will automatically detect the Python project and prepare to deploy

### 3. Set Up Environment Variables

You'll need to set the following environment variables in the Railway dashboard:

```
MONGODB_URI=mongodb+srv://your-mongodb-connection-string
OPENAI_API_KEY=your-openai-api-key
PINECONE_API_KEY=your-pinecone-api-key
COHERE_API_KEY=your-cohere-api-key
INDEX_NAME=uni-search
JWT_SECRET=your-jwt-secret
RAZORPAY_KEY_ID=your-razorpay-key-id
RAZORPAY_KEY_SECRET=your-razorpay-key-secret
```

### 4. Add Redis Service

1. From your project dashboard, click "New"
2. Select "Redis"
3. Railway will automatically add Redis to your project and connect it with variables like `REDIS_URL`

### 5. Start the Worker Process

1. In your project settings, go to the "Settings" tab
2. Scroll down to "Service Commands" section
3. Add a new command for the worker:
   ```
   celery -A app.celery worker --loglevel=info
   ```
4. Save changes

### 6. Verify Deployment

1. Once deployed, Railway will provide you with a project URL
2. Test the API by making a request to `https://your-app-url/auth/verify`
3. Check the logs in Railway dashboard to ensure both the web and worker processes are running

### Troubleshooting

If you encounter issues:

1. Check the logs in the Railway dashboard
2. Verify environment variables are set correctly
3. Make sure MongoDB and Redis connections are working

For Redis-specific issues:
```python
# Add this to temporarily test Redis in your app
@app.route('/api/healthcheck', methods=['GET'])
def healthcheck():
    try:
        # Test Redis connection
        redis_client.set('test_key', 'test_value')
        redis_value = redis_client.get('test_key')
        
        return jsonify({
            'status': 'ok',
            'redis': redis_value == 'test_value',
            'time': datetime.utcnow().isoformat()
        })
    except Exception as e:
        return jsonify({
            'status': 'error',
            'message': str(e)
        }), 500
```

## Updating Your Frontend

Make sure your frontend points to the Railway URL:

```
NEXT_PUBLIC_API_URL=https://your-railway-app-url
```

## Monitoring & Scaling

Railway's dashboard provides monitoring tools. For MVP testing, the free tier should be sufficient, and you can scale up resources as needed.

## Post-Deployment

After confirming everything works:

1. Test the WebSocket connections with browser console: 
   ```javascript
   const socket = io('https://your-railway-app-url');
   socket.on('connect', () => console.log('Connected'));
   socket.on('university_update', data => console.log('Update:', data));
   ```

2. Test the full flow by adding a university and verifying status updates automatically

Remember that Railway's free tier has limited resources, which is perfect for MVP testing but may need upgrades as your user base grows.