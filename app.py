# Local development runner
# For production, use: gunicorn wsgi:app

if __name__ == "__main__":
    from wsgi import app
    import os
    debug_mode = os.environ.get("FLASK_ENV") != "production"
    app.run(debug=debug_mode, host="127.0.0.1", port=5000)
