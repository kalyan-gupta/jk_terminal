import os
import sys
import subprocess
import secrets
import string

def setup_env():
    """Generate a .env file if it doesn't exist."""
    env_path = os.path.join(os.getcwd(), '.env')
    if not os.path.exists(env_path):
        print("🪄 Generating .env file...")
        
        # Generate random SECRET_KEY
        chars = string.ascii_letters + string.digits + string.punctuation.replace('"', '').replace("'", "").replace('\\', '')
        secret_key = ''.join(secrets.choice(chars) for _ in range(50))
        
        # Generate random ENCRYPTION_KEY
        encryption_key = secrets.token_urlsafe(32)
        
        with open(env_path, 'w') as f:
            f.write(f'SECRET_KEY="{secret_key}"\n')
            f.write(f'ENCRYPTION_KEY="{encryption_key}"\n')
            f.write(f'DEBUG="True"\n')
        print("✅ .env file created with new SECRET_KEY, ENCRYPTION_KEY, and DEBUG=True.")

def ensure_venv():
    """Ensure a virtual environment exists and is being used."""
    venv_dir = os.path.join(os.getcwd(), 'venv')
    
    # Determine the path to the python executable in the venv
    if os.name == 'nt': # Windows
        python_exe = os.path.join(venv_dir, 'Scripts', 'python.exe')
    else: # Linux/Mac
        python_exe = os.path.join(venv_dir, 'bin', 'python')

    if not os.path.exists(venv_dir):
        print("🐍 Creating virtual environment...")
        subprocess.run([sys.executable, "-m", "venv", "venv"], check=True)
        
        print("📦 Installing dependencies from requirements.txt...")
        subprocess.run([python_exe, "-m", "pip", "install", "--upgrade", "pip"], check=True)
        subprocess.run([python_exe, "-m", "pip", "install", "-r", "requirements.txt"], check=True)
        print("✅ Environment setup complete.")

    # If we are not running from the venv, restart the script using the venv python
    if os.path.abspath(sys.executable) != os.path.abspath(python_exe):
        print("🔄 Switching to virtual environment...")
        if os.name == 'nt':
            subprocess.call([python_exe] + sys.argv)
            sys.exit()
        else:
            os.execv(python_exe, [python_exe] + sys.argv)

def run():
    """Wrapper to run the production ASGI server (Daphne)."""
    print("🚀 Starting JK Terminal (Production Mode)...")
    
    # Ensure venv exists and is used
    ensure_venv()
    
    # Setup environment variables
    setup_env()
    
    try:
        # Run migrations
        print("📦 Checking database migrations...")
        subprocess.run([sys.executable, "manage.py", "migrate"], check=True)
        
        # Collect static files (optional but good for production)
        print("📁 Collecting static files...")
        subprocess.run([sys.executable, "manage.py", "collectstatic", "--noinput"], check=True)
        
        # Start Production Server (Daphne)
        # We use Daphne because the project uses Django Channels/WebSockets
        print("🌐 Production server starting at http://0.0.0.0:8000")
        
        # Determine daphne path
        venv_bin = os.path.join(os.getcwd(), 'venv', 'bin' if os.name != 'nt' else 'Scripts')
        daphne_bin = os.path.join(venv_bin, 'daphne' if os.name != 'nt' else 'daphne.exe')
        
        # If daphne is not found in bin, fallback to 'python -m daphne'
        if not os.path.exists(daphne_bin):
            command = [sys.executable, "-m", "daphne", "-b", "0.0.0.0", "-p", "8000", "trading_platform.asgi:application"]
        else:
            command = [daphne_bin, "-b", "0.0.0.0", "-p", "8000", "trading_platform.asgi:application"]
            
        subprocess.run(command, check=True)
        
    except KeyboardInterrupt:
        print("\n👋 Shutting down...")
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    run()
