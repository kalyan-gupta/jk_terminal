# 🚀 JK Terminal - Advanced Trading Platform

[![Python Version](https://img.shields.io/badge/python-3.13-blue.svg)](https://www.python.org/downloads/)
[![Django Version](https://img.shields.io/badge/django-6.0%2B-green.svg)](https://www.djangoproject.com/)
[![License](https://img.shields.io/badge/license-MIT-orange.svg)](LICENSE)

A high-performance, real-time trading terminal built with **Django**, **Channels**, and the **Kotak Neo API**. Designed for traders who need speed, reliability, and advanced order management.

---

## ✨ Key Features

- **⚡ Real-time Market Data**: Live LTP updates via WebSockets with DuckDB in-memory caching for sub-millisecond data access.
- **🧺 Basket Order Manager**: Create, modify, and sequence complex order baskets. Includes margin checking and strategic order reordering (Buy before Sell).
- **📊 Portfolio Dashboard**: Comprehensive view of Holdings and Positions with one-click "Square Off" and "Add to Basket" functionality.
- **🔐 Secure Session Management**: Automated session expiry, TOTP authentication, and encrypted credential storage.
- **📜 Advanced Logging**: Asynchronous multiline logging system for detailed audit trails and debugging.

---

## 🛠️ Installation & Setup

### 1. Prerequisites
- **Python 3.13.12** (Recommended)
- **Git**

### 2. Quick Start (Automated Setup)
The terminal includes a `run.py` script that handles the entire setup process for you.

1.  **Clone the Repository**:
    ```bash
    git clone <repository-url>
    cd jk_terminal
    ```

2.  **Start the Terminal**:
    ```bash
    python run.py
    ```

**What `run.py` does automatically:**
- Creates a Python **Virtual Environment** (`venv`).
- Installs all **Dependencies** from `requirements.txt`.
- Generates a secure **`.env` file** with unique security keys.
- Runs **Database Migrations**.
- Collects **Static Files** for production.
- Starts the **Production ASGI Server** (Daphne) on port 8000.

---

### 3. Manual Setup (Optional)
If you prefer to manage the environment yourself:
- Create and activate your venv.
- Run `pip install -r requirements.txt`.
- Run `python manage.py migrate`.
- Run `python manage.py runserver`.

### 6. Create Admin Account
```bash
# Create a standard Django superuser
python manage.py createsuperuser
```

---

## 🚀 Running the Terminal

The easiest way to start the terminal is using the included `run.py` script, which automatically checks for migrations and starts the server:

```bash
python run.py
```

Alternatively, you can use the standard Django command:

```bash
python manage.py runserver
```

Once the server is running:
1.  Open [http://127.0.0.1:8000](http://127.0.0.1:8000) in your browser.
2.  Login with your admin credentials.
3.  **Important**: On the first run, use the **"Refresh Scrip Master"** button in the UI to download the latest instrument tokens from Kotak Neo.

---

## 📂 Project Overview

| Component | Description |
| :--- | :--- |
| **`trades/`** | Core app logic (Models, Views, WebSocket Consumers) |
| **`trading_platform/`** | Project configuration, settings, and middleware |
| **`scrip_data/`** | Local CSV storage for instrument master files |
| **`manage.py`** | Django's command-line utility |
| **`app_activity.log`**| Centralized application logs |

---

## 🛡️ Security & Best Practices

- **Never** commit `credentials.py` or `db.sqlite3` to public repositories.
- Keep your `SECRET_KEY` private in production.
- Regularly update the Scrip Master data to ensure accurate trading tokens.

---

Built with ❤️ for the trading community.
