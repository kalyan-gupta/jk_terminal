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

### 👤 Zero-Config Admin Setup
For a seamless onboarding experience:
- If no users exist, the system allows you to register the **first account** as an administrator.
- The first registrant is automatically promoted to **Superuser** status.
- OTP verification is skipped for the first user to allow immediate access for system configuration (SMTP, API keys, etc.).

---

### 🔧 Administrative Commands
You can perform advanced administrative tasks using the standard Django management interface:

- **Reset a User's Password**:
  ```bash
  python manage.py changepassword <username>
  ```
- **Create a Superuser Manually**:
  ```bash
  python manage.py createsuperuser
  ```
- **Promote an Existing User**:
  ```bash
  python manage.py makemysuperuser <username>
  ```

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

## 🏁 Getting Started (First-Time Users)

If you are setting up your account for the first time, follow this workflow to enable trading:

### 1. Registration
- Navigate to the login page and click **"Sign up here"**.
- After creating your account, you will be redirected to the Dashboard.

### 2. Configure Neo API SDK
The terminal needs your Kotak Neo API credentials to interact with the exchange.
- Go to the **Profile** or **Credentials** section.
- Provide your **MPIN**, **Consumer Key**, **Mobile Number** (formatted as `+91XXXXXXXXXX`), and **UCC**.
- Your sensitive data (MPIN, Consumer Key) is automatically encrypted using the `ENCRYPTION_KEY` in your `.env` file before being saved to the database.

### 3. Authenticate Session
- On the dashboard, click **"Authenticate Session"**.
- Enter the **TOTP code** from your authenticator app to establish a secure connection with Kotak Neo.
- **Note**: For security, SDK sessions expire automatically. You will need to re-authenticate if you have been inactive or if the session interval (default 30 min) has passed.

### 4. 🔄 Daily Maintenance: Refresh Scrip Master
Instrument tokens and strike prices change frequently. **You must refresh the Scrip Master data every day before you start trading.**
- Click the **"Refresh Scrip Master"** button on the dashboard.
- This will download the latest instrument files from the Kotak Neo server and update your local DuckDB cache.

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

- **Never** commit your `.env` file or `db.sqlite3` to public repositories.
- Keep your **SECRET_KEY** and **ENCRYPTION_KEY** private.
- Regularly update the Scrip Master data to ensure accurate trading tokens.

---

Built with ❤️ for the trading community.
