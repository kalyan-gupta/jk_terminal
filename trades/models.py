from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from cryptography.fernet import Fernet
from django.conf import settings
import os


class UserNeoCredentials(models.Model):
    """Store encrypted Kotak Neo API credentials for each user"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='neo_credentials')
    
    mpin = models.CharField(max_length=500)  # Encrypted
    consumer_key = models.CharField(max_length=500)  # Encrypted
    mobile_number = models.CharField(max_length=500)  # Encrypted
    totp_secret = models.CharField(max_length=500, blank=True, null=True)  # Encrypted
    
    auth_mode = models.CharField(
        max_length=20, 
        default='manual', 
        choices=[
            ('manual', 'Manual Auth using Auth Code'),
            ('secret', 'Direct Auth using Secret')
        ]
    )

    # Plain text fields
    ucc = models.CharField(max_length=100)
    account_name = models.CharField(max_length=255)
    
    # Metadata
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    last_used = models.DateTimeField(null=True, blank=True)
    is_active = models.BooleanField(default=True)
    
    class Meta:
        verbose_name = "User Neo Credentials"
        verbose_name_plural = "User Neo Credentials"
    
    def __str__(self):
        return f"{self.user.username} - {self.account_name}"

    @staticmethod
    def get_cipher():
        """Get the Fernet cipher for encryption/decryption"""
        # In production, store the key securely (e.g., environment variable)
        key = os.environ.get('ENCRYPTION_KEY', 'default-key-change-in-production')
        # Ensure key is 32 bytes and base64 encoded for Fernet
        import hashlib
        import base64
        hash_key = hashlib.sha256(key.encode()).digest()
        return Fernet(base64.urlsafe_b64encode(hash_key))
    
    def encrypt_field(self, value):
        """Encrypt a field value"""
        if not value:
            return value
        if self.is_encrypted(value):
            return value
        cipher = self.get_cipher()
        return cipher.encrypt(value.encode()).decode()
    
    def is_encrypted(self, value):
        """Determine whether a field value is already encrypted with Fernet."""
        if not isinstance(value, str) or not value.startswith('gAAAAA'):
            return False
        try:
            cipher = self.get_cipher()
            cipher.decrypt(value.encode())
            return True
        except Exception:
            return False
    
    def decrypt_field(self, encrypted_value):
        """Decrypt a field value, allowing for repeated encryption layers."""
        if not encrypted_value:
            return encrypted_value
        cipher = self.get_cipher()
        current = encrypted_value
        for _ in range(5):
            try:
                decrypted = cipher.decrypt(current.encode()).decode()
            except Exception:
                break
            if decrypted == current:
                break
            current = decrypted
        return current
    
    def save(self, *args, **kwargs):
        """Encrypt sensitive fields before saving"""
        self.mpin = self.encrypt_field(self.mpin)
        self.consumer_key = self.encrypt_field(self.consumer_key)
        self.mobile_number = self.encrypt_field(self.mobile_number)
        if self.totp_secret:
            self.totp_secret = self.encrypt_field(self.totp_secret)
        super().save(*args, **kwargs)
    
    def get_decrypted_credentials(self):
        """Get all credentials in decrypted form"""
        return {
            'MPIN': self.decrypt_field(self.mpin),
            'CONSUMER_KEY': self.decrypt_field(self.consumer_key),
            'MOBILE_NUMBER': self.decrypt_field(self.mobile_number),
            'TOTP_SECRET': self.decrypt_field(self.totp_secret) if self.totp_secret else '',
            'UCC': self.ucc,
            'ACCOUNT_NAME': self.account_name,
        }
    
    def update_credentials(self, mpin, consumer_key, mobile_number, ucc, account_name, totp_secret=None, auth_mode='manual'):
        """Update credentials (will be encrypted on save)"""
        self.mpin = mpin
        self.consumer_key = consumer_key
        self.mobile_number = mobile_number
        self.ucc = ucc
        self.account_name = account_name
        self.totp_secret = totp_secret
        self.auth_mode = auth_mode
        self.updated_at = timezone.now()
        self.save()


class SessionActivity(models.Model):
    """Track user session activity for expiry"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='session_activities')
    last_activity = models.DateTimeField(auto_now=True)
    session_key = models.CharField(max_length=40, null=True, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    
    # SDK session metadata per browser session
    sdk_session_active = models.BooleanField(default=False)
    sdk_session_started_at = models.DateTimeField(null=True, blank=True)
    sdk_session_expires_at = models.DateTimeField(null=True, blank=True)
    sdk_session_data = models.BinaryField(null=True, blank=True)
    
    class Meta:
        verbose_name = "Session Activity"
        verbose_name_plural = "Session Activities"
    
    def __str__(self):
        return f"{self.user.username} - Last activity: {self.last_activity}"
    
    def is_expired(self, timeout_seconds=None):
        """Check if session has expired. Uses PlatformSettings if timeout_seconds is not provided."""
        if timeout_seconds is None:
            settings = PlatformSettings.get_settings()
            if not settings.session_timeout_enabled:
                return False
            timeout_seconds = settings.session_timeout_seconds
            
        return (timezone.now() - self.last_activity).total_seconds() > timeout_seconds

    def is_sdk_session_valid(self, timeout_seconds=1800):
        """Return whether the stored SDK session is still valid."""
        if not self.sdk_session_active or not self.sdk_session_expires_at:
            return False
        return timezone.now() < self.sdk_session_expires_at

    def mark_sdk_session_active(self, duration_seconds=1800):
        """Mark a SDK session as active for the given duration."""
        self.sdk_session_active = True
        self.sdk_session_started_at = timezone.now()
        self.sdk_session_expires_at = timezone.now() + timezone.timedelta(seconds=duration_seconds)
        self.save()

    def deactivate_sdk_session(self):
        """Mark the SDK session as inactive."""
        self.sdk_session_active = False
        self.sdk_session_started_at = None
        self.sdk_session_expires_at = None
        self.sdk_session_data = None
        self.save()

    @staticmethod
    def get_cipher():
        """Get the Fernet cipher for encryption/decryption"""
        key = os.environ.get('ENCRYPTION_KEY', 'default-key-change-in-production')
        import hashlib
        import base64
        from cryptography.fernet import Fernet
        hash_key = hashlib.sha256(key.encode()).digest()
        return Fernet(base64.urlsafe_b64encode(hash_key))

    def encrypt_data(self, data: bytes) -> bytes:
        """Encrypt binary data."""
        if not data:
            return data
        cipher = self.get_cipher()
        return cipher.encrypt(data)

    def decrypt_data(self, encrypted_data: bytes) -> bytes:
        """Decrypt binary data."""
        if not encrypted_data:
            return encrypted_data
        cipher = self.get_cipher()
        try:
            return cipher.decrypt(encrypted_data)
        except Exception:
            return encrypted_data


class PlatformSettings(models.Model):
    """Global platform configuration editable by superusers"""
    session_timeout_enabled = models.BooleanField(default=True, help_text="Enable automatic logoff after inactivity")
    session_timeout_seconds = models.IntegerField(default=300, help_text="User session timeout in seconds (default 5 min)")
    
    sdk_timeout_enabled = models.BooleanField(default=True, help_text="Enable mandatory SDK re-authentication after duration")
    sdk_timeout_seconds = models.IntegerField(default=1800, help_text="SDK session timeout in seconds (default 30 min)")

    enable_user_registration = models.BooleanField(default=True, help_text="Allow new users to register via the signup button")
    allow_session_restore = models.BooleanField(default=False, help_text="Allow restoring user and SDK sessions after server restarts")
    allow_direct_secret_auth = models.BooleanField(default=True, help_text="Allow users to use a TOTP secret for direct session activation")

    class Meta:
        verbose_name = "Platform Settings"
        verbose_name_plural = "Platform Settings"

    def __str__(self):
        return "Global Platform Configuration"

    @classmethod
    def get_settings(cls):
        obj, created = cls.objects.get_or_create(id=1)
        return obj


DEFAULT_OTP_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f5f7; margin: 0; padding: 0; color: #1e293b; }
    .container { max-width: 600px; margin: 40px auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; }
    .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: #ffffff; padding: 32px 24px; text-align: center; }
    .header h1 { margin: 0; font-size: 24px; font-weight: 700; letter-spacing: -0.5px; }
    .content { padding: 32px 24px; line-height: 1.6; }
    .footer { text-align: center; padding: 24px; font-size: 12px; color: #64748b; background-color: #f8fafc; border-top: 1px solid #f1f5f9; }
    .highlight-box { background-color: #f1f5f9; border-left: 4px solid #667eea; padding: 16px; border-radius: 0 8px 8px 0; font-family: monospace; font-size: 24px; letter-spacing: 4px; text-align: center; margin: 24px 0; color: #0f172a; font-weight: bold; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>JK Terminal</h1>
    </div>
    <div class="content">
      <p>Hello <strong>{{ username }}</strong>,</p>
      <p>Your account verification code is:</p>
      <div class="highlight-box">{{ otp }}</div>
      <p>Please enter this code to complete your registration. If you did not request this code, you can safely ignore this email.</p>
      <p>Thank you,<br>JK Terminal Team</p>
    </div>
    <div class="footer">
      This is an automated notification from JK Terminal. Please do not reply to this email.
    </div>
  </div>
</body>
</html>"""

DEFAULT_PASSWORD_CHANGED_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f5f7; margin: 0; padding: 0; color: #1e293b; }
    .container { max-width: 600px; margin: 40px auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; }
    .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: #ffffff; padding: 32px 24px; text-align: center; }
    .header h1 { margin: 0; font-size: 24px; font-weight: 700; letter-spacing: -0.5px; }
    .content { padding: 32px 24px; line-height: 1.6; }
    .footer { text-align: center; padding: 24px; font-size: 12px; color: #64748b; background-color: #f8fafc; border-top: 1px solid #f1f5f9; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>JK Terminal</h1>
    </div>
    <div class="content">
      <p>Hello <strong>{{ username }}</strong>,</p>
      <p>Your password has been successfully changed.</p>
      <p>If you did not authorize this change, please contact an administrator immediately to secure your account.</p>
      <p>Thank you,<br>JK Terminal Team</p>
    </div>
    <div class="footer">
      This is an automated notification from JK Terminal. Please do not reply to this email.
    </div>
  </div>
</body>
</html>"""

DEFAULT_FORGOT_PASSWORD_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f5f7; margin: 0; padding: 0; color: #1e293b; }
    .container { max-width: 600px; margin: 40px auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; }
    .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: #ffffff; padding: 32px 24px; text-align: center; }
    .header h1 { margin: 0; font-size: 24px; font-weight: 700; letter-spacing: -0.5px; }
    .content { padding: 32px 24px; line-height: 1.6; }
    .footer { text-align: center; padding: 24px; font-size: 12px; color: #64748b; background-color: #f8fafc; border-top: 1px solid #f1f5f9; }
    .highlight-box { background-color: #f1f5f9; border-left: 4px solid #667eea; padding: 16px; border-radius: 0 8px 8px 0; font-family: monospace; font-size: 20px; text-align: center; margin: 24px 0; color: #0f172a; font-weight: bold; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>JK Terminal</h1>
    </div>
    <div class="content">
      <p>Hello <strong>{{ username }}</strong>,</p>
      <p>Your temporary password is:</p>
      <div class="highlight-box">{{ temp_password }}</div>
      <p>Please login using this temporary password. You will be prompted to set a new permanent password immediately upon logging in.</p>
      <p>Thank you,<br>JK Terminal Team</p>
    </div>
    <div class="footer">
      This is an automated notification from JK Terminal. Please do not reply to this email.
    </div>
  </div>
</body>
</html>"""

DEFAULT_ORDER_PLACED_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f5f7; margin: 0; padding: 0; color: #1e293b; }
    .container { max-width: 600px; margin: 40px auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; }
    .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: #ffffff; padding: 32px 24px; text-align: center; }
    .header h1 { margin: 0; font-size: 24px; font-weight: 700; letter-spacing: -0.5px; }
    .content { padding: 32px 24px; line-height: 1.6; }
    .footer { text-align: center; padding: 24px; font-size: 12px; color: #64748b; background-color: #f8fafc; border-top: 1px solid #f1f5f9; }
    .details-list { width: 100%; border-collapse: collapse; margin: 24px 0; }
    .details-list td { padding: 12px; border-bottom: 1px solid #e2e8f0; }
    .details-list td.label { font-weight: 600; color: #475569; width: 40%; }
    .details-list td.value { color: #0f172a; }
    .badge-buy { display: inline-block; padding: 2px 8px; background-color: #dcfce7; color: #15803d; border-radius: 4px; font-weight: 600; font-size: 12px; }
    .badge-sell { display: inline-block; padding: 2px 8px; background-color: #fee2e2; color: #b91c1c; border-radius: 4px; font-weight: 600; font-size: 12px; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>JK Terminal</h1>
    </div>
    <div class="content">
      <p>Hello <strong>{{ username }}</strong>,</p>
      <p>A new trade order has been successfully placed on your account:</p>
      
      <table class="details-list">
        <tr>
          <td class="label">Instrument</td>
          <td class="value"><strong>{{ trading_symbol }}</strong></td>
        </tr>
        <tr>
          <td class="label">Transaction Type</td>
          <td class="value">
            {% if transaction_type == "B" or transaction_type == "BUY" %}
              <span class="badge-buy">BUY</span>
            {% else %}
              <span class="badge-sell">SELL</span>
            {% endif %}
          </td>
        </tr>
        <tr>
          <td class="label">Quantity</td>
          <td class="value">{{ quantity }}</td>
        </tr>
        <tr>
          <td class="label">Price</td>
          <td class="value">{% if order_type != "MKT" %}{{ price }}{% else %}Market{% endif %}</td>
        </tr>
        <tr>
          <td class="label">Order Type</td>
          <td class="value">{{ order_type }}</td>
        </tr>
        <tr>
          <td class="label">Product</td>
          <td class="value">{{ product_type }}</td>
        </tr>
        <tr>
          <td class="label">Exchange</td>
          <td class="value">{{ exchange_segment }}</td>
        </tr>
        <tr>
          <td class="label">Order ID</td>
          <td class="value"><code>{{ order_id }}</code></td>
        </tr>
      </table>
      
      <p>Regards,<br>JK Terminal</p>
    </div>
    <div class="footer">
      This is an automated notification from JK Terminal. Please do not reply to this email.
    </div>
  </div>
</body>
</html>"""


DEFAULT_ORDER_STATUS_TEMPLATE = """<!DOCTYPE html>
<html>
<head>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; background-color: #f4f5f7; margin: 0; padding: 0; color: #1e293b; }
    .container { max-width: 600px; margin: 40px auto; background: #ffffff; border-radius: 12px; overflow: hidden; box-shadow: 0 4px 6px rgba(0,0,0,0.05); border: 1px solid #e2e8f0; }
    .header { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: #ffffff; padding: 32px 24px; text-align: center; }
    .header h1 { margin: 0; font-size: 24px; font-weight: 700; letter-spacing: -0.5px; }
    .content { padding: 32px 24px; line-height: 1.6; }
    .footer { text-align: center; padding: 24px; font-size: 12px; color: #64748b; background-color: #f8fafc; border-top: 1px solid #f1f5f9; }
    .details-list { width: 100%; border-collapse: collapse; margin: 24px 0; }
    .details-list td { padding: 12px; border-bottom: 1px solid #e2e8f0; }
    .details-list td.label { font-weight: 600; color: #475569; width: 40%; }
    .details-list td.value { color: #0f172a; }
    .status-badge { display: inline-block; padding: 4px 12px; border-radius: 9999px; font-weight: 700; font-size: 14px; text-transform: uppercase; }
    .status-complete { background-color: #dcfce7; color: #15803d; }
    .status-open { background-color: #dbeafe; color: #1d4ed8; }
    .status-rejected { background-color: #fee2e2; color: #b91c1c; }
    .status-cancelled { background-color: #f3f4f6; color: #374151; }
    .status-other { background-color: #fef9c3; color: #a16207; }
  </style>
</head>
<body>
  <div class="container">
    <div class="header">
      <h1>JK Terminal</h1>
    </div>
    <div class="content">
      <p>Hello <strong>{{ username }}</strong>,</p>
      <p>Your order status has updated:</p>
      
      <div style="text-align: center; margin: 24px 0;">
        <span class="status-badge {% if last_status == 'complete' %}status-complete{% elif last_status == 'open' %}status-open{% elif last_status == 'rejected' %}status-rejected{% elif last_status == 'cancelled' %}status-cancelled{% else %}status-other{% endif %}">
          {{ last_status }}
        </span>
      </div>

      <table class="details-list">
        <tr>
          <td class="label">Instrument</td>
          <td class="value"><strong>{{ trading_symbol }}</strong></td>
        </tr>
        <tr>
          <td class="label">Transaction Type</td>
          <td class="value">{{ transaction_type }}</td>
        </tr>
        <tr>
          <td class="label">Quantity</td>
          <td class="value">{{ quantity }}</td>
        </tr>
        <tr>
          <td class="label">Price</td>
          <td class="value">₹{{ price }}</td>
        </tr>
        <tr>
          <td class="label">Order ID</td>
          <td class="value"><code>{{ order_id }}</code></td>
        </tr>
      </table>
      
      <p>Regards,<br>JK Terminal</p>
    </div>
    <div class="footer">
      This is an automated notification from JK Terminal. Please do not reply to this email.
    </div>
  </div>
</body>
</html>"""


class SMTPSettings(models.Model):
    """Store global SMTP settings editable by superusers"""
    host = models.CharField(max_length=255, default='smtp.gmail.com')
    port = models.IntegerField(default=587)
    use_tls = models.BooleanField(default=True)
    host_user = models.CharField(max_length=255, blank=True, null=True)
    from_address = models.CharField(max_length=255, blank=True, null=True)
    host_password = models.CharField(max_length=500, blank=True, null=True)  # Will be encrypted
    enable_password_reset = models.BooleanField(default=False)
    enable_registration_otp = models.BooleanField(default=False)
    enable_order_notifications = models.BooleanField(default=True)

    # Customizable Sender and Email Templates
    from_name = models.CharField(max_length=255, default='JK Terminal', blank=True, null=True)
    
    otp_subject = models.CharField(max_length=255, default='JK Terminal - Registration Verification')
    otp_template = models.TextField(default=DEFAULT_OTP_TEMPLATE)
    
    password_changed_subject = models.CharField(max_length=255, default='JK Terminal - Password Changed Successfully')
    password_changed_template = models.TextField(default=DEFAULT_PASSWORD_CHANGED_TEMPLATE)
    
    forgot_password_subject = models.CharField(max_length=255, default='JK Terminal - Temporary Password')
    forgot_password_template = models.TextField(default=DEFAULT_FORGOT_PASSWORD_TEMPLATE)
    
    order_placed_subject = models.CharField(max_length=255, default='JK Terminal - Trade Order Placed: {{ transaction_type }} {{ quantity }} {{ trading_symbol }}')
    order_placed_template = models.TextField(default=DEFAULT_ORDER_PLACED_TEMPLATE)

    order_status_subject = models.CharField(max_length=255, default='JK Terminal - Trade Order: {{ transaction_type }} {{ quantity }} {{ trading_symbol }} is {{ last_status }}')
    order_status_template = models.TextField(default=DEFAULT_ORDER_STATUS_TEMPLATE)
    
    class Meta:
        verbose_name = "SMTP Settings"
        verbose_name_plural = "SMTP Settings"

    def send_html_email(self, subject_template, body_template, context_dict, to_emails, connection=None):
        from django.template import Template, Context
        from django.core.mail import EmailMultiAlternatives
        from django.utils.html import strip_tags
        
        ctx = Context(context_dict)
        subject = Template(subject_template).render(ctx).strip()
        html_content = Template(body_template).render(ctx)
        text_content = strip_tags(html_content)
        
        from_addr = self.from_address if self.from_address else self.host_user
        if self.from_name:
            from_email = f"{self.from_name} <{from_addr}>"
        else:
            from_email = from_addr
            
        email_msg = EmailMultiAlternatives(
            subject=subject,
            body=text_content,
            from_email=from_email,
            to=to_emails,
            connection=connection
        )
        email_msg.attach_alternative(html_content, "text/html")
        return email_msg

    def __str__(self):
        return f"SMTP Configuration ({self.host}:{self.port})"

    @staticmethod
    def get_settings():
        """Retrieve the singleton settings or create one with defaults"""
        obj, created = SMTPSettings.objects.get_or_create(id=1)
        return obj

    @staticmethod
    def get_cipher():
        key = os.environ.get('ENCRYPTION_KEY', 'default-key-change-in-production')
        import hashlib
        import base64
        from cryptography.fernet import Fernet
        hash_key = hashlib.sha256(key.encode()).digest()
        return Fernet(base64.urlsafe_b64encode(hash_key))

    def encrypt_field(self, value):
        if not value:
            return value
        if value.startswith('gAAAAA'):
            return value
        cipher = self.get_cipher()
        return cipher.encrypt(value.encode()).decode()

    def decrypt_field(self, encrypted_value):
        if not encrypted_value:
            return encrypted_value
        cipher = self.get_cipher()
        try:
            return cipher.decrypt(encrypted_value.encode()).decode()
        except Exception:
            return encrypted_value

    def get_decrypted_password(self):
        return self.decrypt_field(self.host_password)

    def save(self, *args, **kwargs):
        self.host_password = self.encrypt_field(self.host_password)
        super().save(*args, **kwargs)


class UserSecurity(models.Model):
    """Store additional security settings for a user"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='security')
    force_password_change = models.BooleanField(default=False)

    class Meta:
        verbose_name = "User Security"
        verbose_name_plural = "User Security"

    def __str__(self):
        return f"{self.user.username} Security"


class BasketOrder(models.Model):
    """Store orders in a basket for sequential execution"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='basket_orders')
    
    # Scrip identifiers
    instrument_token = models.CharField(max_length=100)
    exchange_segment = models.CharField(max_length=50)
    trading_symbol = models.CharField(max_length=255)
    
    # Order parameters
    quantity = models.IntegerField()
    price = models.FloatField() # Note: 0 for MKT orders
    transaction_type = models.CharField(max_length=5) # 'B' for Buy, 'S' for Sell
    product_type = models.CharField(max_length=50) # MIS, CNC, NRML
    order_type = models.CharField(max_length=5, default='L') # 'L' (Limit), 'MKT' (Market)
    
    # Ordering metadata
    sort_order = models.IntegerField(default=0)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        ordering = ['sort_order', 'created_at']
        verbose_name = "Basket Order"
        verbose_name_plural = "Basket Orders"

    def __str__(self):
        return f"{self.user.username} - {self.transaction_type} {self.quantity} {self.trading_symbol}"


class TrackedOrder(models.Model):
    """Persist and track order lifecycle states for status change email alerts"""
    user = models.ForeignKey(User, on_delete=models.CASCADE, related_name='tracked_orders')
    order_id = models.CharField(max_length=100, unique=True)
    trading_symbol = models.CharField(max_length=255)
    transaction_type = models.CharField(max_length=10) # B / S
    quantity = models.IntegerField()
    price = models.FloatField()
    order_type = models.CharField(max_length=10) # L / MKT
    product_type = models.CharField(max_length=20) # MIS / CNC / NRML
    exchange_segment = models.CharField(max_length=20)
    last_status = models.CharField(max_length=50) # open, complete, rejected, cancelled, placed
    is_terminal = models.BooleanField(default=False)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        verbose_name = "Tracked Order"
        verbose_name_plural = "Tracked Orders"

    def __str__(self):
        return f"{self.user.username} - Order {self.order_id} ({self.last_status})"
