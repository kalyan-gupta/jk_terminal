from django.db import models
from django.contrib.auth.models import User
from django.utils import timezone
from cryptography.fernet import Fernet
from django.conf import settings
import os


class UserNeoCredentials(models.Model):
    """Store encrypted Kotak Neo API credentials for each user"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='neo_credentials')
    
    # Encrypted fields (stored encrypted)
    mpin = models.CharField(max_length=500)  # Encrypted
    consumer_key = models.CharField(max_length=500)  # Encrypted
    mobile_number = models.CharField(max_length=500)  # Encrypted

    # SDK session metadata
    sdk_session_active = models.BooleanField(default=False)
    sdk_session_started_at = models.DateTimeField(null=True, blank=True)
    sdk_session_expires_at = models.DateTimeField(null=True, blank=True)
    
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
        self.save()
    
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
        super().save(*args, **kwargs)
    
    def get_decrypted_credentials(self):
        """Get all credentials in decrypted form"""
        return {
            'MPIN': self.decrypt_field(self.mpin),
            'CONSUMER_KEY': self.decrypt_field(self.consumer_key),
            'MOBILE_NUMBER': self.decrypt_field(self.mobile_number),
            'UCC': self.ucc,
            'ACCOUNT_NAME': self.account_name,
        }
    
    def update_credentials(self, mpin, consumer_key, mobile_number, ucc, account_name):
        """Update credentials (will be encrypted on save)"""
        self.mpin = mpin
        self.consumer_key = consumer_key
        self.mobile_number = mobile_number
        self.ucc = ucc
        self.account_name = account_name
        self.updated_at = timezone.now()
        self.save()


class SessionActivity(models.Model):
    """Track user session activity for expiry"""
    user = models.OneToOneField(User, on_delete=models.CASCADE, related_name='session_activity')
    last_activity = models.DateTimeField(auto_now=True)
    session_key = models.CharField(max_length=40, null=True, blank=True)
    ip_address = models.GenericIPAddressField(null=True, blank=True)
    
    class Meta:
        verbose_name = "Session Activity"
        verbose_name_plural = "Session Activities"
    
    def __str__(self):
        return f"{self.user.username} - Last activity: {self.last_activity}"
    
    def is_expired(self, timeout_seconds=300):
        """Check if session has expired (default 5 minutes)"""
        return (timezone.now() - self.last_activity).total_seconds() > timeout_seconds


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
    
    class Meta:
        verbose_name = "SMTP Settings"
        verbose_name_plural = "SMTP Settings"

    def __str__(self):
        return f"SMTP Configuration ({self.host}:{self.port})"

    @classmethod
    def get_settings(cls):
        obj, created = cls.objects.get_or_create(id=1)
        return obj

    @staticmethod
    def get_cipher():
        key = os.environ.get('ENCRYPTION_KEY', 'default-key-change-in-production')
        import hashlib
        import base64
        hash_key = hashlib.sha256(key.encode()).digest()
        return Fernet(base64.urlsafe_b64encode(hash_key))

    def encrypt_field(self, value):
        if not value:
            return value
        if self.is_encrypted(value):
            return value
        cipher = self.get_cipher()
        return cipher.encrypt(value.encode()).decode()

    def is_encrypted(self, value):
        if not isinstance(value, str) or not value.startswith('gAAAAA'):
            return False
        try:
            cipher = self.get_cipher()
            cipher.decrypt(value.encode())
            return True
        except Exception:
            return False

    def decrypt_field(self, encrypted_value):
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
