from django import forms
from django.contrib.auth.models import User
from django.contrib.auth.forms import UserCreationForm, UserChangeForm
from .models import UserNeoCredentials


class LoginForm(forms.Form):
    """Login form for user authentication"""
    username = forms.CharField(
        label="Username",
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter your username',
            'autocomplete': 'username'
        }),
        max_length=150
    )
    password = forms.CharField(
        label="Password",
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter your password',
            'autocomplete': 'current-password'
        })
    )
    remember_me = forms.BooleanField(
        required=False,
        widget=forms.CheckboxInput(attrs={'class': 'form-check-input'}),
        label="Remember me"
    )


class RegistrationForm(UserCreationForm):
    """Registration form for new users"""
    email = forms.EmailField(
        required=True,
        widget=forms.EmailInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter your email'
        })
    )
    first_name = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'First name (optional)'
        })
    )
    last_name = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Last name (optional)'
        })
    )
    
    class Meta:
        model = User
        fields = ['username', 'email', 'first_name', 'last_name', 'password1', 'password2']
        widgets = {
            'username': forms.TextInput(attrs={
                'class': 'form-control',
                'placeholder': 'Choose a username'
            }),
        }

    def clean_username(self):
        username = self.cleaned_data.get("username")
        # Delete inactive ghost accounts before validation
        if username:
            User.objects.filter(username__iexact=username, is_active=False).delete()
        return super().clean_username()

    def clean_email(self):
        email = self.cleaned_data.get("email")
        if email:
            User.objects.filter(email__iexact=email, is_active=False).delete()
            if User.objects.filter(email__iexact=email).exists():
                raise forms.ValidationError("This email is already registered.")
        return email

    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['password1'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': 'Password'
        })
        self.fields['password2'].widget.attrs.update({
            'class': 'form-control',
            'placeholder': 'Confirm password'
        })
        # Remove help texts
        for field in self.fields:
            if field in ['password1', 'password2']:
                self.fields[field].help_text = ''

class OTPVerifyForm(forms.Form):
    """Form to verify email 6-digit OTP code"""
    otp = forms.CharField(
        max_length=6,
        min_length=6,
        required=True,
        label="Verification Code",
        widget=forms.TextInput(attrs={
            'class': 'form-control form-control-lg text-center fw-bold',
            'placeholder': '123456',
            'style': 'letter-spacing: 0.5em; font-family: monospace;',
            'autocomplete': 'one-time-code'
        })
    )

class UserNeoCredentialsForm(forms.ModelForm):
    """Form for managing Neo API credentials"""
    
    # These will be decrypted on display, encrypted on save
    mpin = forms.CharField(
        label="MPIN",
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Your MPIN'
        })
    )
    consumer_key = forms.CharField(
        label="Consumer Key",
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Your Consumer Key'
        })
    )
    mobile_number = forms.CharField(
        label="Mobile Number",
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': '+91XXXXXXXXXX'
        }),
        max_length=20
    )
    ucc = forms.CharField(
        label="UCC",
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Your UCC'
        }),
        max_length=100
    )
    account_name = forms.CharField(
        label="Account Name",
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Your Account Name'
        }),
        max_length=255
    )
    
    class Meta:
        model = UserNeoCredentials
        fields = ['mpin', 'consumer_key', 'mobile_number', 'ucc', 'account_name']
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # If there's an instance, decrypt the credentials for display
        if self.instance and self.instance.pk:
            decrypted = self.instance.get_decrypted_credentials()
            self.fields['mpin'].initial = decrypted['MPIN']
            self.fields['consumer_key'].initial = decrypted['CONSUMER_KEY']
            self.fields['mobile_number'].initial = decrypted['MOBILE_NUMBER']


class TOTPForm(forms.Form):
    """Prompt user for one-time Neo API TOTP code"""
    totp = forms.CharField(
        label="One-Time TOTP Code",
        max_length=10,
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter current authenticator code',
            'autocomplete': 'one-time-code'
        })
    )


class UserProfileForm(UserChangeForm):
    """Form for managing user profile"""
    
    class Meta:
        model = User
        fields = ['first_name', 'last_name', 'email']
        widgets = {
            'first_name': forms.TextInput(attrs={'class': 'form-control'}),
            'last_name': forms.TextInput(attrs={'class': 'form-control'}),
            'email': forms.EmailInput(attrs={'class': 'form-control'}),
        }


class ForgotPasswordForm(forms.Form):
    """Form to request a password reset email"""
    email = forms.EmailField(
        label="Email Address",
        widget=forms.EmailInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter your registered email address'
        })
    )


class SetNewPasswordForm(forms.Form):
    """Form to force a password change after reset"""
    new_password = forms.CharField(
        label="New Password",
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Create a new password'
        })
    )
    confirm_password = forms.CharField(
        label="Confirm Password",
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Confirm your new password'
        })
    )

    def clean(self):
        cleaned_data = super().clean()
        new_password = cleaned_data.get("new_password")
        confirm_password = cleaned_data.get("confirm_password")

        if new_password and confirm_password and new_password != confirm_password:
            self.add_error('confirm_password', "Passwords do not match.")
        
        return cleaned_data


class ChangePasswordForm(SetNewPasswordForm):
    """Form to manually change password from profile"""
    current_password = forms.CharField(
        label="Current Password",
        widget=forms.PasswordInput(attrs={
            'class': 'form-control',
            'placeholder': 'Enter your current password'
        })
    )

    # Reorder fields so current is first
    field_order = ['current_password', 'new_password', 'confirm_password']
