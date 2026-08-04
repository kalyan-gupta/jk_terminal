from .models import PlatformSettings, SMTPSettings

def platform_settings(request):
    return {
        'platform_settings': PlatformSettings.get_settings(),
        'smtp_settings': SMTPSettings.get_settings(),
    }
