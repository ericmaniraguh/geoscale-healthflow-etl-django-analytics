from django.apps import AppConfig

class AccountsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'app.accounts'   # ← must match INSTALLED_APPS
    label = 'accounts'      # ← gives app label "accounts"
