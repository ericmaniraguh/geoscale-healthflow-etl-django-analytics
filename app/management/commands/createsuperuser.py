# app\management\commands\createsu.py
import os
from django.core.management.base import BaseCommand
from django.contrib.auth import get_user_model
from decouple import config

User = get_user_model()

class Command(BaseCommand):
    help = "Create a default superuser using environment variables"

    def handle(self, *args, **kwargs):
        username = config("DJANGO_SUPERUSER_USERNAME", default="").strip('"').strip("'")
        email = config("DJANGO_SUPERUSER_EMAIL", default="").strip('"').strip("'")
        password = config("DJANGO_SUPERUSER_PASSWORD", default="").strip('"').strip("'")

        # Validate that all required environment variables are set
        if not all([username, email, password]):
            self.stdout.write(
                self.style.ERROR(
                    "Error: Missing required environment variables. "
                    "Ensure DJANGO_SUPERUSER_USERNAME, DJANGO_SUPERUSER_EMAIL, "
                    "and DJANGO_SUPERUSER_PASSWORD are set in your .env file."
                )
            )
            return

        if not User.objects.filter(username=username).exists():
            User.objects.create_superuser(
                username=username,
                email=email,
                password=password
            )
            self.stdout.write(self.style.SUCCESS(f"Superuser '{username}' created"))
        else:
            self.stdout.write(self.style.WARNING(f"Superuser '{username}' already exists"))