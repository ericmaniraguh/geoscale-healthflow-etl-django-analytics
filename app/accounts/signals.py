from django.db.models.signals import post_save
from django.dispatch import receiver
from django.contrib.auth import get_user_model

User = get_user_model()

@receiver(post_save, sender=User)
def verify_superuser_email(sender, instance, created, **kwargs):
    if created and instance.is_superuser:
        instance.email_verified = True
        instance.save()
