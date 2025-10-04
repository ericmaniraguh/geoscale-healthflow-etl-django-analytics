# app/accounts/management/commands/test_email.py

from django.core.management.base import BaseCommand
from django.core.mail import send_mail
from django.conf import settings
import smtplib

class Command(BaseCommand):
    help = 'Test email configuration and sending'

    def add_arguments(self, parser):
        parser.add_argument(
            '--to',
            type=str,
            default='kaalicy@gmail.com',
            help='Email address to send test email to',
        )

    def handle(self, *args, **options):
        to_email = options['to']
        
        self.stdout.write("Testing email configuration...")
        self.stdout.write(f"EMAIL_HOST: {settings.EMAIL_HOST}")
        self.stdout.write(f"EMAIL_PORT: {settings.EMAIL_PORT}")
        self.stdout.write(f"EMAIL_USE_TLS: {settings.EMAIL_USE_TLS}")
        self.stdout.write(f"EMAIL_HOST_USER: {settings.EMAIL_HOST_USER}")
        self.stdout.write(f"EMAIL_BACKEND: {settings.EMAIL_BACKEND}")
        self.stdout.write(f"DEFAULT_FROM_EMAIL: {settings.DEFAULT_FROM_EMAIL}")
        
        # Test SMTP connection
        try:
            self.stdout.write("\n1. Testing SMTP connection...")
            server = smtplib.SMTP(settings.EMAIL_HOST, settings.EMAIL_PORT)
            server.starttls()
            server.login(settings.EMAIL_HOST_USER, settings.EMAIL_HOST_PASSWORD)
            server.quit()
            self.stdout.write(self.style.SUCCESS("✅ SMTP connection successful"))
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ SMTP connection failed: {e}"))
            return
        
        # Test sending plain email
        try:
            self.stdout.write(f"\n2. Sending test email to {to_email}...")
            result = send_mail(
                'Test Email from GeoScale Malaria Platform',
                'This is a test email to verify that email sending is working correctly.\n\nIf you receive this email, your Django email configuration is working!',
                settings.DEFAULT_FROM_EMAIL,
                [to_email],
                fail_silently=False,
            )
            
            if result == 1:
                self.stdout.write(self.style.SUCCESS(f"✅ Email sent successfully to {to_email}"))
                self.stdout.write("Check your inbox and spam folder!")
            else:
                self.stdout.write(self.style.ERROR(f"❌ Email sending failed - no result"))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ Email sending failed: {e}"))
            
        # Test OTP email
        try:
            self.stdout.write(f"\n3. Testing OTP email format...")
            from app.accounts.views import send_otp_email_simple
            
            result = send_otp_email_simple(to_email, "Test User", "123456")
            
            if result:
                self.stdout.write(self.style.SUCCESS(f"✅ OTP email sent successfully to {to_email}"))
            else:
                self.stdout.write(self.style.ERROR(f"❌ OTP email sending failed"))
                
        except Exception as e:
            self.stdout.write(self.style.ERROR(f"❌ OTP email test failed: {e}"))
        
        self.stdout.write("\n" + "="*50)
        self.stdout.write("Email test completed!")
        self.stdout.write("If you received the test emails, your configuration is working.")
        self.stdout.write("If not, check:")
        self.stdout.write("1. Gmail App Password is correct")
        self.stdout.write("2. 2-Step Verification is enabled on Gmail")
        self.stdout.write("3. USE_CONSOLE_EMAIL=false in your .env file")
        self.stdout.write("4. Check spam/junk folder")