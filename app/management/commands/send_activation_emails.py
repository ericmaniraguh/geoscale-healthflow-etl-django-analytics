# app/accounts/management/commands/send_activation_emails.py
# Create this file in: accounts/management/commands/send_activation_emails.py
# Make sure to create the directories if they don't exist:
# accounts/management/ (with __init__.py)
# accounts/management/commands/ (with __init__.py)

from django.core.management.base import BaseCommand
from django.core.mail import send_mail
from django.conf import settings
from django.template.loader import render_to_string
from accounts.models import CustomUser
import logging

logger = logging.getLogger(__name__)

class Command(BaseCommand):
    help = 'Send activation emails to all existing users who haven\'t verified their email'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be sent without actually sending emails',
        )
        parser.add_argument(
            '--force-all',
            action='store_true',
            help='Send activation emails to all users, even those already verified',
        )
        parser.add_argument(
            '--user-id',
            type=int,
            help='Send activation email to specific user by ID',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        force_all = options['force_all']
        user_id = options['user_id']

        if user_id:
            # Send to specific user
            try:
                user = CustomUser.objects.get(id=user_id)
                users_to_process = [user]
                self.stdout.write(f"Processing specific user: {user.username} ({user.email})")
            except CustomUser.DoesNotExist:
                self.stdout.write(
                    self.style.ERROR(f"User with ID {user_id} does not exist")
                )
                return
        else:
            # Get users based on verification status
            if force_all:
                users_to_process = CustomUser.objects.filter(is_active=True, email__isnull=False)
                self.stdout.write("Processing all active users with email addresses...")
            else:
                users_to_process = CustomUser.objects.filter(
                    email_verified=False,
                    is_active=True,
                    email__isnull=False
                ).exclude(email='')
                self.stdout.write("Processing unverified users...")

        total_users = users_to_process.count()
        
        if total_users == 0:
            self.stdout.write(
                self.style.WARNING("No users found matching the criteria.")
            )
            return

        self.stdout.write(f"Found {total_users} users to process.")

        if dry_run:
            self.stdout.write(
                self.style.WARNING("DRY RUN - No emails will be sent")
            )

        sent_count = 0
        failed_count = 0

        for user in users_to_process:
            try:
                if dry_run:
                    self.stdout.write(f"Would send activation email to: {user.email}")
                    sent_count += 1
                else:
                    # Generate OTP for the user
                    otp = user.generate_otp()
                    
                    # Send activation email
                    success = self.send_activation_email(user, otp)
                    
                    if success:
                        self.stdout.write(
                            self.style.SUCCESS(f"✓ Sent activation email to: {user.email}")
                        )
                        sent_count += 1
                    else:
                        self.stdout.write(
                            self.style.ERROR(f"✗ Failed to send email to: {user.email}")
                        )
                        failed_count += 1

            except Exception as e:
                self.stdout.write(
                    self.style.ERROR(f"✗ Error processing {user.email}: {str(e)}")
                )
                failed_count += 1

        # Summary
        if dry_run:
            self.stdout.write(
                self.style.SUCCESS(f"\nDRY RUN COMPLETE: Would have sent {sent_count} emails")
            )
        else:
            self.stdout.write(
                self.style.SUCCESS(f"\nCOMPLETE: Sent {sent_count} emails successfully")
            )
            if failed_count > 0:
                self.stdout.write(
                    self.style.WARNING(f"Failed to send {failed_count} emails")
                )

    def send_activation_email(self, user, otp):
        """Send activation email to a user"""
        try:
            subject = "Activate Your Account - Email Verification Required"
            
            # Create HTML email content
            html_message = render_to_string('accounts/emails/existing_user_activation.html', {
                'user': user,
                'otp_code': otp,
                'site_name': 'GeoScale Malaria Platform',
                'activation_url': f"{settings.SITE_URL}/accounts/verify-otp/" if hasattr(settings, 'SITE_URL') else None,
            })
            
            # Create plain text version
            plain_message = f"""
Hello {user.first_name or user.username},

We're implementing email verification for all user accounts on GeoScale Malaria Platform.

Your account verification code is: {otp}

Please log in and verify your email address using this code to continue using your account.
This code will expire in 5 minutes.

Visit: /accounts/verify-otp/

If you have any questions, please contact our support team.

Best regards,
The GeoScale Malaria Team
            """
            
            send_mail(
                subject=subject,
                message=plain_message,
                from_email=settings.DEFAULT_FROM_EMAIL,
                recipient_list=[user.email],
                html_message=html_message,
                fail_silently=False
            )
            return True
            
        except Exception as e:
            logger.error(f"Failed to send activation email to {user.email}: {str(e)}")
            return False