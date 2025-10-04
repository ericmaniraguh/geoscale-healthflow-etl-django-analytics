# app/accounts/management/commands/cleanup_unverified_users.py

from django.core.management.base import BaseCommand
from django.utils import timezone
from datetime import timedelta
from accounts.models import CustomUser

class Command(BaseCommand):
    help = 'Clean up unverified users from the database'

    def add_arguments(self, parser):
        parser.add_argument(
            '--dry-run',
            action='store_true',
            help='Show what would be deleted without actually deleting',
        )
        parser.add_argument(
            '--days',
            type=int,
            default=7,
            help='Delete unverified users older than X days (default: 7)',
        )

    def handle(self, *args, **options):
        dry_run = options['dry_run']
        days = options['days']
        
        # Find unverified users older than specified days
        cutoff_date = timezone.now() - timedelta(days=days)
        unverified_users = CustomUser.objects.filter(
            email_verified=False,
            date_joined__lt=cutoff_date
        )
        
        count = unverified_users.count()
        
        if dry_run:
            self.stdout.write(
                self.style.WARNING(f"DRY RUN: Would delete {count} unverified users older than {days} days")
            )
            for user in unverified_users[:10]:  # Show first 10
                self.stdout.write(f"  - {user.username} ({user.email}) - created: {user.date_joined}")
            if count > 10:
                self.stdout.write(f"  ... and {count - 10} more")
        else:
            if count > 0:
                unverified_users.delete()
                self.stdout.write(
                    self.style.SUCCESS(f"Successfully deleted {count} unverified users older than {days} days")
                )
            else:
                self.stdout.write("No unverified users found to delete")

        # Show current statistics
        total_users = CustomUser.objects.count()
        verified_users = CustomUser.objects.filter(email_verified=True).count()
        unverified_users = CustomUser.objects.filter(email_verified=False).count()
        
        self.stdout.write(f"\nCurrent user statistics:")
        self.stdout.write(f"  Total users: {total_users}")
        self.stdout.write(f"  Verified users: {verified_users}")
        self.stdout.write(f"  Unverified users: {unverified_users}")