"""
Create superuser admin account
"""
import os
import django

os.environ.setdefault('DJANGO_SETTINGS_MODULE', 'app.settings')
django.setup()

from django.contrib.auth import get_user_model

User = get_user_model()

def create_superuser():
    """Create superuser if it doesn't exist"""
    username = input("Enter superuser username [admin]: ").strip() or "admin"
    email = input("Enter superuser email [admin@example.com]: ").strip() or "admin@example.com"
    password = input("Enter superuser password [admin123]: ").strip() or "admin123"

    if User.objects.filter(username=username).exists():
        print(f"[WARNING] User '{username}' already exists")
        update = input("Update password? (y/n): ").lower()
        if update == 'y':
            user = User.objects.get(username=username)
            user.set_password(password)
            user.is_staff = True
            user.is_superuser = True
            user.save()
            print(f"[SUCCESS] Password updated for user '{username}'")
        else:
            print("[INFO] No changes made")
    else:
        user = User.objects.create_superuser(
            username=username,
            email=email,
            password=password
        )
        print(f"[SUCCESS] Superuser '{username}' created successfully!")

    print("\n" + "="*50)
    print("SUPERUSER CREDENTIALS")
    print("="*50)
    print(f"Username: {username}")
    print(f"Email: {email}")
    print(f"Password: {password}")
    print("="*50)
    print("\nYou can now login at: http://localhost:8000/accounts/login/")

if __name__ == "__main__":
    try:
        create_superuser()
    except Exception as e:
        print(f"[ERROR] {str(e)}")
