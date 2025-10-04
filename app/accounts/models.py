# app/accounts/models.py
from django.contrib.auth.models import AbstractUser
from django.db import models
import random, string, datetime
from django.utils import timezone

class Province(models.Model):
    name = models.CharField(max_length=100, unique=True)
    class Meta: 
        ordering = ["name"]
    def __str__(self): 
        return self.name

class District(models.Model):
    province = models.ForeignKey('accounts.Province', on_delete=models.CASCADE, related_name='districts')
    name = models.CharField(max_length=100)
    class Meta:
        unique_together = ('province', 'name')
        ordering = ['province__name', 'name']
    def __str__(self): 
        return f"{self.name} ({self.province})"

class Sector(models.Model):
    district = models.ForeignKey('accounts.District', on_delete=models.CASCADE, related_name='sectors')
    name = models.CharField(max_length=100)
    class Meta:
        unique_together = ('district', 'name')
        ordering = ['district__name', 'name']
    def __str__(self): 
        return f"{self.name} ({self.district})"

class CustomUser(AbstractUser):
    phone_number = models.CharField(max_length=20, blank=True, null=True)
    country = models.CharField(max_length=80, default="Rwanda")
    province = models.ForeignKey('accounts.Province', on_delete=models.SET_NULL, null=True, blank=True)
    district = models.ForeignKey('accounts.District', on_delete=models.SET_NULL, null=True, blank=True)
    sector = models.ForeignKey('accounts.Sector', on_delete=models.SET_NULL, null=True, blank=True)
    health_centre_name = models.CharField(max_length=255, blank=True, null=True)
    position = models.CharField(max_length=255, blank=True, null=True)
    
    # OTP fields
    otp_code = models.CharField(max_length=6, blank=True, null=True)
    otp_created = models.DateTimeField(blank=True, null=True)
    
    # Email verification status
    email_verified = models.BooleanField(default=False)
    verification_token = models.CharField(max_length=100, blank=True, null=True)

    def generate_otp(self):
        """Generate a 6-digit OTP code"""
        code = ''.join(random.choices(string.digits, k=6))
        self.otp_code = code
        self.otp_created = timezone.now()
        self.save(update_fields=['otp_code', 'otp_created'])
        return code

    def verify_otp(self, code):
        """Verify the provided OTP code"""
        if not self.otp_code or not self.otp_created:
            return False
        
        # Check if OTP has expired (5 minutes)
        time_diff = timezone.now() - self.otp_created
        if time_diff.total_seconds() > 300:  # 5 minutes in seconds
            return False
            
        return self.otp_code == code

    def clear_otp(self):
        """Clear OTP data after successful verification"""
        self.otp_code = None
        self.otp_created = None
        self.save(update_fields=['otp_code', 'otp_created'])

    def is_otp_expired(self):
        """Check if current OTP is expired"""
        if not self.otp_created:
            return True
        time_diff = timezone.now() - self.otp_created
        return time_diff.total_seconds() > 300