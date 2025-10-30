# app/accounts/models.py - OPTIMIZED VERSION
from django.contrib.auth.models import AbstractUser
from django.db import models
import random, string
from django.utils import timezone

class Province(models.Model):
    name = models.CharField(max_length=100, unique=True, db_index=True)  # ✅ Added index
    
    class Meta: 
        ordering = ["name"]
        indexes = [
            models.Index(fields=['name']),  # ✅ Explicit index
        ]
    
    def __str__(self): 
        return self.name


class District(models.Model):
    province = models.ForeignKey(
        'accounts.Province', 
        on_delete=models.CASCADE, 
        related_name='districts',
        db_index=True  # ✅ Added index
    )
    name = models.CharField(max_length=100, db_index=True)  # ✅ Added index
    
    class Meta:
        unique_together = ('province', 'name')
        ordering = ['province__name', 'name']
        indexes = [
            models.Index(fields=['province', 'name']),  # ✅ Composite index
            models.Index(fields=['name']),  # ✅ Single field index
        ]
    
    def __str__(self): 
        return f"{self.name} ({self.province})"


class Sector(models.Model):
    district = models.ForeignKey(
        'accounts.District', 
        on_delete=models.CASCADE, 
        related_name='sectors',
        db_index=True  # ✅ Added index
    )
    name = models.CharField(max_length=100, db_index=True)  # ✅ Added index
    
    class Meta:
        unique_together = ('district', 'name')
        ordering = ['district__name', 'name']
        indexes = [
            models.Index(fields=['district', 'name']),  # ✅ Composite index
            models.Index(fields=['name']),  # ✅ Single field index
        ]
    
    def __str__(self): 
        return f"{self.name} ({self.district})"


class CustomUser(AbstractUser):
    phone_number = models.CharField(max_length=20, blank=True, null=True, db_index=True)  # ✅ Added index
    country = models.CharField(max_length=80, default="Rwanda")
    
    # Location fields with indexes
    province = models.ForeignKey(
        'accounts.Province', 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        db_index=True  # ✅ Added index
    )
    district = models.ForeignKey(
        'accounts.District', 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        db_index=True  # ✅ Added index
    )
    sector = models.ForeignKey(
        'accounts.Sector', 
        on_delete=models.SET_NULL, 
        null=True, 
        blank=True,
        db_index=True  # ✅ Added index
    )
    
    health_centre_name = models.CharField(max_length=255, blank=True, null=True)
    position = models.CharField(max_length=255, blank=True, null=True)
    
    # OTP fields with indexes
    otp_code = models.CharField(max_length=6, blank=True, null=True, db_index=True)  # ✅ Added index
    otp_created = models.DateTimeField(blank=True, null=True, db_index=True)  # ✅ Added index
    
    # Email verification status with index
    email_verified = models.BooleanField(default=False, db_index=True)  # ✅ Added index
    verification_token = models.CharField(max_length=100, blank=True, null=True, db_index=True)  # ✅ Added index

    class Meta:
        indexes = [
            models.Index(fields=['email', 'email_verified']),  # ✅ Composite index for email queries
            models.Index(fields=['username', 'email_verified']),  # ✅ Composite index for login
            models.Index(fields=['otp_code', 'otp_created']),  # ✅ Composite index for OTP verification
            models.Index(fields=['province', 'district', 'sector']),  # ✅ Composite index for location queries
        ]

    def generate_otp(self):
        """Generate a 6-digit OTP code - OPTIMIZED"""
        code = ''.join(random.choices(string.digits, k=6))
        self.otp_code = code
        self.otp_created = timezone.now()
        self.save(update_fields=['otp_code', 'otp_created'])  # ✅ Only update these fields (faster)
        return code

    def verify_otp(self, code):
        """Verify the provided OTP code - OPTIMIZED"""
        if not self.otp_code or not self.otp_created:
            return False
        
        # Check if OTP has expired (5 minutes)
        time_diff = timezone.now() - self.otp_created
        if time_diff.total_seconds() > 300:  # 5 minutes
            return False
            
        return self.otp_code == code

    def clear_otp(self):
        """Clear OTP data after successful verification - OPTIMIZED"""
        self.otp_code = None
        self.otp_created = None
        self.email_verified = True  # ✅ Set verified in same query
        self.save(update_fields=['otp_code', 'otp_created', 'email_verified'])  # ✅ Batch update (faster)

    def is_otp_expired(self):
        """Check if current OTP is expired"""
        if not self.otp_created:
            return True
        time_diff = timezone.now() - self.otp_created
        return time_diff.total_seconds() > 300