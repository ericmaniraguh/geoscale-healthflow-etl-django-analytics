
# app/accounts/admin.py
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from .models import Province, District, Sector, CustomUser

@admin.register(Province)
class ProvinceAdmin(admin.ModelAdmin):
    list_display = ("name",)
    search_fields = ("name",)

@admin.register(District)
class DistrictAdmin(admin.ModelAdmin):
    list_display = ("name", "province")
    list_filter = ("province",)
    search_fields = ("name", "province__name")

@admin.register(Sector)
class SectorAdmin(admin.ModelAdmin):
    list_display = ("name", "district")
    list_filter = ("district__province", "district")
    search_fields = ("name", "district__name", "district__province__name")

@admin.register(CustomUser)
class CustomUserAdmin(UserAdmin):
    list_display = ('username', 'email', 'first_name', 'last_name', 'province', 'district', 'health_centre_name')
    list_filter = ('province', 'district', 'is_staff', 'is_active', 'date_joined')
    search_fields = ('username', 'first_name', 'last_name', 'email', 'phone_number')
    
    # Add custom fields to the user admin
    fieldsets = UserAdmin.fieldsets + (
        ('Location Information', {
            'fields': ('phone_number', 'country', 'province', 'district', 'sector')
        }),
        ('Professional Information', {
            'fields': ('health_centre_name', 'position')
        }),
    )