# app/accounts/urls.py
from django.urls import path
# from . import views
from app.accounts import views


app_name = 'accounts'  # This creates the namespace

urlpatterns = [
    path('signup/', views.signup_view, name='signup'),
    path('login/', views.login_view, name='login'),
    path('logout/', views.logout_view, name='logout'),
  
     # OTP verification URLs
    path('verify-otp/', views.verify_otp_view, name='verify_otp'),
    path('resend-otp/', views.resend_otp, name='resend_otp'),

    # AJAX endpoints
    path('ajax/districts/', views.load_districts, name='ajax_districts'),
    path('ajax/sectors/', views.load_sectors, name='ajax_sectors'),

]