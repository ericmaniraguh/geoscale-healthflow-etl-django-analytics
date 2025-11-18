from django.urls import path
from .views import (
    login_view, 
    signup_view, 
    logout_view,
    verify_otp_view,
    resend_otp,
    load_districts,
    load_sectors,
    CustomPasswordResetView,
    CustomPasswordResetDoneView,
    CustomPasswordResetConfirmView,
    CustomPasswordResetCompleteView  # Correct name
)

app_name = 'accounts'

urlpatterns = [
    path('login/', login_view, name='login'),
    path('signup/', signup_view, name='signup'),
    path('logout/', logout_view, name='logout'),
    path('verify-otp/', verify_otp_view, name='verify_otp'),
    path('resend-otp/', resend_otp, name='resend_otp'),
    
    # AJAX endpoints
    path('ajax/districts/', load_districts, name='load_districts'),
    path('ajax/sectors/', load_sectors, name='load_sectors'),
    
    # Password reset URLs
    path('password-reset/', CustomPasswordResetView.as_view(), name='password_reset'),
    path('password-reset/done/', CustomPasswordResetDoneView.as_view(), name='password_reset_done'),
    path('password-reset-confirm/<uidb64>/<token>/', CustomPasswordResetConfirmView.as_view(), name='password_reset_confirm'),
    path('password-reset-complete/', CustomPasswordResetCompleteView.as_view(), name='password_reset_complete'),
]