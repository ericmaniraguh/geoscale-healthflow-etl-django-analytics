# app/accounts/views.py - FIXED VERSION

import json
from django.shortcuts import render, redirect, get_object_or_404
from django.contrib.auth import login, logout
from django.contrib.auth.forms import AuthenticationForm
from django.contrib.auth.decorators import login_required
from django.http import HttpResponse, JsonResponse
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_GET, require_POST
from django.contrib import messages
from django.core.mail import send_mail, EmailMessage
from django.conf import settings
from django.template.loader import render_to_string
from django.utils.html import strip_tags
from django.urls import reverse
from django.utils import timezone
import logging
import os
import random
import string

from .forms import CustomUserCreationForm
from .models import CustomUser, District, Sector

logger = logging.getLogger(__name__)

def login_view(request):
    if request.method == "POST":
        form = AuthenticationForm(request, data=request.POST)
        if form.is_valid():
            user = form.get_user()
            
            # Check if user's email is verified
            if not user.email_verified:
                messages.error(request, "Please verify your email address before logging in.")
                return redirect("accounts:login")
            
            login(request, user)

            # Redirect based on role
            if user.is_superuser or user.is_staff:
                return redirect(reverse("upload_app:admin_dashboard"))
            else:
                return redirect(reverse("upload_app:user_dashboard"))
    else:
        form = AuthenticationForm(request)

    return render(request, "accounts/login.html", {"form": form})

def signup_view(request):
    form = CustomUserCreationForm(request.POST or None)
    
    if request.method == "POST":
        if form.is_valid():
            try:
                # DON'T save to database yet - store in session instead
                form_data = {
                    'first_name': form.cleaned_data['first_name'],
                    'last_name': form.cleaned_data['last_name'],
                    'email': form.cleaned_data['email'],
                    'phone_number': form.cleaned_data.get('phone_number', ''),
                    'username': form.cleaned_data['username'],
                    'password1': form.cleaned_data['password1'],
                    'country': form.cleaned_data.get('country', 'Rwanda'),
                    'province_id': form.cleaned_data.get('province').id if form.cleaned_data.get('province') else None,
                    'district_id': form.cleaned_data.get('district').id if form.cleaned_data.get('district') else None,
                    'sector_id': form.cleaned_data.get('sector').id if form.cleaned_data.get('sector') else None,
                    'health_centre_name': form.cleaned_data.get('health_centre_name', ''),
                    'position': form.cleaned_data.get('position', ''),
                }
                
                # Generate OTP
                otp_code = ''.join(random.choices(string.digits, k=6))
                otp_created = timezone.now()
                
                # Store form data and OTP in session (not database)
                request.session['signup_data'] = form_data
                request.session['otp_code'] = otp_code
                request.session['otp_created'] = otp_created.isoformat()
                request.session['signup_email'] = form.cleaned_data['email']
                
                # Send OTP email
                success = send_otp_email_simple(form.cleaned_data['email'], form.cleaned_data['first_name'], otp_code)
                
                if success:
                    messages.success(
                        request, 
                        f"We've sent a verification code to {form.cleaned_data['email']}. Please check your email."
                    )
                    return redirect("accounts:verify_otp")
                else:
                    messages.error(
                        request, 
                        "Failed to send verification email. Please try again or contact support."
                    )
                    
            except Exception as e:
                logger.error(f"Signup error: {str(e)}")
                messages.error(request, "An error occurred during registration. Please try again.")
                
        else:
            messages.error(request, "Please correct the errors below.")
    
    return render(request, "accounts/signup.html", {"form": form})


def send_otp_email_simple(user_email, user_name, otp):
    """Send OTP email"""
    try:
        subject = "Verify Your Email - Registration Confirmation"
        
        html_message = render_to_string('accounts/emails/otp_verification.html', {
            'user_email': user_email,
            'user_name': user_name,
            'otp_code': otp,
            'site_name': 'GeoScale Malaria Platform',
        })
        
        plain_message = f"""
Hello {user_name},

Thank you for registering with GeoScale Malaria Platform!

Your verification code is: {otp}

Please enter this code on the verification page to complete your registration.
This code will expire in 5 minutes.

If you didn't create this account, please ignore this email.

Best regards,
The GeoScale Malaria Team
Powered by Upanzi Network
        """
        
        result = send_mail(
            subject=subject,
            message=plain_message,
            from_email=settings.DEFAULT_FROM_EMAIL,
            recipient_list=[user_email],
            html_message=html_message,
            fail_silently=False,
        )
        
        logger.info(f"Email sent to {user_email}. Result: {result}")
        print(f"✅ Email sent successfully to {user_email}")
        return result == 1
        
    except Exception as e:
        logger.error(f"Failed to send OTP email to {user_email}: {str(e)}")
        print(f"❌ Email error: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

# def verify_otp_view(request):
#     signup_data = request.session.get('signup_data')
#     otp_code = request.session.get('otp_code')
#     otp_created_str = request.session.get('otp_created')
    
#     if not signup_data or not otp_code or not otp_created_str:
#         messages.error(request, "No pending verification found. Please register again.")
#         return redirect("accounts:signup")
    
#     # Check if OTP has expired (5 minutes)
#     otp_created = timezone.datetime.fromisoformat(otp_created_str.replace('Z', '+00:00'))
#     if timezone.is_naive(otp_created):
#         otp_created = timezone.make_aware(otp_created)
    
#     time_diff = timezone.now() - otp_created
#     if time_diff.total_seconds() > 600:  # 5 minutes
#         messages.error(request, "Verification code has expired. Please register again.")
#         # Clear session data
#         for key in ['signup_data', 'otp_code', 'otp_created', 'signup_email']:
#             request.session.pop(key, None)
#         return redirect("accounts:signup")

#     if request.method == "POST":
#         entered_code = request.POST.get("otp_code", "").strip()
        
#         if not entered_code:
#             messages.error(request, "Please enter the verification code.")
#         elif entered_code == otp_code:
#             # OTP is valid - NOW create the user in database
#             try:
#                 from .models import Province, District, Sector
                
#                 # Get related objects if they exist
#                 province = None
#                 district = None
#                 sector = None
                
#                 if signup_data.get('province_id'):
#                     try:
#                         province = Province.objects.get(id=signup_data['province_id'])
#                     except Province.DoesNotExist:
#                         pass
                
#                 if signup_data.get('district_id'):
#                     try:
#                         district = District.objects.get(id=signup_data['district_id'])
#                     except District.DoesNotExist:
#                         pass
                
#                 if signup_data.get('sector_id'):
#                     try:
#                         sector = Sector.objects.get(id=signup_data['sector_id'])
#                     except Sector.DoesNotExist:
#                         pass
                
#                 # Create the user NOW (after OTP verification)
#                 user = CustomUser.objects.create_user(
#                     username=signup_data['username'],
#                     email=signup_data['email'],
#                     password=signup_data['password1'],
#                     first_name=signup_data['first_name'],
#                     last_name=signup_data['last_name'],
#                     phone_number=signup_data.get('phone_number', ''),
#                     country=signup_data.get('country', 'Rwanda'),
#                     province=province,
#                     district=district,
#                     sector=sector,
#                     health_centre_name=signup_data.get('health_centre_name', ''),
#                     position=signup_data.get('position', ''),
#                     is_active=True,  # Account is active after verification
#                     email_verified=True,  # Email is verified
#                 )
                
#                 # Clear session data
#                 for key in ['signup_data', 'otp_code', 'otp_created', 'signup_email']:
#                     request.session.pop(key, None)
                
#                 # Auto-login the user
#                 login(request, user)
                
#                 messages.success(request, "Your account has been created and verified successfully! Welcome!")
#                 return redirect("upload_app:user_dashboard")
                
#             except Exception as e:
#                 logger.error(f"Error creating user: {str(e)}")
#                 messages.error(request, "An error occurred while creating your account. Please try again.")
#         else:
#             messages.error(request, "Invalid verification code. Please try again.")

#     return render(request, "accounts/otp_verify.html", {
#         "otp_method": "email",
#         "user_email": signup_data['email']
#     })

def verify_otp_view(request):
    signup_data = request.session.get('signup_data')
    otp_code = request.session.get('otp_code')
    otp_created_str = request.session.get('otp_created')
    
    if not signup_data or not otp_code or not otp_created_str:
        messages.error(request, "No pending verification found. Please register again.")
        return redirect("accounts:signup")
    
    # Check if OTP has expired (5 minutes)
    otp_created = timezone.datetime.fromisoformat(otp_created_str.replace('Z', '+00:00'))
    if timezone.is_naive(otp_created):
        otp_created = timezone.make_aware(otp_created)
    
    time_diff = timezone.now() - otp_created
    if time_diff.total_seconds() > 300:  # 5 minutes
        messages.error(request, "Verification code has expired. Please register again.")
        # Clear session data
        for key in ['signup_data', 'otp_code', 'otp_created', 'signup_email']:
            request.session.pop(key, None)
        return redirect("accounts:signup")

    if request.method == "POST":
        entered_code = request.POST.get("otp_code", "").strip()
        
        if not entered_code:
            messages.error(request, "Please enter the verification code.")
        elif entered_code == otp_code:
            # OTP is valid - NOW create the user in database
            try:
                from .models import Province, District, Sector
                
                # Get related objects if they exist
                province = None
                district = None
                sector = None
                
                if signup_data.get('province_id'):
                    try:
                        province = Province.objects.get(id=signup_data['province_id'])
                    except Province.DoesNotExist:
                        pass
                
                if signup_data.get('district_id'):
                    try:
                        district = District.objects.get(id=signup_data['district_id'])
                    except District.DoesNotExist:
                        pass
                
                if signup_data.get('sector_id'):
                    try:
                        sector = Sector.objects.get(id=signup_data['sector_id'])
                    except Sector.DoesNotExist:
                        pass
                
                # Create the user NOW (after OTP verification)
                user = CustomUser.objects.create_user(
                    username=signup_data['username'],
                    email=signup_data['email'],
                    password=signup_data['password1'],
                    first_name=signup_data['first_name'],
                    last_name=signup_data['last_name'],
                    phone_number=signup_data.get('phone_number', ''),
                    country=signup_data.get('country', 'Rwanda'),
                    province=province,
                    district=district,
                    sector=sector,
                    health_centre_name=signup_data.get('health_centre_name', ''),
                    position=signup_data.get('position', ''),
                    is_active=True,
                    email_verified=True,
                )
                
                # Clear session data
                for key in ['signup_data', 'otp_code', 'otp_created', 'signup_email']:
                    request.session.pop(key, None)
                
                # FIX: Specify the backend when logging in
                from django.contrib.auth import get_backends
                backend = get_backends()[0]  # Get the first configured backend
                user.backend = f'{backend.__module__}.{backend.__class__.__name__}'
                
                # Auto-login the user
                login(request, user, backend=user.backend)
                
                messages.success(request, "Your account has been created and verified successfully! Welcome!")
                return redirect("upload_app:user_dashboard")
                
            except Exception as e:
                logger.error(f"Error creating user: {str(e)}")
                messages.error(request, "An error occurred while creating your account. Please try again.")
        else:
            messages.error(request, "Invalid verification code. Please try again.")

    return render(request, "accounts/otp_verify.html", {
        "otp_method": "email",
        "user_email": signup_data['email']
    })

@require_POST
def resend_otp(request):
    signup_data = request.session.get('signup_data')
    
    if not signup_data:
        return JsonResponse({"success": False, "message": "No pending verification found."})
    
    try:
        # Generate new OTP
        otp_code = ''.join(random.choices(string.digits, k=6))
        otp_created = timezone.now()
        
        # Update session
        request.session['otp_code'] = otp_code
        request.session['otp_created'] = otp_created.isoformat()
        
        # Send email
        success = send_otp_email_simple(
            signup_data['email'], 
            signup_data['first_name'], 
            otp_code
        )
        
        if success:
            return JsonResponse({"success": True, "message": "Verification code sent successfully!"})
        else:
            return JsonResponse({"success": False, "message": "Failed to send email. Please try again."})
            
    except Exception as e:
        logger.error(f"Resend OTP error: {str(e)}")
        return JsonResponse({"success": False, "message": "An error occurred. Please try again."})

@login_required
def logout_view(request):
    logout(request)
    messages.success(request, "You have been logged out successfully.")
    return redirect("accounts:login")

# Keep your existing AJAX views unchanged
@require_GET
def load_districts(request):
    """AJAX view to load districts based on selected province"""
    province_id = request.GET.get("province")
    
    if not province_id:
        html = '''
        <label for="id_district">District</label>
        <select id="id_district" name="district" class="form-input"
                hx-get="/accounts/ajax/sectors/"
                hx-target="#sector-container"
                hx-trigger="change"
                hx-include="[name='csrfmiddlewaretoken']">
            <option value="">Select district</option>
        </select>
        '''
        return HttpResponse(html)
    
    try:
        districts = District.objects.filter(province_id=province_id).order_by("name")
        district_options = "".join(
            f'<option value="{d.id}">{d.name}</option>' for d in districts
        )
        
        html = f'''
        <label for="id_district">District</label>
        <select id="id_district" name="district" class="form-input"
                hx-get="/accounts/ajax/sectors/"
                hx-target="#sector-container"
                hx-trigger="change"
                hx-include="[name='csrfmiddlewaretoken']">
            <option value="">Select district</option>
            {district_options}
        </select>
        '''
        return HttpResponse(html)
        
    except Exception as e:
        logger.error(f"Error loading districts: {str(e)}")
        html = '''
        <label for="id_district">District</label>
        <select id="id_district" name="district" class="form-input">
            <option value="">Error loading districts</option>
        </select>
        '''
        return HttpResponse(html)

@require_GET
def load_sectors(request):
    """AJAX view to load sectors based on selected district"""
    district_id = request.GET.get("district")
    
    if not district_id:
        html = '''
        <label for="id_sector">Sector</label>
        <select id="id_sector" name="sector" class="form-input">
            <option value="">Select sector</option>
        </select>
        '''
        return HttpResponse(html)
    
    try:
        sectors = Sector.objects.filter(district_id=district_id).order_by("name")
        sector_options = "".join(
            f'<option value="{s.id}">{s.name}</option>' for s in sectors
        )
        
        html = f'''
        <label for="id_sector">Sector</label>
        <select id="id_sector" name="sector" class="form-input">
            <option value="">Select sector</option>
            {sector_options}
        </select>
        '''
        return HttpResponse(html)
        
    except Exception as e:
        logger.error(f"Error loading sectors: {str(e)}")
        html = '''
        <label for="id_sector">Sector</label>
        <select id="id_sector" name="sector" class="form-input">
            <option value="">Error loading sectors</option>
        </select>
        '''
        return HttpResponse(html)