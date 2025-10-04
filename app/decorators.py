# app/upload_app/decorators.py
from functools import wraps
from django.contrib.auth.decorators import login_required
from django.contrib import messages
from django.shortcuts import redirect
from django.http import HttpResponseForbidden

def admin_required(view_func):
    """
    Decorator that requires user to be admin (staff or superuser)
    """
    @wraps(view_func)
    def wrapper(request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect('login')
        
        if not (request.user.is_staff or request.user.is_superuser):
            messages.error(request, "Access denied. Administrator privileges required.")
            return redirect('upload_app:upload_dashboard')
        
        return view_func(request, *args, **kwargs)
    return wrapper

def admin_required_class(view_class):
    """
    Class-based view decorator that requires admin privileges
    """
    original_dispatch = view_class.dispatch
    
    def dispatch(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            return redirect('login')
        
        if not (request.user.is_staff or request.user.is_superuser):
            messages.error(request, "Access denied. Administrator privileges required.")
            return redirect('upload_app:upload_dashboard')
        
        return original_dispatch(self, request, *args, **kwargs)
    
    view_class.dispatch = dispatch
    return view_class

