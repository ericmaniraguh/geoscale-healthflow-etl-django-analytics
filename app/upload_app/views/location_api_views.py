from django.http import JsonResponse
from django.views.decorators.http import require_http_methods
from django.contrib.auth.decorators import login_required
from django.apps import apps

@login_required
@require_http_methods(["GET"])
def get_provinces(request):
    """Get all provinces"""
    try:
        Province = apps.get_model("accounts", "Province")
        provinces = Province.objects.all().values('id', 'name').order_by('name')
        return JsonResponse({
            'status': 'success',
            'provinces': list(provinces)
        })
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)

@login_required
@require_http_methods(["GET"])
def get_districts(request):
    """Get districts by province"""
    try:
        province_id = request.GET.get('province_id')
        District = apps.get_model("accounts", "District")
        
        if province_id:
            districts = District.objects.filter(
                province_id=province_id
            ).values('id', 'name').order_by('name')
        else:
            districts = District.objects.all().values('id', 'name', 'province__name').order_by('name')
        
        return JsonResponse({
            'status': 'success',
            'districts': list(districts)
        })
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)

@login_required
@require_http_methods(["GET"])
def get_sectors(request):
    """Get sectors by district"""
    try:
        district_id = request.GET.get('district_id')
        Sector = apps.get_model("accounts", "Sector")
        
        if district_id:
            sectors = Sector.objects.filter(
                district_id=district_id
            ).values('id', 'name').order_by('name')
        else:
            sectors = Sector.objects.all().values('id', 'name', 'district__name').order_by('name')
        
        return JsonResponse({
            'status': 'success',
            'sectors': list(sectors)
        })
    except Exception as e:
        return JsonResponse({
            'status': 'error',
            'error': str(e)
        }, status=500)