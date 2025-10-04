
# etl_app/forms.py
from django import forms

class RwandaBoundariesForm(forms.Form):
    """Form for Rwanda Administrative Boundaries ETL - Based on Postman screenshot"""
    district = forms.CharField(
        max_length=100, 
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., Bugesera'
        })
    )
    sector = forms.CharField(
        max_length=100, 
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., Kamabuye'
        })
    )
    # Hidden default values - not shown to user
    transform_coords = forms.BooleanField(initial=True, required=False, widget=forms.HiddenInput())
    save_to_postgres = forms.BooleanField(initial=True, required=False, widget=forms.HiddenInput())
    update_mode = forms.CharField(initial='replace', required=False, widget=forms.HiddenInput())

class MalariaAPIForm(forms.Form):
    """Form for Malaria API Calculator ETL - Based on Postman screenshot"""
    province = forms.CharField(
        max_length=100,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., East'
        })
    )
    district = forms.CharField(
        max_length=100,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., Bugesera'
        })
    )
    years = forms.CharField(
        max_length=200,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., 2021,2022,2023'
        })
    )
    # Hidden default values
    save_to_postgres = forms.BooleanField(initial=True, required=False, widget=forms.HiddenInput())

class HealthCenterForm(forms.Form):
    """Form for Health Center Lab Data ETL - Based on Postman screenshot"""
    years = forms.CharField(
        max_length=200,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., 2021,2022,2023'
        })
    )
    district = forms.CharField(
        max_length=100, 
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., Bugesera'
        })
    )
    sector = forms.CharField(
        max_length=100, 
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., Kamabuye'
        })
    )
    # Hidden default values
    save_to_postgres = forms.BooleanField(initial=True, required=False, widget=forms.HiddenInput())
    show_available = forms.BooleanField(initial=False, required=False, widget=forms.HiddenInput())

class WeatherPrecTempForm(forms.Form):
    """Form for Weather Precipitation & Temperature ETL - Based on Postman screenshot"""
    years = forms.CharField(
        max_length=200,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., 2021,2022,2023'
        })
    )
    station_temp = forms.CharField(
        max_length=100, 
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., Nyamata'
        })
    )
    station_prec = forms.CharField(
        max_length=100, 
        required=False,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., Juru'
        })
    )
    # Hidden default values
    save_to_postgres = forms.BooleanField(initial=True, required=False, widget=forms.HiddenInput())
    show_available = forms.BooleanField(initial=False, required=False, widget=forms.HiddenInput())

class SlopeGeoJSONForm(forms.Form):
    """Form for Slope GeoJSON ETL - Based on Postman screenshot"""
    EXTRACTION_CHOICES = [
        ('coordinates', 'Extract by Coordinates'),
        ('administrative', 'Extract by Administrative Area')
    ]
    
    extraction_type = forms.ChoiceField(
        choices=EXTRACTION_CHOICES,
        initial='coordinates',
        widget=forms.Select(attrs={'class': 'form-control'})
    )
    
    district = forms.CharField(
        max_length=100,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., Kigali'
        })
    )
    sector = forms.CharField(
        max_length=100,
        required=True,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'e.g., Kigali'
        })
    )
    
    # Coordinates for bbox extraction (only shown when coordinates type is selected)
    min_lon = forms.FloatField(
        required=False,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': '29.85',
            'step': '0.000001'
        })
    )
    min_lat = forms.FloatField(
        required=False,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': '-1.06',
            'step': '0.000001'
        })
    )
    max_lon = forms.FloatField(
        required=False,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': '29.87',
            'step': '0.000001'
        })
    )
    max_lat = forms.FloatField(
        required=False,
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': '-1.04',
            'step': '0.000001'
        })
    )
    
    # Hidden default values
    save_to_postgres = forms.BooleanField(initial=True, required=False, widget=forms.HiddenInput())
    calculate_statistics = forms.BooleanField(initial=True, required=False, widget=forms.HiddenInput())
    update_mode = forms.CharField(initial='replace', required=False, widget=forms.HiddenInput())