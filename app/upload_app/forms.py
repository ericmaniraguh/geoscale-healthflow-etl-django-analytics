

# app/upload_app/forms.py
from django import forms


class ShapefileUploadForm(forms.Form):
    """Form for uploading shapefile (.zip)"""
    file = forms.FileField(
        label="Upload Shapefile (.zip)",
        required=True,
        help_text="Upload a ZIP file containing .shp, .shx, .dbf, and .prj files *"
    )
    dataset_name = forms.CharField(
        max_length=255,
        required=False,
        # help_text="Optional: Name for this dataset",
        widget=forms.TextInput(attrs={
            'placeholder': 'e.g., Rwanda Administrative Boundaries 2024'
        })
    )
    country = forms.CharField(
        max_length=100,
        initial="Rwanda",
        # help_text="Country name",
        widget=forms.TextInput(attrs={
            'placeholder': 'Country name (e.g Rwanda)'
        })
    )
    year = forms.IntegerField(
        initial=2024,
        # help_text="Year of the data",
        widget=forms.NumberInput(attrs={
            'placeholder': 'Year of the data (e.g 2024)'
        })
    )
    source = forms.CharField(
        max_length=255,
        required=False,
        # help_text="Data source (e.g., RBC, WHO, MINISANTE)",
        widget=forms.TextInput(attrs={
            'placeholder': 'Data source (e.g., Rwanda Biomedical Center (RBC), WHO, MINISANTE)'
        })
    )
    description = forms.CharField(
        widget=forms.Textarea(
            attrs={
                "rows": 5,
                "cols": 100,
                "maxlength": 200,
                "placeholder": "Enter a brief description of this shapefile dataset (max 200 characters)"
            }
        ),
        required=False,
        # help_text="Brief description of this dataset (max 200 characters)"
    )


class SlopeGeoJsonUploadForm(forms.Form):
    """Form for uploading slope/elevation GeoTIFF data"""

    # Mandatory
    tif_file = forms.FileField(
        label="Upload GeoTIFF (.tif/.tiff)",
        required=True,
        help_text="Upload elevation/slope raster data *"
    )
    region = forms.CharField(
        max_length=100,
        required=True,
        initial="Rwanda",
        # help_text="Region name",
        widget=forms.TextInput(attrs={
            'placeholder': 'Country name (e.g Rwanda)'
        })
    )
    year_uploaded = forms.IntegerField(
        required=True,
        initial=2025,
        # help_text="Year the dataset is uploaded",
        widget=forms.NumberInput(attrs={
            'placeholder': 'Year the dataset is uploaded (e.g 2025) *'
        })
    )

    # Optional metadata
    data_type = forms.CharField(
        max_length=100,
        required=False,
        # help_text="Data type (e.g., slope, elevation)",
        widget=forms.TextInput(attrs={
            'placeholder': 'Data type (e.g., Digital Elevation Model, Slope)'
        })
    )

    description = forms.CharField(
        widget=forms.Textarea(
            attrs={
                "rows": 5,
                "cols": 100,
                "maxlength": 200,
                "placeholder": "Enter a brief description of this slope/elevation dataset (max 200 characters)"
            }
        ),
        required=False,
        # help_text="Brief description of this dataset (max 200 characters)"
    )

    resolution = forms.CharField(
        max_length=50,
        required=False,
        # help_text="Resolution (e.g., '30m')",
        widget=forms.TextInput(attrs={
            'placeholder': 'Resolution (e.g., 30m)'
        })
    )
    projection = forms.CharField(
        max_length=50,
        required=False,
        # help_text="Projection (e.g., 'EPSG:32735')",
        widget=forms.TextInput(attrs={
            'placeholder': 'Projection e.g., EPSG:32735'
        })
    )
    source = forms.CharField(
        max_length=255,
        required=False,
        # help_text="General source of the dataset (e.g., USGS SRTM 30m DEM)",
        widget=forms.TextInput(attrs={
            'placeholder': 'General source of the dataset (e.g., USGS SRTM 30m Digital Elevation Model)'
        })
    )
   

    source_website = forms.URLField(
    required=False,
    widget=forms.TextInput(
        attrs={
            "class": "form-input",  
            "placeholder": "Direct URL to the data source website (https://earthexplorer.usgs.gov/)"
        }
    )
)



    extracted_from = forms.CharField(
        max_length=255,
        required=False,
        # help_text="Platform or portal where the data was extracted",
        widget=forms.TextInput(attrs={
            'placeholder': 'Platform or portal where the data was extracted (e.g., Google Earth Engine, NASA Earthdata)'
        })
    )
    data_provider = forms.CharField(
        max_length=255,
        required=False,
        # help_text="Organization providing the data",
        widget=forms.TextInput(attrs={
            'placeholder': 'Organization providing the data (e.g., United States Geological Survey (USGS))'
        })
    )


    acquisition_date = forms.CharField(
    max_length=50,
    required=False,
    # help_text="Date when the data was acquired",
    widget=forms.TextInput(
        attrs={
            "class": "form-input",
            "placeholder": "Acquisition date (e.g., 2023-01-15)"
        }
    )
)

    processing_level = forms.CharField(
        max_length=50,
        required=False,
        # help_text="Processing level (raw, processed, derived)",
        widget=forms.TextInput(attrs={
            'placeholder': 'Processing level (e.g., raw, processed, derived, Level 2)'
        })
    )
    license_info = forms.CharField(
        max_length=255,
        required=False,
        # help_text="Data usage rights and restrictions",
        widget=forms.TextInput(attrs={
            'placeholder': 'Data usage rights and restrictions (e.g., Public Domain, Creative Commons)'
        })
    )
    doi = forms.CharField(
        max_length=100,
        required=False,
        # help_text="DOI (Digital Object Identifier) for citation",
        widget=forms.TextInput(attrs={
            'placeholder': 'DOI (Digital Object Identifier) for citation (e.g., 10.5066/F7PR7TFT)'
        })
    )

class HealthCenterLabDataForm(forms.Form):
    """Form for uploading health center laboratory data"""
    file = forms.FileField(
        label="Upload Health Center Data (CSV/Excel)",
        required=True,
          help_text="CSV or Excel file with health center laboratory records",
        widget=forms.ClearableFileInput(attrs={
            'class': 'form-control',
            'accept': '.csv, .xls, .xlsx'
        })
    
    )
    dataset_name = forms.CharField(
        max_length=255,
        help_text="Name for this dataset",
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Name for this dataset (e.g., Bugesera Health Center Malaria Records 2024)'
        })
    )
    district = forms.CharField(
        max_length=100,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'District name (e.g., Bugesera)'
        })
    )
    sector = forms.CharField(
        max_length=100,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Sector name (e.g., Kamabuye)'
        })
    )
    health_center = forms.CharField(
        max_length=100,
        widget=forms.TextInput(attrs={
            'class': 'form-control',
            'placeholder': 'Health center name (e.g., Kamabuye Health Center)'
        })
    )
    year = forms.IntegerField(
        widget=forms.NumberInput(attrs={
            'class': 'form-control',
            'placeholder': 'Year of the data (e.g. 2024)'
        })
    )
    description = forms.CharField(
        widget=forms.Textarea(attrs={
            'class': 'form-control',
            "rows": 5,
            "cols": 100,
            "maxlength": 200,
            "placeholder": "Enter a brief description of this health center dataset (max 200 characters)"
        }),
        required=False
    )

class HMISAPIDataForm(forms.Form):
    """Form for uploading HMIS API malaria data"""

    # Mandatory fields
    file = forms.FileField(
        required=True,
        help_text="CSV or Excel file with HMIS malaria records",
        widget=forms.ClearableFileInput(attrs={
            'class': 'form-control',
            'accept': '.csv, .xls, .xlsx'
        })
    )
    dataset_name = forms.CharField(
        max_length=255,
        required=True,
        # help_text="Name for this dataset",
        widget=forms.TextInput(attrs={
            'placeholder': 'Name for this dataset (e.g., HMIS Malaria Data Bugesera 2024)'
        })
    )
    district = forms.CharField(
        max_length=100,
        required=True,
        # help_text="District name",
        widget=forms.TextInput(attrs={
            'placeholder': 'District name (e.g., Bugesera) *'
        })
    )
    health_facility = forms.CharField(
        max_length=100,
        required=True,
        # help_text="Health facility name",
        widget=forms.TextInput(attrs={
            'placeholder': 'Health facility name (e.g., Kamabuye Health Center) *'
        })
    )
    year = forms.CharField(
        required=True,
        # help_text="Year of the data",
        widget=forms.NumberInput(attrs={
            'placeholder': 'Year of the data (e.g 2024) *'
        })
    )

    # Optional metadata
    period = forms.CharField(
    max_length=50,
    required=True,
    widget=forms.TextInput(attrs={
        "placeholder": "Period covered (e.g., '2021-2023') *"
    })
)

    reporting_level = forms.CharField(
        max_length=50,
        required=False,
        # help_text="Reporting level (facility, district, national)",
        widget=forms.TextInput(attrs={
            'placeholder': 'Reporting level (e.g. facility, district, national)'
        })
    )
    source = forms.CharField(
        max_length=255,
        required=False,
        # help_text="Data source (e.g., 'RBC HMIS database')",
        widget=forms.TextInput(attrs={
            'placeholder': 'Data source (e.g., RBC HMIS database)'
        })
    )

    description = forms.CharField(
        widget=forms.Textarea(
            attrs={
                "rows": 5,
                "cols": 100,
                "maxlength": 200,
                "placeholder": "Enter a brief description of this HMIS dataset (max 200 characters)"
            }
        ),
        required=False,
        # help_text="Brief description of this dataset (max 200 characters)"
    )


class TemperatureDataForm(forms.Form):
    """Form for uploading temperature data"""

    temperature = forms.FileField(
        label="Upload Precipitation Data (CSV/Excel)",
        required=True,
        help_text="CSV or Excel file with precipitation measurements",
        widget=forms.ClearableFileInput(attrs={
            'class': 'form-control',
            'accept': '.csv, .xls, .xlsx'
        })
    )

    
    dataset_name = forms.CharField(
        max_length=255,
        required=True,
        initial="Temperature Dataset",
        help_text="Name for this dataset",
        widget=forms.TextInput(attrs={
            'placeholder': 'e.g., Rwanda Weather Stations Temperature 2024'
        })
    )

 
    station = forms.CharField(
        max_length=100,
        required=True,
        # help_text="Weather station name(s)",
        widget=forms.TextInput(attrs={
            'placeholder': 'Weather station name(s) (e.g., Kigali International Airport Station)'
        })
    )
    years = forms.CharField(
        max_length=50,
        required=True,
        help_text="Years covered (e.g., '2021, 2022, 2023')",
        widget=forms.TextInput(attrs={
            'placeholder': 'e.g., 2021, 2022, 2023'
        })
    )
    description = forms.CharField(
        widget=forms.Textarea(
            attrs={
                "rows": 5,
                "cols": 100,
                "maxlength": 200,
                "placeholder": "Enter a brief description of this temperature dataset (max 200 characters)"
            }
        ),
        required=False,
        help_text="Brief description of this dataset (max 200 characters)"
    )


class PrecipitationDataForm(forms.Form):
    """Form for uploading precipitation data"""
    # precipitation = forms.FileField(
    #     max_length=255,
    #     required=True,
    #     # initial="Precipitation Dataset",
    #     help_text="CSV or Excel file with precipitation measurements",
    #     widget=forms.TextInput(attrs={
    #         'placeholder': 'Upload Precipitation Data (CSV/Excel)'
    #     })
    # )

    precipitation = forms.FileField(
    label="Upload Precipitation Data (CSV/Excel)",
    required=True,
    help_text="CSV or Excel file with precipitation measurements",
    widget=forms.ClearableFileInput(attrs={
        'class': 'form-control',
        'accept': '.csv, .xls, .xlsx'
    })
    )



    dataset_name = forms.CharField(
        max_length=255,
        required=True,
        initial="Precipitation Dataset",
        help_text="Name for this dataset",
        widget=forms.TextInput(attrs={
            'placeholder': 'e.g., Rwanda Weather Stations Precipitation 2024'
        })
    )

    station = forms.CharField(
            max_length=255,
            required=True,
            help_text="Years covered (e.g., '2021, 2022, 2023')",
            widget=forms.TextInput(attrs={
                'placeholder': 'Weather station name(s) (e.g., Kigali International Airport Station)'
            })
        )


    years = forms.CharField(
        max_length=50,
        required=True,
        # help_text="Years covered (e.g., '2021, 2022, 2023')",
        widget=forms.TextInput(attrs={
            'placeholder': 'Years covered (e.g., 2021, 2022, 2023)'
        })
    )
    description = forms.CharField(
        widget=forms.Textarea(
            attrs={
                "rows": 5,
                "cols": 100,
                "maxlength": 200,
                "placeholder": "Enter a brief description of this precipitation dataset (max 200 characters)"
            }
        ),
        required=False,
        # help_text="Brief description of this dataset (max 200 characters)"
    )

    