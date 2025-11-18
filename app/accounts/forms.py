# app/accounts/forms.py
from django import forms
from django.contrib.auth.forms import UserCreationForm, AuthenticationForm
from .models import CustomUser, Province, District, Sector

BASE_POSITIONS = [
    ("Lab Engineer", "Lab Engineer"),
    ("Researcher", "Researcher"),
    ("HC Data Manager", "HC Data Manager"),
    ("Analyst", "Analyst"),
]

POSITION_CHOICES = sorted(BASE_POSITIONS, key=lambda x: x[1]) + [("Other", "Other")]

class CustomUserCreationForm(UserCreationForm):
    position = forms.ChoiceField(
        choices=[("", "Select position")] + POSITION_CHOICES,
        required=True,
        widget=forms.Select(attrs={
            "class": "input",
            "autocomplete": "organization-title",
            "style": "width: 100%; padding: 10px 12px; border: 1px solid #dadce0; border-radius: 6px; font-size: 14px; box-sizing: border-box;"
        })
    )
    custom_position = forms.CharField(
        required=False,
        widget=forms.TextInput(attrs={
            "placeholder": "Enter your position if Other",
            "class": "input"
        })
    )

    class Meta:
        model = CustomUser
        fields = [
            "first_name", "last_name", "email", "phone_number",
            "username", "password1", "password2",
            "country", "province", "district", "sector",
            "health_centre_name", "position", "custom_position",
        ]
        widgets = {
            "first_name": forms.TextInput(attrs={
                "placeholder": "First name *",
                "class": "input",
                "autocomplete": "given-name"
            }),
            "last_name": forms.TextInput(attrs={
                "placeholder": "Last name *",
                "class": "input",
                "autocomplete": "family-name"
            }),
            "email": forms.EmailInput(attrs={
                "placeholder": "Email *",
                "class": "input",
                "autocomplete": "email"
            }),
            "phone_number": forms.TextInput(attrs={
                "placeholder": "Phone number",
                "class": "input",
                "autocomplete": "tel"
            }),
            "username": forms.TextInput(attrs={
                "placeholder": "Username *",
                "class": "input",
                "autocomplete": "username"
            }),
            "health_centre_name": forms.TextInput(attrs={
                "placeholder": "Health centre",
                "class": "input",
                "autocomplete": "organization"
            }),
            "country": forms.Select(attrs={
                "class": "input",
                "autocomplete": "country-name"
            }),
            "province": forms.Select(attrs={
                "class": "input",
                "autocomplete": "address-level1"
            }),
            "district": forms.Select(attrs={
                "class": "input",
                "autocomplete": "address-level2"
            }),
            "sector": forms.Select(attrs={
                "class": "input",
                "autocomplete": "address-level3"
            }),
        }

    def clean_position(self):
        position = self.cleaned_data.get("position")
        custom_position = self.cleaned_data.get("custom_position")
        if position == "Other" and not custom_position:
            raise forms.ValidationError("Please specify your position if 'Other' is selected.")
        return custom_position if position == "Other" else position

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        # Default country
        self.fields["country"].initial = "Rwanda"

        # ✅ OPTIMIZED: Only load provinces initially
        self.fields["province"].queryset = Province.objects.all().only('id', 'name')
        self.fields["district"].queryset = District.objects.none()
        self.fields["sector"].queryset = Sector.objects.none()

        # Password placeholders
        self.fields["password1"].widget.attrs.update({
            "placeholder": "Password *",
            "class": "input",
            "autocomplete": "new-password"
        })
        self.fields["password2"].widget.attrs.update({
            "placeholder": "Confirm password *",
            "class": "input",
            "autocomplete": "new-password"
        })

        # ✅ ONLY populate if form has data (during POST validation errors)
        if 'province' in self.data:
            try:
                province_id = int(self.data.get('province'))
                self.fields['district'].queryset = (
                    District.objects
                    .filter(province_id=province_id)
                    .only('id', 'name')
                    .order_by('name')
                )
            except (ValueError, TypeError):
                pass
        
        if 'district' in self.data:
            try:
                district_id = int(self.data.get('district'))
                self.fields['sector'].queryset = (
                    Sector.objects
                    .filter(district_id=district_id)
                    .only('id', 'name')
                    .order_by('name')
                )
            except (ValueError, TypeError):
                pass


class CustomLoginForm(AuthenticationForm):
    username = forms.CharField(widget=forms.TextInput(attrs={
        "placeholder": "Your username",
        "class": "input",
        "autocomplete": "username"
    }))
    password = forms.CharField(widget=forms.PasswordInput(attrs={
        "placeholder": "Your password",
        "class": "input",
        "autocomplete": "current-password"
    }))