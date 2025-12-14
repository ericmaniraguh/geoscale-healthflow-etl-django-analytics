import pytest
from django.utils import timezone
from django.db.utils import IntegrityError

from app.accounts.models import CustomUser, Province, District, Sector


# ----------------------------
# Fixtures
# ----------------------------

@pytest.fixture
def user(db):
    return CustomUser.objects.create_user(
        username="testuser_model",
        email="test_model@example.com",
        password="Password123!",
    )


@pytest.fixture
def location(db):
    province = Province.objects.create(name="Kigali City Model Test")
    district = District.objects.create(name="Gasabo Model", province=province)
    sector = Sector.objects.create(name="Kacyiru Model", district=district)
    return province, district, sector


# ----------------------------
# CustomUser OTP Tests
# ----------------------------

@pytest.mark.django_db
class TestCustomUserOTP:

    def test_generate_otp(self, user):
        otp = user.generate_otp()

        assert len(otp) == 6
        assert otp.isdigit()
        assert user.otp_code == otp
        assert user.otp_created is not None

    def test_verify_otp_success(self, user):
        otp = user.generate_otp()
        assert user.verify_otp(otp) is True

    def test_verify_otp_wrong_code(self, user):
        user.generate_otp()
        assert user.verify_otp("000000") is False

    def test_verify_otp_no_otp(self, user):
        assert user.verify_otp("123456") is False

    def test_verify_otp_expired(self, user):
        user.generate_otp()
        user.otp_created = timezone.now() - timezone.timedelta(minutes=6)
        user.save()

        assert user.is_otp_expired() is True
        assert user.verify_otp(user.otp_code) is False

    def test_clear_otp(self, user):
        user.generate_otp()
        user.clear_otp()

        assert user.otp_code is None
        assert user.otp_created is None
        assert user.email_verified is True


# ----------------------------
# Location Model Tests
# ----------------------------

@pytest.mark.django_db
class TestLocationModels:

    def test_province_str(self, location):
        province, _, _ = location
        assert str(province) == "Kigali City Model Test"

    def test_district_str_and_relation(self, location):
        province, district, _ = location
        assert district.province == province
        assert str(district) == "Gasabo Model (Kigali City Model Test)"

    def test_sector_str_and_relation(self, location):
        _, district, sector = location
        assert sector.district == district
        assert str(sector) == "Kacyiru Model (Gasabo Model (Kigali City Model Test))"

    def test_province_name_uniqueness(self, location):
        with pytest.raises(IntegrityError):
            Province.objects.create(name="Kigali City Model Test")

    def test_cascade_delete(self, location):
        province, district, sector = location

        province.delete()

        assert not Province.objects.filter(id=province.id).exists()
        assert not District.objects.filter(id=district.id).exists()
        assert not Sector.objects.filter(id=sector.id).exists()

    def test_district_uniqueness(self, location):
        """Test unique_together for District (province + name)"""
        province, district, _ = location
        with pytest.raises(IntegrityError):
            District.objects.create(name=district.name, province=province)

    def test_sector_uniqueness(self, location):
        """Test unique_together for Sector (district + name)"""
        _, district, sector = location
        with pytest.raises(IntegrityError):
            Sector.objects.create(name=sector.name, district=district)

    def test_province_ordering(self, db):
        """Test Province ordering by name"""
        p1 = Province.objects.create(name="B Province")
        p2 = Province.objects.create(name="A Province")
        provinces = list(Province.objects.all())
        # Filter to only our test provinces to avoid interference from fixture data if any
        # actually db fixture is isolated usually, but let's just check relative order or exact match
        # The 'location' fixture creates "Kigali City..." so we should probably fetch all and check order
        # or better: verify the order of these specific two is correct in the result list
        
        # Simpler approach: check if A is before B in the full list
        # But wait, 'location' fixture runs for other tests, but 'db' fixture is fresh? 
        # No, 'location' fixture is not requested here, so only 'db' is used. 
        # But other tests in class use 'location', pytest-django resets db per test usually.
        # So we should be good checking exact list if we created only these two.
        # EXCEPT: ordering is on name.
        assert provinces == [p2, p1]

    def test_district_ordering(self, db):
        """Test District ordering (by province then name)"""
        p1 = Province.objects.create(name="A Province")
        d1 = District.objects.create(name="B District", province=p1)
        d2 = District.objects.create(name="A District", province=p1)
        
        districts = list(District.objects.filter(province=p1))
        assert districts == [d2, d1]

    def test_sector_ordering(self, db):
        """Test Sector ordering (by district then name)"""
        p = Province.objects.create(name="Test Prov")
        d = District.objects.create(name="Test Dist", province=p)
        s1 = Sector.objects.create(name="B Sector", district=d)
        s2 = Sector.objects.create(name="A Sector", district=d)
        
        sectors = list(Sector.objects.filter(district=d))
        assert sectors == [s2, s1]


# ----------------------------
# CustomUser Field & Location Tests
# ----------------------------

@pytest.mark.django_db
class TestCustomUserFields:

    def test_user_defaults_and_optional_fields(self):
        user = CustomUser.objects.create_user(
            username="metauser",
            email="meta@example.com",
            password="Password123!",
            phone_number="0788888888",
            health_centre_name="Kacyiru HC",
            position="Data Manager",
        )

        assert user.country == "Rwanda"
        assert user.email_verified is False
        assert user.phone_number == "0788888888"
        assert user.health_centre_name == "Kacyiru HC"
        assert user.position == "Data Manager"

    def test_user_location_assignment_and_set_null(self):
        province = Province.objects.create(name="Northern Province")
        district = District.objects.create(name="Musanze", province=province)
        sector = Sector.objects.create(name="Muhoza", district=district)

        user = CustomUser.objects.create_user(
            username="locuser",
            password="Password123!",
            province=province,
            district=district,
            sector=sector,
        )

        assert user.province == province
        assert user.district == district
        assert user.sector == sector

        sector.delete()
        user.refresh_from_db()

        assert user.sector is None
        assert user.district is not None

    def test_verification_token_field(self):
        """Test verification_token field on CustomUser"""
        user = CustomUser.objects.create_user(
            username="tokenuser",
            password="Password123!",
            verification_token="abc-123-xyz"
        )
        user.refresh_from_db()
        assert user.verification_token == "abc-123-xyz"

    def test_create_superuser(self):
        """Test creating a superuser"""
        admin = CustomUser.objects.create_superuser(
            username="superadmin", 
            email="super@example.com", 
            password="Password123!"
        )
        assert admin.is_superuser
        assert admin.is_staff
        assert admin.is_active

    def test_user_manager_create_user(self):
        """Test explicitly using the manager's create_user method"""
        user = CustomUser.objects.create_user("manager_user", "man@example.com", "Password123!")
        assert user.username == "manager_user"
        assert user.email == "man@example.com"
        assert user.check_password("Password123!")
        assert not user.is_superuser
        assert not user.is_staff
