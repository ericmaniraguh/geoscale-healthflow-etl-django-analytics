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
