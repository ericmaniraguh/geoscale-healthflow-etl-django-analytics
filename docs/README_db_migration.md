# README — Database Migrations & Rwanda Locations Load

This document describes, step-by-step, how I prepared the PostgreSQL database, created/applied Django migrations, and populated the Rwanda administrative locations using the custom management command `load_rwanda_locations`.

It’s written as a reproducible checklist so you (or any team member) can run these steps again in the future.

---

## Prerequisites

1. Python 3.13 (or compatible Python 3.x)
2. A working virtual environment with project dependencies installed (`pip install -r requirements.txt`).
3. PostgreSQL running and accessible from the Django project (in this project it was a Docker container mapped to `127.0.0.1` on a custom port).
4. Docker containers (if used) running the Postgres instance(s).
5. `manage.py` is at the project root and Django settings point to the intended database.

---

## 1. Verify running containers & ports (Docker)

If your Postgres is running in Docker, inspect containers and port mappings:

```bash
# list running containers with ports
docker ps
```

Example relevant output in this project (ports shown):

* `airflow-postgres` -> `0.0.0.0:5444->5432`
* `staging-postgres` -> `0.0.0.0:5434->5432`

> Note: Django must point to the host port (e.g., `5434` or `5444`) — **not** the container port `5432` directly when using `127.0.0.1`.

---

## 2. Confirm Django's DATABASES configuration

Open `settings.py` (or your settings loader) and confirm `DATABASES['default']` has the correct host, port, name and credentials matching the running Postgres instance.

Example for `staging-postgres` mapped to `5434`:

```python
DATABASES = {
    'default': {
        'ENGINE': 'django.db.backends.postgresql',
        'NAME': 'postgres',
        'USER': 'postgres',
        'PASSWORD': 'postgres',
        'HOST': '127.0.0.1',
        'PORT': '5434',
    }
}
```

Adjust values to match your environment.

---

## 3. Check app models are discoverable

From the project root run Django shell and confirm models are present:

```bash
python manage.py shell
```

Inside the shell:

```python
from django.apps import apps
models = apps.get_models()
for m in models:
    print(m)
```

This confirms Django loads your apps and models (e.g. `Province`, `District`, `Sector`, `CustomUser`, etc.).

Exit the shell when done (`exit()`).

---

## 4. Create and apply migrations

> Important: do **not** run `--run-syncdb` unless you fully understand the consequences (it can create tables for apps without migrations in an order that causes FK problems). Prefer standard `makemigrations` and `migrate`.

From the project root run:

```bash
# create migrations for custom apps (example list)
python manage.py makemigrations accounts analytics_dashboard etl_app geospatial_merger upload_app

# apply all migrations
python manage.py migrate
```

Expected outcome:

* `makemigrations` creates `0001_initial.py` files for apps that need them — or prints `No changes detected` if migrations already exist.
* `migrate` applies all pending migrations and creates the standard Django tables (`django_session`, `auth_user` / custom user table, etc.).

If you see `No migrations to apply` but tables are missing, check whether some apps use a custom migration strategy or were previously created using `--run-syncdb`.

---

## 5. Troubleshooting common migration problems

### `relation "accounts_sector" does not exist`

This happens when Django attempts to create tables referencing another app's table that hasn't been created yet. Steps:

1. Ensure the app that defines `accounts.Sector` has migrations (run `makemigrations`).
2. Avoid `--run-syncdb` unless necessary.
3. If there are cyclic dependencies, inspect models for `ForeignKey` references and use `apps.get_model()` or `"app.Model"` string references where appropriate.

### `Manager isn't available; 'auth.User' has been swapped`

This means you’re using a custom user model (`AUTH_USER_MODEL` set). Use `get_user_model()` in shells/scripts instead of importing `django.contrib.auth.models.User`.

---

## 6. Populate Rwanda administrative locations (custom command)

This project includes a management command to upsert provinces, districts, sectors, etc:

```bash
python manage.py load_rwanda_locations
```

What it does (as implemented in this project):

* Upserts provinces (5)
* Upserts districts (~30)
* Upserts sectors (~408)
* Optionally upserts cells and villages if implemented

Sample output from the successful run in this environment:

```
Provinces upserted: 5
Districts upserted: 30
Sectors upserted: 408
Done.
```

---

## 7. Verify the data was inserted

Use Django shell to check counts:

```bash
python manage.py shell
```

Inside the shell:

```python
from app.accounts.models import Province, District, Sector
Province.objects.count()
District.objects.count()
Sector.objects.count()
```

Expect numbers consistent with the command output.

---

## 8. Create a superuser (admin)

Once migrations are applied, create a superuser:

```bash
python manage.py createsuperuser
```

Or create via shell for a custom user model:

```bash
python manage.py shell
from django.contrib.auth import get_user_model
User = get_user_model()
User.objects.create_superuser(username='admin', email='admin@example.com', password='YOUR_PASSWORD')
```

---

## 9. Post-migration checks

1. Visit the admin panel: `http://127.0.0.1:8000/admin/` and log in with the superuser.
2. Confirm `Django sessions`, `Auth` and custom tables exist and have rows where expected.
3. Run `python manage.py showmigrations` to confirm migrations are applied.

---

## 10. Reproducible checklist (copy-paste)

```bash
# 1. Ensure Postgres (Docker) is running and note host port
docker ps

# 2. Verify Django settings point to correct DB host/port
# (edit settings.py or environment variables)

# 3. Create migrations for custom apps
python manage.py makemigrations accounts analytics_dashboard etl_app geospatial_merger upload_app

# 4. Apply migrations
python manage.py migrate

# 5. Populate Rwanda locations
python manage.py load_rwanda_locations

# 6. Verify counts
python manage.py shell -c "from app.accounts.models import Province, District, Sector; print(Province.objects.count(), District.objects.count(), Sector.objects.count())"

# 7. Create superuser
python manage.py createsuperuser
```

---

## 11. Useful commands for debugging DB from host (Docker)

```bash
# run psql inside the container
docker exec -it staging-postgres psql -U postgres
# list dbs
\l
# connect to a db
\c postgres
# list tables
\dt
# describe a table
\d accounts_province
```

---

## 12. Notes & best-practices

* Prefer explicit migrations (`makemigrations` + `migrate`) over `--run-syncdb` to keep schema under version control.
* Keep `management/commands` idempotent (use `get_or_create` or `bulk_create` with checks) so the load script can be run multiple times without duplicating rows.
* Use environment variables for DB credentials and ports to avoid editing `settings.py` repeatedly.
* Document any manual steps (e.g., initial data files, external sources) in the repo so new contributors can reproduce the environment.

---