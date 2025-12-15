import pytest
import os
import yaml

class TestAirflowConfig:
    
    @pytest.fixture
    def project_root(self):
        # Assuming tests are in <root>/tests/
        return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

    def test_docker_compose_exists(self, project_root):
        """Verify docker-compose.yml exists."""
        dc_path = os.path.join(project_root, 'docker-compose.yml')
        assert os.path.exists(dc_path), "docker-compose.yml not found"

    def test_airflow_services_defined(self, project_root):
        """Verify airflow services are defined in docker-compose.yml."""
        dc_path = os.path.join(project_root, 'docker-compose.yml')
        if not os.path.exists(dc_path):
            pytest.skip("docker-compose.yml missing")
            
        with open(dc_path, 'r') as f:
            try:
                config = yaml.safe_load(f)
            except yaml.YAMLError:
                pytest.fail("docker-compose.yml is not valid YAML")
        
        services = config.get('services', {})
        assert 'airflow-webserver' in services, "airflow-webserver service missing"
        assert 'airflow-scheduler' in services, "airflow-scheduler service missing"

    def test_env_file_consistency(self, project_root):
        """Verify .env_airflow exists and contains core keys."""
        env_path = os.path.join(project_root, '.env_airflow')
        assert os.path.exists(env_path), ".env_airflow not found"
        
        with open(env_path, 'r') as f:
            content = f.read()
            
        assert 'AIRFLOW_UID' in content, "AIRFLOW_UID missing in .env_airflow"
        assert 'AIRFLOW__CORE__EXECUTOR' in content, "Executor config missing in .env_airflow"
