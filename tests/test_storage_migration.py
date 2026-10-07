"""Cross-file configuration checks. No Docker Engine, imports of jobs or secrets."""

import ast
import json
import re
import subprocess
import unittest
import xml.etree.ElementTree as ET
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
BUCKETS = {
    "demandflow-raw", "demandflow-bronze", "demandflow-silver",
    "demandflow-gold", "demandflow-quarantine", "demandflow-checkpoints",
}


def read(relative):
    return (ROOT / relative).read_text(encoding="utf-8-sig")


class StorageMigrationTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        result = subprocess.run(
            ["docker", "compose", "--profile", "*", "config",
             "--no-env-resolution", "--format", "json"],
            cwd=ROOT, capture_output=True, text=True, encoding="utf-8",
            timeout=30,
        )
        if result.returncode:
            # Rendered Compose can contain unrelated application secrets.
            raise RuntimeError("Compose validation failed; captured output withheld.")
        cls.config = json.loads(result.stdout)
        cls.services = cls.config["services"]

    def test_python_syntax_without_importing_or_running_jobs(self):
        paths = [
            *ROOT.joinpath("src").rglob("*.py"),
            *ROOT.joinpath("airflow/dags").rglob("*.py"),
        ]
        for path in paths:
            with self.subTest(file=str(path.relative_to(ROOT))):
                ast.parse(path.read_text(encoding="utf-8-sig"), filename=str(path))

    def test_minio_storage_network_and_license(self):
        self.assertNotIn("localstack", self.services)
        service = self.services["minio"]
        self.assertEqual(service["image"],
                         "quay.io/minio/aistor/minio:RELEASE.2026-09-19T17-05-25Z")
        ports = {(str(p["published"]), p["target"], p["host_ip"])
                 for p in service["ports"]}
        self.assertEqual(ports, {("9000", 9000, "127.0.0.1"),
                                ("9001", 9001, "127.0.0.1")})
        mounts = {v["target"]: v for v in service["volumes"]}
        self.assertEqual(mounts["/mnt/data"]["source"], "demandflow_minio_data")
        for target in ("/opt/demandflow/bootstrap.sh", "/opt/demandflow/lakehouse-policy.json"):
            self.assertTrue(mounts[target]["read_only"])
        self.assertIn("/run/secrets/minio_license", service["command"])
        self.assertEqual(service["environment"]["MINIO_ROOT_USER_FILE"],
                         "/run/secrets/minio_root_user")
        self.assertEqual(service["environment"]["MINIO_ROOT_PASSWORD_FILE"],
                         "/run/secrets/minio_root_password")
        self.assertNotIn("MINIO_ROOT_PASSWORD", service["environment"])
        self.assertEqual(service["healthcheck"]["test"], ["CMD", "mc", "ready", "local"])
        self.assertEqual(int(service["mem_limit"]), 1024 ** 3)
        self.assertEqual(float(service["cpus"]), 1.0)

    def test_consumers_share_external_pipeline_credentials(self):
        expected_file = self.config["secrets"]["s3_credentials"]["file"]
        consumers = ["spark", "hive-metastore", "trino"] + [
            name for name in self.services
            if name.startswith("airflow-") and name not in ("airflow-db", "airflow-redis")
        ]
        self.assertGreaterEqual(len(consumers), 9)
        for name in consumers:
            with self.subTest(service=name):
                service = self.services[name]
                self.assertTrue(any(f["path"] == expected_file for f in service["env_file"]))
                environment = service["environment"]
                self.assertEqual(environment["S3_INTERNAL_ENDPOINT"], "http://minio:9000")
                # Explicit environment values override env_file: forbid stale test credentials.
                self.assertNotIn("AWS_ACCESS_KEY_ID", environment)
                self.assertNotIn("AWS_SECRET_ACCESS_KEY", environment)
                self.assertFalse(service.get("secrets"), "Pipeline must not receive root/license secrets.")
        self.assertEqual(self.services["spark"]["image"], "demandflow-spark:3.5.9")
        self.assertEqual(self.services["spark"]["build"]["dockerfile"], "infra/spark/Dockerfile")
        for name in ("spark", "hive-metastore", "trino"):
            self.assertEqual(self.services[name]["depends_on"]["minio"]["condition"],
                             "service_healthy")
        self.assertEqual(self.services["hive-metastore"]["depends_on"]["hive-metastore-init"]["condition"],
                         "service_completed_successfully")
        self.assertIn("hive-metastore", self.services["trino"]["depends_on"])

    def test_legacy_volume_retained_but_never_mounted(self):
        # Compose omits unused volumes from its normalized output.
        self.assertRegex(read("compose.yaml"), r"(?m)^  demandflow_localstack_data:")
        for service in self.services.values():
            for mount in service.get("volumes", []):
                self.assertNotEqual(mount["source"], "demandflow_localstack_data")

    def test_policy_and_bootstrap_agree_on_exact_six_buckets(self):
        policy = json.loads(read("infra/minio/lakehouse-policy.json"))
        bucket_resources = set()
        object_resources = set()
        allowed_actions = {
            "s3:GetBucketLocation", "s3:ListBucket", "s3:ListBucketMultipartUploads",
            "s3:GetObject", "s3:PutObject", "s3:DeleteObject",
            "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts",
        }
        actual_actions = set()
        for statement in policy["Statement"]:
            self.assertEqual(statement["Effect"], "Allow")
            actual_actions.update(statement["Action"])
            for resource in statement["Resource"]:
                (object_resources if resource.endswith("/*") else bucket_resources).add(resource)
        self.assertEqual(actual_actions, allowed_actions)
        self.assertEqual(bucket_resources, {"arn:aws:s3:::" + b for b in BUCKETS})
        self.assertEqual(object_resources, {"arn:aws:s3:::" + b + "/*" for b in BUCKETS})
        bootstrap = read("infra/minio/bootstrap.sh")
        self.assertEqual(set(re.findall(r"demandflow-(?:raw|bronze|silver|gold|quarantine|checkpoints)\b", bootstrap)), BUCKETS)
        self.assertNotIn("GetCallerIdentity", read("scripts/bootstrap-s3.ps1"))
        self.assertNotIn(b"\r", (ROOT / "infra/minio/bootstrap.sh").read_bytes())

    def test_hive_and_trino_configuration_contract(self):
        document = ET.fromstring(read("infra/hive-metastore/core-site.xml"))
        properties = {p.findtext("name"): p.findtext("value") for p in document}
        self.assertEqual(properties["fs.s3a.endpoint"], "${env.S3_INTERNAL_ENDPOINT}")
        self.assertEqual(properties["fs.s3a.endpoint.region"], "${env.AWS_DEFAULT_REGION}")
        self.assertEqual(properties["fs.s3a.aws.credentials.provider"],
                         "com.amazonaws.auth.EnvironmentVariableCredentialsProvider")
        self.assertNotIn("fs.s3a.access.key", properties)
        self.assertNotIn("fs.s3a.secret.key", properties)
        catalog = dict(line.split("=", 1) for line in read("infra/trino/catalog/delta.properties").splitlines() if "=" in line)
        self.assertEqual(catalog["s3.endpoint"], "${ENV:S3_INTERNAL_ENDPOINT}")
        self.assertEqual(catalog["s3.aws-access-key"], "${ENV:AWS_ACCESS_KEY_ID}")
        self.assertEqual(catalog["s3.aws-secret-key"], "${ENV:AWS_SECRET_ACCESS_KEY}")
        self.assertEqual(catalog["s3.path-style-access"], "true")

    def test_spark_jobs_and_airflow_credentials(self):
        jobs = [path for path in ROOT.joinpath("src/spark").rglob("*.py")
                if "SparkSession" in path.read_text(encoding="utf-8-sig")]
        self.assertEqual(len(jobs), 9)
        for path in jobs:
            text = path.read_text(encoding="utf-8-sig")
            with self.subTest(job=path.name):
                self.assertIn("S3_INTERNAL_ENDPOINT", text)
                self.assertIn("AWS_ACCESS_KEY_ID", text)
                self.assertIn("AWS_SECRET_ACCESS_KEY", text)
                self.assertNotIn('"test"', text)
        tree = ast.parse(read("airflow/dags/demandflow_pipeline.py"))
        public_env = next(node.value for node in tree.body if isinstance(node, ast.Assign)
                          and any(isinstance(t, ast.Name) and t.id == "SPARK_ENV" for t in node.targets))
        keys = {key.value for key in public_env.keys}
        self.assertIn("S3_INTERNAL_ENDPOINT", keys)
        self.assertNotIn("AWS_SECRET_ACCESS_KEY", keys)
        private = [kw.value for node in ast.walk(tree) if isinstance(node, ast.Call)
                   for kw in node.keywords if kw.arg == "private_environment"]
        self.assertEqual(len(private), 1)
        self.assertEqual({key.value for key in private[0].keys},
                         {"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"})

    def test_no_runtime_localstack_references_or_missing_script_targets(self):
        for directory in ("src", "scripts", "infra", "airflow/dags"):
            for path in ROOT.joinpath(directory).rglob("*"):
                if path.suffix not in (".py", ".ps1", ".sh", ".xml", ".properties"):
                    continue
                text = path.read_text(encoding="utf-8-sig")
                if path.name == "validate-static-syntax.ps1":
                    continue
                self.assertNotIn("localstack", text.lower(), str(path))
                self.assertNotIn("4566", text, str(path))
                for script in re.findall(r'\$PSScriptRoot\\([\w-]+\.ps1)', text):
                    self.assertTrue(ROOT.joinpath("scripts", script).is_file(), script)
        references = set()
        for directory in ("src", "scripts"):
            for path in ROOT.joinpath(directory).rglob("*"):
                if path.suffix in (".py", ".ps1"):
                    references.update(re.findall(r"s3a?://(demandflow-[a-z-]+)", path.read_text(encoding="utf-8-sig")))
        self.assertTrue(references <= BUCKETS)


if __name__ == "__main__":
    unittest.main()
