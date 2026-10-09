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

    def test_spark_image_contains_only_spark_runtime_files(self):
        dockerfile = read("infra/spark/Dockerfile")
        expected_base = (
            "FROM apache/spark:3.5.9-scala2.12-java17-python3-ubuntu@"
            "sha256:f3d6eaa8bab8ec2e38f3c3918a5b2f8b253c95bcb19f9e87ad0e0cdacf9df2d5"
        )
        self.assertEqual(dockerfile.splitlines()[0], expected_base)
        self.assertNotIn("python3-r-ubuntu", dockerfile)
        self.assertIn("COPY src/spark /opt/demandflow/src/spark", dockerfile)
        self.assertIn(
            "COPY config/tables.json /opt/demandflow/config/tables.json",
            dockerfile,
        )
        self.assertNotRegex(dockerfile, r"(?m)^COPY\s+src\s")
        self.assertNotRegex(dockerfile, r"(?m)^COPY\s+config\s")
        self.assertRegex(dockerfile, r"(?m)^USER spark$")

        service = self.services["spark"]
        mounts = {mount["target"]: mount for mount in service["volumes"]}
        self.assertEqual(
            set(mounts),
            {
                "/opt/demandflow/src/spark",
                "/opt/demandflow/config/tables.json",
                "/opt/demandflow/.ivy2",
            },
        )
        expected_sources = {
            "/opt/demandflow/src/spark": ROOT / "src/spark",
            "/opt/demandflow/config/tables.json": ROOT / "config/tables.json",
        }
        for target, expected_source in expected_sources.items():
            with self.subTest(target=target):
                self.assertTrue(mounts[target]["read_only"])
                self.assertEqual(
                    Path(mounts[target]["source"]).resolve(),
                    expected_source.resolve(),
                )

        ivy_mount = mounts["/opt/demandflow/.ivy2"]
        self.assertEqual(ivy_mount["type"], "volume")
        self.assertEqual(ivy_mount["source"], "demandflow_spark_ivy_cache")
        self.assertFalse(ivy_mount.get("read_only", False))
        self.assertIn("mkdir -p /opt/demandflow/.ivy2", dockerfile)
        self.assertIn("chown -R spark:spark /opt/demandflow", dockerfile)
        self.assertNotIn("/tmp/.ivy2", dockerfile)

        # Isolation applies only to the Spark image. CDC source components stay
        # present and referenced by their own workflow.
        self.assertTrue(ROOT.joinpath("src/generator/main.py").is_file())
        self.assertTrue(
            ROOT.joinpath("config/debezium/postgres-connector.json").is_file()
        )
        self.assertIn(
            r"config\debezium\postgres-connector.json",
            read("scripts/register-debezium-connector.ps1"),
        )
        for service_name in ("postgres", "kafka", "debezium", "spark"):
            self.assertIn(service_name, self.services)

    def test_debezium_password_is_injected_only_at_runtime(self):
        connector = json.loads(read("config/debezium/postgres-connector.json"))
        connector_config = connector["config"]
        placeholder = "__DEMANDFLOW_RUNTIME_SECRET__"
        self.assertEqual(connector_config["database.password"], placeholder)
        self.assertEqual(connector_config["database.user"], "__FROM_ENV__")
        self.assertEqual(connector_config["database.dbname"], "__FROM_ENV__")

        script = read("scripts/register-debezium-connector.ps1")
        self.assertIn(
            '$cdcPassword = Get-DotEnvValue "DEBEZIUM_POSTGRES_PASSWORD"',
            script,
        )
        self.assertNotIn('Get-DotEnvValue "POSTGRES_PASSWORD"', script)
        self.assertIn(
            "$payload.config.'database.password' = $cdcPassword",
            script,
        )
        self.assertIn(placeholder, script)
        self.assertNotRegex(
            script,
            r"(?i)Write-(?:Host|Output)[^\n]*cdcPassword",
        )
        self.assertLess(
            script.index(placeholder),
            script.index("$payload.config.'database.password' = $cdcPassword"),
        )
        self.assertLess(
            script.index("$payload.config.'database.password' = $cdcPassword"),
            script.index("ConvertTo-Json -Depth 20"),
        )
        self.assertIn(".env", read(".gitignore").splitlines())

    def test_debezium_password_rotation_is_isolated_and_non_disclosing(self):
        script = read("scripts/rotate-debezium-password.ps1")

        self.assertIn('"DEBEZIUM_POSTGRES_PASSWORD"', script)
        self.assertNotIn('"POSTGRES_PASSWORD"', script)
        self.assertIn("RandomNumberGenerator", script)
        self.assertIn("docker compose up -d --no-deps postgres", script)
        self.assertIn("docker compose stop -t 30 postgres", script)
        self.assertIn("[System.IO.File]::Replace", script)
        self.assertIn(".env.rotation-backup-", script)
        self.assertNotIn("$envFile, $null", script)
        self.assertIn("[Convert]::ToBase64String", script)
        self.assertIn("PGPASSWORD=$(printf", script)
        self.assertIn("docker exec -i $script:containerName sh", script)
        self.assertIn('-h postgres', script)
        self.assertNotIn('-h 127.0.0.1', script)
        self.assertIn("# end", script)
        self.assertIn("export PGPASSWORD PGUSER PGDATABASE", script)
        self.assertIn("SELECT rolpassword FROM pg_authid", script)
        self.assertIn("SCRAM-SHA-256", script)
        self.assertIn("Set-CdcDatabasePassword $originalVerifier", script)
        self.assertNotIn("Set-CdcDatabasePassword $oldPassword", script)
        self.assertLess(
            script.index("$originalVerifier = Get-CdcPasswordVerifier"),
            script.index("Set-CdcDatabasePassword $newPassword"),
        )
        self.assertNotIn('Write-Host $newPassword', script)
        self.assertNotIn('Write-Output $newPassword', script)
        self.assertNotRegex(
            script,
            r"(?i)Write-(?:Host|Output)[^\n]*(?:verifier|rolpassword)",
        )
        self.assertNotRegex(script, r'(?i)-v\s+["\']?cdc_password=')
        self.assertNotRegex(script, r'(?i)-e\s+["\']?PGPASSWORD=')

    def test_postgres_cdc_setup_is_isolated_and_non_disclosing(self):
        script = read("scripts/configure-postgres-cdc.ps1")
        setup_sql = read("infra/postgres/cdc/setup.sql")

        self.assertIn("Set-StrictMode -Version Latest", script)
        self.assertIn("está duplicada no .env", script)
        self.assertIn("está vazia no .env", script)
        self.assertIn("docker compose up -d --no-deps postgres", script)
        self.assertIn("docker compose stop -t 30 postgres", script)
        self.assertIn("docker exec -i $script:containerName sh", script)
        self.assertIn("[Convert]::ToBase64String", script)
        self.assertIn("DEBEZIUM_POSTGRES_PASSWORD=$(printf", script)
        self.assertIn("export PGUSER PGDATABASE DEBEZIUM_POSTGRES_USER", script)
        self.assertIn("-h postgres", script)
        self.assertIn("Invoke-CdcSetup", script)
        self.assertGreaterEqual(script.count("Invoke-CdcSetup"), 3)
        self.assertNotRegex(script, r'(?i)-v\s+["\']?cdc_password=')
        self.assertNotRegex(script, r'(?i)-e\s+["\']?PGPASSWORD=')
        self.assertNotRegex(
            script,
            r"(?i)Write-(?:Host|Output)[^\n]*cdcPassword",
        )

        self.assertIn(
            r"\getenv cdc_password DEBEZIUM_POSTGRES_PASSWORD",
            setup_sql,
        )
        self.assertIn(r"\getenv cdc_user DEBEZIUM_POSTGRES_USER", setup_sql)
        self.assertIn(r"\getenv database_name POSTGRES_DATABASE", setup_sql)

    def test_cdc_runtime_flow_is_sequential_reproducible_and_cleanup_safe(self):
        register = read("scripts/register-debezium-connector.ps1")
        validator = read("scripts/validate-cdc.ps1")
        generator = read("src/generator/main.py")
        raw = read("scripts/run-raw-ingestion.ps1")
        rebuild = read("scripts/run-lakehouse-rebuild.ps1")

        postgres_start = register.index("up -d --no-deps postgres")
        kafka_start = register.index("up -d --no-deps kafka")
        debezium_start = register.index("up -d --no-deps debezium")
        self.assertLess(postgres_start, kafka_start)
        self.assertLess(kafka_start, debezium_start)
        self.assertIn('Wait-ContainerHealthy', register)
        self.assertIn('localhost", "127.0.0.1", "::1', register)
        self.assertIn("$connectUri.Port -ne 8083", register)
        self.assertIn("$i -le 90", register)
        self.assertIn(
            '$connectUrl = "http://127.0.0.1:8083"',
            register,
        )
        self.assertNotRegex(
            register,
            r"(?i)Write-(?:Host|Output)[^\n]*cdcPassword",
        )

        self.assertIn('ExpectedOperations @("c", "u", "d")', validator)
        self.assertIn('ExpectedOperations @("c", "d")', validator)
        self.assertIn(
            '$wrappedPayload = Get-RecordProperty $message "payload"',
            validator,
        )
        self.assertIn(
            "REPLICA IDENTITY DEFAULT não garante pré-imagem em UPDATE",
            validator,
        )
        self.assertIn('demandflow-connect-offsets', validator)
        self.assertIn('Wait-SlotState "inactive"', validator)
        self.assertIn('Wait-SlotState "active"', validator)
        self.assertIn("[int]$Attempts = 90", validator)
        self.assertIn("[int]$Attempts = 45", validator)
        self.assertIn('--action sale', validator)
        self.assertIn('@("debezium", "kafka", "postgres")', validator)
        self.assertIn(
            '$connectUrl = "http://127.0.0.1:8083"',
            validator,
        )
        self.assertNotIn("DEBEZIUM_POSTGRES_PASSWORD", validator)

        self.assertIn('"--action"', generator)
        self.assertIn('"--seed"', generator)
        self.assertIn('"sale": create_sale', generator)
        self.assertIn("random.seed(random_seed)", generator)

        self.assertLess(
            raw.index("docker compose stop -t 30 minio"),
            raw.index("configure-postgres-cdc.ps1"),
        )
        self.assertLess(
            raw.index("configure-postgres-cdc.ps1"),
            raw.index("start-storage.ps1"),
        )
        self.assertLess(
            raw.index("start-storage.ps1"),
            raw.index("register-debezium-connector.ps1"),
        )
        self.assertNotIn("--profile cdc up -d", rebuild)
        self.assertNotIn("start-storage.ps1", rebuild)

    def test_snapshot_recovery_uses_kafka_signals_and_targeted_retention(self):
        connector = json.loads(read("config/debezium/postgres-connector.json"))[
            "config"
        ]
        self.assertEqual(connector["snapshot.mode"], "initial")
        self.assertEqual(connector["signal.enabled.channels"], "kafka")
        self.assertEqual(connector["signal.kafka.topic"], "demandflow-signal")
        self.assertEqual(
            connector["signal.kafka.bootstrap.servers"],
            "kafka:29092",
        )
        self.assertEqual(
            connector["message.prefix.exclude.list"],
            "demandflow-snapshot-resume",
        )
        self.assertNotIn("topic.creation.default.retention.ms", connector)
        self.assertNotIn("topic.creation.default.retention.bytes", connector)

        register = read("scripts/register-debezium-connector.ps1")
        topics = read("scripts/configure-kafka-cdc-topics.ps1")
        recovery = read("scripts/recover-raw-snapshot.ps1")
        self.assertLess(
            register.index("configure-kafka-cdc-topics.ps1"),
            register.index("up -d --no-deps debezium"),
        )
        self.assertIn('"--partitions", "1"', topics)
        self.assertIn('"retention.ms=-1,retention.bytes=-1"', topics)
        self.assertIn('"demandflow.message"', topics)
        self.assertIn('"--delete-config"', topics)
        expected_topics = {
            f"demandflow.public.{table}"
            for table in json.loads(read("config/tables.json"))
        }
        configured_topics = set(
            re.findall(r'"(demandflow[.]public[.][a-z_]+)"', topics)
        )
        self.assertEqual(configured_topics, expected_topics)

        self.assertIn('type = "execute-snapshot"', recovery)
        self.assertIn('type = "blocking"', recovery)
        self.assertIn('"data-collections" = $collections', recovery)
        self.assertIn("Advance-LogicalWalForSnapshot", recovery)
        self.assertIn("pg_logical_emit_message", recovery)
        self.assertIn("CreateSnapshot", recovery)
        self.assertIn("ResumePublishedSnapshot", recovery)
        self.assertIn("Informe exatamente uma ação", recovery)
        self.assertIn("Find-LatestPublishedSnapshot", recovery)
        self.assertIn("Assert-NewRecordsAreSnapshotReads", recovery)
        self.assertIn('$operation -cne "r"', recovery)
        self.assertIn('"first_in_data_collection"', recovery)
        self.assertIn('"last_in_data_collection"', recovery)
        self.assertIn("RAW_SNAPSHOT_RECOVERY_SECOND_INPUT=", recovery)
        self.assertIn('@("debezium", "kafka", "postgres", "minio")', recovery)
        for forbidden in (
            "kafka-topics.sh --delete",
            "DELETE /connectors",
            "docker volume rm",
            "snapshot.mode\" = \"always",
        ):
            self.assertNotIn(forbidden, recovery)

    def test_raw_pipeline_reuses_ivy_and_proves_checkpoint_idempotency(self):
        cache_path = "/opt/demandflow/.ivy2"
        cache_volume = "demandflow_spark_ivy_cache"
        pipeline = read("airflow/dags/demandflow_pipeline.py")
        ingestion = read("src/spark/raw/kafka_to_raw.py")
        raw_validation = read("src/spark/raw/validate_raw.py")
        runtime_validation = read("scripts/validate-raw-pipeline.ps1")
        raw_runner = read("scripts/run-raw-ingestion.ps1")

        volume = self.config["volumes"][cache_volume]
        self.assertEqual(volume["name"], cache_volume)

        self.assertIn("from docker.types import Mount", pipeline)
        self.assertIn(f'SPARK_IVY_CACHE_PATH = "{cache_path}"', pipeline)
        self.assertIn(f'SPARK_IVY_CACHE_VOLUME = "{cache_volume}"', pipeline)
        self.assertIn("mounts=[", pipeline)
        self.assertIn("source=SPARK_IVY_CACHE_VOLUME", pipeline)
        self.assertIn("target=SPARK_IVY_CACHE_PATH", pipeline)

        ivy_users = [
            *ROOT.joinpath("scripts").glob("run-*.ps1"),
            *ROOT.joinpath("scripts").glob("validate-*.ps1"),
        ]
        for path in ivy_users:
            text = path.read_text(encoding="utf-8-sig")
            with self.subTest(script=path.name):
                self.assertNotIn("/tmp/.ivy2", text)

        self.assertIn("RAW_INPUT_ROWS_TOTAL=", ingestion)
        for marker in (
            "RAW_EVENT_COUNT=",
            "RAW_DUPLICATE_EVENT_IDS=0",
            "RAW_INVALID_IDENTITY_COUNT=0",
            "RAW_INITIAL_SNAPSHOT_PRESENT=true",
            "RAW_CHECKPOINT_PRESENT=true",
            "RAW_SNAPSHOT_EVENT_COUNT=",
            "RAW_SNAPSHOT_COUNT_",
        ):
            self.assertIn(marker, raw_validation)

        self.assertIn("MINIMUM_INITIAL_SNAPSHOT_COUNTS", raw_validation)
        for table_name in (
            "stores",
            "products",
            "promotions",
            "inventory",
            "demand_forecasts",
        ):
            self.assertIn(f'"{table_name}"', raw_validation)
        self.assertIn(
            "Recupere o snapshot antes de executar a Bronze.",
            raw_validation,
        )

        self.assertIn("invoke-raw-spark.ps1", raw_runner)
        self.assertIn("RAW_INPUT_ROWS_TOTAL=(?<count>[0-9]+)", runtime_validation)
        self.assertIn("$inputCount -ne 0", runtime_validation)
        self.assertIn("Compare-Object $cacheBefore $cacheAfter", runtime_validation)
        self.assertIn("$secondRawCount -ne $firstRawCount", runtime_validation)
        self.assertIn(
            '@("debezium", "kafka", "postgres", "minio")',
            runtime_validation,
        )

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
        self.assertIn("while IFS='=' read -r key value; do", bootstrap)
        self.assertNotRegex(bootstrap, r"\$\(\s*(?:sed|awk|cut|grep)\b")
        self.assertNotRegex(bootstrap, r"(?m)^\s*(?:\.|source)\s+/run/secrets/s3_credentials\b")
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
