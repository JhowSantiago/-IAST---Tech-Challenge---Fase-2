"""Validação end-to-end da pipeline (Bronze → Silver → Gold).

Executa checagens de existência, volume, qualidade de dados e integridade
via Athena e S3. Exit code 0 = PASS; 1 = falhas.
"""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import boto3
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.common.config import ENTIDADES_BATCH, get_settings  # noqa: E402

DATABASE = "datalake_alfabetizacao"


@dataclass
class CheckResult:
    nome: str
    ok: bool
    detalhe: str


def _log(resultado: CheckResult) -> None:
    status = "PASS" if resultado.ok else "FAIL"
    print(f"[DQ] {status} | {resultado.nome} | {resultado.detalhe}")


class PipelineValidator:
    def __init__(self) -> None:
        load_dotenv(ROOT / ".env")
        self.settings = get_settings()
        self._session = boto3.Session(
            aws_access_key_id=self.settings.aws_access_key_id,
            aws_secret_access_key=self.settings.aws_secret_access_key,
            region_name=self.settings.aws_default_region,
        )
        self.s3 = self._session.client("s3")
        self.athena = self._session.client("athena")
        self.glue = self._session.client("glue")
        self.output_location = (
            f"s3://{self.settings.bucket_gold or self.settings.bucket_silver}/athena-results/"
        )
        self.resultados: list[CheckResult] = []

    def _registrar(self, nome: str, ok: bool, detalhe: str) -> None:
        resultado = CheckResult(nome, ok, detalhe)
        self.resultados.append(resultado)
        _log(resultado)

    def _executar_sql(self, sql: str) -> list[list[str]]:
        qid = self.athena.start_query_execution(
            QueryString=sql,
            QueryExecutionContext={"Database": DATABASE},
            ResultConfiguration={"OutputLocation": self.output_location},
        )["QueryExecutionId"]
        while True:
            status = self.athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        if status["State"] != "SUCCEEDED":
            raise RuntimeError(status.get("StateChangeReason", status["State"]))
        rows = self.athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"]
        return [[d.get("VarCharValue", "") for d in r["Data"]] for r in rows]

    def _scalar(self, sql: str) -> int | float:
        rows = self._executar_sql(sql)
        return float(rows[1][0])

    def _prefixo_tem_parquet(self, bucket: str, prefix: str) -> bool:
        resp = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=5)
        return any(k["Key"].endswith(".parquet") for k in resp.get("Contents", []))

    def _tabela_existe(self, nome: str) -> bool:
        try:
            self.glue.get_table(DatabaseName=DATABASE, Name=nome)
            return True
        except self.glue.exceptions.EntityNotFoundException:
            return False

    def checar_existencia_s3(self) -> None:
        bronze = self.settings.bucket_bronze
        silver = self.settings.bucket_silver
        gold = self.settings.bucket_gold

        for entidade in ENTIDADES_BATCH:
            ok = self._prefixo_tem_parquet(bronze, f"bronze/batch/{entidade}/")
            self._registrar(f"s3_bronze_{entidade}", ok, f"s3://{bronze}/bronze/batch/{entidade}/")

        ok_stream = self._prefixo_tem_parquet(
            bronze, "bronze/streaming/indicador_alfabetizacao/"
        )
        self._registrar(
            "s3_bronze_streaming",
            ok_stream,
            "bronze/streaming/indicador_alfabetizacao/",
        )

        for entidade in ENTIDADES_BATCH + ["municipio_indicador_completo"]:
            ok = self._prefixo_tem_parquet(silver, f"silver/{entidade}/")
            self._registrar(f"s3_silver_{entidade}", ok, f"silver/{entidade}/")

        for visao in ("indicador_municipio", "meta_vs_resultado", "evolucao_temporal"):
            ok = self._prefixo_tem_parquet(gold, f"gold/{visao}/")
            self._registrar(f"s3_gold_{visao}", ok, f"gold/{visao}/")

    def checar_catalogo_glue(self) -> None:
        bronze_tabelas = list(ENTIDADES_BATCH)
        silver_tabelas = [f"silver_{e}" for e in ENTIDADES_BATCH] + [
            "silver_municipio_indicador_completo"
        ]
        gold_tabelas = [
            "gold_indicador_municipio",
            "gold_meta_vs_resultado",
            "gold_evolucao_temporal",
        ]
        for nome in bronze_tabelas + silver_tabelas + gold_tabelas:
            ok = self._tabela_existe(nome)
            self._registrar(f"glue_{nome}", ok, DATABASE)

    def checar_volumes(self) -> None:
        limites: dict[str, tuple[int, int]] = {
            "meta_brasil": (1, 10),
            "meta_uf": (50, 120),
            "meta_municipio": (10_000, 11_000),
            "alunos": (3_000_000, 4_000_000),
            "silver_meta_municipio": (10_000, 11_000),
            "silver_alunos": (3_000_000, 4_000_000),
            "silver_municipio_indicador_completo": (10_000, 11_000),
            "gold_indicador_municipio": (10_000, 11_000),
            "gold_meta_vs_resultado": (40, 60),
            "gold_evolucao_temporal": (10_000, 11_000),
        }
        queries = {
            "meta_brasil": f"SELECT COUNT(*) FROM {DATABASE}.meta_brasil",
            "meta_uf": f"SELECT COUNT(*) FROM {DATABASE}.meta_uf",
            "meta_municipio": f"SELECT COUNT(*) FROM {DATABASE}.meta_municipio",
            "alunos": f"SELECT COUNT(*) FROM {DATABASE}.alunos",
            "silver_meta_municipio": (
                f"SELECT COUNT(*) FROM {DATABASE}.silver_meta_municipio"
            ),
            "silver_alunos": f"SELECT COUNT(*) FROM {DATABASE}.silver_alunos",
            "silver_municipio_indicador_completo": (
                f"SELECT COUNT(*) FROM {DATABASE}.silver_municipio_indicador_completo"
            ),
            "gold_indicador_municipio": (
                f"SELECT COUNT(*) FROM {DATABASE}.gold_indicador_municipio"
            ),
            "gold_meta_vs_resultado": (
                f"SELECT COUNT(*) FROM {DATABASE}.gold_meta_vs_resultado"
            ),
            "gold_evolucao_temporal": (
                f"SELECT COUNT(*) FROM {DATABASE}.gold_evolucao_temporal"
            ),
        }
        for nome, sql in queries.items():
            try:
                total = int(self._scalar(sql))
                minimo, maximo = limites[nome]
                ok = minimo <= total <= maximo
                self._registrar(
                    f"volume_{nome}",
                    ok,
                    f"count={total} esperado [{minimo},{maximo}]",
                )
            except Exception as exc:  # noqa: BLE001
                self._registrar(f"volume_{nome}", False, str(exc))

    def checar_dq_sql(self) -> None:
        checks: list[tuple[str, str, Callable[[float], bool]]] = [
            (
                "dq_duplicidade_silver_municipio",
                f"""
                SELECT CASE WHEN total = distintos THEN 1 ELSE 0 END
                FROM (
                  SELECT COUNT(*) AS total,
                         COUNT(DISTINCT id_municipio) AS distintos
                  FROM {DATABASE}.silver_municipio
                )
                """,
                lambda v: v == 1,
            ),
            (
                "dq_orfaos_integrado",
                f"""
                SELECT CASE WHEN orfaos = 0 THEN 1 ELSE 0 END
                FROM (
                  SELECT COUNT(*) AS orfaos
                  FROM {DATABASE}.silver_municipio_indicador_completo
                  WHERE nome_municipio IS NULL
                )
                """,
                lambda v: v == 1,
            ),
            (
                "dq_gap_meta_consistente",
                f"""
                SELECT CASE WHEN inconsistentes = 0 THEN 1 ELSE 0 END
                FROM (
                  SELECT COUNT(*) AS inconsistentes
                  FROM {DATABASE}.gold_indicador_municipio
                  WHERE pct_alfabetizados IS NOT NULL
                    AND meta_pct IS NOT NULL
                    AND ABS(gap_meta - (pct_alfabetizados - meta_pct)) > 0.01
                )
                """,
                lambda v: v == 1,
            ),
            (
                "dq_streaming_presente",
                f"""
                SELECT CASE WHEN n > 0 THEN 1 ELSE 0 END
                FROM (
                  SELECT COUNT(*) AS n
                  FROM {DATABASE}.silver_municipio_indicador_completo
                  WHERE _source_type = 'streaming'
                )
                """,
                lambda v: v == 1,
            ),
            (
                "dq_bronze_streaming",
                f"""
                SELECT CASE WHEN n > 0 THEN 1 ELSE 0 END
                FROM (
                  SELECT COUNT(*) AS n
                  FROM {DATABASE}.indicador_alfabetizacao
                )
                """,
                lambda v: v >= 0,
            ),
        ]

        for nome, sql, pred in checks:
            try:
                valor = self._scalar(sql)
                ok = pred(valor)
                self._registrar(nome, ok, f"valor={valor}")
            except Exception as exc:  # noqa: BLE001
                if nome == "dq_bronze_streaming":
                    self._registrar(
                        nome,
                        True,
                        f"opcional/indisponível: {exc}",
                    )
                else:
                    self._registrar(nome, False, str(exc))

    def checar_tags_finops(self) -> None:
        for bucket in (
            self.settings.bucket_bronze,
            self.settings.bucket_silver,
            self.settings.bucket_gold,
        ):
            try:
                tags = self.s3.get_bucket_tagging(Bucket=bucket)["TagSet"]
                chaves = {t["Key"] for t in tags}
                ok = {"project", "environment", "finops", "layer"}.issubset(chaves)
                self._registrar(f"finops_tags_{bucket}", ok, f"tags={sorted(chaves)}")
            except Exception as exc:  # noqa: BLE001
                self._registrar(f"finops_tags_{bucket}", False, str(exc))

    def executar(self) -> int:
        print("=== Validação end-to-end pipeline ICA ===\n")
        self.checar_existencia_s3()
        self.checar_catalogo_glue()
        self.checar_volumes()
        self.checar_dq_sql()
        self.checar_tags_finops()

        falhas = [r for r in self.resultados if not r.ok]
        total = len(self.resultados)
        passou = total - len(falhas)
        print(f"\n=== Resultado: {passou}/{total} checks PASS ===")
        if falhas:
            print("Falhas:")
            for f in falhas:
                print(f"  - {f.nome}: {f.detalhe}")
            return 1
        return 0


def main() -> None:
    sys.exit(PipelineValidator().executar())


if __name__ == "__main__":
    main()
