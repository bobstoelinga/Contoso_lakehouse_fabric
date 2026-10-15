# Contoso Lakehouse Fabric

Een zelfstandig project voor de migratie en implementatie van het
metadata-gedreven ETL-framework op Microsoft Fabric. Deze repository bevat
Fabric-code, Fabric-deploymentbestanden, tests, uitvoeringsbewijzen en
migratiedocumentatie. De Databricks-runtime en bijbehorende projectbestanden
blijven in de afzonderlijke
[Contoso Lakehouse v2-repository](https://github.com/bobstoelinga/Contoso_lakehouse_v2).

## Structuur

- `src/fabric_etl*`: Fabric-framework en runtime
- `fabric/`, `environments/`, `packaging/`: Fabric-items, configuratie en packages
- `projects/contoso/`, `sql/migrations/fabric/`: Fabric-projectcontracten en SQL
- `metadata/seed/`, `sql/05_gold/`: gedeelde bronmetadata en Gold-SQL die als
  Fabric-migratie-input worden gebruikt
- `deploy/`: deploymenttools, plannen en uitvoeringsbewijzen
- `tests/`: Fabric-gerichte regressietests
- `docs/`: migratieverslag, ontwerp, acceptatie en platformbewijs

## Lokale tests

Gebruik Python 3.10 of hoger met `pytest` en `pyspark` geïnstalleerd:

```powershell
python -m pytest -q
```

De tests valideren de lokale Fabric-code en bewijzen geen succesvolle
uitvoering of productieacceptatie in een Fabric-omgeving.

## Migratiedocumentatie

- [Migratieverslag](docs/09_fabric_migratieverslag.md)
- [Baseline en acceptatiecontract](docs/10_fabric_baseline_acceptatie.md)
- [Templatearchitectuur](docs/11_fabric_template_architectuur.md)
- [Deployment en beheer](docs/12_fabric_deployment_beheer.md)
- [Fase-2-ontwerpcontract](docs/13_fabric_ontwerpcontract_fase2.md)
- [Fase-3-release en acceptatie](docs/14_fabric_fase3_release_acceptatie.md)
- [Fase-4-platformbewijs](docs/15_fabric_fase4_platformbewijs.md)
