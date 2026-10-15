# Fabric fase 3 - projectrelease en deploymentstructuur

Datum: 10 oktober 2026. Mandaat: doorgaan tot en met fase 3.

**Fase 3 afgerond voor de lokale template-, projectrelease- en
deploymentvoorbereiding. Geen operationele Fabric ETL-vrijgave.**
De volgende fase is 4: controlruntime, echte itemdeployment en platformproeven.

## 1. Uitkomst en bewijsgrens

De Contoso-configuratie staat nu los van de oorspronkelijke Databricks-package
en heeft een strikt, geversioneerd kandidaatcontract. Het framework wordt als
zelfstandige wheel gebouwd. Environment, dun verificatienotebook en twee
SQL-migraties worden samen in een controleerbaar lokaal deploymentplan verpakt.

Er zijn in deze fase geen Fabric-items aangemaakt, geen Spark-jobs gestart,
geen controltabellen gevuld en geen brongegevens verwerkt. De eerder bewezen
drie Lakehouses blijven de bestaande foundation. Dit werk bewijst dus niet
dat Raw tot en met Gold Current al functioneert.

De exit betreft de implementatie- en voorbereidingskant van T-01 en T-12 uit
het [ontwerpcontract](13_fabric_ontwerpcontract_fase2.md).
Spark-type/hashpariteit van T-02, mutexproeven, runtimecompilatie, SQL-endpoint-
en consumerbewijs blijven verplicht. Twee lokale builds zijn nadrukkelijk
geen twee volledige remote redeploys.

## 2. Deliverables

| Onderdeel | Implementatie | Status |
|---|---|---|
| Projectcontract | [project.py](../src/fabric_etl/project.py) | Strikte veldsets, typen, referenties, mappingkolommen, sleutels, hashkolommen, DAG, policies en fingerprints |
| Expliciete v2-conversie | [contoso.py](../src/fabric_etl/contoso.py) | Herhaalbaar; schrijft uitsluitend een review-gated kandidaat |
| Contoso-kandidaat | [release.json](../projects/contoso/release.json) | Structureel geldig, niet geactiveerd |
| Project-SQL | [sql](../projects/contoso/sql) | 20 gefingerprintte expressies; review verplicht |
| Scope en schemaherkomst | [fixtures](../projects/contoso/fixtures) | Scopefixture en originele Gold-DDL als bewijs; geen echte brondata |
| Afzonderlijke wheel | [pyproject.toml](../packaging/fabric/pyproject.toml) | Alleen fabric_etl, versie 0.1.0; geen v2-package |
| Geordende deployment | [deployment.json](../fabric/deployment.json) | Environment, Notebook, migraties, expliciet open runtimefasen |
| Environmentdefinitie | [Sparkcompute.yml](../fabric/items/etl_environment.Environment/Setting/Sparkcompute.yml) | Runtime 2.0, UTC, starterpool; nog niet remote gepubliceerd |
| Dun notebook | [notebook-content.py](../fabric/items/verify_foundation.Notebook/notebook-content.py) | Alleen read-only visibilitycontrole via framework |
| Namespacemigratie | [001_namespaces.sql](../sql/migrations/fabric/001_namespaces.sql) | Idempotente namespace-DDL, niet uitgevoerd |
| Controlcontract | [002_control_contract.sql](../sql/migrations/fabric/002_control_contract.sql) | 11 Delta-tabeldeclaraties, niet uitgevoerd |
| Lokale releasestap | [prepare-release.ps1](../deploy/prepare-release.ps1) | Valideren, bouwen, plannen; geen API-writes of replay |
| Acceptatietests | [projecttests](../tests/test_fabric_project.py), [plantests](../tests/test_fabric_plan.py) | Positieve en negatieve contract-/packagingproeven |

### Kandidateninventaris

5 bronsystemen, 9 bronobjecten/connectors, 5 producten, 18 Raw Vault-entiteiten,
3 Business Vault-entiteiten inclusief PIT, 3 reference-entiteiten,
19 Gold-entiteiten, 153 mappings, 33 kwaliteitsregels, 46 dependencyedges,
49 schemadeclaraties, 20 expressies, 1 expliciet ECB-filter en 5 policies.
SharePoint is uitgesloten.

CBS volgt de Raw Vault-route. ECB, Nager en Fabric Sales volgen de
reference-route. Historische Gold-entiteiten van die drie referencebronnen
hebben nu expliciete dependencyedges naar hun reference-entiteit.

## 3. Keuzes, afwijkingen en herstel

### B-FAB-027 - kandidaat is niet hetzelfde als runtime-ready

Aanleiding: oude metadata en SQL mogen niet automatisch als correct voor
Fabric worden beschouwd. Een alles-accepteren-converter of stil overslaan
van onbruikbare modellen zou een onjuiste succesmelding geven.

Besluit onder het fase-3-mandaat: alle 15 metadatacollecties zijn aanwezig;
structurele geldigheid en operationele vrijgave zijn afzonderlijke checks.
`validate-project --require-runtime` faalt zolang gates openstaan.
Geen gefingeerde retentie, zakelijke deletegoedkeuring of werkende verbinding.

De bestaande Customers-seed bevat tweemaal freshness, eerst 26 en daarna 24.
Alleen precies deze bekende uitzondering wordt geconverteerd naar 24;
andere dubbele velden falen. De originele seed blijft ongewijzigd.

Nager holiday_types is expliciet JSON-tekst via to_json, niet een onverklaarde
array in het scalaire contract. De bijbehorende Gold-schematypen volgen STRING.
De modelcompilatie moet deze projectie in Fabric nog bevestigen.

### B-FAB-028 - zelfstandige packaging en geordende lokale definitie

Alternatief: de volledige v2-package uitrollen met Databricks-afhankelijkheden.
Dat is afgewezen: platform- en projectgrenzen zouden opnieuw vermengd raken.
De aparte wheel bevat alleen fabric_etl en heeft geen externe runtime-
dependencies voor de huidige contract-/plantooling.

De planner controleert wheelnaam/versie via Python-package-METADATA, volledige
Python-module-inventaris en bytevergelijking met de aanwezige frameworkcode.
Definities krijgen Base64-parts en fingerprints; migraties krijgen checksums
en expliciete recovery-instructies. Notebookdependency-injectie faalt bij een
ontbrekende of meervoudige header.

De Environment-ID blijft bewust `${etl_environment_item_id}`. De fase-4-runner
moet die vervangen door de daadwerkelijk gereconcilieerde item-ID voordat hij
de Notebookdefinitie verstuurt. Het lokale plan is niet rechtstreeks uitvoerbaar.

### B-FAB-029 - volledige Gold-kolomdeclaraties, compilegate behouden

Aanvankelijk bevatte Gold alleen grainkolommen. Dat was onvoldoende als
outputcontract. De converter leest nu alle getypeerde kolommen en nullability
uit de vaste bestaande Gold-DDL-grammatica; onbekende declaraties falen.
De originele DDL wordt als gefingerprintte herkomstfixture bewaard, niet
als Fabric-migratie uitgevoerd.

Gold heeft daardoor geen grain-only schema's meer. De 40 Vault/Business/Gold-
modelschemas blijven REQUIRES_COMPILE: de fysieke eventkolommen, gegenereerde
hashkeys, afgeleide intervallen en daadwerkelijke SELECT-output moeten bij de
runtimecompiler worden vastgelegd en vergeleken. Vooral een link-businesskey
is niet automatisch een fysiek opgeslagen kolom; oude `_h`-tabellen/views
worden niet blind overgenomen. Quality/reference heeft 9 gedeclareerde schemas.

### B-FAB-030 - control-DDL zonder onveilige lockinitialisatie

De tweede migratie declareert metadatarelease, metadata-pointer, project_owner,
delivery, batch_context, work_item, audit.events, commit_manifest en drie
Gold-publicatietabellen. Logische keys zijn geen afgedwongen Delta-PK's.
Er wordt geen FREE-owner-rij via runtime INSERT/MERGE aangemaakt.

Fase 4 moet onder exclusieve bootstrap precies een owner-rij initialiseren,
bestaande schema's reconciliëren en echte concurrerende claims testen.
CREATE IF NOT EXISTS is geen schema-upgrademethode en biedt op zichzelf geen
veilig mutexprotocol. Een incompatibele tabel vereist een nieuwe migratie.

### Buildproblemen en oplossing

- De eerste wheelcontrole ging uit van LF-regelafbreking in METADATA.
  Dit is vervangen door de standaard MIME-headerparser; verkeerde versies
  en stale code worden negatief getest.
- Een planbestandsnaam met drie volledige SHA-256's overschreed de Windows-
  padgrens. Bestandsnamen gebruiken nu drie prefixes van 12 tekens.
  Volledige fingerprints staan in het plan; afwijkende inhoud bij dezelfde
  naam wordt geweigerd. Geen stille overwrite bij een prefixcollision.
- Afhankelijkheden van collapsed Quality-objecten worden niet als cyclische
  bron-op-bronedges bewaard. Delivery-completeness blijft een afzonderlijke
  runtimegate van het vastgestelde manifestcontract; die moet fase 5 bewijzen.

## 4. Uitgevoerde acceptatie

| Controle | Werkelijk resultaat |
|---|---|
| Volledige regressiesuite | **284 passed**, 2.43 seconden |
| Contoso-conversie tweemaal | Identieke release en projectbestanden; gewijzigde bestanden worden niet overschreven |
| Negatieve contractproeven | Onbekende velden/typen, duplicaten, foutieve mappings/keys, referenties, cycles, pathescape en fingerprints falen |
| Runtime-releasecheck | Kandidaat blijft geblokkeerd; geen success-shaped fallback |
| Complete Gold-schema's | Alle 19 bevatten meer dan hun grain, plus technische batchkolom; date_key is INT |
| Lokale voorbereiding tweemaal | Zelfde wheelbytes en hetzelfde plan-ID |
| Geisoleerde wheelimport | Python -I importeert uit de wheel; v2-package niet importeerbaar |
| Definition-/migratieproeven | Decodeerbare parts, expliciete onopgeloste binding, dependencies, 11 controltabellen zonder INSERT/MERGE/DROP |
| Read-only notebookadapter | Fake Spark registreert alleen drie SHOW SCHEMAS; versie-/visibilityfouten falen |
| Originele baseline | Git diff van metadata/seed, src/contoso_lakehouse en originele pyproject leeg |
| Codeanalyse | Pylance-diagnostics voor gewijzigde kernmodules zonder fouten; nieuwe tests/notebook zonder editorfouten |

VS Code ontdekte geen tests; daarom is de bestaande pytest-runner direct
met de geselecteerde C:\Python313\python.exe gebruikt. Geen tooling geinstalleerd.
De wheelimport is een packagingproef, niet een bewijs van Spark-compatibiliteit.

### Definitieve identiteiten

- Projectrelease: `caac34a0cf503bdb14ebe9bc101cf6c789220f4f78a603394805585c38c36705`
- Wheel SHA-256: `68a1bf65894d503ad469d29e9421525c0e29c949ba5558db7404786e7b9078fd`
- Plan-ID: `516f8540f9d1b7780d320e73795341e6e918175b859a405335f58b95cefe48ac`
- Planstatus: `PLANNED_NOT_DEPLOYED`
- Onopgeloste itembinding: `etl_environment_item_id`

## 5. Open gates en overdracht naar fase 4

De kandidaat meldt **70 runtimeblockers**: 40 schema-compilaties,
20 SQL-reviews, 3 expliciete Business Vault/PIT-implementaties, 5 governance-
goedkeuringen, 1 JDBC-connectionverificatie en 1 absence-deletegoedkeuring.
Dit zijn geregistreerde vervolgverplichtingen, geen 70 geslaagde controles.

Volgorde voor de volgende fase:

1. Environment reconciliëren, library publiceren en echte definitie-export
   vergelijken; Notebook binden en read-only verificatie starten.
2. Migraties uitvoeren met checksummed journal en schema-reconciliatie.
3. Controladapter, metadataactivering, batchtijdpersistency en exclusieve
   bootstrap implementeren; concurrentie en lost-response echt toetsen.
4. Python/Spark-type- en hashvectors toetsen op de gekozen runtime.
5. Daarna ingestie, Quality/Vault/reference/PIT, Gold en DirectQuery uitbouwen.

Er is geen volledige ETL-pipeline of consumer gedeployed en geen productie-
of volledige-redeployacceptatie afgegeven. Redeploy blijft data behouden;
replay is een afzonderlijke opdracht.
