# Fabric deployment en redeploy

Datum: 10 oktober 2026. Status: fase-3-projectvalidator, zelfstandige wheel,
lokale deploymentvoorbereiding en bestaande ontwikkelfoundation;
niet de volledige Fabric ETL-release.
Het fase-4-platformbewijs, inclusief de beperkte SQL/OneLake-T-13-baseline,
staat in het [fase-4-platformrapport](15_fabric_fase4_platformbewijs.md).
Fase 5 is actief; de hieronder bewezen slices zijn geen volledige
Contoso-runtime- of businessproductacceptatie.
Fase 2 is inmiddels formeel afgesloten in het
[ontwerpcontract](13_fabric_ontwerpcontract_fase2.md). De hierin beschreven
gepinde DirectQuery-consumers en projectownership worden onderdeel van
de volledige releaserunner; de huidige foundationrunner doet dit nog niet.

## 1. Verplichte einduitkomst

De opdrachtgever wil aan het projecteinde aan de assistent kunnen vragen:
**"Redeploy alle stappen voor Contoso in omgeving dev."**

Dit is een concrete eindacceptatie-eis (B-FAB-019), niet slechts een
beheerwens. Uitrol moet ook zonder de chatgeschiedenis reproduceerbaar zijn.
De assistent bedient ondersteunde commando's; de repository bewaart de
definities, bindings, migraties, policies en verificaties.

De volledige releaserunner moet uiteindelijk omvatten:

1. Releasefingerprint, projectmetadata en doelomgeving valideren.
2. Exacte wijziginventaris tonen en alleen de geregistreerde scope beheren.
3. Foundation verifiëren of aanmaken, met schema-enabled Lakehouses.
4. Gepinde Environment/package en versieerbare controlmigraties deployen.
5. Metadata als volledige kandidaat schrijven, controleren en activeren.
6. Notebooks, pipelines en scheduling als versiebeheeritems deployen.
7. Gold SQL-contracten en de gekozen DirectQuery-consumerbinding deployen.
8. Provisioning, technische uitvoering en businessaudit volgen.
9. Een verificatierapport met release-, artifact- en job-ID's opleveren.

**Redeploy is niet hetzelfde als replay.** Standaard blijven Raw, historie,
audit en laatste goede Gold-publicatie behouden. Geen DROP/VACUUM/reset,
orphan deletion, stil opnieuw extraheren of opnieuw laden bij redeploy.
Reprocess/replay moet afzonderlijk benoemd en auditeerbaar zijn. Breaking
schemawijzigingen vereisen een migratiepad en kunnen deployment blokkeren.

## 2. Wat nu bestaat

Nieuwe kern in `src/fabric_etl`, naast de ongewijzigde v2-package:

- Strikte JSON-reader, inclusief afwijzen van dubbele velden en NaN/Infinity.
- Omgevingsbinding met drie expliciete rollen en aparte Spark-/T-SQL-namen.
- Hash v1 met namespace, vaste volgorde en onderscheid NULL/lege string.
- Batchcontext met deterministische identiteit en herstel van vastgelegde UTC-tijd.
- Gesloten-manifestvalidatie voor scope, bestanden en expliciete lege snapshots.
- Streaming SHA-256-controle van lokale Raw-bestanden.
- Chronologiegate die oudere geregistreerde incomplete leveringen niet overslaat.
- Foundationreconciliatie via de publieke Fabric REST API.

Daarnaast bestaat nu een volledige structurele projectvalidator en expliciete
Contoso-conversie; zie [fase-3-acceptatie](14_fabric_fase3_release_acceptatie.md).
Dit is **geen volledige ETL-runtime**.
De controladapter implementeert nu duurzame batchtijd en herstel bij retry;
het echte Spark-bewijs hiervan is nog een open fase-4-gate.
De manifestkern is een eerste gesloten-bestandscontract; bronwatermarks,
publicatietijd en duurzaam deliveryregister volgen in de ingestieadapter.
De lokale fileverificatie bewijst nog geen OneLake-write- of Spark-readgedrag.

### Beschikbare commando's

Vanuit de repository in PowerShell, zonder package-installatie:

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
& 'C:\Python313\python.exe' -m fabric_etl validate-environment environments\contoso.dev.json
& 'C:\Python313\python.exe' -m fabric_etl validate-json metadata\seed\meta_source_system.json
& 'C:\Python313\python.exe' -m fabric_etl deploy-foundation environments\contoso.dev.json --timeout-seconds 600
```

`C:\Python313\python.exe` is de huidige ontwikkelinterpreter, geen universeel
templatepad. Gebruik elders de geselecteerde interpreter.
`validate-json` accepteert ieder bestaand JSON-pad en valideert uitsluitend
JSON-syntax en dubbele velden. De oorspronkelijke source-objectseed faalt
bewust op het eerder gevonden dubbele `freshness_sla_hours`-veld; die baseline
is niet aangepast. De toekomstige Fabric-projectrelease krijgt expliciete,
gevalideerde waarden en een eigen contract.
`validate-environment` controleert configuratie en gebonden IDs, niet de
remote gezondheid van Fabric.

Er is nog **geen werkend commando voor redeploy van de volledige keten**.
`deploy-foundation` rapporteert expliciet `scope: foundation only`.

### Lokale fase-3-releasevoorbereiding

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
& 'C:\Python313\python.exe' -m fabric_etl convert-contoso metadata\seed --output projects\contoso
& 'C:\Python313\python.exe' -m fabric_etl validate-project projects\contoso\release.json
& .\deploy\prepare-release.ps1 -Python 'C:\Python313\python.exe'
```

Conversie gebruikt de originele seed plus Gold-DDL uit dezelfde repository.
Een identieke kandidaat kan opnieuw worden gegenereerd; afwijkende bestaande
projectbestanden worden niet overschreven. Voor een gewijzigde kandidaat een
nieuwe outputmap gebruiken en de verschillen expliciet reviewen.

De voorbereidingsstap bouwt de zelfstandige wheel en schrijft een plan naar
dist met projectrelease-, wheel- en bindingfingerprintprefixes in de naam.
Een gewijzigde definitie met dezelfde naam wordt geweigerd; archiveer de
oude release en maak een nieuwe releaseversie in plaats van blind overschrijven.
Het plan bevat volledige fingerprints, itemdefinities, migraties en
runtimeblockers. Planstatus is PLANNED_NOT_DEPLOYED.
Er worden geen API-aanroepen, datawrites, migratie-executies of jobs gestart.

`validate-project --require-runtime` faalt nu bewust: schemas/SQL,
Business Vault/PIT, policies, verbinding en deletegoedkeuring zijn nog
niet operationeel vrijgegeven. De Environment-ID in de Notebookdependency
wordt pas bij echte itemdeployment opgelost. De twee lokale builds uit fase 3
bewijzen geen volledige remote redeploy.

## 3. Foundation-runner: garanties en foutgedrag

- Ondersteunde Azure CLI-authenticatie; access token alleen in procesgeheugen.
- Geen browsertokens, credentialbestanden of tokens in rapportage.
- Credentials alleen naar het Fabric API-origin; HTTP-redirects geweigerd.
- Lakehouses op naam hergebruiken of gepinde IDs controleren; ambiguiteit faalt.
- Aanmaak met `creationPayload.enableSchemas=true`.
- Long-running provisioning met deadline, statuspolling en Retry-After.
- Alleen GET automatisch herhalen bij 429/502/503/504; geen blinde POST-retry.
- Default schema en succesvolle SQL-endpointprovisioning controleren.
- Geen DELETE/PATCH/datawrites en geen automatische bindingsoverschrijving.
- Een fout kan volgen op een remote succesvolle create: dan remote toestand
  opnieuw inspecteren en reconciliëren, niet aannemen dat niets is geschreven.
- Uitvoer bevat pas SUCCEEDED als alle drie de rollen geverifieerd zijn.

API-bronnen:
[Create Lakehouse](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/items/create-lakehouse)
en [Get Lakehouse](https://learn.microsoft.com/en-us/rest/api/fabric/lakehouse/items/get-lakehouse).

## 4. Echt ontwikkelfoundationbewijs

Via de reeds door opdrachtgever aangemelde Fabric-browser aangemaakt:

| Rol | Lakehouse | Item-ID | Bewijs |
|---|---|---|---|
| control | lh_contoso_control | 67751097-52f3-4582-82c2-23fd0a6c89f9 | Item geopend; Tables/dbo en Files zichtbaar; SQL-endpoint aangemaakt |
| processing | lh_contoso_processing | 711abadf-4418-40f9-9599-ef602cac74b2 | Item geopend; Tables/dbo en Files zichtbaar; SQL-endpoint aangemaakt |
| gold | lh_contoso_gold | 5b8eec35-6036-4861-9e58-294add84382d | Item geopend; Tables/dbo en Files zichtbaar; SQL-endpoint aangemaakt |

Workspace: `4be37544-10b9-450d-80f0-f9b3d793b83b`.
IDs vastgelegd in [dev-binding](../environments/contoso.dev.json).
Nog geen data-/controltabellen, ETL-notebooks, pipelines of Power BI-model.
De browserinrichting is een expliciete bootstrap-uitzondering, **geen bewijs
van reproduceerbare commandodeployment**.

### Eerdere blokkade (inmiddels opgelost)

Zowel `az rest` als de nieuwe runner bereikt de publieke Fabric API,
maar GET wordt geweigerd met **HTTP 401 / UserNotLicensed**.
De huidige CLI-identiteit is dus niet geaccepteerd als gelicentieerde Fabric-
caller. Dit bewijst geen ontbrekende workspacecapaciteit: de browserroute
heeft de drie items daadwerkelijk aangemaakt.

Nodig: ondersteunde aanmelding met een gelicentieerde identiteit die rechten
heeft op deze workspace, of een daarvoor ingerichte workloadidentiteit.
Geen license aankoop, credentialextractie of tenantbrede policywijziging
automatisch uitgevoerd.

### Oorzaak vastgesteld en opgelost

Na de afgeronde interactieve aanmelding bleef de standaard CLI-tenant
`5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad` UserNotLicensed melden.
Het zichtbare browseraccount en de CLI-accountnaam waren gelijk; de browser
toonde Pro. Het account heeft toegang tot twee tenants. Een expliciete
tokenaanvraag voor `f9350d85-6f0e-42fd-8385-a59c2f09ec1a` gaf succesvol toegang
tot de doelworkspace en alle zes bestaande items (drie Lakehouses plus drie
SQL-endpoints).

**Root-causebesluit:** tenant wordt een verplicht onderdeel van de
omgevingsbinding, niet impliciet afgeleid van de geselecteerde Azure CLI-
subscription. Omgevingscontract is nu **1.1**, met verplicht `tenant_id`.
Oude 1.0-input faalt expliciet; upgrade door de werkelijke tenant toe te
voegen en de contractversie te verhogen. De runner vraagt ieder token aan
met `az account get-access-token --tenant <tenant_id>`.
Geen globale CLI-accountwissel, license aankoop of credentialextractie.

De echte `deploy-foundation`-runner is daarna **tweemaal succesvol** uitgevoerd:
alle drie rollen `action: verified`, `schema_enabled: true` en
`sql_endpoint_status: Success`. Lakehouse- en endpoint-ID's waren identiek.
Geen create/delete/datawrite nodig. Dit bewijst herhaalde commandogestuurde
reconciliatie van de bestaande foundation; lege-workspace-aanmaak, itemupdates
en de volledige ETL-redeploy zijn daarmee nog niet bewezen.

| Rol | Gecontroleerd SQL-endpoint-ID |
|---|---|
| control | 4fbb2801-39d9-4d28-87ba-88903a572c1a |
| processing | a57f8365-ea7a-448c-880b-a39822956936 |
| gold | 726efadf-8dd3-4e49-823c-7f47ed769918 |

## 5. Validatie en resterende releasegates

Op 10 oktober 2026:

- 69 nieuwe tests geslaagd; twee foundationdeploys tegen een fake API
  behouden dezelfde IDs en veroorzaken precies drie creates, geen deletes.
- Bestaande plus nieuwe suite: **202 passed in 0.60s**.
- Oorspronkelijke metadatareleasefingerprint ongewijzigd:
  `40b0fbb42f32c19b0ca4417f41b561af631d7d22a2e6730133c9f0f895a2694a`.
- Nieuwe CLI echt gestart: configuratiecontrole geslaagd; remote foundation-
  verificatie faalt expliciet op UserNotLicensed, zonder mutatie.
- Geen editorerrors in de vijf nieuwe implementatiemodules; deadline- en
  credential-origin/redirectguards aanvullend getest.

Volledige redeploy-acceptatie vereist vervolgens:

1. Lege doelworkspace volledig uitrollen met passende rechten.
2. Dezelfde release nogmaals uitrollen; geen duplicaten, dataverlies of
   ongeplande herverwerking; stabiele artifactbindings.
3. Upgrade en gedeeltelijke deployfout injecteren; migraties hervatten en
   de laatste goede publicatie behouden.
4. Raw tot Gold Current voor alle vijf bronnen en negen objecten doorlopen.
5. Jobstatus plus row-/multipliciteitsreconciliatie en publicatie controleren.
6. DirectQuery-contract, beveiliging, herstel, workload en capacity toetsen.
7. Een nieuw voorbeeldproject uitsluitend via ondersteunde projectconfiguratie
   uitrollen als bewijs van herbruikbaarheid.

Geen van deze full-chain gates wordt door lokale tests of lege Lakehouses
als automatisch geslaagd beschouwd.

### Validatie na tenantcorrectie

- Gerichte Fabric-suite: **74 passed in 0.67s**.
- Volledige suite: **207 passed in 0.55s**.
- Toegevoegd: ontbrekende/ongeldige tenant en oude contractversie afwijzen;
  tokencommand krijgt exact de tenant uit configuratie.
- Echte foundationreconciliatie tweemaal SUCCEEDED zoals hierboven.
- Actuele v2-metadatareleasecheck geldig:
  `d4746f655f74448732ab5d552d80b9cf591d38976e9204fbae196cfc44a2da60`.
  Deze uitvoer wijkt af van de eerder geregistreerde fingerprint. Daarom
  aanvullend alle seed-JSON vergeleken met Git HEAD: inhoud gelijk, en HEAD
  levert dezelfde actuele fingerprint. Geen v2-seeds of v2-code gewijzigd.
  De oorzaak van de eerder geregistreerde afwijkende waarde is niet
  vastgesteld; die eerdere waarde is geen actueel verificatiebewijs.

## 6. Beperkte fase-5-bundle en runtimeprobe

Sinds 11 oktober 2026 zijn er twee gesloten bundlescopes voor fase 5. Dit
zijn experimentele ontwikkelvoorzieningen, geen algemene releasefunctie en
geen Microsoft-asset-bundle-equivalent. Elke manifest-scope bindt één
specifieke dev-Environment, de vastgepinde add-onwheel en precies één
allowlisted notebook. De deploy weigert wheel-hashdrift of een Environment
die niet als `Success` gepubliceerd is, en controleert de gepubliceerde
librarychecksum vóór itemdeployment. Deployment bewaart ownership in een
journal en verifieert de teruggelezen notebookdefinitie.

| Manifest | Gesloten scope | Remote writegrens |
|---|---|---|
| `fabric/phase5-runtime-bundle.json` | `PHASE5_RUNTIME_PROBE` | Alleen notebookdefinitie; notebook zelf read-only |
| `fabric/phase5-bronze-bundle.json` | `PHASE5_BRONZE_PROBE` | Synthetische Raw files in `Files/phase5-proof/bronze-probe-v1`, één `bronze.phase5_probe_customers` Delta-target en geïsoleerde `PHASE4_PROOF`-batch/commitcontrol |

Geen van beide scopes heeft een delete-, cleanup-, replay-, metadata-
activatie- of businessbron-ingestieactie. De Bronze-scope bevat echte
Fabric-writes, dus voer die alleen met het afgetekende specifieke plan en de
beheerde dev-Environment uit; geen ander doel wordt aanvaard.

Voor handmatig gebruik vanuit de repository-root:

```powershell
$env:PYTHONPATH = Join-Path $PWD 'src'
$plan = Join-Path $env:TEMP 'contoso-phase5-runtime.plan.json'
$journal = Join-Path $env:TEMP 'contoso-phase5-runtime-bundle-journal.json'
$receipt = Join-Path $env:TEMP 'contoso-phase5-runtime-probe-receipt.json'

python -m fabric_etl plan-phase5-bundle `
  fabric\phase5-runtime-bundle.json `
  --repository $PWD --output $plan
python -m fabric_etl deploy-phase5-bundle $plan --journal $journal
python -m fabric_etl run-notebook environments\contoso.dev.json `
  --item-id 13355c3f-c59a-471e-b96e-754460df1c53 `
  --environment-id 7266a872-dd91-4aa6-a0b4-d4cc5b862643 `
  --receipt $receipt
```

Voor het afzonderlijk afgebakende Bronze-experiment gebruik je een eigen
planfile, maar hetzelfde journal zodat notebookownership samen wordt
bijgehouden:

```powershell
$bronzePlan = Join-Path $env:TEMP 'contoso-phase5-bronze.plan.json'
python -m fabric_etl plan-phase5-bundle `
  fabric\phase5-bronze-bundle.json `
  --repository $PWD --output $bronzePlan
python -m fabric_etl deploy-phase5-bundle $bronzePlan --journal $journal
```

Run alleen na een planstatus `PLANNED_NOT_DEPLOYED` en geslaagde deployment-
readback:

```powershell
python -m fabric_etl run-notebook environments\contoso.dev.json `
  --item-id <door-deployer-geretourneerde-notebook-id> `
  --environment-id 7266a872-dd91-4aa6-a0b4-d4cc5b862643 `
  --receipt (Join-Path $env:TEMP 'contoso-phase5-bronze-v1-receipt.json')
```

Plan is alleen lokaal desired state (`PLANNED_NOT_DEPLOYED`); deploy en
notebook-uitvoering zijn afzonderlijke mutatie-/runtimehandelingen. Bewaar
receipt en plan als bewijs. Bij een timeout/fout: lees de bestaande receipt
en Get Item Job Instance uit, start geen dubbele job. Het eerste probe-experiment
bewees waarom de Fabric execution snapshot nodig kan zijn: Job Instance gaf
alleen `System_Cancelled_Session_Statements_Failed`; de snapshot toonde
`UNRESOLVED_COLUMN` voor het contractveld `version`. De probe is met een
fixture die nu alle `RAW_IDENTITY_FIELDS` bevat bijgewerkt via hetzelfde
ownershipjournal. De tweede uitvoering is `Completed` met expliciete
`exit_result.status=SUCCEEDED`, beide packageversies `0.1.0` en Raw-ID
`90d2b85ced455a8fee9cb1a2644b73a8e5c20ccd4f95e55786f0871937038810`
(Python en Spark gelijk). Daarmee zijn Import en Spark-hashprobe afgetekend;
de Bronze-notebook is met plan `f05ae5627123c9946d20612ef21c0733fee240584db76d7ee18f6949d2b3a1e3`
als item `83b67667-6a42-4108-ac1b-ee5f911a8248` aangemaakt en teruggelezen.
De eerste Bronze-job `ffbec3fb-b55e-48e1-bb35-ab8d8ed21d6f` is `Completed`
met 2 ingevoegde records, `bronze_commit_status=COMMITTED`, volledige
readbackfingerprint en `owner_released=true`. De afzonderlijke retryjob
`73f5ef04-98a5-4393-976f-1f66889f0cc0` bevestigde `inserted_count=0`,
`retry_count=2`, dezelfde fingerprint, commit `VERIFIED` en
`owner_released=true`. Synthetic Bronze-write/readback/retry is daarmee
afgetekend. Quality/Reject, Vault, Gold en businessbronkoppeling blijven
onbewezen.

### Quality/Reject en drie-leveringen-historieproef

De dev-Environment is voor de volgende slices expliciet geupgrade naar
`fabric-etl-phase5==0.2.0`; de basis `fabric-etl==0.1.0` en Spark-settings
blijven byte-identiek. De oude 0.1.0-bundleplannen blijven historische pins,
geen opnieuw uitvoerbare plannen op deze geupgrade Environment.
Gebruik niet een nieuwe wheel met hetzelfde versienummer.

| Manifest | Scope | Afgebakende writes |
|---|---|---|
| `fabric/phase5-quality-bundle.json` | `PHASE5_QUALITY_PROBE` | Processing Raw-map `Files/phase5-proof/quality-probe-v1`; `bronze.phase5_quality_customers`, `quality.phase5_probe_customers`, `reject.phase5_probe_customers`, `quality.phase5_probe_filtered_customers`; geïsoleerde proofbatch/commits |
| `fabric/phase5-chain-bundle.json` | `PHASE5_CHAIN_PROBE` | Drie eigen Raw-leveringsmappen onder `Files/phase5-proof/chain-probe-v1`; eigen `phase5_chain_*`-tabellen in Bronze/Quality/Raw Vault/Business Vault; `historical.phase5_chain_customer_releases` en `current.phase5_chain_customer_releases` in Gold; geïsoleerde proofbatch/commits |
| `fabric/phase5-recovery-bundle.json` | `PHASE5_RECOVERY_PROBE` | Alleen negatieve kandidaatwrites tegen de reeds gecommitte Qualityfixture; zes verwachte contractblokkades en readback van ongewijzigde target/commit |

De chainproef is een testharness, niet de generieke implementatie van alle
Contoso-entities: twee customer/segmenthubs, één link, immutable satellite-
events, een afgeleide BV-projectie en PIT, versiegepinde historische/current
Gold-snapshots. De Gold `current`-namespace wordt onder ownership aangemaakt
als die ontbreekt. Er wordt geen actieve businessproductpointer of
Power BI-consumerbinding gewijzigd. Geen absence-delete of bronextractie.

Publiceer alleen de afgebakende runtime-upgrade met:

```powershell
$env:PYTHONPATH = Join-Path $PWD 'src'
python deploy\publish-phase5-quality.py `
  --receipt deploy\evidence\phase5-quality-runtime-upgrade.json
```

Het script verlangt de eerder geverifieerde exacte basiswheel/settings,
weigert afwijkende Environmentinventaris, controleert exclusieve setup,
vervangt alleen de eigen add-on en verifieert de gepubliceerde bytes.
Bij reeds aanwezige correcte publicatie wordt alleen geverifieerd; geen
onnodige herpublicatie.
Bij onzekere update/publicatierespons: lees receipt en remote toestand;
geen blinde tweede update of nieuwe job.

Plan/deploy verloopt via dezelfde bestaande bundlecommando's, met een eigen
planfile per scope. Bewaar het ownershipjournal duurzaam; een ontbrekend
journal geeft geen toestemming om bestaande gelijknamige notebooks te
adopteren. Start de writers sequentieel en gebruik een nieuwe receiptnaam
voor de afzonderlijke retryjob. De Qualityproef moet `5=1+3+1` bewijzen,
een waarschuwing op een passed record, twee redenen op één rejected record
en het blokkeren bij 24,99 procent versus toelaten bij exact 25 procent.
De chainproef moet over de drie leveringen respectievelijk 2/3/4
historische versies en 2/2/1 actieve klanten bewijzen; een retry mag geen
nieuwe records of veranderde commit-/outputfingerprints opleveren.

Beide eerste jobs en aparte Quality/chainretryjobs zijn inmiddels terminal
geverifieerd. De recoveryjob blokkeerde payloadconflict, verloren ownership,
out-of-scope input, dubbele key, NULL key en commitconflict; de bestaande
target/commit bleef ongewijzigd. Dit zijn frameworkhersteltests, niet nieuwe
SQL/OneLake-autorisatiebewijzen. De beperkte T-13-baseline staat in het
[fase-4-platformrapport](15_fabric_fase4_platformbewijs.md).

Controleer de exacte notebookinhoud, niet alleen jobstatus `Completed`:

```powershell
python deploy\verify-phase5-proofs.py quality `
  deploy\evidence\phase5-quality-v1-receipt.json `
  --retry deploy\evidence\phase5-quality-v1-retry-receipt.json `
  --output deploy\evidence\phase5-quality-retry-verification.json
python deploy\verify-phase5-proofs.py chain `
  deploy\evidence\phase5-chain-v1-receipt.json `
  --retry deploy\evidence\phase5-chain-v1-retry-receipt.json `
  --output deploy\evidence\phase5-chain-retry-verification.json
python deploy\verify-phase5-proofs.py recovery `
  deploy\evidence\phase5-recovery-v1-receipt.json `
  --output deploy\evidence\phase5-recovery-verification.json
```

De verifier verlangt de juiste scope, terminal succes, exacte
counts/thresholds, een vrijgegeven owner en bij retry een andere job-ID,
nul inserts en behouden context/fingerprints/commit-ID's. Hij weigert
synthetische productieacceptatieclaims. Gebruik het duurzame
[ownershipjournal](../deploy/evidence/phase5-bundle-journal.json).
De volledige Contoso-releaserunner en businessproductacceptatie blijven open.

### Post-filter-windowcorrectie en broninspectie

De nieuwe [windowbundle](../fabric/phase5-quality-window-bundle.json) pint
`fabric-etl-phase5==0.3.0` en bevat alleen een read-only Sparkfixture.
Die controleert dat filtered duplicates UNIQUE-windowregels niet beïnvloeden,
dat hyphenated metadataregel-ID's behouden blijven, dat de thresholdpopulatie
alleen eligible rows bevat en dat een all-filtered batch nul violations heeft.
De oude 0.2.0-bundles blijven historische runtimepins, niet automatisch
uitvoerbare plannen op een 0.3.0-Environment.

```powershell
python deploy\publish-phase5-quality.py --bundle quality-window `
  --receipt deploy\evidence\phase5-quality-window-runtime-upgrade.json
```

De werkelijke Fabric Sales-view is daarnaast schema-only gecontroleerd:
33 kolommen en nul bedrijfsrecords. De
[schemareceipt](../deploy/evidence/phase5-fabric-sales-source-schema.json)
bewijst gedelegeerde ontwikkelaarstoegang, geen geactiveerde connectoralias.
Voor een nieuwe inspectie met een nieuwe receiptnaam:

```powershell
.\deploy\inspect-fabric-sales-source.ps1 `
  -SqlClientRoot "$env:TEMP\t13-sqlclient" `
  -Receipt deploy\evidence\phase5-fabric-sales-source-schema-new.json
```

Dit gebruikt de eerder gevalideerde Microsoft.Data.SqlClient-cache,
geen Databricks-secret. De query is begrensd tot TOP(0), met encryptie,
certificaatvalidatie en read-only intent. Een bestaande receipt wordt geweigerd.

0.3.0 is inmiddels `PUBLISHED_READBACK_VERIFIED`, met ongewijzigde
basiswheel/settings. De windownotebook is als item
`3da778b8-2774-4f54-9632-b71d66d02ab4` deployed/readback verified.
Controleer na een terminale job de inhoud met:

```powershell
python deploy\verify-phase5-proofs.py quality-window `
  deploy\evidence\phase5-quality-window-v1-receipt.json `
  --output deploy\evidence\phase5-quality-window-verification.json
```

Deze scope schrijft geen business-/controldata en heeft daarom geen
writerowner; dat versoepelt niet de owner-eis voor de bestaande writerproofs.
Het [migratieverslag](09_fabric_migratieverslag.md) bewaart de exacte
publicatie-/jobidentiteiten en resterende echte metadatagrenzen.

De windowjob is inmiddels terminal `VERIFIED`: 4=1 FILTERED+1 PASSED+
2 REJECTED, post-filter population 3, correcte thresholdblokkade en
all-filtered population 0. Een cross-versioncommitretry met de bestaande
Qualitynotebook is terecht geweigerd: runtimeversie maakt deel uit van de
gecommitte inputfingerprint. Nieuwe runtime onder oude batchprovenance
committen is geen geldige retry. De nieuwe windowplanrevisie controleert
daarom read-only scalar-outputequivalentie en ongewijzigde targets/commits.
Historische definities/plannen worden niet omgepind; commitchecks worden
niet versoepeld. Verlang het extra bewijs expliciet met
`--require-scalar-regression` bij de windowreceiptverificatie.

De [native Sales-sourcebundle](../fabric/phase5-sales-source-bundle.json)
gebruikt dezelfde 0.3.0-library en de officiële SQL database `.mssql()`-
connector met automatisch doorgegeven Entra-identiteit, niet `synapsesql`
(dat is de Warehouse/Lakehouse SQL analytics route).
Scope blijft TOP(0)/schema-only, inclusief controle van de echte
acht-kolommenprojectie en de oorspronkelijke ontbrekende-kolomfout.
De notebook is een begrensde bron/authenticatie/compileproef, geen extractie
of releaseactivatie. Na uitvoering:

```powershell
python deploy\verify-phase5-proofs.py sales-source `
  deploy\evidence\phase5-sales-source-v1-receipt.json `
  --output deploy\evidence\phase5-sales-source-verification.json
```

De sourcejob is geslaagd: echte automatische Entra-authenticatie vanuit
Fabric Spark, 33 bronkolommen, nul records en acht correcte Spark-
projectietypen. De ontbrekende-kolomfout is daadwerkelijk gereproduceerd.
De bronmappingcorrectie is opgenomen in de herhaalbare lokale converter
en de nieuwe niet-geactiveerde kandidaatrelease
`c9beb08ca216ac512739c00a6dde127190bd7e53713327080abe59a10b5406f4`.
De oude release is in deployment evidence bewaard. De echte binding/
unattended-extractrunner en overige metadata-acceptatie blijven open.

Voor de read-only scalar-equivalentieprobe, na de terminale job:

```powershell
python deploy\verify-phase5-proofs.py quality-window `
  deploy\evidence\phase5-quality-window-v3-receipt.json `
  --require-scalar-regression `
  --output deploy\evidence\phase5-quality-window-scalar-verification.json
```

Gebruik de originele Bronze-fingerprinthelper voor Bronze en de
Quality-helper voor Quality; deze zijn verschillende gepinde algoritmen.
Nieuwe planrevisies/evidence worden apart opgeslagen. Behoud oude receipts,
ook van gefaalde proeven; verander geen runtimeprovenance om een retry
succesvol te laten lijken.

De gecorrigeerde scalarregressie is nu terminal `VERIFIED`, job
`f1b2c50a-c34f-4da1-b06f-f6176be61e72`: nieuwe package-output en bestaande
targets hebben dezelfde drie fingerprints, Bronze dezelfde oorspronkelijke
pin, accounting 5=1+3+1 en beide originele commit-ID's/statussen onveranderd.
De eerdere cross-versioncommitfout en probe-fingerprintfout blijven
afzonderlijk in evidence bewaard. Er is geen nieuwe runtime als oude runtime
gecommitteerd. Volledige projectsuite: 433 geslaagde tests.

## Metadata-Quality-bundle 0.4.0

De achtste afgebakende scope is `contoso_phase5_metadata_quality_probe`.
Manifest: [phase5-metadata-quality-bundle.json](../fabric/phase5-metadata-quality-bundle.json).
Deze scope vereist exact add-on 0.4.0 inclusief `mapping.py`; eerdere
scopes houden hun oorspronkelijke versiepin. De planner valideert de
projectrelease inclusief filefingerprints en embedt de niet-geactiveerde
kandidaat als notebookparameter. De basiswheel en Spark-settings blijven
behouden tijdens de gecontroleerde 0.3.0 -> 0.4.0-publicatie.

De read-only notebook voert alle negen echte bronmappings en 33 metadata-
kwaliteitsregels uit op fixtures. Eerste job
`1fa2b802-3ea0-4b55-a09d-7835a281ba65` is `Completed/SUCCEEDED` en exact
`VERIFIED`, inclusief ECB-filter, ongeldige boolean en duplicateblokkade.
Dit is geen durable writer of businessreleaseacceptatie.

Een afzonderlijke herstart gebruikt hetzelfde gedeployde notebook en
dezelfde gepubliceerde bytes, maar een eigen receipt:

```powershell
python -m fabric_etl run-notebook environments\contoso.dev.json `
  --item-id 861b9c7f-115c-40eb-b135-e50195620674 `
  --environment-id 7266a872-dd91-4aa6-a0b4-d4cc5b862643 `
  --receipt deploy\evidence\phase5-metadata-quality-v1-retry-receipt.json
python deploy\verify-phase5-proofs.py metadata-quality `
  deploy\evidence\phase5-metadata-quality-v1-receipt.json `
  --retry deploy\evidence\phase5-metadata-quality-v1-retry-receipt.json `
  --output deploy\evidence\phase5-metadata-quality-retry-verification.json
```

Verifieer pas na terminale jobcompletion. Beide job-ID's moeten verschillen;
de gehele inhoudelijke exit_result moet exact gelijk zijn. Een reeds
bestaande receipt hervat dezelfde job en is geen nieuwe retry. Een
read-only retry bewijst determinisme, geen nul-inserts in duurzame targets.
De 0.4.0-correcties en beperkingen staan uitgebreid in
[F5-METADATA-01](09_fabric_migratieverslag.md#f5-metadata-01---echte-metadata-quality-via-de-eigen-assetbundel).
Volledige lokale regressiesuite na deze implementatie: 460 geslaagde tests.

## Duurzame metadata-Quality/Reject: 0.5.0

Nieuwe gesloten scope `contoso_phase5_metadata_write_probe`, manifest
[phase5-metadata-write-bundle.json](../fabric/phase5-metadata-write-bundle.json).
Deze scope gebruikt de eigen writer in `mapping.py` en vereist exact
0.5.0, SHA-256
`a49d9a84594f12fa6411a876438f57fb75d433442bfa8e84c3443486c3709b66`.
De publicatie controleert de exacte 0.4.0-voorganger en behoudt basiswheel
en Spark-settings. Oudere scope-/wheelpins worden niet aangepast.

De writer accepteert alleen gereconcilieerde, duurzaam gecommitte Bronze:
exacte raw-kolommen, een nonempty/non-NULL context, persistente batchtijd,
bronidentity, commitstatus/layer/entity, Bronze-targetnamespace, volledige
row-equivalentie en oorspronkelijke Bronze-fingerprint. De daadwerkelijke
mapping-/Qualityadapter wordt daarna uitgevoerd. PASSED, REJECTED en
FILTERED worden afzonderlijk immutable geschreven en teruggelezen; pas
dan volgt de QUALITY-commit. Insert-/retryaantallen zitten niet in de
stabiele commitinhoud, runtimeversie en releasemetadata wel.

Proofmode is expliciet: `PHASE4_PROOF` en tableprefix `phase5_metadata_`.
Businessmode verlangt de actieve goedgekeurde release. De eerste
proof gebruikt daadwerkelijke SALES.CUSTOMERS-metadata, maar twee
synthetische records en geisoleerde targets:

- `bronze.phase5_metadata_customers_v1`: twee raw-records.
- `quality.phase5_metadata_customers_v1`: een PASSED record.
- `reject.phase5_metadata_customers_v1`: een REJECTED record met castcode.
- `quality.phase5_metadata_customers_v1_filtered`: nul records.

Dit zijn geen vrijgegeven businessproducten. De writer ondersteunt nog
geen referentieafhankelijke sources: ontbrekende committed referencebinding
blokkeert voor writes. Empty snapshots blokkeren expliciet zolang het
expliciete context-/empty-contract niet is geimplementeerd.

**Runtimepin bij handmatig testen:** de dev Environment is gedeeld.
Na een upgrade naar 0.5.0 weigert een oud notebook met 0.4.0-pin terecht
uitvoering. Verander niet handmatig de versieparameter om de guard te
omzeilen; gebruik het notebook dat bij de gepubliceerde wheel hoort.
De eerder bewezen 0.4.0-receipts blijven geldig als historisch bewijs.

Publicatie en deployment van 0.5.0 zijn nu succesvol teruggelezen.
Notebook-ID: `9df60181-44b6-4863-b2e6-d4041a13091b`.
De eerste writejob en een afzonderlijke herstart zijn inhoudelijk
geverifieerd: alle vier targets hebben bij de herstart nul inserts,
dezelfde fingerprints, persistente batchtijd en dezelfde twee commits
(bij herstart `VERIFIED`). Controleer het bestaande bewijs read-only:

```powershell
python deploy\verify-phase5-proofs.py metadata-write `
  deploy\evidence\phase5-metadata-write-v1-receipt.json `
  --retry deploy\evidence\phase5-metadata-write-v1-retry-receipt.json `
  --output deploy\evidence\phase5-metadata-write-retry-verification.json
```

De bestaande receipts hervatten hun bestaande jobs; overschrijf ze niet.
Een bewuste nieuwe herstart gebruikt een nieuw receiptpad en hetzelfde
gedeployde notebook:

```powershell
python -m fabric_etl run-notebook environments\contoso.dev.json `
  --item-id 9df60181-44b6-4863-b2e6-d4041a13091b `
  --environment-id 7266a872-dd91-4aa6-a0b4-d4cc5b862643 `
  --receipt <nieuw-receiptpad>
```

## Duurzame delivery-ingestie en exitcontrole: 0.6.0

De gedeelde dev Environment bevat nu gepubliceerd en teruggelezen
`fabric_etl_phase5-0.6.0-py3-none-any.whl`, SHA-256
`6c2c2a2679dd1435e4b3c6904c3d9f16ac532719315f757c9c1a5a41b5611d9c`.
De [exitbundle](../fabric/phase5-exit-bundle.json) beheert notebook
`783d4f39-d323-4804-a7ab-dc33a961fff3`. De publicatiereceipt is
[phase5-exit-runtime-upgrade.json](../deploy/evidence/phase5-exit-runtime-upgrade.json).
Oudere notebooks weigeren terecht hun niet meer beschikbare runtimepin.

De runtime registreert het volledige closed manifest duurzaam in
control.delivery.scope_json, controleert fysieke inventory/manifest/bytes,
respecteert oudere unfinished deliveries en registreert batchcontext,
workattempts en audittransities. INGEST-commit volgt pas na immutable
Bronze-readback. Expliciete empty snapshots krijgen context en zero-count
commit; ontbreken van bestanden is niet automatisch een empty snapshot.
Dit verandert niet de afzonderlijke empty-beperking van de metadatawriter.

**Geen geslaagde exit claimen:** de eerste exitjob en de diagnostische
vervolgjob zijn FAILED door een Parquet-bytehashdiscrepantie. De
[oorspronkelijke exitreview](../deploy/evidence/phase5-exit-review.json)
blijft BLOCKED. Een nieuwe diagnostische notebookdefinitie verandert
geen gepubliceerde wheelbytes. Packagecorrecties vereisen een nieuwe
versie en gecontroleerde publicatie; nooit een checksum/pin versoepelen.

Plan en deploy met het bestaande ownershipjournal:

```powershell
python -m fabric_etl plan-phase5-bundle fabric\phase5-exit-bundle.json `
  --repository . --output <nieuw-planpad>
python -m fabric_etl deploy-phase5-bundle <nieuw-planpad> `
  --journal deploy\evidence\phase5-bundle-journal.json
```

Gebruik bij bewuste nieuwe uitvoering een nieuw receiptpad; een bestaand
receipt hervat alleen de bestaande job. Start geen tweede writer zolang
de eerste job niet terminal is. De bedoelde eerste succesvolle exitjob
laat een geisoleerde cross-job levering bewust ongecommit achter; alleen
een aparte recoveryjob kan dat deel van het exitcontract bewijzen.
Vijf andere onderbrekingen worden binnen dezelfde job hersteld: dat is
niet hetzelfde als een onverwachte proceskill. Ook failure-auditrepair
moet afzonderlijk live bewezen blijven, niet uit lokale tests afgeleid.

De exitbundle is inmiddels gericht op patch 0.6.1, SHA-256
`7211506d031508eb7942f2b6a95a10bc7679fea5f90513604ce809bf792097a9`.
Deze vervangt alleen het fysieke hashleespad door counted Hadoop-
streaming; volledige inventory/manifest/size/hashvalidatie blijft behouden.
De oorspronkelijke 0.6.0-wheel is niet overschreven. Publicatie via
`--bundle exit` verlangt nu die exacte 0.6.0-voorganger. De aparte
[patchpublicatiereceipt](../deploy/evidence/phase5-exit-streaming-runtime-upgrade.json)
is leidend voor de publicatiestatus; een ingediende publicatie is nog
geen gepubliceerde en werkende runtime. De sourcebestanden van alle
vijf Pythonmodules zijn byte-identiek aan de gebouwde patchwheel.

**Dagafsluiting:** patch 0.6.1 is inmiddels
`PUBLISHED_READBACK_VERIFIED`. Exitjob
`adea8361-6f4d-4fa2-add4-aeb65044a77f` is bij de afgesproken pauze nog
lopend. Hervat alleen zijn bestaande
[receipt](../deploy/evidence/phase5-exit-streaming-v1-receipt.json);
geen nieuwe job indienen om de status op te halen:

```powershell
python -m fabric_etl run-notebook environments\contoso.dev.json `
  --item-id 783d4f39-d323-4804-a7ab-dc33a961fff3 `
  --environment-id 7266a872-dd91-4aa6-a0b4-d4cc5b862643 `
  --receipt deploy\evidence\phase5-exit-streaming-v1-receipt.json
```

De afzonderlijke recoveryjob is nog niet gestart. Fase-5-exit blijft
BLOCKED totdat het volledige inhoudelijke bewijs is geverifieerd.

De verifier vergelijkt de volledige retry-uitkomst met de eerste run,
met uitsluitend de verwachte wijzigingen in insert-/retrytellingen en
commitstatus. Reference-free Customers is bewezen; andere sources en
committed referenties moeten nog op deze writer worden aangesloten.
