# Fase 4 - platformdeployment en controlbewijzen

Bijgewerkt: 11 oktober 2026. Status: **fase 4 afgerond; controlfoundation en
T-13-least-privilegebewijs gevalideerd**.
Deze fase omvat de echte Fabric-deployment van de foundation/controlruntime
en het bewijs op Spark. Geen business-ingestie, replay of Gold-publicatie.

## 1. Vastgestelde platformfeiten

- De drie schema-enabled Lakehouses behouden hun bestaande bindings.
- Environment `7266a872-dd91-4aa6-a0b4-d4cc5b862643` en verificatienotebook
  `91ec449c-1462-4a48-8fbe-7b146a8a5381` zijn daadwerkelijk aangemaakt.
- Fabric accepteerde een Git-source-definitie maar exporteerde lege
  notebookinhoud. De runner converteert de dunne adapters daarom expliciet
  naar ipynb en vergelijkt uitgevoerde cellen, parametertags en dependencies.
- Regionale LRO-Location-headers worden niet gevolgd met credentials.
  De runner reconstrueert een publieke API-URL met het operation-ID.
- Update-operaties kunnen slagen zonder `/result`-resource. De waargenomen
  `OperationHasNoResult` was een runnerfout na een geslaagde platformactie,
  geen bewijs dat de Environment-publicatie was mislukt.
- Publicatieversie `83a64094-ba9e-4464-92f4-71990465332b` werd gestart om
  08:15:03 UTC en afgerond om 08:19:06 UTC. Spark settings en libraries
  rapporteren beide `Success`.
- Deze publicatie betrof een tussentijdse wheel met SHA-256
  `56c57ccc2cf89d414aa21ff7ef90ec088ffa9558cb5b5f8ac434b6edb30f2da0`.
  Dat is niet de volledige laatste control/proof-implementatie.

## 2. Negatieve feedback blijft onderdeel van het bewijs

De eerste echte Spark-run, job
`5b1a4224-c9e3-485e-96a1-7f1ed9cb9e76`, faalde met een module-importfout.
Monitoring toonde default compute in plaats van de Environment-instellingen.
De runner gebruikt nu de notebook-specifieke GA-execution-API met expliciete
Environment- en default-Lakehouse-binding. Een algemene jobstatus `Completed`
is onvoldoende: een geldig JSON-exitresultaat is verplicht.

Het eerste falen staat in
[de eerste jobreceipt](../deploy/evidence/phase4-visibility-01.json).
Een gestarte vervolgjob of geslaagde Environment-publicatie mag niet als
geslaagd Spark-/controlbewijs worden gerapporteerd.

De tweede job `86e67185-7187-498d-b8d1-a599253ff753` gebruikte aantoonbaar
de juiste compute: driver/executor 4 cores, 28 GB, dynamisch 1-2 executors,
runtime 2.0. De package-import slaagde; de snapshottraceback liep door tot
`fabric_etl.runtime.verify_foundation`. De concrete fout was
`SCHEMA_NOT_FOUND` bij `SHOW SCHEMAS IN workspace.lakehouse`.
De read-only verificatie adresseert nu expliciet de bestaande `dbo`-schema's
met `SHOW TABLES IN workspace.lakehouse.dbo`. Lege foundationtabellijsten
zijn toegestaan; een echte resolutiefout blijft een fout.

De jobdetail-API plaatst `exitValue` onder `properties`, niet op het hoogste
niveau. Ook deze waargenomen responsvorm heeft een regressietest.

De eerste controljob `49f43cbd-f771-4600-995b-a29ff9a1aed8` faalde bij
het schrijven van het migratiejournal. Spark 4.1 gaf
`INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE` voor
`decode(unhex(...), 'UTF-8')` binnen een inline `VALUES`-definitie.
De veilige literalencoder blijft behouden; alle control-/journalinserts
gebruiken nu `INSERT INTO ... SELECT ...` voor scalar expressions.
Dit betreft bootstrap, kandidaat, metadata-pointer, batchcontext en audit,
niet alleen de eerst gefaalde journalinsert.

De bestaande namespace-DDL en journaltabel worden bij retry behouden.
Er is geen purge/reset uitgevoerd. De gefaalde receipt blijft in
[controlpoging 1](../deploy/evidence/phase4-control-01/01-setup.json);
een nieuwe bewijsfolder voorkomt stil resubmitten van een reeds gefaalde job.

De tweede controlpoging behaalde ook de cross-session setupretry.
De daaropvolgende echte race had een winnaar
`5f4e2cf1-673e-434b-b820-a2f1e50658e3`, van vrije generation 2 naar owner
generation 3; deze owner werd correct vrijgegeven.
De andere job `9d7eac08-0966-4f06-9a54-bc45e7e8c469` werd door Delta
afgewezen op een concurrerende UPDATE. Fabric leverde deze specifieke
`io.delta.exceptions.ConcurrentAppendException` echter als `Py4JJavaError`,
niet als de Python Delta-exceptionclass.

De proofadapter herkent nu uitsluitend de twee expliciet toegestane Java
Delta-conflicttypen als `BLOCKED`; alle andere Javafouten worden opnieuw
opgeworpen. Geen brede foutonderdrukking of automatische writerretry.
De nieuwe volledige proofrun gebruikt een nieuwe bewijsfolder en pinning
van iedere receipt aan het concrete deploymentplan.

## 3. Implementatie en verificatievolgorde

1. Export van de exacte wheel, compute-instellingen en notebookcellen.
2. Echte package-import en read-only foundationcontrole via Spark.
3. Allowlisted namespace-/controlmigraties met exact schema-readback en
   immutable bron-/renderchecksums in het migratiejournal.
4. Dezelfde setup in twee afzonderlijke Spark-jobs: geen dubbele eigenaar,
   kandidaat of batchcontext; de tweede migratie alleen `verified`.
5. Volledige metadata-kandidaat inclusief bestandsteksten en checksums.
   De 70 runtime-/governancegates blijven de activatie blokkeren.
6. Python/Spark-canonicalisatie: gelijke JSON-bytes en SHA-256 voor NULL,
   decimalgrenzen, UTC-microseconden en beide DST-overlapoffsets.
7. Twee gelijktijdige Spark-writers die dezelfde vrije generation lezen:
   precies een winnaar en een expliciet geblokkeerde verliezer.
8. Opzettelijke writerstop: owner blijft HELD, geen TTL-takeover.
   Eerst terminale job en Livy-session via API verifiëren, daarna
   afzonderlijke geauditeerde recovery en afsluitend owner FREE.
9. Deployment opnieuw uitvoeren met stabiele item-ID's, zonder datareplay.

Accepted jobreceipts worden voor het wachten opgeslagen. Een crash tijdens
de HTTP-submit zelf blijft een onzekerheidsgrens: geen onbewezen automatische
resubmit. Bewijsfolders en terminale recoveryparameters worden bij hervatten
hergebruikt; verschillen blokkeren hervatten.

## 4. Reproduceerbare commando's

Voer vanuit de repository uit; kies de geselecteerde lokale interpreter.

```powershell
$env:PYTHONPATH = Join-Path (Get-Location) 'src'
& .\deploy\prepare-release.ps1 -Python 'C:\Python313\python.exe'
# Gebruik hieronder het concrete planpad dat de build rapporteert.
& 'C:\Python313\python.exe' -m fabric_etl deploy-items <planpad> --journal environments\contoso.dev.items.json --timeout-seconds 2400
& 'C:\Python313\python.exe' -m fabric_etl prepare-control environments\contoso.dev.json --project projects\contoso\release.json --repository (Get-Location).Path --output dist\contoso.dev.control-parameters.json
& 'C:\Python313\python.exe' -m fabric_etl prove-control environments\contoso.dev.json --journal environments\contoso.dev.items.json --parameters dist\contoso.dev.control-parameters.json --evidence deploy\evidence\phase4-control-01
```

De developmentwheel heeft voorlopig versie `0.1.0`; bytes en planchecksums
identificeren de snapshots. Dit is geen immutable productie-releasebeleid.
De fase-3-checksums blijven historische bewijzen en worden niet vervangen
door tussentijdse fase-4-artifacts.

## 5. Acceptatiestatus

De complete control/proof-snapshot is gepubliceerd als Environment-versie
`17e118e1-9e3c-4fff-9233-eee0a1c5d215`; libraries en settings beide Success.
Wheel SHA-256:
`ef8c7568b42ce5d11231042116281ab53d53b6e980da53f3abc251a32f999c70`.
Plan-ID:
`6017cd85b1fd49983beb4c13cdaad09991155b7bc52326006ba4bf98867e200a`.

De derde foundationjob `6c1a3f59-91f1-466e-aa6c-454f42577d0f` is echt
Completed met JSON-exit `SUCCEEDED` en zichtbaarheid van alle drie rollen.
Zie [de geslaagde foundationreceipt](../deploy/evidence/phase4-visibility-03.json).

Een tweede deployment van dezelfde snapshot resulteerde voor alle vier items
in `action: verified`, dezelfde item-ID's, exportchecksums en publicatieversie.
De eerste controlpreflight overlapte deze journalverificatie en blokkeerde
veilig op een tijdelijk `verified: false`-record; er werd toen geen
controljob gestart. Deployment en controlproof moeten na elkaar plaatsvinden.
De controlrun is daarna met het volledig geverifieerde journal gestart.

Na de scalar-insertcorrectie is snapshot
`69532618c3ed8802565158b3b51948fe5ec9c17f9fdce0a3ef86f47f1334b68c`
gepubliceerd als Environment-versie `41ffdbc2-1723-4e76-ab1e-ff9bf50df538`.
Plan-ID `70bb8c51bdc8467fab6e8c36048307c6c74cb50ef93afef7e26293d201e2ab56`.
De eerste setup van [controlpoging 2](../deploy/evidence/phase4-control-02/01-setup.json),
job `37626a06-5b2b-475b-9998-06f798edd6b3`, is daadwerkelijk SUCCEEDED:

- Drie migraties APPLIED, alle twaalf tabellen met exact schema-readback.
- Volledige kandidaat geregistreerd; activatie terecht BLOCKED op 70 gates.
- Eerste proofbatch `c0dfbe8a-e5b1-5e11-a60d-80dbf22d77dd`:
  loadtijd `2026-10-10T13:27:04.422312+00:00`.
- Tweede proofbatch `cad00944-85b6-569c-bc4f-3e5ddaeda25a`:
  loadtijd `2026-10-10T13:27:04.422313+00:00`, ondanks teruggezette klok.
- Retry behoudt dezelfde batch en tijd; owner is vrijgegeven.
- Zeven hashvectors en negen getypeerde Python/Spark-vectors gelijk,
  inclusief decimal(38,18), negatieve nul en beide DST-overlapoffsets.

De tweede setup, writer-race en stopped-writer recovery blijven afzonderlijke
bewijsstappen. Deze eerste succesvolle job sluit fase 4 dus nog niet af.

| Onderdeel | Huidig bewijs |
|---|---|
| Lokale regressie | 352 tests geslaagd |
| Environment publiceren | Echte platformstatus Success, beide componenten |
| Dunne notebook export | Cellen/tags/dependencies semantisch geverifieerd |
| Foundation Spark + package | Derde job SUCCEEDED; alle drie rollen zichtbaar |
| Herhaalde itemdeployment | Alle vier verified met stabiele IDs/checksums, geen republish |
| Duurzame migraties en batchretry | Twee afzonderlijke jobs geslaagd, gelijke batchtijden; tweede migratie alleen verified |
| Type-/hashvectors | 7 hashvectors + 9 typevectors echt gelijk op Spark |
| Gelijktijdige writers en recovery | Echte race: 1 SUCCEEDED + 1 BLOCKED; writerstop, no-takeover en recovery geslaagd |
| Least privilege T-13 | Aparte service principal leest Gold-fixture via SQL en OneLake; SQL-toegang tot Control/Processing geweigerd; OneLake Control/Processing-reads en write geweigerd; uitsluitend tabelspecifieke SELECT-grant |
| Volledige ketenredeploy | Niet bewezen; valt buiten fase 4 en blijft vervolgwerk |

Geen toestemming voor bronextractie, data-reset of privileges uitbreiden
wordt uit dit document afgeleid. De fase wordt pas afgesloten na de
werkelijk vereiste bewijzen; onopgeloste gates worden expliciet gerapporteerd.

## 6. Eindbewijs en fase-4-exit

De volledige derde controlproof is **SUCCEEDED** in
[het machineleesbare acceptatieregister](../deploy/evidence/phase4-control-03/acceptance.json).
De laatste geverifieerde release heeft wheel SHA-256
`b9da29e98d1d06440b3a04515c32d71a60913ce108d50a705033156b2bab0dbb`,
plan-ID `314ef93edeff6a558608892950a42fe6b02880ec8479d1f7a99282d79e10c626`
en Environment-versie `d5bc4a93-2cae-454b-b036-ed5e51584118`.

| Bewijs | Echte job-ID | Resultaat |
|---|---|---|
| Setup | `9c98e0b4-e9d0-49b4-85e0-853dc477336a` | SUCCEEDED |
| Cross-session setupretry | `0b7658e8-067d-412d-87f6-462b5f6ace42` | SUCCEEDED; migraties verified |
| Writer A | `79ce41ae-a19e-41ae-b043-78b7eb6cc5c3` | SUCCEEDED |
| Writer B | `54ac4375-154e-435d-923f-ad07af1bf43f` | BLOCKED op dezelfde vrije generation |
| Opzettelijke writerstop | `9ea22527-7903-4e25-bda7-0b78380f37d2` | Verwacht Failed; owner blijft HELD |
| Ownerinspectie | `d6a45b95-71d3-4a83-86ae-53915b5210be` | HELD bevestigd |
| Geen TTL-takeover | `0908f4ee-79e3-4fca-90fe-2888f86a33d5` | BLOCKED |
| Expliciete recovery | `0465da50-e4d8-4adc-a75d-e62367def5e0` | SUCCEEDED na API-terminalbewijs |
| Finale ownerinspectie | `fefecc39-5aa2-42c5-8760-5a4a54896b12` | FREE, generation 7 |

Terminale oude Livy-session `1ec332e3-cd75-472e-8d38-612514bf9431`
en bijbehorende gestopte job waren beide Failed voordat recovery plaatsvond.
De laatste owner heeft geen owner-ID of job-ID meer.
Een afsluitende exacte itemredeploy behield alle vier ID's, checksums en
publicatieversie. Vervolgens is de proofcoordinator opnieuw gestart met
dezelfde bewijsfolder: alle negen receipts hergebruikt, geen jobresubmit,
hetzelfde terminalbewijs en opnieuw SUCCEEDED.

Alle 352 lokale tests slagen. De oorspronkelijke v2-runtime, seeds en
projectmanifest zijn niet aangepast; geen commits/pushes, businessreplay,
data-reset, DROP of VACUUM uitgevoerd.
De lokale Pylance-interpreter mist `delta.exceptions`; de echte race importeerde
de runtimebibliotheek succesvol. Dit verschil is geen lokaal geïnstalleerde
dependency of stil onderdrukte diagnose.

**Fase-4-exit: T-13 is bewezen met de afzonderlijke consumeridentity.** De
service principal leest uitsluitend de synthetische Gold-fixture via zowel
SQL als directe OneLake DFS-toegang. SQL-verbindingen met Control en Processing
worden geweigerd; directe OneLake-reads op beide paden en een write-probe
worden geweigerd. De SQL-grant is beperkt tot `SELECT` op de ene fixturetabel.
De tijdelijke authenticatiecredential is verwijderd. De aparte Spark-job is
niet gebruikt als tweede route: de directe OneLake-proef valideert de
OneLake-route zelf, zonder de consumer een workspace-rol te geven. Een aparte
notebookrun als service principal zou daarvoor een workspace-rol Admin,
Member of Contributor vereisen en is daarom niet verleend.

Daarmee is fase 4 afgerond binnen de vastgestelde scope van platformdeployment
en controlbewijzen. De volledige Raw-tot-Gold-keten, productie-
releasepinning, business-ingestie en DirectQuery-consumers zijn expliciet
vervolgwerk en geen open fase-4-exitcriteria.

## 7. T-13-uitvoeringsregistratie: consumeridentity aanvragen

**Datum en tijd:** 10 oktober 2026, circa 18:44 CEST  
**Status op dat moment:** de eerste poging was geblokkeerd door de
tenantcontext/rechten van die context; zie sectie 8 voor de daaropvolgende
succesvolle, tenantgerichte uitvoering.

### Aanleiding en besluit

De opdrachtgever gaf expliciet toestemming om een aparte Entra-service
principal voor de T-13-Gold-consumer aan te maken. De voorgenomen display name
is `contoso-fabric-dev-gold-consumer-t13`. Doel is uitsluitend de aparte
consumeridentiteit te kunnen gebruiken voor de positieve Gold-leestest en de
negatieve toegangstests op Raw/control via SQL en OneLake/Spark. Deze
toestemming is geen toestemming om de principal workspace-brede toegang te
geven of andere rechten te verruimen.

### Uitvoering en waarneming

1. De Azure CLI-beschikbaarheid en bestaande Azure-login zijn gecontroleerd.
   `az account show` slaagde en rapporteerde tenant-ID en subscription-ID
   `5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad`. De gerapporteerde subscription-ID
   is de CLI-context, niet bewijs van Fabric- of Entra-autorisatie.
2. De eerste controle op een bestaande appregistratie met dezelfde display name via
   `az ad app list --display-name contoso-fabric-dev-gold-consumer-t13` faalde
   met `ERROR: Insufficient privileges to complete the operation.` De actieve
   CLI-tenant was `5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad`, niet de Contoso-
   tenant. Deze mislukte lookup zegt daarom niets over een bestaande registratie
   in de Contoso-tenant.
3. Op expliciete opdracht is vervolgens geprobeerd de appregistratie aan te
   maken met `az ad app create --display-name
   contoso-fabric-dev-gold-consumer-t13 --sign-in-audience AzureADMyOrg`.
   Entra wees ook deze operatie in de actieve tenantcontext af met
   `ERROR: Insufficient privileges to complete the operation.`

### Gevolgen en validatie

- Door deze eerste CLI-poging is geen appregistratie of service principal
  aangemaakt.
- Er is geen client secret of certificaat aangemaakt of weergegeven.
- Er is geen Fabric-workspace-, Lakehouse-, SQL- of OneLake-recht toegekend
  of gewijzigd.
- Er is geen SQL- of OneLake/Spark-toegangstest gestart; T-13 blijft open.
- De app-listfout betekent dat het ontbreken van een bestaande registratie
  niet is bewezen. Een bevoegde beheerder moet eerst op naam/object-ID
  controleren voordat een nieuwe registratie wordt aangemaakt, om duplicaten
  te voorkomen.

## 8. T-13-uitvoeringsregistratie: tenantgerichte identity-provisioning

**Datum:** 10 oktober 2026.  
**Status:** Entra-appregistratie en service principal aangemaakt en
teruggelezen; authenticatiecredential, Fabric-toegang en T-13-testbewijs
blijven open.

### Aanleiding en diagnose

Na de eerste geweigerde CLI-poging gaf de opdrachtgever aan dat de handeling
met `bobstoelinga@4kjwn2.onmicrosoft.com` uitvoerbaar moest zijn. De identiteit
is daarom opnieuw gecontroleerd, met expliciete targettenant uit de dev-
binding:

- De Azure CLI-accounttoestand rapporteerde
  `userName: bobstoelinga@4kjwn2.onmicrosoft.com` en `userType: user`.
- De CLI-directorycontext was echter
  `5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad`; de projectbinding wijst naar
  Contoso-tenant `f9350d85-6f0e-42fd-8385-a59c2f09ec1a`.
- De Graph `/me`-read in de Contoso-tenant bevestigde Bob met object-ID
  `3bf7742d-180c-4de3-a7f5-71c27f12b5ed`, UPN
  `bobstoelinga@4kjwn2.onmicrosoft.com` en `userType: Member`.
- De lijst beschikbare tenantcontexten bevatte zowel de CLI-context als de
  Contoso-tenant. De eerdere `az ad app list/create`-opdrachten hadden geen
  expliciete Contoso-tenanttarget en bewezen dus geen ontbrekende bevoegdheid
  in de Contoso-tenant.

### Uitvoering

1. Met een tenantgerichte Microsoft Graph-token is uitsluitend gezocht naar
   de exacte display name `contoso-fabric-dev-gold-consumer-t13` in de
   Contoso-tenant. De query leverde geen bestaande appregistratie op.
2. De appregistratie is aangemaakt in tenant
   `f9350d85-6f0e-42fd-8385-a59c2f09ec1a`, als single-tenant app
   (`signInAudience: AzureADMyOrg`):
   - display name: `contoso-fabric-dev-gold-consumer-t13`
   - application/client ID: `05b4a703-823e-47bf-9cda-bdd35e6a03bd`
   - application object ID: `efd3474a-c62f-46bc-96f0-7ae1d27d763d`
3. De bijbehorende service principal is aangemaakt:
   - service-principal object ID: `653455eb-7bb1-4000-8330-9aeef97e10ca`
   - application/client ID: `05b4a703-823e-47bf-9cda-bdd35e6a03bd`
   - type: `Application`; account enabled: `true`.
4. Een afzonderlijke Graph-readback van appregistratie en service principal
   bevestigde de IDs, tenant, display name, audience en status. De
   appregistratie heeft nul `keyCredentials` en nul `passwordCredentials`.

### Reikwijdte, risico en resterende acties

- Er is geen client secret of certificaat aangemaakt. Er is dus nog geen
  bruikbaar client-authenticatiemiddel uitgegeven of opgeslagen.
- Er zijn geen API-permissies, workspace-rollen, Lakehouse-permissies,
  SQL-grants of OneLake-securityrollen toegekend of gewijzigd.
- De service principal krijgt geen brede workspace-memberrol als shortcut.
  De minimale consumerinterface en de door Fabric ondersteunde
  SQL-/OneLake-autorisatieroute moeten eerst expliciet worden vastgesteld.
- Er zijn nog geen positieve Gold-read- of negatieve Raw/control-tests
  uitgevoerd. T-13 en formele fase-4-exit blijven open.
- De object-ID's en client-ID zijn geen geheimen; tokens, private keys en
  secrets worden niet in Git, receipts of dit verslag opgeslagen.

**Vervolg:** kies een beheersbare workload-authenticatiemethode (bij voorkeur
federatie wanneer de daadwerkelijke runner dit ondersteunt, anders een
beheerd certificaat), sla private credentials uitsluitend op in de
goedgekeurde secret store en wijs alleen expliciet noodzakelijke Gold-
consumerrechten toe. Verifieer vervolgens de effectieve rechten afzonderlijk
via SQL en OneLake/Spark: Gold-lezen moet slagen; Raw/control en
credentialtoegang moeten geweigerd worden. Leg identiteit, item-/endpoint-
IDs, testquery's, resultaten en eventuele Fabric-beleidsbeperkingen vast.
Fase 4 wordt pas formeel afgesloten na beoordeling van dit bewijs.

## 9. T-13-uitvoeringsregistratie: Fabric-autorisatieonderzoek

**Datum:** 10 oktober 2026.  
**Status:** read-only autorisatieonderzoek geslaagd; noodzakelijke directe
Lakehouse-itemdeling en toegangstests nog niet uitgevoerd.

### Onderzoeksvraag en beveiligingsdoel

Na provisionering van de aparte service principal is onderzocht hoe die
identiteit uitsluitend consumerrechten op dev Gold kan krijgen, zonder
workspacebrede toegang en zonder toegang tot control of processing. Het
acceptatiedoel is:

1. het Gold-Lakehouse-item kunnen openen;
2. Gold-data kunnen lezen via het SQL analytics endpoint;
3. Gold-data kunnen lezen via Spark/OneLake;
4. geen toegang tot control- of processingdata krijgen;
5. geen rechten kunnen beheren, data wijzigen of toegang doorgeven.

### Officieel platformcontract geraadpleegd

De actuele Microsoft-documentatie beschrijft Lakehouse-deling als
itemgerichte toegang, zonder workspace membership of schrijfrechten.
Lakehouse-sharing heeft een basisrecht **Read**; aanvullende rechten zijn
**Read all with SQL analytics endpoint** voor TDS/SQL en
**Read all with Apache Spark** voor Spark/OneLake. De documentatie beschrijft
de toekenning en het beheer hiervan via de Fabric-portal (Manage permissions
-> Direct access), waaronder directe toegang voor serviceaccounts.
OneLake security roles zijn een aparte, fijnmazige leesbeveiliging op
folder-/tablepad; zij verlenen geen schrijfrechten en vervangen de basis-
itemdeling niet. Een Lakehouse-deling kan tot twee uur nodig hebben om door
te werken naar een al aangemelde principal.

Bronnen, geraadpleegd op 10 oktober 2026:

- [Lakehouse sharing and permission management](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sharing)
- [Share items in Microsoft Fabric: item permission model](https://learn.microsoft.com/en-us/fabric/fundamentals/share-items)
- [OneLake security roles: create and manage](https://learn.microsoft.com/en-us/fabric/onelake/security/create-manage-roles)
- [OneLake security roles, permissions, and scopes](https://learn.microsoft.com/en-us/fabric/onelake/security/data-access-control-model)
- [OneLake Data Access Security: List Data Access Roles API](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-data-access-security/list-data-access-roles)
- [OneLake Data Access Security: Create or Update Data Access Roles API](https://learn.microsoft.com/en-us/rest/api/fabric/core/onelake-data-access-security/create-or-update-data-access-roles)

### Read-only controle via Fabric REST API

Met de reeds aangemelde gebruikersidentiteit is een Fabric API-token voor
de Contoso-tenant verkregen. Alleen GET-operaties zijn uitgevoerd; er is geen
permission- of role-update verstuurd.

1. `GET /v1/workspaces/4be37544-10b9-450d-80f0-f9b3d793b83b/roleAssignments`
   gaf HTTP 200. De geretourneerde workspace role assignment was Bob
   Stoelinga (`3bf7742d-180c-4de3-a7f5-71c27f12b5ed`) als **Admin**.
   Deze response toont de workspace-role assignments van de workspace,
   niet alle mogelijke directe itemshares.
2. `GET /v1/workspaces/{workspaceId}/items/{lakehouseId}/dataAccessRoles`
   gaf voor elk van de drie gebonden Lakehouses HTTP 200:
   - control `67751097-52f3-4582-82c2-23fd0a6c89f9`;
   - processing `711abadf-4418-40f9-9599-ef602cac74b2`;
   - gold `5b8eec35-6036-4861-9e58-294add84382d`.
3. Iedere response bevatte alleen de platformrol `DefaultReader`, met
   `Permit`, `Action: Read`, `Path: *` en een `fabricItemMembers`-binding
   `itemAccess: ReadAll` voor het betreffende item. De drie rol-ID's waren
   respectievelijk `20027dc4-7ed5-4aff-8eb8-ac4242ad87fe`,
   `ccc68833-aaca-4140-9574-503ff53d9288` en
   `9950da06-d762-40b7-a8fe-09c073c7ff58`.

Deze OneLake-role responses zijn geen volledige item-sharing-inventaris en
bewijzen niet dat de consumerprincipal al dan niet directe toegang heeft.
De aparte app/service principal is niet toegevoegd aan een workspace-role
assignment. De `DefaultReader`-rol wordt niet gewijzigd: de officiële
create/update API vervangt de rolset overeenkomstig de aangeleverde payload;
zonder volledige, gevalideerde round-trip van alle bestaande rolvelden wordt
die PUT niet gebruikt.

### Geauthenticeerde portalinspectie en directe share-proef

De opdrachtgever heeft de aangemelde Fabric-browserpagina gedeeld. De
workspace `Contoso_lakehouse_ontwikkel` en het dev Gold-Lakehouse
`lh_contoso_gold` zijn in de portal geopend.

De Lakehouse Explorer toonde schemas `dbo`, `historical`, `publication` en
`release_data`. In `publication` waren drie tabellen zichtbaar:
`active_product`, `consumer_catalog` en `product_release`. Er waren op dat
moment geen tabellen zichtbaar onder `dbo`, `historical` of `release_data`;
de Explorer toonde ook geen bestanden onder Files. Dit is een UI-waarneming,
geen volledige SQL- of OneLake-inhoudsinventaris.

Het vastgestelde projectcontract beschrijft deze drie publication-tabellen
als control-/consumerpublicatiestatus, niet als businessdatamart:
`product_release` bewaart release-/input-/validatiestatus,
`active_product` de actieve en vorige gevalideerde datarelease en generatie,
en `consumer_catalog` de gepinde consumerrelease met model-/reportreferenties
en promotietoestand. Zie het
[fase-2-ontwerpcontract](13_fabric_ontwerpcontract_fase2.md).

In de Share-dialoog van `lh_contoso_gold`:

1. De zoekopdracht op `contoso-fabric-dev-gold-consumer-t13` leverde de
   appregistratie op met AppID
   `05b4a703-823e-47bf-9cda-bdd35e6a03bd`; de identity is dus vindbaar als
   service principal in de portal.
2. Het scherm legde uit dat basisdeling toegang geeft om Lakehouse en SQL-
   endpoint te openen en de default dataset te lezen; aparte checkboxen
   boden `Read all SQL endpoint data`, `Read all Apache Spark and subscribe
   to events`, `Build reports on the default semantic model` en
   `Execute Apache Spark jobs on lakehouse`.
3. De service principal is uitsluitend in de ontvangerspicker geselecteerd.
   Er is niet op **Grant** gedrukt; de dialoog is gesloten zonder wijziging.
   Er zijn dus geen nieuwe directe rechten toegekend. De actuele lijst van
   eerder verleende itemshares is via deze handeling niet uitgelezen.

### Least-privilegebesluit en blokkade

Een standaardshare met SQL `Read all` en Spark `Read all` is Lakehouse-breed.
Op basis van de zichtbare inhoud en het contract zou dat de drie
release-/consumerpointertabellen blootstellen, zonder dat er een daadwerkelijk
business-Goldobject is om de positieve consumerroute te bewijzen. Alleen
basis-`Read` bewijst evenmin Spark-dataread of de bedoelde business-SQL-
toegang. Daarom is geen brede Lakehouse-read toegekend en is ook geen
workspace-role, SQL-grant, credential of OneLake custom role gewijzigd.

OneLake Data Access Security ondersteunt afzonderlijke read-only
folder-/tablepaden, maar die rol kan de basis itemdeling niet vervangen.
Ook het SQL-grantcontract en het concrete, gepubliceerde consumerobject moeten
bekend zijn om toegang aantoonbaar tot een businessproduct te beperken. Er is
geen businessconsumerinterface in de huidige dev Gold-lakehouse bevestigd.
T-13 blijft daarom open op ontbrekende, goedgekeurde testscope; dit is nu geen
browser- of identity-provisioningblokkade.

### Veilige vervolgstap en testplan

Kies eerst één expliciet testoppervlak:

- een reeds goedgekeurd business-Goldobject zodra dit bestaat; of
- een expliciet toegestane, synthetische T-13-testfixture zonder echte
  bedrijfsgegevens, met vastgelegde tabel-/padnaam en opruim-/retentiebeleid.

Daarna kan de service principal minimaal worden gedeeld op Gold en kunnen
SQL-toegang (basis Read plus expliciete SQL `GRANT SELECT` op alleen het
testobject) en Spark/OneLake-toegang (basis Read plus een custom role met
alleen het goedgekeurde tabelpad) afzonderlijk worden getest. Controleer de
Microsoft-ondersteunde interactie van itemdeling, SQL-policy en OneLake-role
voordat een wijziging wordt gepubliceerd; voer eerst eventuele beschikbare
dry-run/read-back uit. Verleen geen Lakehouse-brede ReadAll, **Edit**,
**Share**, **Execute Apache Spark**, workspace-role of control/processing-
toegang zonder expliciete noodzaak en bewijs.

Gebruik vervolgens de goedgekeurde workload-authenticatiemethode en secret-
store; houd credentialmateriaal buiten repository en receipts. Voer positieve
SQL-SELECT en Spark/OneLake-read op het goedgekeurde Gold-testobject uit,
negatieve reads op control en processing uit en test dat writes en delen
worden geweigerd. Leg query/job-ID's, principal-ID, grants/roledefinities,
uitkomsten, propagatiewachttijd en eventuele platformbeperkingen vast.

**Exitbesluit:** T-13 blijft open totdat het testoppervlak expliciet is
goedgekeurd, de effectieve least-privilege-rechten read-back zijn gecontroleerd
en positieve Gold plus negatieve control/processing-proeven via SQL én
Spark/OneLake met de service principal zijn geslaagd.

### Uitvoeringsupdate 11 oktober 2026: fixture, rol, itemtoegang en OneLake-proef

De opdrachtgever heeft expliciet ingestemd met een synthetische testfixture.
In `lh_contoso_gold` is daarom `publication.t13_gold_read_test` aangemaakt
met uitsluitend het record `T13_SYNTHETIC_FIXTURE` / `fixture_id = 1`.
Een afzonderlijke Spark SQL-SELECT bevestigde dat ene record. Er zijn geen
bedrijfsgegevens gebruikt.

In OneLake Security is rol `T13GoldTestRead` aangemaakt voor de T-13-service
principal (role-ID `42373883-c2c4-4bc2-8ce2-e4a4baff5fa0`). De Fabric
REST-readback van
`GET /v1/workspaces/4be37544-10b9-450d-80f0-f9b3d793b83b/items/5b8eec35-6036-4861-9e58-294add84382d/dataAccessRoles`
bevestigde exact:

- `Permit`, actie `Read`, pad
  `/Tables/publication/t13_gold_read_test`;
- Entra-member object-ID
  `653455eb-7bb1-4000-8330-9aeef97e10ca`;
- geen `ReadWrite` of ander tabel-/bestandspad in deze rol.

Dezelfde readback toont ook de platformrol `DefaultReader`: die verleent
`Read` op pad `*` aan identiteiten met itemtoegang `ReadAll`. Dat is een
belangrijke grens: een Lakehouse-brede `Read all with Apache Spark`-grant zou
deze consumer niet beperken tot de custom rol. Die grant is daarom niet
gegeven. OneLake-rollen vervangen bovendien de vereiste basis-itemtoegang
niet.

De eerste Gold Lakehouse Share-poging blokkeerde **Grant** met **“ReShare
permission required to do the share.”** Daarna is via **Manage permissions >
Add user** de minimale basis-itemtoegang verleend. De Direct access-readback
bevestigde de T-13-principal met uitsluitend `Read` op Gold. Er zijn geen
rechten toegekend op Control of Processing, geen workspace membership,
`Read all SQL endpoint data`, `Read all Apache Spark` of **Execute Apache
Spark**.

Een volgende niet-exporteerbare, kortlevende certificaatcredential is
toegevoegd aan de bestaande appregistratie. De geregistreerde certificaat-
thumbprint is teruggelezen en kwam exact overeen met het lokale certificaat.
Na een korte propagatiewachttijd slaagde client-credentials-authenticatie voor
de Storage-audience; de resulterende bearer token is niet opgeslagen in
repository of bewijsbestanden. Met die service-principal-token zijn de
volgende OneLake DFS-proeven uitgevoerd:

| Proef | Resultaat | Betekenis |
|---|---|---|
| Lijst van `Gold/Tables/publication/t13_gold_read_test` | HTTP 200 | Gold-fixturepad is leesbaar |
| GET van fixture-Delta-log `.../_delta_log/00000000000000000000.json` | HTTP 200 | Fixturebestand is leesbaar met de consumeridentity |
| Lijst van `Control/Tables` | HTTP 403 | Control-pad niet leesbaar |
| Lijst van `Processing/Tables` | HTTP 403 | Processing-pad niet leesbaar |
| PUT naar uniek, tijdelijk Gold-`Files`-pad | HTTP 403 | Schrijven naar Gold geweigerd; er is geen bestand aangemaakt |

De lijstproeven gebruikten de OneLake DFS `resource=filesystem`-route met het
workspace-GUID en item-GUID in het directory-pad. Het eerdere foutieve
directory-URL-formaat is niet als autorisatiebewijs gebruikt. De tijdelijke
appregistratiecredential is na de proeven verwijderd; ook het lokale
certificaat/private key is verwijderd. Het was een apart kortlevend
certificaat na de eerdere mislukte registratiepoging; er is geen credential
achtergebleven.

De positieve en negatieve resultaten hierboven gelden voor directe OneLake
DFS-bestands-/padtoegang met de service principal. Ze bewijzen niet dat SQL-
endpointtoegang of een Spark-job met deze identity werkt. Een read-only SQL-
query is geprobeerd met een kortlevend client-credentials-token; DNS-resolutie
en TCP op poort 1433 slaagden, maar de bestaande .NET SQL-client liep vast op
de pre-login-handshake naar de routeringsbestemming. Er is dus geen SQL-login,
query of autorisatie-uitkomst vastgesteld. SQL-`GRANT SELECT` is niet
uitgevoerd. Delen/share-weigering vanuit de consumeridentity is evenmin
getest. De Lakehouse-brede item-`Read`-grant is zichtbaar; de geteste
OneLake-paden zijn desondanks effectief begrensd zoals hierboven waargenomen.

#### SQL-consumerproef 11 oktober 2026

De eerste SQL-poging met de ingebouwde .NET SQL-client liep vast tijdens de
pre-login-handshake; dat was geen autorisatiebewijs. De test is herhaald met
Microsoft.Data.SqlClient 6.1.3. De service principal kon daarmee inloggen op
Gold. Aanvankelijk was de fixture niet SQL-zichtbaar; na een expliciete,
tabelspecifieke `GRANT SELECT` kon de consumer exact één rij lezen:
`T13_SYNTHETIC_FIXTURE` / `fixture_id = 1`. SQL-permission-readback bevestigde
`GRANT SELECT` voor `contoso-fabric-dev-gold-consumer-t13` op uitsluitend
`publication.t13_gold_read_test`.

Dezelfde client-credentials-identiteit kreeg SQL-fout 18456 bij het openen
van zowel het Control- als Processing-Lakehouse. Dat bewijst dat de consumer
geen SQL-endpointtoegang tot die databases heeft. Een SQL-INSERT-probe op de
fixture retourneerde fout 24559 omdat DML op dit Lakehouse-tabeltype niet
wordt ondersteund; dat is geen SQL-permission-denial. De effectieve
write-weigering is afzonderlijk bewezen via de OneLake DFS PUT-probe (HTTP
403). De mislukte INSERT is in een transactie geprobeerd; de connectie is
afgesloten en er is geen fixturedata gewijzigd.

Een read-only evaluatie met `HAS_PERMS_BY_NAME` onder de service-principal-
identiteit bevestigde op de Gold-fixture `SELECT = 1` en `INSERT`, `UPDATE`,
`DELETE` en `ALTER` elk `= 0`. Dit bevestigt effectief niet-schrijfbare SQL-
objectrechten onafhankelijk van de endpointbeperking op DML. De Direct
access-readback voor het Lakehouse-item toont alleen basis-`Read`; er is geen
`Reshare`- of workspace-role-toekenning aan de consumer.

**Exitbesluit:** fase 4 is afgerond. T-13 is bewezen voor de afgesproken
SQL- en OneLake-routes: Gold lezen lukt via beide; Control en Processing zijn
via SQL en OneLake ontoegankelijk; schrijven via OneLake wordt geweigerd.
Geen brede `ReadAll`, workspace-rol of **Execute Apache Spark** is verleend.
De Direct Lakehouse DFS-proef is de OneLake-consumerproef; een aparte Spark-
notebookrun is niet vereist voor deze exit en zou een bredere workspace-rol
voor de service principal vereisen.

## 10. Bestaande bron- en tabelmetadata voor de ETL-onboarding

Bij het hervatten van fase 4 is de repository gecontroleerd op reeds
beschikbare bron-, schema-, mapping- en doeltabelmetadata. Die metadata is
aanwezig; de bron-/ETL-scope hoeft dus niet opnieuw uit alleen mondelinge
specificaties te worden opgebouwd. De metadata beschrijft echter contracten
en transformaties, niet de aanwezigheid of volledigheid van de originele
bronbestanden en evenmin een reeds gedeployde Fabric-ETL-keten.

### Gevonden metadata en implementatiebestanden

- `projects/contoso/release.json` registreert vijf bronsystemen en negen
  bronobjecten, connectorinstellingen, load/dependency-volgorde en de
  Data Vault-, business-vault- en Gold-entitycontracten.
- `metadata/seed/meta_source_system.json` beschrijft onder meer SALES als
  `Contoso Sales (ERP export)` met dagelijkse levering, en CBS, ECB, NAGER en
  FABRIC_SALES als afzonderlijke bronsystemen. De ingestelde
  `landing_volume_path`-waarden zijn Databricks Volume-paden, geen bewijs van
  bestaande Fabric- of Blob-opslag.
- `metadata/seed/meta_source_object.json` bevat de vijf SALES-objecten met
  Parquet-patronen, business keys, change-trackingvelden,
  snapshot/SCD2-loadstrategie, soft-delete-semantiek, freshness en
  schema-driftbeleid. `projects/contoso/release.json` registreert de
  bijbehorende SALES `FILE_MANIFEST`-connectors. De seed
  `metadata/seed/meta_source_connector.json` bevat connectorgegevens voor
  CBS, ECB, NAGER en FABRIC_SALES; de objecten/connectoren sluiten dus niet
  allemaal aan op een uniforme seedstructuur.
- `metadata/seed/meta_mapping.json` bevat kolom- en datatypecontracten,
  business-key/nullability-indicaties en bronexpressies naar Quality-
  doelkolommen. `meta_quality_rule.json` bevat aanvullende kwaliteitsregels.
- `metadata/seed/meta_dv_entity.json`, `meta_dv_mapping.json` en de
  `vault_entities`/`business_entities` in `release.json` leggen onder meer
  Raw Vault hubs, links, satellites en business-vault-afleidingen vast,
  inclusief hash-/business keys, record source en laadvolgorde.
- `metadata/seed/meta_gold_entity.json` en `gold_entities` in `release.json`
  specificeren Gold-doelschema's/-tabellen, SELECT-expressies,
  SCD-/mergegedrag, sleutels, clustering/partitionering en afhankelijkheden.
  Voorbeelden van SQL-transformaties staan in `projects/contoso/sql/`,
  waaronder `GH_FCT_SALES.sql` en `SOURCE_FABRIC_SALES.sql`.
- `sql/migrations/fabric/002_control_contract.sql` bevat de Fabric Delta-DDL
  voor delivery/batch/work-item/audit/commit-control en Gold
  publication-tabellen. Dit is platform/control-DDL en geen DDL of bewijs
  dat de business-bron-, Raw-, Vault- en Gold-tabellen al zijn aangemaakt.

### Broninterpretatie en bewijsgrens

De scopefixture bevestigt de negen objecten
`SALES.CUSTOMERS`, `SALES.PRODUCTS`, `SALES.ORDERS`, `SALES.RETURNS`,
`SALES.EMPLOYEES`, `CBS.JEUGDZORG_WIJK_2025`, `ECB.EXCHANGE_RATE`,
`NAGER.HOLIDAYS_NL` en `FABRIC_SALES.ORDER_LINES`; SHAREPOINT is expliciet
uitgesloten. De metadata ondersteunt daarmee een gemengde bronarchitectuur:

- SALES is de interne Contoso ERP-exportroute met vijf Parquet-
  bronobjecten en `FILE_MANIFEST`-connectors.
- CBS, ECB en NAGER zijn afzonderlijke externe HTTP/API-bronnen met JSON- of
  CSV-responscontracten.
- FABRIC_SALES is een aparte JDBC-extractie uit
  `SalesLT.vw_databricks_sales_order_line`; de secret scope/connection en
  werkende netwerk-/authenticatiebereikbaarheid moeten runtime worden
  bevestigd.

De repositorymetadata specificeert de SALES bestandsnamen en verwachte
bronkolommen, maar er zijn geen oorspronkelijke Contoso SALES-Parquet-
leveringen aangetroffen in de onderzochte projectfixtures/sample-data.
Fixtures bevatten metadata/contracten en zijn geen vervanging voor die
oorspronkelijke data. Er is ook geen aparte Azure Blob Storage-account of
Fabric-shortcut aangemaakt. De eerder voorgestelde Blob/ADLS Gen2-opslag
blijft een nog te besluiten landings-/Raw-ontwerp; Fabric-doelpaden,
identiteit, retentie, immutability en eigenaarschap moeten expliciet worden
vastgelegd voordat provisioning of writes plaatsvinden.

### Gevolg voor het vervolg

Gebruik de bestaande seeds en project-SQL als implementatiebaseline en
valideer die eerst tegen de gewenste Fabric-doelbindingen. Stel daarna per
bron vast of de echte levering/API-/JDBC-route beschikbaar en reproduceerbaar
is, inclusief bronhouder, bereik, schema-/kolomcontract, extractmoment en
toegestane toegang. Bouw geen synthetische SALES-data als
`Contoso-brondata`; eventueel synthetisch materiaal moet expliciet als
afzonderlijke testfixture zijn gemarkeerd. Pas nadat bron- en Raw-opslagkeuzes
zijn goedgekeurd, kan de ingestie voor de negen objecten worden uitgevoerd
met gesloten manifests, fingerprints en volledigheidsbewijs volgens het
fase-2-contract.

## 11. Read-only controle: Azure Blob Storage-provisioning onder Bob

**Controle:** 10 oktober 2026, op verzoek van de opdrachtgever. Er zijn
uitsluitend Azure CLI-account-, tenant-, subscription-, provider- en RBAC-
readcontroles uitgevoerd. Er is geen storage account, resource group,
container, roltoewijzing of andere Azure-resource aangemaakt of gewijzigd.

### Waargenomen account- en subscriptioncontext

- `az account list --all` toont `bobstoelinga@4kjwn2.onmicrosoft.com` als
  tenant-level account in Contoso-tenant
  `f9350d85-6f0e-42fd-8385-a59c2f09ec1a`; de subscriptionnaam is
  `N/A(tenant level account)`. Dit is geen Azure-subscription waarop een
  storage account kan worden gedeployed.
- Dezelfde lijst toont eveneens `N/A(tenant level account)` voor de Bob-
  accountcontext in tenant `5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad`.
- Er is één subscription zichtbaar: `Azure subscription 1`,
  subscription-ID `9fe6cc02-a83b-4130-8d57-5e8247ce2565`, gekoppeld aan
  tenant `5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad`, niet aan de Contoso-tenant.
  De accountlijst associeert deze subscription met
  `stoelingabob@gmail.com`.
- `az ad signed-in-user show` retourneerde een gast-/B2B-weergave van het
  aangemelde object (`bobstoelinga_4kjwn2.onmicrosoft.com#EXT#...`), object-ID
  `4c446ffb-edd6-448e-a678-e605e7238fae`. Daarom is de accountlijstcontext
  niet gelijk aan bewijs dat de Contoso-tenant een eigen subscription heeft.
- `az account tenant list` bevestigde dat beide tenant-ID's in de CLI-context
  voorkomen. Dit voegt op zichzelf geen Azure-subscription of deploymentrecht
  toe aan de Contoso-tenant.

### Toegangssignalen op de zichtbare subscription

- Een read-only subscription-scope RBAC-query voor het aangemelde object
  retourneerde geen directe roltoewijzingen. De CLI waarschuwde dat Graph het
  object niet kon opzoeken en behandelde de opgegeven GUID als object-ID;
  daarom bewijst deze lege uitkomst niet dat er geen groepsgeërfde of andere
  effectieve rechten bestaan.
- `az storage account list` op de zichtbare subscription retourneerde geen
  storage accounts. Dit is geen bewijs over een eventuele andere subscription
  in de Contoso-tenant.
- `Microsoft.Storage` rapporteerde `Registered` in de zichtbare subscription.
  Dit providerresultaat geldt alleen voor die subscription en verhelpt het
  ontbreken van een subscription in de Contoso-tenant niet.

### Beoordeling en vervolgstap

**Conclusie:** op basis van de huidige Azure-accountcontext is het niet
mogelijk een Blob Storage-account in de Contoso-tenant te provisionen:
`bobstoelinga@4kjwn2.onmicrosoft.com` heeft daar volgens de CLI alleen een
tenant-level account en geen zichtbare subscription. De aparte subscription
staat in een andere tenant; er is geen bevestiging dat Bob daar een effectieve
`Contributor`-/`Owner`-toewijzing heeft of dat die subscription de bedoelde
tenant-/billing-/governancegrens voor deze Raw-opslag is. Een Blob-account is
daarom niet aangemaakt.

Voordat provisioning kan worden overwogen, moet een Azure-subscription in de
goedgekeurde Contoso-tenant aan deze identity beschikbaar zijn, of moet de
opdrachtgever expliciet bevestigen dat de andere subscription de juiste
doelgrens is. Daarnaast moet ten minste een passende `Contributor`-toegang
op de gekozen resource group/subscription aantoonbaar zijn; provider-
registratie, regio, naamgeving, netwerk, retentie/immutability, kostenplaats
en opslagbeveiliging moeten voor diezelfde subscription worden gevalideerd.
De bestaande gegevens zijn uitsluitend een read-only haalbaarheidscontrole,
geen autorisatie om een resource te maken.

## 12. Externe dev-Raw-opslag daadwerkelijk ingericht in North Europe

**Datum:** 10 oktober 2026. **Nieuw mandaat:** de opdrachtgever koos expliciet
de subscription onder `stoelingabob@gmail.com` voor opslag, met dezelfde regio
als de Fabric-workspace. Daarna zijn ADLS Gen2, Standard LRS, private
containers, Entra-only authenticatie, zeven dagen soft delete en geen
automatische verwijdering of locked WORM-retentie expliciet goedgekeurd.
Deze registratie volgt op de historische read-only controle uit sectie 11.

### Regio en werkelijke deploymentidentity

De Fabric REST-workspacecontrole retourneerde capacity-ID
`103e56d3-ccdb-42d8-a56d-ff3d5a6c92f8`. De capacitylijst rapporteerde deze
actieve FTL64-trialcapacity in **North Europe**. Azure-regio
`northeurope` is daarom gekozen; het eerdere voorstel West Europe is niet
uitgevoerd. Een toekomstige workspace-capacityverplaatsing verplaatst dit
Storage Account niet automatisch.

De subscriptiongerichte managementtoken bevatte tenant
`5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad`, identity
`live.com#stoelingabob@gmail.com` en object-ID
`03667848-cd1f-48b4-bf68-cba53d7eeafc`. Dit is niet het Bob-gastobject uit
sectie 11. De effectieve subscription-permissions-API retourneerde
management actions `*` zonder `notActions`; de subscriptiongerichte
deployment was dus niet gebaseerd op de eerder lege gastobject-RBAC-query.
Data actions waren leeg: managementtoegang alleen gaf geen blob-datarechten.
De voorafgaande resource-group- en storage-accountlijsten waren leeg.

### Aangemaakte resources en instellingen

| Onderdeel | Gerealiseerd en teruggelezen |
|---|---|
| Subscription | `9fe6cc02-a83b-4130-8d57-5e8247ce2565` |
| Azure-tenant | `5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad` |
| Resource group | `rg-contoso-raw-dev-neu`, North Europe |
| Storage Account | `stcontosorawdev9fe6cc02`, provisioning `Succeeded` |
| Type / redundantie | StorageV2, ADLS Gen2 hierarchical namespace, Standard LRS |
| Blobendpoint | `https://stcontosorawdev9fe6cc02.blob.core.windows.net/` |
| DFS-endpoint | `https://stcontosorawdev9fe6cc02.dfs.core.windows.net/` |
| Containers | `raw`, `test-fixtures`; beide `publicAccess=None` |
| Transport | HTTPS verplicht, minimaal TLS 1.2 |
| Authenticatie | Entra; shared-key access uitgeschakeld |
| Anonieme blobtoegang | Uitgeschakeld |
| Netwerk | Public network access aan; geen private endpoint/firewallbeperking ingericht |
| Soft delete | Blobs en containers: enabled, zeven dagen |
| Lifecycle / WORM | Geen automatische dataverwijdering of locked WORM-policy ingericht |

Private containers betekenen hier **geen anonieme dataread**, niet een
privaat netwerkendpoint. De internetbereikbare endpoints vereisen
geautoriseerde Entra-authenticatie. Standard LRS is de goedgekeurde
dev-configuratie, geen geo-redundante productievoorziening.

De deploymentuser kreeg `Storage Blob Data Contributor`, uitsluitend op de
nieuwe storage-accountscope. Role-assignment-ID:
`18a8cc3d-abe0-4aaa-87a8-54eb7c540ce9`; role-definition-ID:
`ba92f5b4-2d11-453d-a403-e96b0029c9fe`. Dit is voor dev-provisioning en
fixturedata; geen Fabric-consumergrant. Geen accountsleutel, client secret,
SAS of cross-tenant Fabric-credential is aangemaakt of opgeslagen.

### Folderinrichting en leveringscontract

Beide containers bevatten daadwerkelijk aangemaakte ADLS-directories:

```text
raw/
  SALES/
  CBS/
  ECB/
  NAGER/
  FABRIC_SALES/
test-fixtures/
  SALES/
  CBS/
  ECB/
  NAGER/
  FABRIC_SALES/
```

De vervolgstructuur per echte of synthetische levering blijft
`<source_system>/<delivery_id>/<source_object>/`. Voor SALES betreft dit de
objecten CUSTOMERS, PRODUCTS, ORDERS, RETURNS en EMPLOYEES. Een deliveryfolder
wordt pas met een concrete delivery-ID aangemaakt; lege placeholder-
leveringen worden niet als complete snapshots geregistreerd.

Synthetische, metadata-conforme SALES-testdata komt eerst in
`test-fixtures`, met expliciete fixtureherkomst. Alleen een gevalideerde,
bewust gepubliceerde testlevering kan vervolgens via de Raw-ingestieroute
worden aangeboden; die blijft als synthetisch herkenbaar. Externe API-data
en JDBC-extracten houden hun eigen werkelijke bronprovenance.

Raw-immutability blijft een applicatie-/leveringscontract: geen overschrijven
van gesloten leveringen; correcties krijgen een nieuwe delivery-ID. Soft
delete is geen immutable/WORM-beveiliging. De dev-Contributor kan technisch
wijzigen/verwijderen; ingestie-identiteiten, permissions en manifestcontrole
moeten dit in het vervolg afdwingen. Er is geen locked WORM-policy
voorgewend.

### Verificatie en persistente registratie

- Resource group en Storage Account rapporteerden `Succeeded`.
- Accountinstellingen zijn read-back gecontroleerd tegen regio, SKU,
  HNS, HTTPS, TLS en verboden shared-key/anonieme blobtoegang.
- Retentie is via de Blob Service management-API teruggelezen als zeven dagen
  voor blobs en containers.
- Beide containers zijn teruggelezen als `publicAccess=None`.
- Alle tien source-directories zijn met storage-scoped Entra-token via DFS
  aangemaakt (HTTP 201) en daarna via recursive filesystem listing
  teruggelezen. De listings bevatten uitsluitend vijf directories per
  container en **nul datafiles**.
- Configuratie staat in
  [de afzonderlijke Azure Raw dev-binding](../environments/contoso.raw.dev.json).
  Deze is niet stil in de Fabric-runtimebinding opgenomen.
- De machineleesbare read-back staat in
  [het provisioningbewijs](../deploy/evidence/raw-storage-dev-20261010.json),
  inclusief resources, settings, principal, role-assignment, folders en
  verificatietijd. Tokens en credentialmateriaal zijn niet geregistreerd.

Een voorbereidende `az storage account list-skus`-probe werd door de
geinstalleerde CLI niet ondersteund; dat is geen deploymentbewijs. De
daadwerkelijke creatie en account-read-back bevestigden de gekozen regio/SKU.

**Actuele status:** externe dev-opslag en bronsysteemfolders werken. Er is
nog geen data gegenereerd/geupload, geen gesloten deliverymanifest,
cross-tenant Fabric-connection of shortcut, en geen nieuwe ETL-/T-13-proef.
Het volgende werk is een deterministische SALES-generator op basis van de
gepinde projectmetadata met schema-/relatie-/scenarioverificatie, gevolgd door
expliciete fixturepublicatie en een afzonderlijk goedgekeurde cross-tenant
ingestietoegang. Dit sluit fase 4 of de gehele Raw-tot-Gold-keten niet af.

## 13. Bevestigde bronverdeling: alleen SALES wordt gegenereerd

**Datum:** 10 oktober 2026. De opdrachtgever bevestigde na de opslagaanmaak
de volgende bronverdeling. Dit is de actuele uitvoeringsafspraak; de vijf
bronsystemen en negen objecten blijven ongewijzigd.

| Bronsysteem | Herkomst en verkrijging | Objecten | Genereren als vervanging? |
|---|---|---|---|
| SALES | Door de assistent gegenereerde, metadata-conforme synthetische testdata; Parquet via FILE_MANIFEST | CUSTOMERS, PRODUCTS, ORDERS, RETURNS, EMPLOYEES | Ja, uitsluitend deze SALES-dev-bron |
| CBS | Echte externe CBS StatLine OData API; JSON met paginering | JEUGDZORG_WIJK_2025 | Nee |
| ECB | Echte externe ECB Data API; CSV | EXCHANGE_RATE | Nee |
| NAGER | Echte externe Nager.Date API; JSON | HOLIDAYS_NL | Nee |
| FABRIC_SALES | Extract uit de bestaande SQL-database in Fabric via de project-source-query | ORDER_LINES | Nee |

De concrete API-endpoints en response-instellingen staan in
[de gepinde projectmetadata](../projects/contoso/release.json).
De huidige query in
[SOURCE_FABRIC_SALES.sql](../projects/contoso/sql/SOURCE_FABRIC_SALES.sql)
is `SELECT * FROM SalesLT.vw_databricks_sales_order_line`. Het historische
`databricks`-deel van de viewnaam verandert de bron niet: FABRIC_SALES is
volgens de bevestigde afspraak een SQL-database in Fabric. Bereikbaarheid,
toegangsrechten, het bestaan van de view en het werkelijke response-schema
zijn nog niet runtime geverifieerd.

De generator richt zich alleen op de vijf SALES-objecten, met reproduceerbare
business keys en consistente relaties tussen klanten, producten, medewerkers,
orders/orderregels en returns. De output is expliciet synthetisch en wordt
eerst onder `test-fixtures/SALES/<delivery_id>/<source_object>/` geplaatst.
Deze opdracht vervangt voor de SALES-dev-bron de eerdere noodzaak om originele
Contoso-Parquetexports te verkrijgen; zij geeft geen toestemming om
gegenereerde records als oorspronkelijke Contoso-exports te presenteren.

CBS, ECB, NAGER en FABRIC_SALES leveren hun echte response-/extractdata via
hun eigen Raw-route onder
`raw/<source_system>/<delivery_id>/<source_object>/`, met extractparameters,
extractmoment, fingerprints en completeness-bewijs. Een mislukte of
onbereikbare bron wordt expliciet gerapporteerd; geen stille vervanging door
synthetische data en geen lege complete snapshot als fallback.

De reeds aangemaakte lege folders CBS, ECB, NAGER en FABRIC_SALES in
`test-fixtures` zijn uitsluitend lege provisioningstructuur. Ze betekenen
niet dat voor die bronnen data wordt gegenereerd en worden niet gevuld als
onderdeel van de SALES-generator. Er zijn tijdens deze verduidelijking geen
Azure-resources gewijzigd, externe responses opgehaald, SQL-extracten
uitgevoerd of datafiles gegenereerd.
