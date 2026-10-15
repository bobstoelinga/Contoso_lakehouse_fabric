# Fabric ETL-template: fase 2 architectuur en contracten

Datum: 10 oktober 2026. Status: fase 2 afgerond; architectuurversie 1.0.
Het [normatieve ontwerpcontract en de fase-2-exit](13_fabric_ontwerpcontract_fase2.md)
sluiten de hieronder bewaarde eerdere voorstellen. Geen implementatie-
of productiecertificering van de volledige keten.
Zie het [migratieverslag](09_fabric_migratieverslag.md) en de
[fase-1-acceptatiebasis](10_fabric_baseline_acceptatie.md).

## 1. Doel, bestaande besluiten en grenzen

Een productiegeschikt, herbruikbaar metadata-gedreven Fabric-template:
Raw/Landing tot Gold Current, met Quality/Reject, reconciliatie, Vault,
referentiedata, audit en operationeel herstel.

Bevestigde uitgangspunten:

- Fabric als eerste ondersteund uitvoerplatform.
- Contoso als voorbeeldproject; SALES, CBS, ECB, NAGER en FABRIC_SALES.
- Drie schema-enabled Lakehouses: control, processing en gold.
- Geen verplichte gelijkheid met de verdwenen Databricks-omgeving.
- Reproduceerbare uitrol vanuit VS Code en commandogestuurde feedback.

Dit document werkt contracten uit. Aanvankelijk waren de vervolgkeuzes
voorstellen. Opdrachtgever heeft inmiddels expliciet opdracht gegeven door
te gaan tot het einde (B-FAB-018). Besluiten en bewijs worden afzonderlijk
vastgelegd; open implementatie-/acceptatiegates blijven open.

Niet inbegrepen: generieke Databricks-adapters, nieuwe SharePoint-ontsluiting,
automatisch repareren van businessdata of volledig autonome productiecode-
deployment op basis van alerts.

## 2. Templategrenzen en projectstructuur

### Vastgestelde scheiding

| Component | Verantwoordelijkheid | Mag Contoso-specifiek zijn? |
|---|---|---|
| Framework | Validatie, planning, context, ingestiepatronen, DQ, audit, laad-/publicatiepatronen | Nee |
| Projectpakket | Bronnen, schemas, mappings, DQ, DV-/Gold-modellen, business-SQL | Ja |
| Omgevingsbinding | Workspace/item-ID's, lakehouses, connections, policies | Ja, per omgeving |
| Platformadapter | Fabric filesystem, runparameters, job-/deployment-API en namespace | Fabric-specifiek, niet bron-specifiek |
| Deployment | Gevalideerde itemdefinities, package, setup en migraties | Generieke runner met projectconfiguratie |
| Tests | Contract-/unit-/integratietests plus projectfixtures | Kern generiek; fixtures per project |

Doelstructuur; de kern en omgevingsbinding bestaan al, overige onderdelen
worden in de implementatiefasen gemaakt:

```text
src/fabric_etl/                 generieke kern en Fabric-adapter
projects/contoso/metadata/      projectconfiguratie
projects/contoso/sql/           versieerbare businessprojecties
environments/                  niet-geheime doelbindingen
fabric/items/                  ondersteunde itemdefinities
deploy/                        validatie, publicatie, setup, runfeedback
sql/migrations/                versieerbare platform/control-migraties
tests/                         unit-, contract- en integratietests
docs/                          ontwerp, besluiten en beheer
```

**Waarom:** geen kopie van de loader per bron of project; alleen configuratie
wijzigt binnen ondersteunde patronen. Een nieuw connectortype blijft een
geteste frameworkuitbreiding.
**Alternatief:** bestaande Contoso-package direct omnoemen. Dat is sneller,
maar verspreidt defaults en importwijzigingen. Advies: gecontroleerd nieuwe
templatekern ontwikkelen en bruikbare helpers met regressietests overnemen;
de oorspronkelijke baseline niet in een enkele bulkrename herschrijven.
Distributiebesluit: versiegepinde wheel in Fabric Environment; bestaande
repository blijft ontwikkelbasis. Packaging en itemdefinities volgen in fase 3.

## 3. Fysieke lagen en namespaces

| Lakehouse | Voorgestelde schemas / Files | Logische functie |
|---|---|---|
| lh_contoso_control | metadata, audit, control | Configuratiereleases, runcontext, work-items, audit |
| lh_contoso_processing | Files/raw, Files/staging; bronze, quality, reject, raw_vault, business_vault | Bronleveringen en verwerking |
| lh_contoso_gold | historical, release_data, publication, current | Historie, kandidaten, releasebeheer en consumercontract |

Andere projecten krijgen eigen bindingswaarden, niet dezelfde Contoso-
tabellen. Omgevingsscheiding bij voorkeur via afzonderlijke workspaces.
De huidige ontwikkelworkspace is de bekende dev-binding; tst/prd zijn nog
niet ingericht.

### Naamgevingscontract

Metadata gebruikt een logisch doel:
`lakehouse_role + schema + table`. De resolver bepaalt de fysieke verwijzing.

- Rollen zijn control, processing en gold; geen impliciete Contoso-default.
- Workspace- en item-ID's komen uit de doelconfiguratie.
- Spark-namespace kan `workspace.lakehouse.schema.table` zijn; platform-
  identifiers worden gevalideerd en per segment correct gequote.
- Een GUID voor API-aanroepen is niet automatisch een Spark-catalogusnaam.
- SQL-endpointnamen en T-SQL-objecten hebben een apart resolvercontract.
- Geen globale stringvervanging op willekeurige business-SQL. Alleen
  geregistreerde placeholderbindings worden gerenderd; restanten falen.

De oude identifierhelper veronderstelt UC-namen en mag niet ongecontroleerd
worden hergebruikt voor Fabric-workspacenamen.
Cross-lakehouse-read/write en rechten worden in fase 4 daadwerkelijk getest.

**Beveiligingsgrens:** drie Lakehouses bieden logische/fysieke scheiding,
maar geen automatische least-privilege. SQL-rechten alleen beschermen geen
Spark-/OneLake-toegang. Geen brede workspace-memberrol voor BI-consumenten
als alternatief voor gecontroleerde Gold-toegang.

## 4. Metadata- en releasecontract

Een projectrelease bevat:

- project_id, metadata_contract_version, framework_version en releasefingerprint;
- source_systems, source_objects en connectorverwijzingen;
- schema-/mappingcontracten met kolomtype, nullable, ordinal en sleutelrol;
- DQ-regels met severity, scope, threshold en actie;
- dependencies en ondersteunde verwerkingsroute;
- DV-entiteiten/mappings en Gold-product-/publicatiegroepen;
- governance, onderhoud en retry-/timeoutpolicies.

Nieuwe validatie moet weigeren:

- Dubbele JSON-velden, dubbele keys en ontbrekende verwijzingen.
- Cyclische afhankelijkheden, niet-ondersteunde laadpatronen of connectoren.
- Onopgeloste bindings, ontbrekende verplichte mapping en ongeldige typen.
- Gewijzigde sleutel-/hash-/schema-contracten zonder expliciet migratiepad.
- Secrets in configuratie; alleen connection-/secretreferenties toegestaan.

**Releaseconsistentie:** metadatarecords krijgen een release-ID. Deployment
schrijft een kandidaatrelease, valideert alle onderdelen en activeert de
release pas na succes. Een run pinnt het gekozen release-ID; hij leest niet
halverwege een nieuwere "is_active"-configuratie.
Dit voorkomt een gedeeltelijke metadatarelease tijdens meerdere MERGEs.
Breaking changes vereisen een nieuwe contractversie en gecontroleerde migratie.

## 5. Raw, connectoren en Bronze

### Raw/Landing

Voorgesteld pad:
`Files/raw/<source_system>/<delivery_id>/<source_object>/`.
Delivery-ID's voor paden worden veilig gecodeerd; geen willekeurige userinput
als padsegment.

Een gesloten manifest bevat minimaal:

- manifest_contract_version, project_id, source_system_id, delivery_id;
- delivery_sequence, bronpeildatum/watermark en schema_contract_version;
- verplichte object-ID's en per object exacte bestandslijst;
- grootte en inhoudsfingerprint per bestand; recordcount indien betrouwbaar;
- expliciete snapshot-completenessverklaring indien deletes daarop steunen;
- status CLOSED en publicatietijd.

Een nul-recordobject kan geldig zijn wanneer het expliciet in het manifest
als volledige lege levering staat. Het ontbreken van een bestand bewijst
nooit een lege snapshot.

Extract schrijft eerst staging; de consument accepteert pas alle gevalideerde
bestanden plus het als laatste gepubliceerde gesloten manifest. Geen aanname
dat een filesystem-move in OneLake een atomische directorytransactie is.
Een gesloten levering is immutable. Correcties krijgen een nieuwe versie/
levering en een expliciete relatie met de oorspronkelijke levering.

### Connectoren

| Type | Contract |
|---|---|
| Bestandslevering | Producer levert bestanden plus gesloten manifest; registratie voor incomplete deliveries ondersteunt de chronologiegate |
| HTTP JSON | Begrensde paginering, lusdetectie, timeout/retry, bronprovenance; originele responsbytes/pagina's in Raw bewaren waar toegestaan |
| HTTP CSV | Header/encoding/schema expliciet; originele respons bewaren; technische parsefout is geen geslaagde lege levering |
| JDBC | Expliciete kolomselectie en authentieke connectionbinding; bronsnapshot/watermark garanderen of beperking melden |

De bestaande JDBC-URL en `SELECT *` zijn input voor migratie, niet de
generieke standaard. Bronconsistentie over meerdere JDBC-partities vereist
een daadwerkelijk ondersteund bronsnapshotcontract; parallel lezen alleen
levert dat niet.
CBS-dataset, ECB-reeks en Nager-jaar zijn projectparameters.

### Bronze

- Batch-ingest per manifest/object met duurzaam bestandsregister.
- Herkomst: delivery/object/file, recordlocator, ingesttijd, release-ID en run-ID.
- Businesskeys zijn geen vervanging voor bronrecordidentiteit: echte
  dubbele bronrecords worden niet stil met `dropDuplicates` verwijderd.
- Schema drift: STRICT, expliciete approval of RESCUE, met originele payload.
- Bronrecordidentiteit moet herleesbaar zijn. Voor gestructureerde bestanden
  wordt een deterministische locator of multisetstrategie per formaat ontworpen;
  `monotonically_increasing_id` is geen persistente bronidentiteit.
- Een overschreven bestand met hetzelfde pad is een manifestconflict, niet
  automatisch dezelfde reeds ingelezen levering.

## 6. Runcontext, volgorde en herstel

### Identiteiten

| Identiteit | Betekenis |
|---|---|
| delivery_id | Immutable bronlevering |
| batch_id | Logische verwerking van die levering onder een gepinde release |
| attempt_id | Nieuwe technische uitvoerpoging, inclusief Fabric jobinstance-ID |
| load_date_utc | Eenmalig gepersisteerde batchtijd, hergebruikt over lagen/retries |
| metadata_release_id | Gepinde configuratie en modelcontract |
| lease_id / fencing_token | Ownership van uitvoer; verlopen writer mag niet publiceren |

Retry hergebruikt context; een bewuste herverwerking met gewijzigde metadata
is geen retry en vraagt expliciet rebuild-/reprocessbeleid.
Geen veranderlijke batchtijd afleiden uit losse notebookstarts.

### Voorstel state-machine

Delivery: REGISTERED -> WAITING/READY -> PROCESSING -> SUCCEEDED.
Afwijkingen: INVALID, FAILED, QUARANTINED, SUPERSEDED; iedere overgang auditeerbaar.
Work-item: PLANNED -> RUNNING -> SUCCEEDED, of RETRY_WAIT/DEAD_LETTER.
Een run zonder ready delivery krijgt een expliciet NO_WORK-resultaat.

De oudste geregistreerde niet-uitgezonderde delivery blokkeert nieuwere
bij incompleetheid; eerst rangschikken, dan readiness bepalen.
De gate kan geen levering blokkeren waarvan het bestaan niet bekend is:
registratie van verwachtingen/manifestOPEN is onderdeel van de producerroute.
Brongebonden volgnummer, niet alleen een datumfolder, ondersteunt meerdere
leveringen op dezelfde dag.

### Transactionele grenzen

Delta-transacties gelden per tabel. Audit, data en state zijn niet automatisch
een multi-tabletransactie. Daarom:

1. Eerste versie: geserialiseerde verwerking per bron/projectgroep.
2. Deterministische write-identiteiten en commitreconciliatie na storingen.
3. Eerst duurzame data, daarna controleerbare voortgang; auditfout kan een
   gefaalde poging geven terwijl een datacommit al bestaat.
4. Leaseclaimconflicten en initialisatie daadwerkelijk concurrencytesten;
   Delta MERGE en niet-afgedwongen PK's zijn niet vanzelf een veilige mutex.
5. Lease-vernieuwing, fencingcontrole en maximale looptijd; expiratie alleen
   is geen toestemming voor een oude writer om door te schrijven.

## 7. Quality, Vault en Gold Historical

**Quality/Reject:** expliciete projectie, typeregels en DQ. Ongeldige waarden
naar Reject of batchblokkade volgens policy; geen stille cast-default.
Reconciliatie telt ruwe Bronze, expliciet gefilterde records, passed en rejected,
en controleert identiteit/multipliciteit. DQ-uitvoer krijgt batch-/commit-ID;
downstream gebruikt alleen een volledig geslaagde objectcommit.

**Raw Vault:** immutable hub-/linkkeys en insert-only satellitehistorie.
Voorgesteld: idempotente satellite-MERGE op parentkey + persistente batch-ID,
zonder purge van dezelfde batch. Eén versie per sleutel per delivery is het
eerste contract; conflicterende meerdere versies falen tenzij een expliciet
multi-active-/eventvolgordecontract is geconfigureerd.
Actuele hashdiff-state is een herstelbare cache, niet de enige waarheid.
Na retry state reconciliëren met committed historie.

**Hashing:** SHA-256, expliciete kolomvolgorde, collision namespace, NULL-/
lege-stringbeleid, typecanonicalisatie en conventieversie. Bestaande
`^^`/`||`-normalisatie is input; delimiter-/NULL-tokenambiguiteit moet met
tests en een bewust canonicalisatiebesluit worden opgelost voor vrijgave.
Geen automatische conventiewijziging bij een upgrade.

**Reference Data:** eigen SCD2-contract met idempotente schrijfstrategie.
Absence-delete uitsluitend voor bewezen volledige snapshot. Een partiële
snapshot of HTTP-pagina is geen verklaring dat ontbrekende sleutels weg zijn.

**Business Vault:** versieerbare businessregels, as-of joins, referenties en
toepasselijke PIT/bridges. Historische facts mogen geen huidige attributen
aan oude versies koppelen zonder expliciete businessbedoeling.

**Gold Historical:** sleutels, geldigheidsintervallen, deletedbeleid en
temporale joins expliciet. Insert-only Gold-MERGE mag oude einddatums niet
bevriezen als zij bij volgende leveringen moeten veranderen. Daarom per
model kiezen voor afgeleide historieviews of gecontroleerde SCD2-maintenance;
niet blind de oude inserts overnemen.

## 8. Gold Current en consumercontract: belangrijk keuzepunt

**Functioneel contract:** alleen een volledig geslaagde productrelease
zichtbaar; laatste goede release blijft beschikbaar bij falen.

**Voorgesteld publicatiemodel:** immutable release-ID per productgroep;
meerdere releases in release_data met expliciete releasekolom, een complete
releasemanifest en een enkele actieve pointer in gold.publication.
Geen automatisch hergebruik van twee slots met mogelijk actieve lezers.
Een consumer pinnt een release-ID voor een meertabelsessie/refresh.

De pointer staat dicht bij de Gold-data om consumerafhankelijkheid van
control te beperken. Audit bewaart de relatie met batch/delivery/attempt.
Fysieke writes, pointer en SQL-endpointzichtbaarheid blijven verschillende
commit-/syncgrenzen en moeten worden bewezen.

| Consumeroptie | Voordeel | Verplichte beperking/test |
|---|---|---|
| SQL-views / DirectQuery | Flexibele projecties en filtering op release | T-SQL-objecten apart deployen; meerdere queries kunnen verschillende pointers zien tenzij release gepind |
| Power BI Import | Gepinde release voor een volledige modelrefresh | Refresh-/commitgedrag en behoud vorige modelversie testen; geheugen, duplicatie en latency meten |
| Direct Lake | Directe toegang tot Delta met beperkte datareplicatie | Fysieke tabellen, framing en versie-/releasefiltercontract ontwerpen; niet automatisch dezelfde semantiek als SQL-views |

**Consumerkeuze bevestigd: DirectQuery zonder Import (B-FAB-017).** Eén atomische Delta-pointer is niet
voldoende bewijs dat alle Power BI-visuals of SQL-queries dezelfde release
zien. Release-retentie houdt rekening met readers, herstel en opslagkosten;
oude releases niet verwijderen terwijl zij nog nodig kunnen zijn.

### Opdrachtgevervoorkeur: geen Power BI Import

Op 10 oktober 2026 geeft de opdrachtgever aan bij voorkeur geen Import te
gebruiken, zoals ook bij de eerdere Databricks-aanpak. Het eerdere advies
voor Import wordt daarom ingetrokken als primaire ontwerpoptie.
Dit bevestigt niet welke leesmodus destijds in Databricks is gebruikt en
is geen impliciete keuze voor Direct Lake.

De resterende primaire opties:

- **DirectQuery via het Lakehouse SQL analytics endpoint:** T-SQL-views kunnen
  het Gold Current-releasecontract aanbieden. Queries lezen via SQL, zonder
  een volledige Import-kopie van de data. Querylatency, endpointzichtbaarheid,
  rechten en consistente releasekeuze over meerdere queries moeten worden
  getest. DirectQuery alleen pinnt geen dashboardbrede release.
- **Direct Lake:** model leest fysieke Delta-tabellen via OneLake. Geen
  volledige Import-kopie, maar framing en modelcache blijven onderdeel van
  het gedrag. SQL-viewlogica en een dynamische pointer zijn niet zonder meer
  het leescontract; releasefiltering, refresh/framing en rechten moeten
  expliciet worden ontworpen en getest.

**Nieuw advies:** DirectQuery als eerste consumerroute wanneer Gold Current
via SQL-views beschikbaar moet zijn; Direct Lake kiezen wanneer fysieke
Delta-consumptie en framing bewust de primaire route worden.
**Status bij dit oorspronkelijke advies:** DirectQuery versus Direct Lake
nog niet bevestigd; vervolgens bevestigd zoals hieronder geregistreerd.
Geen model of refresh ingericht.

**Vervolgens bevestigd:** opdrachtgever kiest DirectQuery via het SQL-endpoint,
zonder Import (B-FAB-017). Dit is de primaire consumerroute voor de eerste
implementatie. Direct Lake blijft een mogelijke toekomstige uitbreiding,
niet een vereiste voor deze release.
Gold Current krijgt afzonderlijk te deployen T-SQL-views. Delta-writes blijven
in Spark; het Lakehouse SQL-endpoint is geen schrijfruntime voor de ETL.
Consistentie over afzonderlijke rapportqueries blijft te ontwerpen en te
bewijzen; een pointer kan tussen twee queries wijzigen.

## 9. Deployment, feedback en operations

Voorgestelde cyclus: validate -> deploy -> await provisioning -> setup ->
start job -> observe -> reconcile -> report.

- Secrets via ondersteunde Entra-/connectionbindingen, niet browserextractie.
- Expliciete doelworkspace en artifactscope; geen standaard orphan-deletion.
- HTTP 202 volgen tot eindstatus; deadline, Retry-After/backoff en run-ID bewaren.
- Geen blinde dubbele jobstart bij onzekere response.
- Job Completed is onvoldoende: delivery, layercommit, reconciliatie en
  productpublicatie bepalen het businessresultaat.
- Validatie-/configuratiefouten falen direct; retries alleen bij geclassificeerde
  tijdelijke fouten en idempotente acties.
- Interactieve assistentfeedback vervangt geen duurzame productiemonitor.

Metrics: bronrecords/files, filter/Quality/Rejectcounts, historie-/publicatie-
counts, schema drift, leaseconflicten, backlog en duur.
Logs bevatten correlation-ID's en beperkte foutcontext, geen secrets of
standaard volledige vertrouwelijke payloads.
Technische audit append-only; materialized status is herstelbaar uit events
waar die reconstructie daadwerkelijk ontworpen is.

## 10. Acceptatie, implementatievolgorde en fase-exit

| Contract | Bewijs in vervolgfase |
|---|---|
| Configuratie/namespace/release | Validator-unit-tests en echte Fabric compile-/bindingtest |
| Ingestie en manifest | Lege/incomplete/corrupte/gewijzigde levering en retry |
| Context en claim | Gelijke persistente tijd, twee starters en verlopen writer |
| Quality/Reject | Nul onverklaard verschil, correcte multipliciteit en expliciete filters |
| Vault/references/historie | Wijziging, delete, ongewijzigde levering en commitgrensfouten |
| Gold/consumer | Build/publicatiefalen, pinned reader, sync en modelrefresh |
| Deployment | Lege-workspace-uitrol, herdeploy, upgrade en terugval |

Implementatievolgorde: configuratievalidator/resolver -> context/control ->
foundationdeploy -> manifest/Bronze -> Quality/Reject -> Vault/references ->
Gold Historical -> publicatie/consumer -> operations en uitbreidingsbewijs.
Beveiliging en audit worden bij iedere stap toegepast.

**Fase 2 afgerond:** metadata-/type-/recordidentiteit-/ownership-/historie-
en consumercontracten zijn onder expliciet mandaat vastgesteld in het
[ontwerpcontract](13_fabric_ontwerpcontract_fase2.md).
Fysieke migraties, validators, runtime- en productietests volgen in de
implementatiefasen. Zakelijke workload-/retentie-/SLA-waarden zijn expliciete
go-live-inputs; zij worden niet verzonnen om een fase af te ronden.
Een ontwerpdocument is geen bewijs van werkende Fabric-ETL.

### Voortgang na vervolgmandaat en redeploy-requirement

Onder B-FAB-018/019 is een nieuwe `fabric_etl`-kern naast v2 gebouwd en een
eerste REST-foundationrunner toegevoegd. De drie Lakehouses zijn daadwerkelijk
via de aangemelde browser ingericht, inclusief schemas en SQL-endpoints.
API-commandotoegang was geblokkeerd op HTTP 401 / UserNotLicensed; daarna
opgelost door expliciete binding aan de juiste tenant. De foundationrunner
is tweemaal succesvol tegen Fabric uitgevoerd met gelijke IDs.
Zie [deployment- en redeploycontract](12_fabric_deployment_beheer.md).

Concrete eerste kernkeuzes:

- **Identifiers:** eerste versie beperkt tot ASCII-letters, cijfers en
  underscore, met letter/underscore als eerste teken. Daarmee blijven
  Spark- en SQL-bindings controleerbaar. Bredere namen zijn geen stille
  fallback; uitbreiding vereist tests en correcte segmentquoting.
- **Hash v1:** UTF-8-JSON van
  `["fabric-dv-key-v1", namespace, [canonical_value_1, ...]]`, SHA-256.
  Strings worden getrimd en uppercased, NULL blijft JSON null, leeg blijft
  lege string. Arrays behouden metadata-volgorde; JSON-objectkeys zijn
  gesorteerd. Geen delimiter-/NULL-tokenvervanging. Typecanonicalisatie
  gebeurt expliciet voor deze functie; numerieke inputs rechtstreeks
  aanbieden faalt. Spark/SQL-equivalentie is nog niet bewezen.
- **Context:** identity van project/source/delivery/metadatarelease bepaalt
  deterministisch het batch-ID. Tijd is verplichte timezone-aware input;
  restore valideert identiteit en canonieke UTC-tijd. Een controladapter
  moet create slechts bij registratie doen en daarna persisted context
  gebruiken. Het Python-object alleen is geen duurzaam register.
- **Chronologie:** READY mag pas starten als eerdere bekende leveringen
  SUCCEEDED of expliciet SUPERSEDED zijn. INVALID/QUARANTINED worden niet
  stil als uitzondering overgeslagen; operatoractie blijft auditeerbaar.
- **Redeploy:** standaard geen delete, datareset of replay. Elke volledige
  release moet tweemaal kunnen uitrollen met behoud van data en bindings.
- **Tenantbinding:** omgevingscontract 1.1 vereist `tenant_id`. API-authenticatie
  vraagt tokens expliciet voor deze tenant; geen toevallige CLI-default.

De toen nog open fase-2-contracten zijn nu gesloten in het normatieve
[ontwerpcontract](13_fabric_ontwerpcontract_fase2.md). Belangrijkste
vervolgbesluiten: projectbrede exclusive owner zonder automatische TTL-
overname, format-specifieke locators/multisets, committed-eventhistorie
en release-specifieke views plus gepinde DirectQuery-consumers.
De beperkte bestaande kern implementeert niet automatisch al deze contracten.
De formele fase-2-exit geeft vrij voor fase 3, niet voor productie.

## 11. Platformreferenties

Geraadpleegde bronnen; daadwerkelijke workspace-ondersteuning blijft te testen:

- [Lakehouse schemas](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas).
- [Lakehouse SQL analytics endpoint](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint).
- [Direct Lake overview](https://learn.microsoft.com/en-us/fabric/fundamentals/direct-lake-overview).
- [Notebook activity](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity).
- [Fabric deployment en jobfeedback](09_fabric_migratieverslag.md).
