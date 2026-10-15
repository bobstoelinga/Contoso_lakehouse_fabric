# Fabric ontwerpcontract en fase-2-exit

Datum: 10 oktober 2026. Contract: 1.0.
Status: vastgesteld ontwerp onder expliciet opdrachtgevermandaat.
Dit document sluit de open ontwerpkeuzes uit het
[architectuurdocument](11_fabric_template_architectuur.md).
Het is geen bewijs dat de volledige ETL al werkt.

## 1. Scope en besluitgrens

Doel: een herbruikbaar Fabric-template met Raw, Bronze, Quality/Reject,
Raw Vault, Business Vault, referentiedata, Gold Historical en Gold Current.
De vijf bronnen en negen objecten blijven verplicht in de eindacceptatie.
Referentiedata volgen hun eigen expliciete historisatieroute; niet ieder
bronobject hoeft kunstmatig een hub/link te krijgen.
SharePoint en historische Databricks-pariteit blijven uitgesloten.
Power BI gebruikt DirectQuery, geen Import.

Het mandaat is voldoende om de onderstaande ontwerpkeuzes vast te stellen.
Dit creëert geen toestemming voor betaalde productiecapaciteit, verborgen
credentialgebruik of het kiezen van zakelijke SLA-/retentiewaarden.
Die waarden blijven expliciete go-live-inputs, geen ontbrekende architectuur.
Een productieconfiguratie zonder zulke inputs moet door de validator falen.

Normatieve volgorde: dit contract gaat voor oudere voorstellen in het
architectuurdocument; het besluitenregister bewaart de chronologie.

## 2. Template, package en deploymentgrenzen

| Onderdeel | Vastgesteld contract |
|---|---|
| Framework | `src/fabric_etl`; generiek, geen impliciete Contoso-default |
| Distributie | Versiegepinde wheel als Environment-library; Python-imports blijven intern toegestaan |
| Project | `projects/<project_id>` met metadata, business-SQL en testfixtures |
| Binding | `environments/<project_id>.<environment>.json`, tenant/workspace/item-ID's en connectionreferenties |
| Fabric-items | Versiebeheerde Environment, notebooks, pipelines en consumerdefinities |
| Migraties | Genummerd, checksum-gecontroleerd, met prerequisites en herstelprocedure |
| Uitrol | Expliciete doelbinding, gevalideerde release, geen orphan deletion of dataload als bijeffect |
| Repository | Huidige repository als ontwikkelbasis; geen nieuwe remote of push zonder aparte opdracht |

De voorkeur "geen Import" gaat over Power BI-dataopslag, niet over normale
Python-module-imports. Notebooks zijn dunne adapters rond het framework.
Geen duplicatie van volledige loadercode in ieder notebook.
De eerste wheel kan vanuit deze repository worden gebouwd; een zelfstandige
frameworkrepository is geen prerequisite voor de eerste Fabric-release.
De oorspronkelijke v2-package blijft een regressiebaseline, geen Fabric-runtime.

Vereiste uitrolvolgorde:
validate -> inspect/plan -> foundation -> Environment -> controlmigraties ->
metadata-kandidaat -> notebooks/pipelines -> SQL/consumercontract ->
verificatie -> activering.
Dataverwerking is een apart jobcommando na succesvolle uitrol.
Een half gedeployde release mag niet actief worden.

## 3. Projectmetadata: logisch schema 1.0

Een projectrelease is een volledig pakket, geen losse updates van actieve
metadatatabellen. Verplicht envelop:
`project_id`, `contract_version`, `framework_version`, `hash_contract_version`,
`schema_contract_version`, `release_id` en onderstaande collecties.
`release_id` is SHA-256 van canonieke pakketinhoud zonder het release_id-veld.
Een bestandmanifest bevat relatieve bestandsnamen en SHA-256 per project-SQL-
en fixturebestand; SQL-inhoud behoort dus ook tot de releasefingerprint.
Geen timestamps of secrets in de fingerprintinput.
Bindings voor dev/tst/prd staan buiten businessmetadata, maar worden met
hun eigen fingerprint in het deploymentrapport opgenomen.

| Collectie / sleutel binnen projectrelease | Verplichte inhoud |
|---|---|
| source_systems / source_system_id | Naam, deliverymodus, volgorde-scope, owner, governance-policy |
| source_objects / source_object_id | Bronsysteem, connector_id, file/recordformat, businesskeys, laadroute, schema_id, deletebeleid, mandatory |
| connectors / connector_id | FILE_MANIFEST, HTTP_JSON, HTTP_CSV of JDBC; parameters, connection_ref indien nodig, timeout/paginering/retrypolicy |
| schemas / schema_id | Geordende kolommen: naam, type, nullable, bronnaam, expliciete tijdzone bij lokale timestamps |
| mappings / mapping_id | Object/model, doelkolom, ordinal, outputtype, expression_ref of geregistreerde expressie, null-/castbeleid |
| filters / filter_id | Object, versieerbare predicate, reden, uitvoervolgorde; apart accounting |
| quality_rules / rule_id | Object, rule_type, kolommen/expression, severity, actie, rejectcode, threshold en denominator |
| dependencies / dependency_id | entity_id, depends_on_entity_id, laag; alleen bestaande entiteiten, acyclisch |
| vault_entities / entity_id | HUB/LINK/SATELLITE/LINK_SATELLITE/PIT, logische target, key/hashkolomordening en dependencies |
| reference_entities / entity_id | Businesskeys, payloadkolommen, SCD2-eventcontract, expliciet snapshot/deletebeleid |
| business_entities / entity_id | Logische target, geversioneerde rule/projectie, temporal_join_policy en dependencies |
| gold_entities / entity_id | HISTORICAL/CURRENT, expliciet kolomschema, grain, projectie, productgroep en dependencies |
| products / product_id | Verplichte Gold-entiteiten, businesscutoff, consumerbeleid, releasechecks en compatibility_policy |
| policies / policy_id | DQ, retries, onderhoud, dataclassificatie, retentionreferentie en operationele grenzen |

Alle entiteits-ID's zijn uniek binnen hun collectie en hebben een expliciete
entiteitssoort in DAG-verwijzingen. Geen naamtoeval als joinmechanisme.
Logische targets zijn `role/schema/table`; geen UC-catalogusvelden of
willekeurige SQL-stringvervanging. SQL-placeholderbindings staan in een
allowlist en worden per engine gerenderd. DDL/DML zit in beheerde scripts,
niet in een vrije bronkolomexpressie.

### Validatie en migratie van Contoso

- Exacte veldsets per contracttype; onbekende velden en dubbele JSON-velden falen.
- Verplichte collecties mogen alleen leeg zijn als de gekozen route ze niet gebruikt.
- Alle foreign keys, unieke targetkolommen, ordinals, sleutels en DAG controleren.
- Uitsluitend geregistreerde connector-, DQ-, datatype- en entiteitssoorten.
- Geen runtime is_active-filter: de gepinde release bepaalt de hele actieve scope.
- Nieuwe kolommen alleen via schema-release; geen automatische schema-evolutie
  van Vault/Gold. RESCUE bewaart onbekende bronvelden vóór een schemawijziging.
- Framework-/metadata-/schema-/hashcompatibiliteit vooraf controleren.
- Veranderde sleutels/hashconventie vereisen nieuw target of expliciet rebuildplan.
- Secrets verboden; credentials via Entra/connectionreferenties.

Contoso-adapter converteert expliciet de bestaande metadata. Hij kopieert
geen UC-paden, Auto Loader-checkpoints of Databricks-secret-scopes.
Customers-freshness wordt **24 uur**, conform het eerder effectieve seedveld;
de oorspronkelijke dubbele 26/24-sleutel wordt niet stil geparseerd.
DQ- en freshnesswaarden moeten in de nieuwe projectrelease expliciet staan.
De bestaande 2555-dagenretentie is geen goedgekeurde productiepolicy.
Nager-jaar, CBS-dataset en ECB-reeks zijn projectparameters.
Iedere active bron krijgt een manifest met de eigen verplichte objectscope.

| Bronsysteem | Verplichte objecten | Laad-/deletecontract |
|---|---|---|
| SALES | CUSTOMERS, PRODUCTS, ORDERS, RETURNS, EMPLOYEES | Snapshot voor dimensions, append voor Orders/Returns; expliciete soft-deleteflag, geen afgeleide absence-delete |
| CBS | JEUGDZORG_WIJK_2025 | Gepagineerde extract; Raw Vault-route; geen absence-delete zonder afzonderlijk volledig-snapshotcontract |
| ECB | EXCHANGE_RATE | Reference SCD2; OBS_VALUE-filter met apart accounting; geen delete uit ontbrekende pagina/rij |
| NAGER | HOLIDAYS_NL | Reference SCD2; jaar/land expliciet; bronjaar-scope nooit behandelen als globale afwezigheid |
| FABRIC_SALES | ORDER_LINES | Reference SCD2; soft-deleteflag; absence-delete alleen na bewezen complete snapshot voor expliciete extractscope |

Bronbereikbaarheid en volledigheid zijn integratietests, niet verondersteld.
Geen synthetische bron als stil substituut voor een ontbrekende echte bron.
Synthetische fixtures zijn uitsluitend benoemd testbewijs.

## 4. Typecanonicalisatie, hashes en Raw-recordidentiteit

### Hashinput

Hashconventie blijft fabric-dv-key-v1: UTF-8-JSON-array van conventie,
namespace en geordende canonieke waarden; SHA-256. Niet de oude ^^/||-tokens.
Namespace is expliciet `project/source/entity/key_contract`; hergebruik
van een sleutel tussen bronnen alleen via een bewuste same-as/businessregel.
Key- en hashdiffkolomlijsten zijn onafhankelijk en versieerbaar.

| Inputtype na Quality | Canonieke string vóór hash_key |
|---|---|
| NULL | Python None / JSON null, niet de tekst "NULL" |
| STRING | Trim/uppercase volgens v1; lege string blijft leeg |
| BOOLEAN | "TRUE" of "FALSE"; geen truthiness van willekeurige tekst |
| BYTE/SHORT/INT/LONG | Decimale integernotatie, geen plus of voorloopnullen |
| DECIMAL(p,s) | Fixed-point met exact s decimalen; -0 wordt 0; geen afronding/overflow zonder mappingregel |
| DATE | YYYY-MM-DD |
| TIMESTAMP | UTC, YYYY-MM-DDTHH:MM:SS.ffffffZ, microseconden |
| BINARY, ARRAY, MAP, STRUCT | Niet toegestaan voor businesshash in v1; expliciete projectie naar ondersteund scalair type vereist |
| FLOAT/DOUBLE | Toegestaan als bronpayload; niet als key/hashdiff zonder expliciete decimal-/stringprojectie |

Lokale timestampinput vereist een bronzone; ambiguous/nonexistent DST-tijd
gaat naar Reject tenzij bronoffset aanwezig is. Geen sessietijdzone-default.
NaN/Infinity en ongeldige casts worden expliciet verworpen.
De case-insensitive v1-normalisatie betekent dat case-only wijzigingen in
een hashdiff geen nieuwe versie veroorzaken; Raw bewaart wel de originele
input. Casegevoelige attributen vereisen vóór onboarding een andere
versiegepinde hashconventie, geen stille uitzondering op v1.
Python/Spark-canonicalisatie moet op dezelfde testvectors identieke bytes
geven; het lokale hashhelperbewijs bewijst dat nog niet.

### Recordidentiteit vóór businessnormalisatie

Raw is immutable; de identiteit van een rij is niet de businesskey.

- JSON: filefingerprint/paginanummer plus array-ordinal binnen de expliciete
  records_key. Originele responsbytes en extractparameters blijven bewaard.
- CSV: filefingerprint plus ordinal van het logische CSV-record na correcte
  parsing, inclusief quoted multiline velden. Header telt niet als data.
  Encoding/delimiter/quote/escape staan in schema; geen line-split-parser.
- Parquet: per bestand een multiset van volledige, typegetagde bronrecords.
  Canonicalisatie behoudt nulls, case, whitespace, kolomnamen en types;
  geen Quality-normalisatie. MAP-keys worden canoniek gesorteerd.
  Niet-ondersteunde complexe types falen bij schema-validatie.
  Per canonieke recordbytes multipliciteit n vaststellen; genereer occurrences
  1..n. Identieke occurrences zijn onderling uitwisselbaar; er wordt geen
  oorspronkelijke fysieke rijpositie beloofd.

`raw_record_id` is SHA-256 van project/object/delivery, relatief bestandpad,
filefingerprint en locator (of recordbytesfingerprint + occurrence).
Bij fingerprintgelijke maar byteverschillende records faalt de ingestie.
Dit multisetmodel behoudt echte duplicaten en is herleesbaar bij gewijzigde
Spark-partitionering; monotonically_increasing_id is niet toegestaan.
Retry met dezelfde immutable filelijst levert dezelfde verzameling IDs.
Een correctie met nieuwe delivery_id levert bewust nieuwe IDs.

Manifest 1.1 breidt de bestaande lokale 1.0-kern uit met `published_at_utc`,
extractscope, `source_as_of_utc` of expliciete best-effort-status,
watermarks indien van toepassing, en optional supersedes_delivery_id.
1.1 moet expliciet geïmplementeerd worden; de huidige 1.0-validator claimt
geen ondersteuning. CLOSED staat pas na filecontrole vast. Ontbrekende
bronconsistentie blokkeert snapshotdeletes, maar wordt niet verborgen.

## 5. Duurzame controltabellen en exclusive-owner-protocol

### Logische tabelcontracten

| Control/audittabel | Logische unieke identiteit en inhoud |
|---|---|
| metadata.release_manifest | project/release; fingerprint, bestandsmanifest, status CANDIDATE/VALIDATED/ACTIVE/RETIRED |
| metadata.release_records | project/release/collection/entity; volledige gevalideerde JSON plus modelkolommen |
| control.metadata_pointer | project; active_release_id, generatie; pas na complete releasevalidatie |
| control.project_owner | project; exact één vooraf aangemaakte rij, owner_id, generation, state, heartbeat, job_instance_id |
| control.delivery | project/source/delivery; uniek source-volgnummer, manifestfingerprint, scope, status en supersedes |
| control.batch_context | batch_id; bronlevering, release_id, één persistente UTC-tijd |
| control.work_item | batch/layer/entity; inputfingerprint, status, commit_id, attempt en foutcode |
| audit.events | deterministic event_id, correlation-ID's, overgang, timestamp en beperkte foutcontext |
| control.commit_manifest | commit_id; batch/layer/entity, owner_generation, verwachte outputtargets/fingerprints/counts en COMMITTED-status |
| publication.product_release | product/release; exacte inputcommits, entitymanifest, cutoff, Spark/SQL-validatie en consumerstatus |
| publication.active_product | product; laatste gevalideerde datarelease, vorige release en generatie |
| publication.consumer_catalog | product; actieve gepinde consumerrelease, model-ID, report-ID/URL, vorige consumer en promotietoestand |

Deze keys zijn **logische contracten**, geen afgedwongen Delta-PK's.
Writers draaien onder het onderstaande exclusieve protocol; checks op
cardinaliteit en conflicten blijven verplicht.
Nieuwe batchtijden worden onder projectownership geregistreerd. Binnen
een historisatie-scope is load_date strikt stijgend; bij gelijke/teruglopende
wall-clock wordt minimaal vorige load_date plus één microseconde gekozen.
De werkelijke registratie-/brontijd blijft afzonderlijk bewaard. Retry
hergebruikt de al geregistreerde tijd; bronvolgnummer blijft het volgordecontract.
Status-/commitmanifest met identieke identiteit maar verschillende inhoud
is een conflict, niet een succesvolle retry.
Technische attempt-events zijn append-only; retries mogen niet de oude
attempt overschrijven. Materialized workstatus kan vanuit events/commits
worden gereconcilieerd.

### Vastgesteld concurrencybesluit

Eerste productieversie heeft **één writer per project**, niet alleen per
bronsysteem: bronnen kunnen dezelfde Vault/Gold-producten beïnvloeden.
De writer omvat metadataactivering, dataverwerking, publicatie en onderhoud.
Bronnen/objecten binnen deze writer worden eerst sequentieel verwerkt.
Pipelineconcurrency=1 is aanvullend, niet het mutexbewijs.

1. Bootstrap maakt project_owner met exact één FREE-rij onder exclusieve
   foundationsetup. Geen runtime INSERT/MERGE-initialisatie van deze lock.
   Ontbrekende of dubbele rij: fail-closed.
2. Starter doet conditionele Delta UPDATE van FREE naar HELD met unieke
   owner_id, job-ID en verhoogde generation, en leest ongecachet terug.
   Hij gaat alleen door als precies één rij zijn owner_id/generation bevat.
3. UPDATE-conflict is een expliciet claimconflict. Een verliezer start geen
   datawrites. Geen algemene exceptioncatch of zero-metric-success.
4. Iedere writefase/publicatie controleert ownership. Alleen geregistreerde
   writers mogen control/data wijzigen; rechtstreeks schrijven buiten het
   framework is een beheerincident, geen ondersteunde route.
5. Heartbeatverlies of deadline zet toestand op onderzoek/incident.
   **Geen automatische TTL-overname.** Oude writer kan nog actief zijn.
6. Recovery bevestigt dat de oude Fabric-job terminal is en geen bijbehorende
   Spark-uitvoering/subjob meer actief is. Bij onzekerheid blijft verwerking
   geblokkeerd. Pas daarna owner-generatie wijzigen en retry starten.
7. Release vereist dezelfde owner_id/generation; anders fail-closed.
   Generation is audit-/toestandsfencing, geen claim dat Delta zelf writes
   op andere tabellen met een verlopen token atomisch verbiedt.

Dit ontwerp gebruikt Delta optimistic concurrency control op een vooraf
bestaande gedeelde rij. Het wordt pas voor operationele inzet vrijgegeven
na echte concurrerende starters op de gekozen Fabric-runtime, inclusief
initialisatie, lost-response en writer-stopproeven.
Een mislukte conflictproef blokkeert deployment van de writer; geen fallback
naar "MERGE werkt meestal". Dan is een transactionele lockdienst een
architectuurwijziging die expliciet moet worden besloten.

Waarom geen automatisch verlopen lease: checking ownership en schrijven
in een andere Delta-tabel is geen multi-tabletransactie. Zonder bewezen
fencing zou een oude writer naast zijn opvolger kunnen schrijven.
Waarom geen extra SQL Database/Redis/Blobdienst nu: dit introduceert een
nieuwe infrastructuur-/rechtenafhankelijkheid terwijl veilige sequentiële
verwerking de eerste scope dekt. Beschikbaarheid gaat hier niet ten koste
van correctheid; automatische failover en parallelisme zijn niet beloofd.

## 6. Laad-, Quality-, historie- en herstelcontract

### Commitprotocol voor iedere laag

Persist context -> valideer inputcommit -> bouw deterministische outputs ->
write outputs -> lees outputs terug en reconcile -> COMMITTED-manifest ->
downstream vrijgeven. Een volledig COMMITTED-objectmanifest is één
Delta-commit met de hele targetlijst; targetdata zijn afzonderlijke commits.
Downstream kiest alleen batch-/commitdata met een geldig manifest.
Geen "alle Spark-taken Completed" als vervanging voor businessreconciliatie.

Bij crash vóór COMMITTED: retry controleert deterministische outputidentiteit,
hervat/verifieert ontbrekende writes en schrijft pas daarna het manifest.
Identieke historie niet batchgewijs purgen. Bij crash na COMMITTED maar vóór
auditstatus: manifest wint; herstel ontbrekende events/status zonder herladen.
Als outputfingerprint afwijkt bij dezelfde input/versie: blokkeren.

### Quality en accounting

Iedere raw_record_id krijgt precies één terminale uitkomst:
FILTERED, PASSED of REJECTED. Warningregels zijn extra annotaties, geen
extra rij in de terminale verdeling. Meerdere Reject-redenen blijven één
Rejected-rij met redenlijst.

Per object:
Bronze = Filtered + Passed + Rejected, zowel counts als record-ID-multipliciteit.
Voor regelthresholds is denominator de na expliciete filters resterende
populatie; 0 records geeft 0 violations, maar snapshot-completeness wordt
onafhankelijk gecontroleerd. Een threshold blokkeert bij `actual > configured`;
NOT_NULL/sleutel/route-integriteit kan onafhankelijk hard-blocking zijn.
Dubbele businesskeys worden niet weggegooid: zij blijven Bronze en krijgen
de passende DQ-uitkomst. Een SCD2-entity met conflicterende payloads voor één
key binnen dezelfde delivery mag niet door naar een willekeurige last-wins.

### Vault en referencehistorie

- Hubs/links: insert-only op expliciete key; record_source en eerste loadtijd
  blijven bij retry behouden.
- Satellites/reference: immutable events met parent/key, batch_id, hashdiff,
  load_date, delete-indicator en source-effective time indien beschikbaar.
- Eén payloadversie per key/delivery in v1. Identieke dubbele input mag door
  een expliciete entity-level DISTINCT-projectie worden samengevoegd nadat
  DQ/accounting de bronmultipliciteit heeft geregistreerd; niet stil in Bronze.
- Geen aparte UPDATE-then-INSERT voor authoritative SCD2-intervallen:
  end_date/is_current worden met LEAD over committed events afgeleid.
  Current-state is een herstelbare cache van deze committed eventhistorie.
- Absence-delete krijgt een expliciet tombstone-event alleen binnen een
  complete snapshot/extractscope; incomplete/quarantined input nooit deletes.
  Een verworpen/gefilterde rij kan een nog aanwezige sleutel vertegenwoordigen:
  absence-delete vereist daarom nul technisch verloren of rejected records
  en nul niet-contractuele presence-filters in deze snapshot. Anders geen
  deletevrijgave, ook niet bij een gesloten extractmanifest.
  Lege snapshots en grote deletepercentages worden getoetst aan een expliciete
  projectpolicy; de validator accepteert geen stil onbeperkt-delete-default.
- Een ongewijzigde key/hashdiff/delete-status schrijft geen nieuw satellite-
  event. Vergelijk met de vorige committed gebeurtenis; geen cache als enige
  waarheid. Een al deleted key krijgt niet iedere levering opnieuw een tombstone.
- Iedere write met bestaande batch/key maar andere hashdiff faalt.
- PIT is een echte Business Vault-projectie op committed historie met
  vastgelegde peildatums en dependencies; geen generieke unsupported-skip.

### Temporaliteit

Technische kennis-/loadtijd en business-effective time zijn verschillende
kolommen. De eerste generieke historisatie volgt loadtijd/bronvolgorde;
business-effective reconstructie vereist per project een expliciete regel.
Backdated data herschrijft niet ongemerkt oude committed technische events.
As-of-joins gebruiken halfopen intervallen `[valid_from, valid_to)`.
Overlappende intervallen of meerdere matches per factgrain blokkeren Gold.
Bij ontbrekende historische referentie gebruikt het model een expliciete
unknown-key of blokkeert volgens metadata; nooit automatisch een current join.

Gold Historical wordt deterministisch afgeleid van committed historie,
inclusief afgeleide einddatums. Iedere productbuild legt de inputcommitlijst
vast. Materialisatie is een versieerbare build, niet een insert-only MERGE
die oude end_date bevriest. Gold Current selecteert de actuele versie bij
de productcutoff en het expliciete deletedbeleid.

## 7. Gold-publicatie en DirectQuery zonder gemengde releases

### Twee consumercontracten, geen verwisseling

1. **Gold Current als algemene SQL-interface:** vaste current-views kiezen
   een gevalideerde actieve productrelease. Het contract is per query;
   losse queries kunnen bij publicatie verschillende releases kiezen.
   Deze views zijn niet het rapportbrede consistentiecontract.
2. **Power BI DirectQuery:** ieder gepubliceerd productmodel is vastgepind
   aan één immutable product_release_id. Alle modeltabellen gebruiken
   release-specifieke views; geen dynamische active-pointer in iedere query.

Voor eerste implementatie krijgt een release een schema met veilige naam
`r_<release_id>` en expliciete kolomprojecties in T-SQL.
Dat schema wordt via ondersteunde schema-enabled Lakehouse-inrichting
geregistreerd en op SQL-zichtbaarheid gecontroleerd; de migratiegenerator
mag niet zonder platformtest aannemen dat willekeurige T-SQL-DDL ondersteund is.
Bijvoorbeeld iedere view filtert release_data op dezelfde literal release_id.
Viewdefinities worden na publicatie niet vervangen. Een semantic model en
bijbehorend rapport worden als kandidaatrelease naast de laatste goede
consumerrelease gedeployed; zij verwijzen allemaal naar dat schema.
Alle fact-/dimensionrelaties en technische releasekolommen worden gevalideerd.
Geen Import-partitions of automatische Direct Lake-keuze.

Release-specifieke model/report-items voorkomen het in-place omzetten van
een model terwijl bestaande visualqueries nog lopen. Nieuwe release wordt
na acceptatie als nieuwe consumer-versie beschikbaar gesteld via het
beheerde productcatalogusregister in Gold publication. Het deploymentrapport
geeft de gevalideerde report-URL en model-ID; een consument gebruikt die
gepubliceerde rapportversie. Oude modellen blijven bruikbaar zolang
de bijbehorende release nodig is. Een atomische wissel van alle bestaande
gebruikerssessies of browserbookmarks wordt **niet** beloofd.
Een stabiel verwijspunt naar het nieuwste rapport is een beheerinterface,
geen database-transactie over alle viewers. Een Power BI-app of nieuwe
Control Room-UI is optioneel vervolgwerk, geen verborgen afhankelijkheid
van het geautomatiseerde publicatie-/redeploycontract.

Dit kost meer consumerartifactbeheer dan één dynamisch model, maar voorkomt
de onbewezen cross-query-pointergarantie. Model-/reportgeneratie, catalogus-
update, rechten en quota moeten in de consumerimplementatie bewezen worden.
Geen automatisch itemverwijderen bij redeploy; retirement is een apart plan.

### Publicatievolgorde

1. Projectowner bevestigd; metadatarelease en inputcommits gepind.
2. Alle release_data-entiteiten immutable schrijven met release_id.
3. Spark-grain, relaties, row accounting, checksums en expected outputs toetsen.
4. Via Gold SQL-endpoint per entity dezelfde releaserijen/schema controleren.
   Eindige syncdeadline; "Spark write klaar" is geen SQL-readiness.
   Consumerdata staan als ondersteunde Delta-tabellen onder Tables, niet
   Files; Spark-/T-SQL-typen en maximale tekstlengtes moeten overeenkomen.
   Geen SQL-foreign-key-DDL als surrogaat voor de relatiechecks; constraints
   kunnen schema-sync bemoeilijken en bewijzen de businessgrain niet.
5. Release-specifieke views, kandidaatmodel en rapport deployen en testen.
6. Productrelease als VALIDATED registreren met datamanifest en consumer-ID's.
7. Actieve data-pointer wijzigen; verifiëren dat general current-queries
   daadwerkelijk de gewenste release zien. Bij falen geen consumerpromotie.
8. Nieuwe gepinde consumerrelease beschikbaar stellen; audit beide activaties.

Pointer, SQL-sync en Power BI-publicatie zijn **geen gezamenlijke transactie**.
Bij crash blijven oude gepinde consumers correct; recovery reconcilieert
de gewenste en waargenomen data-/consumerstatus. De toestand
DATA_ACTIVE_CONSUMER_PENDING is zichtbaar en retrybaar, geen groen eindresultaat.
Pointerrollback naar vorige gevalideerde datarelease is mogelijk, maar wordt
ook pas na SQL-readback als voltooid gemeld.

Retirement vereist controle van geregistreerde consumers, herstelwindow en
retentiepolicy. Geen garantie dat alle ad-hoc SQL-sessies detecteerbaar zijn:
voor zulke lezers moet een expliciete contractduur gelden.
Zonder ingevulde retirementpolicy worden oude releases niet automatisch
verwijderd. Storage/capacity/quota volgen een expliciete acceptatietest.

## 8. Security, operations en open go-live-inputs

- Deploymentidentity, runtimeidentity en consumers hebben verschillende rollen.
  Geen breed workspace-memberschap voor consumers.
- SQL-toegang alleen voor gevalideerde Gold-interfaces; Spark/OneLake-rechten
  apart controleren. Objectnamen en datasensitivity horen bij deploymentplan.
- Verbindingen via Entra/connectionreferenties; geen secrets in Git/metadata,
  notebookparameters, Raw-manifests of logging.
- Onderhoud krijgt dezelfde projectownership als writes. VACUUM/retirement
  mag replay-/reader-/herstelretentie niet ondergraven.
- Alerts voor failed/quarantined delivery, eigenaar zonder heartbeat,
  backlog, drift, reconcileverschil, SQL-syncdeadline en consumerpending.
- Iedere alert heeft owner/escalatieroute; interactieve assistent is niet
  de permanente monitor. Control Room-UI is geen ETL-correctheidsprerequisite.
  Begin met audit-/statusviews en Fabric Monitoring; writeacties alleen via
  gecontroleerde jobs. Een aparte UI-keuze kan later zonder kernherontwerp.

Voor go-live verplicht: concrete workload/concurrency, latency/SLA, RPO/RTO,
broncredentials, productiecapaciteit/budget, bewaartermijnen, delete-/privacy-
policy en alertowners. Trial is alleen ontwikkeling. In tst/prd geen stille
defaults of productieclaim als een verplicht policyveld ontbreekt.

## 9. Ontwerpacceptatie en implementatiebewijzen

Fase 2 vereist een besloten contract met toetsbare gevolgen. De volgende
tests zijn **opdrachten voor vervolgfases**, niet reeds geslaagde tests:

| ID | Bewijsopdracht | Passcriterium | Fase |
|---|---|---|---|
| T-01 | Volledige projectvalidator en Contoso-conversie | 5 bronnen/9 objecten; geen dubbele keys, unresolved references of cycles; invalid fixtures falen | 3 |
| T-02 | Python/Spark-type- en hashvectors | Identieke bytes/hashes; NULL/empty/delimiter/DST/decimalgrenzen correct | 3/4 |
| T-03 | JSON/CSV/Parquet Raw-retry met duplicates | Exact dezelfde record-ID-multiset bij andere Spark-partitionering; nul verlies | 5 |
| T-04 | Twee starters en owner-initialisatie | Hoogstens één eigenaar schrijft; dubbele/ontbrekende lock faalt; loser expliciet gerapporteerd | 4 |
| T-05 | Writerstop en lost response | Geen takeover zolang uitvoering onzeker; gecontroleerde recovery met nieuwe generation | 4/5 |
| T-06 | Crash na iedere data-/manifestgrens | Retry zonder purge/verlies/duplicaten; onvolledige outputs nooit downstream | 5-8 |
| T-07 | DQ/filter/rejectmultipliciteit | Iedere Bronze-ID precies één terminale uitkomst; nul onverklaard verschil | 6/7 |
| T-08 | SCD2 change/delete/unchanged/late arrival | Expected committed events, juiste halfopen intervallen, geen current-factvervuiling | 6/7 |
| T-09 | PIT en alle bronroutes | Geen unsupported-skip; alle verplicht gemodelleerde entiteiten aantoonbaar gemaakt | 6/7 |
| T-10 | Productbuild/SQL-sync/consumerfalen | Laatste goede pinned rapportrelease blijft correct; pending/failure zichtbaar | 6-8 |
| T-11 | Publicatie tijdens rapportqueries | Iedere kandidaat-/oude modelquery blijft op zijn ene release; geen mixed-release joins | 6/8 |
| T-12 | Volledige deploy tweemaal, upgrade/crash | Geen onbedoelde creates/deletes/replay; stable bindings en recoverable releaseactivering | 3/4/9 |
| T-13 | Least privilege | Consumer kan Gold lezen, niet Raw/control/credentials; getest via SQL én OneLake/Spark | 4/8 |
| T-14 | Workload/continuity/retirement | Afgesproken numerieke SLA/RPO/RTO en retentie gehaald; trial geen prod-gate | 8/10 |
| T-15 | Tweede project | Configuratie/SQL/fixtures wijzigen, geen bron-specifieke fork van kern | 8/9 |

De exacte fasebenaming van een test is sequencing; geen reden een gate
weg te laten. Alle vereiste bewijzen komen in het acceptatieregister.

## 10. Formele fase-2-exit

| Ontwerponderdeel | Status bij fase-2-exit |
|---|---|
| Scope en template-/project-/platformgrenzen | Vastgesteld, secties 1-2 |
| Drie Lakehouses en namespace-/tenantbinding | Vastgesteld; bestaande foundation tweemaal via API geverifieerd |
| Metadata-/migratie-/releasecontract | Vastgesteld, sectie 3; implementatie T-01 |
| Typehash en herleesbare recordmultipliciteit | Vastgesteld, sectie 4; implementatie T-02/T-03 |
| Controlschema, exclusive owner en recovery | Vastgesteld, secties 5-6; operationele vrijgave T-04/T-06 |
| Quality, Vault, Reference, PIT en temporale Gold | Vastgesteld, sectie 6; implementatie T-07/T-09 |
| DirectQuery/publicatie zonder Import | Vastgesteld, sectie 7; consumerbewijs T-10/T-11 |
| Deployment/redeploy/operations/security | Vastgesteld; volledige bewijsopdrachten T-12/T-15 |
| Niet bekende zakelijke productieparameters | Expliciet go-live-input, geen verzonnen SLA/retentie |

**Fase 2 afgerond op 10 oktober 2026** onder expliciet mandaat.
Vrijgave is voor het bouwen en toetsen van dit ontwerp, niet voor productie.
De reeds ontwikkelde beperkte kern/foundation is aanvullend bewijs,
geen invulling van nog ontbrekende volledige projectmetadata of ETL.

Volgende actieve fase: **fase 3, implementatie van het template en de
release-/deploymentstructuur**, met fase-4-runtimeproeven als prerequisites
voor operationele writer/publicatie. Het eerstvolgende werkpakket is
volledige projectcontractvalidator + Contoso-conversie, gevolgd door
controlmigraties, gepinde Environment en dunne Fabric-notebooks.

## 11. Bronnen en bewijsgrenzen

- [Delta concurrency control](https://docs.delta.io/concurrency-control/):
  optimistic concurrency en conflictgedrag; geen bewijs van een al geteste
  Fabric-lockimplementatie.
- [SQL endpoint performance/synchronisatie](https://learn.microsoft.com/en-us/fabric/data-engineering/sql-analytics-endpoint-performance):
  backgroundsync en variabele vertraging; een normale subminuutobservatie
  is geen aangenomen SLA of transactionele cross-enginegarantie.
- [Architectuur en bestaande platformreferenties](11_fabric_template_architectuur.md).
- [Echte foundationreconciliatie en redeploy-eis](12_fabric_deployment_beheer.md).
- [Baseline-/eindacceptatiebasis](10_fabric_baseline_acceptatie.md).
