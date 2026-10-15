# Migratieverslag - Contoso Lakehouse v2 naar Microsoft Fabric

## 1. Doel en werkwijze

Dit verslag houdt de functionele, technische en projectmatige voortgang bij
van de migratie van het metadata-gedreven Databricks-project naar Microsoft
Fabric. Het bestaande [besluitenlog](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/docs/00_besluitenlog.md) blijft de bron voor
het oorspronkelijke Databricks-ontwerp; dit verslag registreert de migratie.

Voor het samenwerkingsverhaal en later gebruik in een LinkedIn-artikel:
zie [Mens en twee AI-sessies: praktijkverslag](#mens-en-twee-ai-sessies-praktijkverslag-voor-naslag-en-linkedin).
Dit samenhangende verhaal vult de gedetailleerde F6-COLLAB-registraties aan.

### Doelverduidelijking: productiewaardig, niet productie (12 oktober 2026)

De opdrachtgever verduidelijkt om 08:33 CEST dat dit traject niet naar een
productieomgeving of productie-ingebruikname toewerkt. Het doel is te toetsen
in hoeverre mens en AI samen een functionerende, productiewaardige
doelarchitectuur kunnen realiseren voor een metadata-gestuurde ETL-omgeving
op Microsoft Fabric. Het Contoso v2-model in Databricks is de
referentiearchitectuur.

Productiewaardigheid betekent hier aantoonbare functionele correctheid,
metadata-gestuurd gedrag, reproduceerbare verwerking en deployment,
datakwaliteit en accounting, consistente historie, veilige retries en
herstel, expliciete foutafhandeling, passende toegangsbeveiliging en
traceerbare uitvoeringsbewijzen. Deze eigenschappen moeten in de
ontwikkel-/testomgeving worden getoetst; ze zijn geen claim van bewezen
productieschaal, operationele SLA's of formele productieacceptatie.
Een productie-uitrol, live cutover en inrichting van een productieorganisatie
zijn geen projectdoel of voorwaarde voor afronding. Bestaande technische
acceptatiecriteria blijven gelden voor zover ze deze architectuurkwaliteit
toetsen; een criterium uitsluitend voor productie-ingebruikname moet als
buiten scope worden onderscheiden, niet als ontbrekend frameworkbewijs.

Databricks is de inhoudelijke referentie, geen verplichting om iedere
platformimplementatie letterlijk te kopieren. Vergelijking gebeurt op
bedrijfsuitkomsten, relaties, historie en herstelgedrag met dezelfde
broninputs, regels en businesscutoff. Bewuste platformverschillen worden
onderbouwd; onafhankelijke verwachte uitkomsten blijven nodig om gedeelde
fouten te kunnen herkennen.

Ook de samenwerking wordt verantwoord: menselijke keuzes en acceptatie,
AI-implementatie en onderzoek, controles, correcties, beperkingen en
overdrachten worden herkenbaar vastgelegd. De mens houdt de regie over
doel, afwegingen en eindoordeel; AI-resultaten worden niet op grond van een
succesmelding als correct geaccepteerd.

Eerdere vermeldingen van `production_release_accepted=false` blijven
historisch en machineleesbaar behouden. Zij geven aan dat geen
productieacceptatie is verleend en vormen geen open verplichting om alsnog
een productieomgeving te realiseren. Fase-6-acceptatie blijft afhankelijk
van de volledige Sales-keten en inhoudelijke uitvoeringsbewijzen, niet van
een productie-uitrol.

- **Startdatum:** 10 oktober 2026.
- **Opdrachtgever:** Bob Stoelinga.
- **Architectuurrol:** Principal Data Architect, met focus op Databricks,
  Delta Lake, Data Vault 2.0 en Microsoft Fabric.
- **Bronrepository:** https://github.com/bobstoelinga/Contoso_lakehouse_v2
- **Startbaseline:** branch `main`, commit `bd8936d`.
- **Huidige fase:** fase 5 is afgerond binnen de afgebakende scope generieke ingestie en control-plane-bewijzen; de vier fase-5-exitcriteria zijn met twee afzonderlijke Fabric-jobs geverifieerd. Fase 6 is op 12 oktober 2026 gestart en nog niet afgerond; productieacceptatie is niet verleend. Op 11 oktober 2026 meldde de opdrachtgever dat de Databricks-dev-baseline functioneel is gevalideerd: setup-run `530326121228620` en Sales-pipeline `960091284575444` slaagden tot en met Gold Current; de opdrachtgever leverde zes actuele Gold-viewaantallen en batch-/deliverycontext aan. Dit is een functionele vergelijkingsbaseline, geen bewijs van Fabric-pariteit. Het Databricks-spoor is door de andere AI uitgevoerd en niet onafhankelijk door dit Fabric-spoor gecontroleerd. In Fabric zijn package-import/Spark-hash, synthetische Bronze en Quality/Reject met aparte-jobretries, een drie-leveringenketen tot historische/current Gold met aparte-jobretry, en zes negatieve frameworkhersteltests bewezen. De volledige metadata-gedreven vijf-bronnen/negen-objecten-runtime en nieuwe businessproduct-/consumeracceptatie zijn nog open. De beperkte SQL/OneLake-least-privilegebaseline T-13 is al in fase 4 bewezen; die mag niet worden verward met acceptatie van nieuwe businessproducten. Zie de [fase-4-platformrapportage](15_fabric_fase4_platformbewijs.md) en de uitvoeringsregistraties onderaan dit verslag.

**Actuele fase-5-exitstatus (12 oktober 2026):** de formele exitcontrole is
opnieuw uitgevoerd na de live eerste probe en de afzonderlijke recoveryjob.
Beide Fabric-jobs eindigden `Completed/SUCCEEDED`; de receipts zijn samen
door de exitverifier geaccepteerd (`VERIFIED`, twee verschillende job-ID's).
Daarmee zijn P5-G01 t/m P5-G04 gesloten binnen de synthetische
`PHASE4_PROOF`-scope. De gerichte regressies slagen: **34 passed**.
Fase 5 is binnen die scope afgerond; dit bewijst geen businessdata,
productieacceptatie of fase 6. Zie het
[machineleesbare exitrapport](../deploy/evidence/phase5-exit-review.json),
de [eerste jobreceipt](../deploy/evidence/phase5-exit-streaming-v1-receipt.json),
de [recoveryreceipt](../deploy/evidence/phase5-exit-streaming-v1-retry-receipt.json)
en de [receiptverificatie](../deploy/evidence/phase5-exit-streaming-v1-verification.json).

**Fase-5-checkpoint (11 oktober 2026, historisch):** Python 3.13-projectsuite
**485 passed in 4.04 s** (opnieuw uitgevoerd voor de formele exitcontrole na de duurzame metadatawriter-
en receiptverifierwijzigingen). De eerdere 433- en 460-testbaselines blijven in de
historische registraties behouden. De aparte chainretry is `Completed/SUCCEEDED`:
alle inserts nul, dezelfde drie batchtijden, outputfingerprints en vijftien
commit-ID's; alle commits `VERIFIED`. De zes hersteltests blokkeren de
verwachte conflicten en laten de eerder gecommitte target/commit ongewijzigd.
Dit zijn afgebakende platformbewijzen, niet de volledige fase-5-acceptatie.
Onafhankelijke lokale preflight van `projects/contoso/release.json`
gaf **70** open runtimeblockers: 40 schema's moeten worden gecompileerd, 20 SQL-
expressies formeel beoordeeld, vijf governancepolicies goedgekeurd, drie
Business Vault-implementaties aangevuld, één absence-deletebeleid
goedgekeurd en één Fabric SQL-connectionbinding geverifieerd. Geen van die
gates is door de synthetische probes stilzwijgend goedgekeurd. Echte
API/JDBC/Fabric-SQL-extracts met bedrijfsrecords of Databricks-writes zijn
in deze iteratie niet uitgevoerd. De echte Fabric Sales-view is wel
schema-only gelezen: 33 kolommen, nul records, zowel vanuit de ontwikkel-
interpreter als vanuit een echte Fabric Spark-job met native Entra-
authenticatie. De acht-kolommenprojectie compileert na een bewezen
mappingcorrectie; nieuwe kandidaatrelease
`c9beb08ca216ac512739c00a6dde127190bd7e53713327080abe59a10b5406f4`
blijft niet-geactiveerd. De 0.3.0-windowcorrectie is gepubliceerd en teruggelezen;
de live windowjob is `Completed/SUCCEEDED` en exact receipt-verified:
vier records, population 3, twee UNIQUE-violations, correcte passed A
ondanks filtered duplicate A; thresholdblokkade en all-filtered case
eveneens geslaagd.
De aanvullende read-only regressie met 0.3.0 is eveneens `VERIFIED`:
de oorspronkelijke drie Qualityfingerprints, Bronze-pin, accounting en
twee commits/targets blijven exact gelijk; runtimeprovenance is niet
versoepeld. Geen jobs uit deze bewijsronde staan nog open.

**Aanvullende metadatastand:** de negen werkelijke bronmappings en alle
33 kwaliteitsregels zijn met 0.4.0 op fixtures uitgevoerd en in een
afzonderlijke job exact herhaald. Met 0.5.0 is vervolgens de eerste
duurzame metadata-Customers-slice geverifieerd: Bronze 2, PASSED 1,
REJECTED 1, FILTERED 0, met Bronze- en QUALITY-commit. Dit betreft
geisoleerde proofdata, niet de negen volledige bedrijfsleveringen.
Ook de afzonderlijke durable retry is geverifieerd: nul inserts, dezelfde
fingerprints, batchtijd en commits; zie F5-METADATA-02.

**Formele exitcontrole uitgevoerd op 11 oktober 2026:** controle afgerond,
fase 5 was op dat moment **niet vrijgegeven**. Vier afgebakende ingestie-/controlgaten stonden
open (P5-G01 t/m P5-G04); zie F5-EXIT-01 onderaan. Fase 6 is niet gestart.
Dit oordeel heropent fase 1-4 niet en verlangt niet dat alle Vault/Gold-
of productieacceptatie vooraf onder fase 5 worden voltooid.

Bij iedere volgende migratiestap wordt dit verslag bijgewerkt met datum,
stap-ID, aanleiding, wijzigingen, functionele gevolgen, technische gevolgen,
validatie en resterende acties. Bestaande registraties blijven behouden;
correcties en gewijzigde besluiten worden expliciet toegevoegd.

### Rapportageafspraak: uitgebreide onderbouwing

Op 12 oktober 2026 om 08:34 CEST bevestigt de opdrachtgever dat het project
tot op detailniveau in dit verslag geregistreerd moet blijven voor naslag.
Dit is een doorlopende werkafspraak, niet alleen een eindrapportage.
Na iedere betekenisvolle implementatie-, publicatie-, test- of herstelstap
wordt de registratie bijgewerkt volgens de onderstaande rapportageafspraak.

Voor reproduceerbare naslag worden waar relevant ook metadata-/runtimeversies,
release- en planidentiteiten, artifactchecksums, objectbindingen, job-ID's,
uitvoeringstijden, testscope, verwachte en waargenomen uitkomsten en verwijzingen
naar de concrete receipts en verificaties opgenomen. Fouten, mislukte
benaderingen, oorzaken en herstelmaatregelen blijven zichtbaar; latere
correcties krijgen een nieuwe gedateerde toelichting in plaats van het
historische oordeel stilzwijgend te vervangen.

Het verslag onderscheidt lokale code en tests, gepubliceerde artifacts,
live Fabric-uitvoering, inhoudelijk geverifieerde output en uiteindelijke
architectuuracceptatie. Open punten en onbewezen aannames blijven expliciet.
Menselijke opdrachten en besluiten worden onderscheiden van AI-voorstellen,
uitvoering en controles. Geheimen en vertrouwelijke bronpayloads worden niet
opgenomen; technische details worden via veilige bewijsverwijzingen vastgelegd.
Chatupdates mogen bondig zijn, maar vervangen deze duurzame detailregistratie
niet.

### Publicatievoornemen: LinkedIn-vervolg op Contoso 2.0

Op 11 oktober 2026 om 17:26 CEST geeft de opdrachtgever aan na afronding
van dit traject een LinkedIn-artikel te willen maken als vervolg op
Contoso 2.0. Het artikel wordt pas na afronding opgesteld, met dit verslag
en de uitvoeringsbewijzen als feitelijke basis.

Te bewaren verhaallijnen zijn de migratie van Databricks naar Fabric,
het hergebruik van metadata en bedrijfslogica, de keuze voor drie
Lakehouses, de verschillen in ontwikkel- en deploymentervaring en de
eigen afgebakende REST-bundledeployer met versiegepinde Python-wheels,
ownershipregistratie, readback en inhoudelijke uitvoeringsverificatie.
Ook fouten, herstelstappen en beperkingen horen bij de onderbouwing.

Het artikel moet platformproeven, fixturetests en daadwerkelijke
bedrijfsacceptatie duidelijk onderscheiden. Geen claim van wereldwijde
uniciteit, volledige platformpariteit of productiegeschiktheid zonder
bijbehorend bewijs. Geen tokens, credentials, interne verbindingsdetails
of vertrouwelijke bedrijfsgegevens publiceren. Dit voornemen is geen
opdracht om nu al een artikel te schrijven of te publiceren.

### Uitwerking van de rapportageafspraak

Op 10 oktober 2026 vraagt de opdrachtgever om vanaf nu altijd uitgebreid te
rapporteren over ontwikkelkeuzes, de beslissing, de redenen en de stappen.
Deze afspraak geldt voor vervolgstappen in dit migratieproject, zowel in dit
blijvende verslag als in de terugkoppeling aan de opdrachtgever.

Voor iedere betekenisvolle ontwikkel- of architectuurkeuze wordt vastgelegd:

1. **Aanleiding en doel:** welk probleem of requirement wordt opgelost?
2. **Uitgangssituatie en bewijs:** welke code, configuratie, platformdocumentatie
   of waargenomen uitvoer ondersteunt de keuze; wat is nog een aanname?
3. **Alternatieven:** welke redelijke opties zijn overwogen en waarom vallen
   ze af of blijven ze open?
4. **Advies en afweging:** gevolgen voor functionele werking, historische
   correctheid, beheer, beveiliging, performance, kosten en complexiteit,
   voor zover relevant en onderbouwbaar.
5. **Beslissing:** voorstel of bevestigd besluit, besluit-ID, datum en wie
   heeft bevestigd. Geen gebruikersgoedkeuring afleiden uit een eigen advies.
6. **Uitvoering:** concrete volgorde, gewijzigde bestanden of Fabric-objecten,
   parameters zonder geheimen, en eventuele afwijkingen van het besluit.
7. **Validatie:** wat is gecontroleerd, verwacht en werkelijk waargenomen;
   wat is niet getest en welke beperkingen blijven bestaan?
8. **Herstel en vervolg:** terugval-/hersteloptie, open punten, risico's en
   eerstvolgende stap.

Detail wordt naar betekenis gebundeld: een uitgevoerde migratiestap blijft
herleidbaar, maar iedere losse browserklik krijgt niet een apart besluit.
Een fout, afwijking of gewijzigd inzicht wordt wel expliciet geregistreerd.
Geschatte kosten of performance worden als schatting gemarkeerd en niet als
gemeten resultaat gepresenteerd. Rapportage bevat geen tokens of wachtwoorden.

Statussen: **uitgevoerd**, **in uitvoering**, **voorgesteld**, **besloten**,
**geblokkeerd** en **vervallen**. Een voorstel is geen besluit en een lokale
wijziging is geen bewijs van een geslaagde Fabric-deployment.

Er worden geen wachtwoorden, tokens of andere geheimen in dit verslag opgenomen.

## 2. Functioneel verslag

### Opdracht en uitgangssituatie

De opdrachtgever wil het bestaande metadata-gedreven ETL-project migreren
naar een Fabric-omgeving voor de ontwikkeling van een nieuw project.
Op 10 oktober 2026 is daarnaast gevraagd vanaf nu een herleidbaar technisch,
functioneel en projectverslag bij te houden.

De gelezen projectdocumentatie beschrijft:

- Landing, Bronze, Quality, Reject, Raw Vault, Business Vault, Gold Historisch
  en Gold Actueel.
- Bronobjecten, mappings, kwaliteitsregels en afhankelijkheden in metadata.
- Leveringsgewijze verwerking met een gate voor alle verplichte bronobjecten.
- Chronologische verwerking, gedeelde batchidentiteit en herstartbaarheid.
- Data Vault 2.0-historisatie en behoud van de vorige Gold-publicatie bij falen.
- Push- en pull-bronnen, inclusief aanvullende bronnen naast Contoso Sales.

Dit zijn bestaande ontwerpclaims, nog geen bewezen Fabric-eigenschappen.
De te migreren bronsystemen en de acceptatiecriteria moeten nog worden bevestigd.

### Voorlopige functionele toetsingspunten

Te bevestigen tijdens de scopebepaling:

| ID | Te toetsen gedrag | Status |
|---|---|---|
| F-01 | Een onvolledige levering publiceert geen gedeeltelijke mart | Voorgesteld |
| F-02 | Quality en Reject verklaren samen de ingelezen Bronze-records | Voorgesteld |
| F-03 | Herstart veroorzaakt geen dubbele vault-historie | Voorgesteld |
| F-04 | Hashkeys en historisatie behouden het afgesproken DV2.0-contract | Voorgesteld |
| F-05 | Een mislukte Gold-build laat de vorige consistente publicatie beschikbaar | Voorgesteld |
| F-06 | Herkomst, levering, batch en fouten blijven traceerbaar | Voorgesteld |

## 3. Technisch verslag

### Vastgestelde startbaseline

| Onderdeel | Waarneming | Bewijs |
|---|---|---|
| Lokale broncode | Repository succesvol gecloned naar de projectmap | Geslaagde `git clone` |
| Git-status na clone | `main` gelijk aan `origin/main`; geen lokale wijzigingen | `git status --short --branch` |
| Laatste broncommit | `bd8936d` - Rename praktijkexperiment report files | `git log -1` |
| Databricks-deployment | Asset Bundle met targets `dev`, `tst` en `prd` | [databricks.yml](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/databricks.yml) |
| Opslag en namespaces | Delta-tabellen, Unity Catalog en landingvolumes volgens documentatie | [README](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/README.md), [architectuur](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/docs/01_architecture.md) |
| Orchestratie | Workflows, notebooks, taskValues, leveringsgate en schedules volgens documentatie | [workflow-ontwerp](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/docs/05_workflow_design.md) |
| Fabric-doelomgeving | Nog niet vastgesteld of gecontroleerd | Open punt O-01 |

De projectmap staat op Google Drive. Git meldde dat dit bestandssysteem geen
eigenaarschap registreert. De controles zijn uitgevoerd met een tijdelijke,
repository-specifieke `safe.directory`-optie. De globale Git-configuratie is
niet gewijzigd; reguliere Git-opdrachten kunnen de melding opnieuw geven.

### Nog te onderzoeken migratieoppervlakken

| Onderdeel | Onderzoeksvraag | Status |
|---|---|---|
| Unity Catalog en volumes | Hoe worden namespaces, rechten en landingpaden op Fabric ingericht? | Open |
| Auto Loader | Welke ingestieaanpak bewaart manifests, schema-evolutie en leveringsgrenzen? | Open |
| Metadata en audit | Welke opslag en transacties ondersteunen gates, leases en runstatussen? | Open |
| Notebooks en framework | Welke concrete Databricks-API's en SQL-constructies vragen aanpassing? | Open |
| Delta en Data Vault | Blijven hashing, MERGE, historisatie en idempotentie gelijk? | Open |
| Gold-publicatie | Welke publicatieaanpak garandeert samenhang over meerdere tabellen en lezers? | Open |
| Orchestratie | Hoe worden parameters, retries, parallelisme en monitoring overgezet? | Open |
| Beheer en deployment | Hoe worden omgevingen, identities, CI/CD en onderhoud ingericht? | Open |

Er is nog geen keuze gemaakt voor een specifieke Fabric-runtime,
Lakehouse/Warehouse-indeling, pipeline-aanpak of semantisch model.
Ondersteuning en beperkingen moeten tegen actuele Microsoft-documentatie en
de daadwerkelijke doelomgeving worden geverifieerd voordat implementatiebesluiten
worden genomen.

## 4. Projectverslag

### Stappenregister

| Stap | Datum | Status | Activiteit en resultaat | Validatie |
|---|---|---|---|---|
| M-001 | 2026-10-10 | Uitgevoerd | Bronrepository gecloned; startbaseline vastgelegd | Git-status, remote en laatste commit gecontroleerd |
| M-002 | 2026-10-10 | Uitgevoerd | Eerste inventarisatie van architectuur en migratieoppervlakken | Documentatie, centrale runtimepaden, workflow, SQL en seedmetadata onderzocht; bevindingen in secties 6-9; geen volledige runtimecertificering |
| M-003 | 2026-10-10 | Uitgevoerd | Dit migratieverslag aangemaakt en vindbaar gemaakt vanuit de README | Documentstructuur en relatieve links lokaal gecontroleerd; geen runtimewijzigingen |
| M-004 | 2026-10-10 | Uitgevoerd | Fabric-compatibiliteitsmatrix, doelarchitectuurvoorstel en bewijstests vastgelegd | Statische code-inspectie, lokale JSON-inventarisatie en actuele Microsoft Learn-documentatie; geen Fabric-tests uitgevoerd |
| M-005 | 2026-10-10 | Uitgevoerd | Doelworkspace na gebruikersaanmelding read-only gecontroleerd | Naam en lege itemlijst waargenomen; Workspace settings toont Power BI Pro, niet de verwachte Fabric-capaciteitstoewijzing |
| M-006 | 2026-10-10 | Uitgevoerd | Met expliciete toestemming de ontwikkelworkspace aan de bestaande Fabric Trial-capaciteit toegewezen | UI meldt "Your changes were successfully saved"; Current workspace type = Fabric Trial; capaciteit-ID zichtbaar |
| M-007 | 2026-10-10 | Uitgevoerd | Spark-instellingen en beschikbare runtimes read-only gecontroleerd | Workspace-runtime 2.0 (Spark 4.1, Delta 4.2), default Environment Off; opties bekeken zonder instelling op te slaan |
| M-008 | 2026-10-10 | Uitgevoerd | Fase 1: inventarisatie, lokale baseline, contextproef en acceptatiecontract | 133 tests geslaagd; actuele acceptatiebasis zonder Databricks-pariteit vastgelegd; oorspronkelijke externe bewijsblokkade vervallen volgens B-FAB-014 |
| M-009 | 2026-10-10 | Uitgevoerd | Formele fase-1-exitcontrole en overdracht naar fase 2 | Tests herhaald: 133 passed in 0.50s; metadatarelease geldig; open productie-/implementatiepunten met vervolgfasen geregistreerd |
| M-010 | 2026-10-10 | In uitvoering | Fase 2 gestart op verzoek opdrachtgever; template- en architectuurcontract versie 0.1 opgesteld | Bestaande connector-/mapping-/Gold-/auditcontracten gelezen; ontwerpen en open consumerkeuze vastgelegd; geen Fabric-runtimevalidatie |
| M-011 | 2026-10-10 | Uitgevoerd, vervolg geblokkeerd op API-identiteit | Nieuwe geteste Fabric-kern, foundationrunner en drie ontwikkel-Lakehouses; volledige redeploy als eindacceptatie toegevoegd | Lokale tests en baselinecheck geslaagd; browserfoundation bewezen; echte CLI-run faalt expliciet met UserNotLicensed; geen full-chain runtimebewijs |
| M-012 | 2026-10-10 | Uitgevoerd | API-tenantdiagnose, expliciete tenantbinding en herhaalde echte foundationreconciliatie | Juiste tenant gaf API-toegang; runner tweemaal SUCCEEDED, gelijke IDs; 207 tests geslaagd. Volledige ETL-redeploy nog open |
| M-013 | 2026-10-10 | Uitgevoerd | Fase-2-contracten vastgesteld en formele ontwerp-exit onder expliciet mandaat | Metadata, type-/recordidentiteit, exclusive ownership, eventhistorie en pinned DirectQuery besloten; tests T-01 t/m T-15 overgedragen, niet als uitgevoerd geclaimd |

### Besluitenregister

| Besluit | Datum | Status | Besluit en aanleiding |
|---|---|---|---|
| B-FAB-001 | 2026-10-10 | Besloten | Vanaf nu functionele, technische en projectmatige migratiestappen blijvend vastleggen, op verzoek van de opdrachtgever |
| B-FAB-002 | 2026-10-10 | Voorgesteld | Eerst scope, doelomgeving en compatibiliteit vaststellen; daarna een afgebakende end-to-end proefmigratie |
| B-FAB-003 | 2026-10-10 | Voorgesteld | Spark/Delta en metadata als kern behouden; platformbindingen expliciet vervangen in plaats van het framework integraal naar T-SQL of Dataflows te herschrijven |
| B-FAB-004 | 2026-10-10 | Besloten | Opdrachtgever kiest de eerste proef met alle vijf Contoso Sales-objecten: Customers, Products, Orders, Returns en Employees; aanvullende bronnen vallen buiten de eerste proef |
| B-FAB-005 | 2026-10-10 | Besloten | Opdrachtgever geeft expliciet toestemming de ontwikkelworkspace om te zetten naar Fabric Trial voor de proef; geen reguliere betaalde capaciteit ingericht |
| B-FAB-006 | 2026-10-10 | Besloten | Opdrachtgever vraagt altijd uitgebreide rapportage van ontwikkelkeuzes, alternatieven, beslissing, onderbouwing, uitvoering en validatie; werkwijze vastgelegd in sectie 1 |
| B-FAB-007 | 2026-10-10 | Besloten | Opdrachtgever bevestigt drie schema-enabled Lakehouses voor de Sales-proef: control, processing en gold; afzonderlijke rechten blijven noodzakelijk |
| B-FAB-008 | 2026-10-10 | Besloten | Einddoel is een herbruikbaar metadata-gedreven ETL-template voor eerst Microsoft Fabric, met Contoso als voorbeeldproject; dubbele Fabric-/Databricks-runtimeondersteuning valt buiten de eerste scope |
| B-FAB-009 | 2026-10-10 | Besloten | Opdrachtgever breidt de gewenste migratie- en acceptatiescope uit naar alle daadwerkelijk in Databricks ontsloten bronnen; welke bronnen operationeel ontsloten zijn moet worden bevestigd met bron-/runbewijs |
| B-FAB-010 | 2026-10-10 | Besloten | Opdrachtgever bevestigt de aanpak en stelt productierijpheid als harde einddoelstelling: het Fabric-template moet niet slechts een demo of proof of concept zijn; productieacceptatie vereist aantoonbare kwaliteit en beheerbaarheid |
| B-FAB-011 | 2026-10-10 | Besloten | Opdrachtgever mandateert zelfstandige besluiten en implementatie voor fase 1 zonder aanvullend akkoord; geen automatische uitbreiding naar externe productie-ingrepen |
| B-FAB-012 | 2026-10-10 | Besloten door architect onder mandaat | Referenties op dezelfde immutable input vergelijken, nul onverklaarde businessverschillen en expliciete fout-/herstelcriteria; historische documentatie niet als live geverifieerd bewijs behandelen |
| B-FAB-013 | 2026-10-10 | Besloten door architect onder mandaat | Na melding dat Databricks weg is: live baselineverificatie vervalt; onafhankelijke golden contracttests en Fabric-uitvoering worden het bewijs, zonder onbewezen historische platformpariteit te claimen |
| B-FAB-014 | 2026-10-10 | Besloten door opdrachtgever | Een zelfstandig goed functionerend, herbruikbaar en productiegeschikt Fabric-systeem is het doel; aansluiting op Databricks vervalt als eis. Eigen contracttests blijven noodzakelijk |
| B-FAB-015 | 2026-10-10 | Besloten door opdrachtgever | Volledige keten van Raw/Landing tot en met Gold Current realiseren, met alle tussenlagen; geen gereduceerde Bronze-naar-Gold-oplossing |
| B-FAB-016 | 2026-10-10 | Besloten door architect onder expliciet mandaat | Fase 1 afgerond volgens actuele scope; vrijgave voor fase 2. Implementatie-, bronconnectiviteits- en productietests blijven vervolgvoorwaarden, geen gerealiseerde garanties |
| B-FAB-017 | 2026-10-10 | Besloten door opdrachtgever | Primaire Gold Current-consumerroute: DirectQuery via het Fabric SQL analytics endpoint, zonder Power BI Import; Direct Lake niet vereist in de eerste implementatie |
| B-FAB-018 | 2026-10-10 | Mandaat opdrachtgever | Doorgaan tot het einde; architect neemt vervolgkeuzes binnen afgesproken scope. Geen impliciete productiecertificering, credentialtoegang of aankoop van betaalde capaciteit |
| B-FAB-019 | 2026-10-10 | Besloten door opdrachtgever | Aan het projecteinde alle stappen op verzoek aan de assistent reproduceerbaar kunnen redeployen; tweede volledige uitrol en feedbackrapport verplicht als acceptatiebewijs |
| B-FAB-020 | 2026-10-10 | Besloten door architect onder vervolgmandaat | Nieuwe Fabric-kern naast ongewijzigde v2-baseline; hashconventie fabric-dv-key-v1 gebruikt een JSON-array met namespace en geordende genormaliseerde strings, NULL en leeg onderscheiden; geen migratie van oude hashes zonder expliciet pad |
| B-FAB-021 | 2026-10-10 | Besloten door architect onder vervolgmandaat | Verplichte expliciete tenant_id in omgevingscontract 1.1; tokenacquisitie voor doeltenant, niet CLI-default. Verkeerde tenant was de oorzaak van UserNotLicensed bij deze workspace |
| B-FAB-022 | 2026-10-10 | Besloten door architect onder fase-2-mandaat | Volledige projectmetadata 1.0 met expliciete collecties, DAG, targets en SQL-fingerprints; getagde typecanonicalisatie en Raw-recordlocators/multiset behouden multipliciteit |
| B-FAB-023 | 2026-10-10 | Besloten door architect onder fase-2-mandaat | Eerste runtime één writer per project; vooraf bestaande Delta-owner-rij met conditionele UPDATE, readback en fail-closed cardinaliteit; geen automatische TTL-overname; echte conflict-/stopproeven verplicht vóór writer-vrijgave |
| B-FAB-024 | 2026-10-10 | Besloten door architect onder fase-2-mandaat | Authoritative historie als committed immutable events; intervallen afleiden, geen batchpurge of UPDATE-then-INSERT als SCD2-waarheid; PIT/as-of-joins expliciet |
| B-FAB-025 | 2026-10-10 | Besloten door architect onder fase-2-mandaat | DirectQuery-consumers pinnen immutable productrelease via release-specifieke views en kandidaatmodel/rapport naast vorige versie; general Current-views beloven alleen queryconsistentie |
| B-FAB-026 | 2026-10-10 | Besloten door architect onder fase-2-mandaat | Fase 2 afgerond met ontwerpcontract en bewijsopdrachten; vrijgave voor fase 3, niet voor productie. Numerieke zakelijke policies blijven verplichte go-live-inputs |
| B-FAB-027 | 2026-10-10 | Besloten onder fase-3-mandaat | Strikte complete kandidaatmetadata; runtimevrijgave afzonderlijk en fail-closed. Zie [fase-3-verslag](14_fabric_fase3_release_acceptatie.md) |
| B-FAB-028 | 2026-10-10 | Besloten onder fase-3-mandaat | Zelfstandige Fabric-wheel en geordend lokaal definitieplan; geen stil meenemen van de v2-runtime |
| B-FAB-029 | 2026-10-10 | Besloten onder fase-3-mandaat | Volledige getypeerde Gold-declaraties met DDL-herkomstbewijs; compilegate blijft verplicht |
| B-FAB-030 | 2026-10-10 | Besloten onder fase-3-mandaat | Control-DDL voorbereid zonder runtime-lockinitialisatie; echte bootstrap/concurrentie volgt in fase 4 |

### Open punten en risico's

| ID | Type | Punt | Vervolgactie |
|---|---|---|---|
| O-01 | Deels beantwoord | Fabric Trial-toewijzing geverifieerd; capaciteit-ID, North Europe, SKU FTL64 en workspace-runtime 2.0 bekend; default Environment Off | Expliciete Environment en daadwerkelijke Spark-uitvoering/rechten nog controleren |
| O-02 | Beantwoord | SALES, CBS, ECB, NAGER en FABRIC_SALES in scope; SharePoint uitgesloten | Eigen broncontracten en test-/acceptatieuitkomsten uitwerken; geen oude Databricks-referentieruns vereist |
| O-03 | Vervallen voor Databricks-overname | Aansluiting op verdwenen Databricks-omgeving niet vereist volgens B-FAB-014 | Nieuwe Raw-leveringen en laad-/replaybeleid voor Fabric uitwerken; geen historische reconstructie |
| O-04 | Open punt | Nieuwe projectnaam, branch en toekomstige repository nog niet bepaald | Werk- en versiebeheerafspraken bevestigen; nu geen nieuwe branch of remote aangemaakt |
| O-05 | Deels beantwoord | Technische acceptatiecriteria vastgelegd; productievolumes, SLA's, budget en RPO/RTO nog open | In vervolgfasen concretiseren voor performance-/hersteltests en productieacceptatie; geen blocker voor fase-2-ontwerp |
| R-01 | Risico | Databricks-specifieke ingestie, API's en autorisatie zijn niet zonder meer overdraagbaar | Runtimecode en SQL inventariseren en gericht testen |
| R-02 | Risico | Gold-consistentie over meerdere tabellen is geen automatisch gevolg van Delta | Publicatiecontract expliciet ontwerpen en met falende runs en gelijktijdige lezers testen |
| R-03 | Risico | Hash- of tijdsemantiek kan bestaande vault-historie veranderen | Gelijke hashuitkomsten en historie bewijzen met referentiedatasets |
| R-04 | Risico | Git-controles vragen op deze opslaglocatie een expliciete vertrouwensinstelling | Repository-specifieke aanpak behouden of met opdrachtgever een andere opslaglocatie kiezen |

### Validatie en overdracht

- **Wel uitgevoerd:** clone en controle van de Git-startbaseline; eerste
  document- en runtimeinventarisatie; JSON-aantallen bepaald; Fabric-bronnen
  geraadpleegd; opzetten en uitbreiden van dit verslag.
- **Niet uitgevoerd:** codeaanpassingen aan de ETL, tests van het framework,
  Fabric-inrichting, datamigratie, deployment of end-to-end acceptatie.
- **Eerstvolgende stap:** scope en Fabric-doelomgeving afstemmen, daarna het
  fysieke naamgevings-, ingestie-, runcontext- en publicatiecontract ontwerpen.
- **Versiebeheer:** verslag lokaal opgeslagen; nog niet gecommit of gepusht.

## 5. Registratievorm voor vervolgstappen

Elke volgende stap krijgt een eigen registratie met:

1. Stap-ID, datum en status.
2. Aanleiding en gekoppelde requirement, besluit of risico.
3. Uitgevoerde actie en gewijzigde bestanden of Fabric-objecten.
4. Functionele gevolgen en technische ontwerpkeuzes.
5. Validatie: omgeving, test of run-ID, verwacht en waargenomen resultaat.
6. Afwijkingen, herstel-/terugdraaiaanpak en open acties.
7. Commit- of deploymentreferentie, uitsluitend wanneer daadwerkelijk aanwezig.

## 6. Inventarisatie van de feitelijke architectuur

**Datum:** 10 oktober 2026. **Bewijsniveau:** statische broncode-inspectie en
lokale seedanalyse. Dit bewijst codepaden en configuratie, niet de uitvoering
in Databricks of Fabric.

### 6.1 Omvang en bronnen

De JSON-seeds bevatten zes bronsystemen, waarvan vijf actief, en elf
bronobjecten, waarvan negen actief:

| Bronsysteem | Objecten | Actief | Route en extractie |
|---|---|---|---|
| SALES | Customers, Products, Orders, Returns, Employees | Ja, alle vijf | Landingbestanden naar Raw Vault; Customers, Products en Employees als snapshot; Orders en Returns als append |
| CBS | Jeugdzorg Wijk 2025 | Ja | HTTP JSON naar Landing en Raw Vault |
| ECB | Exchange Rate | Ja | HTTP CSV naar Landing en REFERENCE_DATA |
| NAGER | Holidays NL | Ja | HTTP JSON naar Landing en REFERENCE_DATA |
| FABRIC_SALES | Order Lines | Ja | JDBC-snapshot uit Fabric SQL Database naar Landing en REFERENCE_DATA |
| SHAREPOINT | Price Agreements, Sales Budgets | Nee | Raw Vault-configuratie aanwezig, maar buiten de actieve bronconfiguratie |

SALES heeft in de seed geen expliciete `processing_route`; de runtime vult
`RAW_VAULT` in. De bestaande Fabric-connector maakt Fabric hier een
**bronsysteem**, niet het huidige uitvoerplatform.

| Seedonderdeel | Records, inclusief inactieve configuratie |
|---|---:|
| Bronobjecten | 11 |
| Bronconnectors | 4 |
| Bron-doelmappings | 102 |
| Kwaliteitsregels | 37 |
| Afhankelijkheden | 45 |
| DV-entiteiten | 25: 8 hubs, 5 links, 9 satellites, 2 link-satellites, 1 PIT |
| Gold-entiteiten | 23: 11 historisch, 12 actueel |

Dit zijn aantallen metadatarecords, geen telling van daadwerkelijk uitgerolde
tabellen. De oorspronkelijke README beschrijft een kleinere Sales-scope dan
de huidige seed. Voor de migratiescope is de seedinventarisatie leidend als
startpunt, waarna de opdrachtgever bevestigt wat werkelijk mee moet.

Bewijs: [bronsystemen](../metadata/seed/meta_source_system.json),
[bronobjecten](../metadata/seed/meta_source_object.json),
[connectors](../metadata/seed/meta_source_connector.json),
[DV-entiteiten](../metadata/seed/meta_dv_entity.json) en
[Gold-entiteiten](../metadata/seed/meta_gold_entity.json).

### 6.2 Metadata-aansturing

- [MetadataRepository](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/src/contoso_lakehouse/metadata.py) leest actieve
  configuratie uit Delta, cachet bron-, DV- en Gold-entiteiten en ordent mappings
  op `ordinal_position`.
- [Settings](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/src/contoso_lakehouse/context.py) vertaalt placeholders naar
  omgevingsspecifieke catalogi. De huidige fysieke naam is
  `catalog.schema.table`; dit is een Unity Catalog-binding.
- [Seedloader](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/src/contoso_lakehouse/seed.py) synchroniseert de JSON-release
  met `MERGE` en markeert verdwenen configuratierecords inactief. Een
  SHA-256-fingerprint registreert de metadatarelease.
- [Releasevalidatie](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/src/contoso_lakehouse/release_check.py) controleert
  structuur, sleutels, verwijzingen en policies. Runtimevalidatie zit in
  [notebook 99](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/notebooks/99_validate_metadata.py).
- [Orchestrator](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/src/contoso_lakehouse/orchestration.py) bepaalt de
  topologische volgorde uit `meta_dependency`; laagovergangen staan wel in
  [de workflow](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/workflows/pipeline.job.yml).

De metadata bevat naast logische configuratie ook fysieke paden en uitvoerbare
Spark SQL-expressies. Daarom kan de metadata-inhoud niet integraal ongewijzigd
naar een ander SQL-dialect of andere namespace worden gekopieerd.

### 6.3 ETL-stroom

1. **Validatie en manifestregistratie:** metadata controleren, gesloten
   push-manifests registreren en de te laden Bronze-objecten bepalen.
2. **Bronze:** [BronzeLoader](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/src/contoso_lakehouse/bronze.py) gebruikt
   `cloudFiles`, schemaopslag, checkpoints en bestandsmetadata. `foreachBatch`
   splitst records per levering en verwerkt die gesorteerd.
   [Notebook 10](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/notebooks/10_bronze_autoloader.py) verwerkt de objecten
   na elkaar binnen dezelfde computecontext; de taaknaam fanout betekent
   niet dat deze Bronze-objecten werkelijk parallel laden.
3. **Gate:** [auditviews](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/sql/01_metadata/11_audit_model.sql) en de
   Orchestrator selecteren de eerstvolgende geschikte levering. De
   selectielogica betrekt een gesloten manifest, object-/bestandsaantallen,
   snapshotverklaring, quarantainestatus en bestaande Gold-publicatie.
4. **Quality en Reject:** [QualityEngine](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/src/contoso_lakehouse/quality.py)
   projecteert en typeert mappings, evalueert metadataregels, registreert
   drempeloverschrijdingen en schrijft passed en rejected records.
5. **Reconciliatie:** [ReconciliationEngine](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/src/contoso_lakehouse/reconciliation.py)
   vergelijkt Quality plus Reject met Bronze **na de eventuele
   `quality_filter_expression`**. De bestaande controle bewijst aantallen,
   niet afzonderlijk dat iedere recordidentiteit precies eenmaal voorkomt.
6. **Vault en referentiedata:** hubs en links laden via MERGE, satellites via
   historie-inserts plus een aparte actuele hashdiff-state. De
   REFERENCE_DATA-route gebruikt eigen SCD2-updates en inserts.
7. **Gold Historisch:** metadata-SQL levert de historiserende projectie;
   [GoldLoader](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/src/contoso_lakehouse/gold.py) voegt ontbrekende
   sleutelcombinaties toe met MERGE.
8. **Gold Actueel:** alle entiteiten in een brongebonden publicatiegroep
   worden in inactieve slots gebouwd. Een lease beschermt de groep;
   `audit_gold_publication_group` wordt via een enkele MERGE omgezet.
   [Publieke views](../sql/05_gold/51_gold_current.sql) selecteren hun
   slot via deze groepspointer.
9. **Beheer:** audit-events, work-itemleases, dead letters, SLA-monitoring,
   quarantaineremediation, metadata-onderhoud en een Streamlit Control Room.

### 6.4 Ontwerpclaims die niet als bewezen mogen worden overgenomen

| ID | Geobserveerd verschil of risico | Gevolg voor de migratie |
|---|---|---|
| V-01 | De documentatie claimt een gedeelde `load_date`. `RunContext.create` genereert echter zelf een tijdstip en notebook 30 ontvangt geen gedeelde `load_date`-parameter | Een persistente delivery/runcontext ontwerpen en dezelfde tijd expliciet doorgeven; gelijkheid over lagen en retries testen |
| V-02 | `VaultLoader.load` wist eerst de huidige batch uit de fysieke tabel; satellites vergelijken daarna met een afzonderlijke actuele hashdiff-state | Hypothese: bij falen na de state-MERGE kan herstart historie verwijderen zonder herinsert. Eerst reproduceren; geen herstelgarantie claimen op basis van comments |
| V-03 | De gate filtert manifests en uitgezonderde deliveries voordat de laagste volgorde wordt gekozen | Testen of een oudere open/onvolledige levering werkelijk een nieuwere complete levering blokkeert, inclusief gewenste quarantaine-uitzonderingen |
| V-04 | Gold gebruikt nu een groepspointer; de sequentiediagrammen in het oorspronkelijke workflow-ontwerp beschrijven nog per-view vervanging | Runtimecode als feitelijke baseline nemen; pointeratomiciteit niet verwarren met gegarandeerde consistente lezing over alle tabellen en engines |
| V-05 | De seed bevat een PIT; de generieke `load` ondersteunt alleen hub, link en satellite-typen en `load_zone` slaat PIT over | PIT-opbouw en de daadwerkelijke actieve uitvoerroute afzonderlijk vaststellen voor deze als gemigreerd geldt |
| V-06 | Loaders lezen Databricks-achtige DML-resultaatkolommen; bij ontbrekende rij/statistiek kan het gerapporteerde aantal nul worden | Fabric-resultaatvorm meten en auditmetrics expliciet implementeren; nul is geen bewijs van een geslaagde lege load |

V-02 is een statisch onderbouwde foutscenariohypothese, nog niet met een
runtimeproef gereproduceerd. Er zijn in deze stap geen bestaande ETL-bugs
gewijzigd.

## 7. Fabric-compatibiliteit en doelarchitectuurvoorstel

### 7.1 Geraadpleegde platformfeiten

Microsoft Learn is op 10 oktober 2026 geraadpleegd:

- Fabric Runtime 2.0 wordt als GA beschreven met Spark 4.1, Delta Lake 4.2
  en Python 3.13. Runtime 1.3 heeft Spark 3.5 en Delta 3.2 en staat als EOSA
  beschreven. Niet uitgaan van de runtime die toevallig workspace-default is;
  de werkelijk beschikbare en gekozen runtime moet nog worden vastgesteld.
- Schema-enabled lakehouses ondersteunen schema's en een vierdelige
  Spark-namespace `workspace.lakehouse.schema.table`.
- De Notebook activity ondersteunt notebookparameters uit een parametercel
  en uitvoering met Workspace Identity, onder de beschreven tenant- en
  toegangsvoorwaarden.
- NotebookUtils biedt filesystem-, notebook-, runtime- en credentialhelpers.
  Dit is geen bewijs dat iedere `dbutils`-operatie dezelfde semantiek heeft.
- Het Lakehouse SQL analytics endpoint kan Delta-data lezen, maar geen
  INSERT/UPDATE/DELETE op die data uitvoeren. T-SQL-views kunnen wel worden
  gemaakt. SQL-endpointrechten gelden niet automatisch voor Spark-toegang.

De precieze SQL-constructies van dit project zijn nog niet tegen een Fabric
session gecompileerd. Aanwezigheid van Spark en Delta is onvoldoende om alle
Databricks-extensies ondersteund te verklaren.

### 7.2 Migratiematrix

**Herbruikbaar** betekent behoud van het concept of algoritme na gerichte
validatie, niet dat ongewijzigde code al in Fabric is getest.

| Onderdeel | Beoordeling | Benodigde actie en bewijs |
|---|---|---|
| Logische bron-, mapping-, DQ- en dependency-ID's | Herbruikbaar | Identiteiten en relaties behouden; fysieke velden apart configureren |
| Python-hashing en dependencyplanning | Grotendeels herbruikbaar | SHA-256, NULL/lege-stringbeleid, collision code en kolomvolgorde behouden; werkelijke hashwaarden vergelijken |
| Quality, reconciliatie, Vault en referentiedata | Kern herbruikbaar, SQL valideren | Types, ANSI/castgedrag, tijdzone, functies, MERGE en foutscenario's bewijzen |
| Catalogi en landingvolumes | Aanpassen | Workspace/Lakehouse/schema-mapping en OneLake-/eventuele ADLS-paden; geen blinde stringvervanging |
| Auto Loader (`cloudFiles`) | Vervangen | Voorstel: manifestgestuurde batch-ingest met duurzaam bestandsregister en schema-contract; geen automatische overdracht van Auto Loader-checkpoints |
| Bestandsmetadata en rescued data | Aanpassen | Herkomstvelden en onbekende/type-afwijkende kolommen expliciet verzamelen; geen equivalentie veronderstellen |
| `dbutils.widgets`, taskValues en notebook exits | Vervangen | Parametercellen, pipelineparameter-/outputcontract en NotebookUtils |
| Asset Bundles, jobs en file-arrival-configuratie | Vervangen | Fabric-itemdeployment, Notebook activities, retries, schedules en gekozen triggerontwerp |
| Metadata- en audit-DDL | Aanpassen | Onder andere `USE CATALOG`, PK/FK `RELY`, defaults, CDF, `GROUP BY ALL`, `QUALIFY` en tabelproperties gericht compileren |
| `MERGE WITH SCHEMA EVOLUTION`, `CLUSTER BY` | Runtimeafhankelijk onderzoeken | Ondersteuning en effect meten op de gekozen runtime; alleen noodzakelijke dialectaanpassingen uitvoeren |
| SQL-views en Gold-consumptie | Apart consumercontract | Spark-views niet automatisch als T-SQL-object of Direct Lake-contract beschouwen; SQL-views apart deployen en zichtbaarheid meten |
| Gold-groepspointer en slots | Concept behouden, integraal bewijzen | Slothergebruik, actieve lezers, publicatiefalen, SQL-metadata/datazichtbaarheid en modelrefresh testen |
| Work-item- en publicatieleases | Aanpassen en concurrerend testen | Ownership, expiratie, claimconflicten, retries en herstel aantonen |
| HTTP/JDBC-extracten | Deels herbruikbaar | Extractie behouden; authenticatie, staging, manifesttelling en publicatiesemantiek opnieuw valideren |
| Grants, secrets en identities | Vervangen | Workspace-/item-/OneLake-/SQL-rechten, Workspace Identity en secrets zonder credentialkopie |
| Onderhoud | Aanpassen | [Notebook 90](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/notebooks/90_maintenance.py) gebruikt UC information_schema en managed-table-discovery; Fabric-discovery en OPTIMIZE/VACUUM-policy valideren |
| Control Room | Aanpassen | [App](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/app/streamlit_app.py) gebruikt Databricks SQL en SDK-jobacties; hosting, backend, authenticatie en remediation-API ontwerpen |
| Tests en release-gates | Behouden en uitbreiden | Bestaande checks behouden; Fabric-integratie, failures en consumerconsistentie toevoegen |

### 7.3 Voorgestelde inrichting, nog niet besloten

```text
Bronnen: bestanden / HTTP / JDBC
              |
              v
Landing in OneLake Files of gecontroleerde ADLS-koppeling
              |
      Manifest + bestandsregister
              |
              v
Bronze -> delivery-gate -> Quality + Reject -> reconciliatie
                                              |
                               +--------------+-------------+
                               v                            v
                       Raw / Business Vault          Reference Data
                               +--------------+-------------+
                                              v
                                       Gold Historisch
                                              |
                                       Gold-kandidaten
                                              |
                               Gevalideerde groepspublicatie
                                              |
                              SQL / semantisch model / rapport

Metadata + audit + runcontext sturen alle stappen.
Fabric Data Factory orkestreert; notebooks voeren Spark/Delta-logica uit.
```

Aanbevolen startpunt:

1. Afzonderlijke workspaces voor `dev`, `tst` en `prd`, als de doelomgeving en
   capaciteit dit toelaten; identities en toegang per omgeving vastleggen.
2. Schema-enabled Lakehouse(s) voor de Delta-lagen. De keuze tussen een
   compact proeflakehouse en meerdere lakehouses voor opslag-/toegangsscheiding
   volgt uit governance en doelgroeprechten, niet alleen uit de bestaande
   catalogusnamen.
3. Metadata en audit voorlopig eveneens in Delta, zodat de bestaande Spark-
   toegang niet meteen naar JDBC/T-SQL hoeft. Een afzonderlijke transactionele
   control store alleen kiezen bij aangetoonde eisen die dit rechtvaardigen.
4. Pipelineactiviteiten op laagniveau; entiteiten en volgorde blijven
   metadata-gedreven. Begin met begrensde uitvoering, daarna paralleliseren.
5. Een expliciete, gepubliceerde Fabric Environment met gekozen runtime en
   frameworkpackage; geen afhankelijkheid van toevallige sessie-installaties.
6. Gold-consumptie apart ontwerpen. Eerst kiezen tussen SQL/DirectQuery,
   Import en Direct Lake voordat publicatieconsistentie wordt geaccepteerd.

Een Warehouse of SQL Database is dus geen verplicht doel voor de hele ETL.
Evenmin is besloten dat alle logische lagen fysiek in hetzelfde lakehouse komen.

## 8. Bewijsplan en projectfasering

### 8.1 Benodigde tests voor de eerste proef

| Test | Verplicht scenario | Meetbaar verwacht resultaat |
|---|---|---|
| T-FAB-01 | Dezelfde levering tweemaal aanbieden | Geen extra Bronze-recordidentiteiten of vault-versies |
| T-FAB-02 | Verplicht object ontbreekt; manifest blijft open; nieuwere levering is compleet | Geen verwerking/publicatie die de afgesproken chronologie omzeilt |
| T-FAB-03 | Ongeldige records, waarschuwingen en Quality-filter | Gefilterde Bronze-count = Quality + Reject; daarnaast identiteit-/duplicaatcontrole en expliciete telling van weggefilterde records |
| T-FAB-04 | NULL, lege tekst, spaties, case, Unicode, decimalen en timestamps in keys/attributen | Hashkeys/hashdiffs gelijk aan dezelfde Databricks-referentie-uitkomsten |
| T-FAB-05 | Falen na satellite-insert, na state-update en tijdens retry | Historie behouden; geen ontbrekende of dubbele versies; state overeenstemmend met historie |
| T-FAB-06 | Meerdere lagen, notebookstarts en retries voor een delivery | De afgesproken `batch_id` en persistente `load_date` blijven gelijk |
| T-FAB-07 | Falen tijdens Gold-build en op iedere publicatiegrens | Vorige complete release beschikbaar; geen gemengde nieuwe/oude dimensies en feiten |
| T-FAB-08 | Twee gelijktijdige writers en een langlopende reader; volgend slothergebruik | Eén leasehouder; consumercontract houdt een consistente release vast |
| T-FAB-09 | Nieuwe kolom, typewijziging en ontbrekende goedkeuring | Gedrag conform STRICT/RESCUE/approval-policy, met foutregistratie en herkomst |
| T-FAB-10 | Query via werkelijk gekozen SQL-/Power BI-consumptieroute | Verwachte release zichtbaar; gemeten latency binnen nog af te spreken SLA; geen toegang tot ongepubliceerde slots |
| T-FAB-11 | PIT-/historieopbouw over meerdere leveringen | PIT en SCD2-uitkomsten aantoonbaar correct; niet alleen tabellen aangemaakt |
| T-FAB-12 | Falen en herstel van extract, notebook en control claim | FAILED-event en herleidbare run-ID; incomplete extractie niet beschikbaar als gesloten levering |

De bestaande tests bevatten metadata-/expressiechecks en tests met
`RecordingSpark`-doubles voor de publicatie-SQL. Die leveren nuttige regressie-
dekking, maar bewijzen geen ACID-/concurrencygedrag in Fabric. Ze zijn in deze
inventarisatiestap niet uitgevoerd; er zijn uitsluitend documenten gewijzigd.

### 8.2 Voorgestelde fasen en exitcriteria

| Fase | Deliverable | Exitcriterium |
|---|---|---|
| 0. Inventarisatie | Dit verslag met scopebasis en compatibiliteitsmatrix | Eerste analyse vastgelegd; nu bereikt, niet gelijk aan volledige compatibiliteitscertificering |
| 1. Ontwerp en scope | Bevestigde bronnen, doelomgeving, namespaces, runcontext, ingestie- en consumercontract | Opdrachtgever bevestigt keuzes; technische toegang en runtime vastgesteld |
| 2. Foundation | Workspace(s), identities, Environment, package, metadata-/audit-/Delta-DDL en deploymentroute | Setup herhaalbaar en compileerbaar; metadatarelease traceerbaar |
| 3. End-to-end proef | Geselecteerde Sales-route over meerdere leveringen | Kritische tests uit 8.1 slagen met bewaarde bewijsreferenties |
| 4. Uitbreiding | Overige overeengekomen bronnen, Control Room, monitoring en onderhoud | Bronspecifieke extract-, historie- en operationele tests slagen |
| 5. Overgang | Dataovername/replay, parallelvergelijking, cutover en rollback | Reconciliatie, SLA, autorisatie, beheeracceptatie en terugvalroute bewezen |

Nog geen kalenderplanning of kostenraming: capaciteit, datavolumes, toegang
en scope ontbreken. De Databricks-baseline blijft beschikbaar; er zijn geen
bronworkflows, checkpoints of gegevens gewijzigd.

## 9. Referenties voor de Fabric-analyse

Geraadpleegd op 10 oktober 2026; platformstatus opnieuw controleren bij ontwerp
en deployment:

- [Fabric Spark runtimes](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime).
- [Fabric Runtime 2.0](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-2-0).
- [Fabric Runtime 1.3](https://learn.microsoft.com/en-us/fabric/data-engineering/runtime-1-3).
- [Lakehouse schemas en namespace](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas).
- [Notebook activity en parameters](https://learn.microsoft.com/en-us/fabric/data-factory/notebook-activity).
- [NotebookUtils](https://learn.microsoft.com/en-us/fabric/data-engineering/notebook-utilities).
- [Lakehouse SQL analytics endpoint](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-sql-analytics-endpoint).

**Resultaat M-004:** eerste technisch/functionele migratieanalyse opgeslagen.
Nog geen Fabric-objecten ingericht, geen runtimecode aangepast en geen
Fabric-compatibiliteit of productiegeschiktheid gecertificeerd.

### Scopebevestiging na de analyse

Op 10 oktober 2026 heeft de opdrachtgever voor de eerste proef gekozen voor
**Contoso Sales: Customers, Products, Orders, Returns en Employees**.
Dit bevestigt B-FAB-004. CBS, ECB, Nager, FABRIC_SALES en de inactieve
SharePoint-configuratie worden niet in deze eerste proef opgenomen.
Het oorspronkelijke voorstel voor uitbreiding blijft afhankelijk van latere
scopebevestiging. De Fabric-doelomgeving en Gold-consumptieroute zijn nog open.

### Doelomgeving: eerste bevestiging

Op 10 oktober 2026 bevestigt de opdrachtgever dat een bestaande
ontwikkelworkspace met Fabric-capaciteit beschikbaar is. Dit is een
gebruikersverklaring, nog geen technisch gecontroleerde capaciteitstoewijzing.

De aangeleverde URL
`https://app.powerbi.com/groups/me/list?language=en-US&experience=power-bi`
verwijst naar Mijn werkruimte en identificeert niet de bedoelde afzonderlijke
ontwikkelworkspace. Er is nog geen workspace-ID vastgelegd. De volgende stap
is de exacte doelworkspace vaststellen, zonder credentials in het verslag op
te nemen. Er zijn geen Fabric-objecten geopend, aangemaakt of gewijzigd.

Vervolgens heeft de opdrachtgever de specifieke workspace aangeleverd:

- **Workspace-ID:** `4be37544-10b9-450d-80f0-f9b3d793b83b`.
- **Workspace-URL:** https://app.powerbi.com/groups/4be37544-10b9-450d-80f0-f9b3d793b83b/list?language=en-US&experience=power-bi
- **Status:** doelworkspace geidentificeerd; naam, bestaande items, capaciteit,
  toegangsrechten en Spark-runtime nog niet technisch gecontroleerd.

De workspace-URL is read-only geopend in de geintegreerde browser. Power BI
leidt door naar de Microsoft-aanmeldpagina; deze browsersessie is nog niet
aangemeld. De controle van workspace-items en instellingen wacht daarom op
aanmelding door de opdrachtgever. Er zijn geen credentials ingevoerd en geen
Fabric-objecten gewijzigd.

### Technische controle van de doelworkspace

Na aanmelding door de opdrachtgever is op 10 oktober 2026 read-only vastgesteld:

| Controle | Waarneming |
|---|---|
| Workspace | `Contoso_lakehouse_ontwikkel` |
| Workspace-ID | `4be37544-10b9-450d-80f0-f9b3d793b83b` |
| Bestaande items | De itemlijst toont 0 items en "There's nothing here yet" |
| Workspace settings > Workspace type | `Current workspace type: Power BI Pro` |
| Accountbanner | `Trials activated: 56 days left`; dit bewijst geen capaciteitstoewijzing aan deze workspace |
| Spark-runtime en Environment | Nog niet vastgesteld |

Deze waarneming corrigeert de eerdere verwachting van een reeds aan
Fabric-capaciteit gekoppelde workspace. Fabric-inrichting wacht op een
goedgekeurde capaciteitstoewijzing. Er is niet op Edit gedrukt, geen
workspace-type aangepast, geen trial gestart en geen item aangemaakt.

Met expliciete toestemming is vervolgens Edit geopend om alleen de opties te
bekijken. `Fabric` is uitgeschakeld; `Fabric Trial` is selecteerbaar. Er is
geen capaciteitnaam, SKU of capaciteit-ID vastgesteld: de typekeuze is nog
niet uitgevoerd. `Power BI Pro` bleef geselecteerd en Cancel is gebruikt;
er is niet op Select of een opslagactie gedrukt. Toewijzing of activering van
een trial vereist een afzonderlijk besluit van de opdrachtgever.

### Uitgevoerde capaciteitstoewijzing

De opdrachtgever heeft vervolgens expliciet gekozen: "Ja, zet deze
ontwikkelworkspace om naar Fabric Trial". Na opnieuw openen van Edit is
Fabric Trial geselecteerd, met de reeds aangeboden trialcapaciteit.
De standaardkeuze voor kleine semantische modellen is niet gewijzigd.
Select is uitgevoerd en de UI bevestigt dat de wijzigingen zijn opgeslagen.

| Onderdeel | Gecontroleerd resultaat |
|---|---|
| Current workspace type | `Fabric Trial` |
| Capaciteitnaam | `Trial-20261006T080240Z-eWvV-gOwHUaCQ7l22Iiq7w` |
| Capaciteit-ID | `103E56D3-CCDB-42D8-A56D-FF3D5A6C92F8` |
| Regio in capaciteitskeuze | `North Europe` |
| Opslagbevestiging | `Your changes were successfully saved` |
| Trialduur | Accountbanner toonde 56 dagen resterend; exacte einddatum niet gecontroleerd |

Er is geen nieuwe betaalde capaciteit aangeschaft en geen nieuwe trial via
een start-trialactie aangemaakt. De workspace gebruikt nu de bestaande
trialcapaciteit. SKU, runtime en daadwerkelijke Spark-uitvoering zijn niet
afgeleid uit deze toewijzing en moeten nog worden gecontroleerd.

**Functioneel:** de workspace is voorbereid voor de Fabric-proef, maar er zijn
nog geen Sales-leveringen verwerkt.
**Technisch:** uitsluitend workspace-type/capaciteitstoewijzing gewijzigd;
geen Lakehouse, Environment, notebook of pipeline aangemaakt.
**Project:** de capaciteitsblokkade voor foundation-inrichting is opgeheven;
trialexpiratie vraagt een vervolgafspraak voor duurzame capaciteit.
**Terugval:** de oude keuze was Power BI Pro; eventuele terugzetting alleen
na expliciete opdracht en controle van inmiddels aanwezige Fabric-items.

### CI/CD-verkenning: equivalent van Databricks Asset Bundles

Op verzoek van de opdrachtgever is onderzocht welke Fabric-mechanismen de
rol van Asset Bundles kunnen vervullen. Er is geen een-op-een omzetting van
de bestaande `databricks.yml`.

| Mechanisme | Rol | Aandachtspunt |
|---|---|---|
| Fabric Git integration | Versiebeheer en workspace-synchronisatie van ondersteunde itemdefinities | Niet hetzelfde als deployment van alle infrastructuur, rechten of data |
| Fabric deployment pipelines | Promotie van ondersteunde items tussen omgevingen | Niet verwarren met Data Factory-pipelines die ETL uitvoeren |
| `fabric-cicd` | Code-first deployment van Git-beheerde Fabric-items via Python, met omgevingsparameterisatie | Vereist ondersteunde source-controlformaten en create/update-API's; geen conversie van Databricks-job-YAML |
| Fabric deployment plan | Expliciete deployvolgorde en pre-/postdeployacties | Preview; geen zelfstandig uit te voeren bundle en geen workspaceconfiguratiedeployment |

**Voorstel, nog niet besloten:** gebruik Git-beheerde Fabric-itemdefinities,
`fabric-cicd` en CI om dev/tst/prd-deployment reproduceerbaar te maken.
Behandel capaciteit, identities, connections en workspace-instellingen als
afzonderlijke foundation-inrichting. Deploy Delta-DDL en metadata-seeds met
expliciete setup-/migratiestappen; itemdeployment is geen datamigratie.
Automatisch verwijderen van orphan-items wordt niet als standaard opgenomen.

Bronnen, geraadpleegd op 10 oktober 2026:

- [fabric-cicd documentatie](https://microsoft.github.io/fabric-cicd/latest/).
- [Fabric Git integration](https://learn.microsoft.com/en-us/fabric/cicd/git-integration/intro-to-git-integration).
- [Fabric deployment pipelines](https://learn.microsoft.com/en-us/fabric/cicd/deployment-pipelines/intro-to-deployment-pipelines).
- [Deployment plan, preview](https://learn.microsoft.com/en-us/fabric/cicd/deployment-plan/deployment-plan-overview).

Er is geen CI/CD-tool geinstalleerd, Git-integratie ingesteld of deployment
uitgevoerd tijdens deze verkenning.

### Foundationvoorstel voorafgaand aan inrichting

In de terugkoppeling op 10 oktober 2026 is een indeling in drie
schema-enabled Lakehouses voorgesteld:

| Voorgesteld Lakehouse | Verantwoordelijkheid |
|---|---|
| `lh_contoso_control` | Metadata, audit, leveringen en persistente runcontext |
| `lh_contoso_processing` | Landing, Bronze, Quality, Reject, Raw Vault en Business Vault |
| `lh_contoso_gold` | Gold Historisch en publicatiekandidaten |

**Aanleiding:** de Databricks-catalogusindeling moet worden vertaald naar
Fabric zonder de besturing, verwerking en consumptie onnodig te vermengen.
**Afweging:** een enkel lakehouse is eenvoudiger voor een snelle proef, maar
vermengt alle verantwoordelijkheden. Een lakehouse per laag biedt meer fysieke
scheiding, maar vraagt meer itembeheer, referenties en deploymentconfiguratie.
Drie lakehouses zijn voorgesteld als middenweg voor deze proef.
Dit is nog niet met rechten, runtime of performance gevalideerd. Fysieke
scheiding alleen is geen bewijs van beveiliging of lagere kosten.
**Beslissing:** nog open; de opdrachtgever heeft deze indeling niet bevestigd.
**Uitvoering:** geen lakehouses aangemaakt.
**Eerstvolgende stappen:** indeling afstemmen, runtime en Environment controleren,
deploymentstructuur bepalen, daarna een foundationtest met metadata,
leveringsregistratie, een Sales-object in Bronze en audit uitvoeren.
Gold-consumptie moet worden gekozen voordat het publicatiecontract definitief
wordt ontworpen.

### Foundationbesluit B-FAB-007

Op 10 oktober 2026 bevestigt de opdrachtgever de optie "Drie Lakehouses:
control, processing en gold". Hiermee is de hierboven beschreven indeling
besloten voor de Sales-proef; de eerdere vermelding "beslissing nog open"
beschrijft de toestand voor deze bevestiging.

- **Waarom:** scheiding tussen control plane, verwerking en consumptie met
  begrensde beheercomplexiteit.
- **Alternatieven:** een enkel lakehouse met schema's en een lakehouse per laag
  zijn aan de opdrachtgever voorgelegd; deze zijn niet gekozen.
- **Functioneel:** de afgesproken lagen en Sales-scope blijven behouden.
- **Technisch:** schema-enabled lakehouses zijn het gekozen opslagmodel;
  runtime, fysieke schemanamen, rechten en cross-lakehouseverwijzingen moeten
  nog worden uitgewerkt en getest.
- **Uitvoering:** besluit vastgelegd; nog geen Lakehouse aangemaakt.
- **Vervolg:** beschikbare runtime en workspace-instellingen read-only
  controleren, daarna foundation-items en setupcontract uitwerken.

### Runtimecontrole M-007 en herhaalbare uitrol vanuit VS Code

De read-only controle van Workspace settings toont:

- Capaciteitsdetails: `SKU: FTL64, Region: North Europe`.
- Spark settings > Environment: `Set default environment: Off`.
- Geselecteerde workspace-runtime: `2.0 (Spark 4.1, Delta 4.2)`.
- De runtimekeuzelijst toont ook 1.3 en 1.2; bij 1.2 staat een
  deprecation-/uitschakelwaarschuwing. Zichtbaarheid van een optie betekent
  niet dat deze geschikt of uitvoerbaar is.
- Er is geen runtime gekozen of opgeslagen en geen Spark-session gestart.

**Advies:** voor de proef een expliciete Environment met runtime 2.0 en
frameworkdependencies vastleggen, in plaats van alleen de workspace-default
te vertrouwen. Runtimecompatibiliteit blijft een te testen eigenschap.

De opdrachtgever vraagt of later vanuit dit VS Code-project opnieuw kan
worden uitgerold. **Antwoord:** dit is mogelijk mits de Fabric-deploymentroute
wordt geimplementeerd; de huidige Databricks-bundle levert dat nog niet.
Herhaalbare uitrol wordt als functionele projecteis vastgelegd:

| ID | Eis | Status |
|---|---|---|
| F-07 | Vanuit een versie van het project in VS Code opnieuw deployen naar een bestaande of voorbereide nieuwe Fabric-workspace, met expliciete omgevingsconfiguratie en traceerbare validatie | Gevraagd; implementatie en acceptatietest nog open |

**Waarom:** de inrichting mag niet afhankelijk blijven van losse browseracties
of van herinneringen aan deze sessie. VS Code is de werkplek; scripts,
itemdefinities en configuratie vormen de reproduceerbare deploymentbron.

**Voorgestelde aanpak:** Git-beheerde Fabric-itemdefinities, `fabric-cicd`,
omgevingsconfiguratie en expliciete setup-/migratiescripts. Browseraanmaak
kan een initiele stap zijn, maar itemdefinities moeten daarna in het project
worden opgenomen voordat herhaalbare uitrol als gerealiseerd geldt.

**Afweging:** uitsluitend handmatige UI-inrichting is eenvoudig voor een
eerste proef, maar onvoldoende reproduceerbaar. Git integration en Deployment
pipelines zijn bruikbare alternatieven/aanvullingen; code-first deployment
past bij de gevraagde VS Code-werkwijze. Deze toolkeuze is nog een voorstel.

**Randvoorwaarden en grenzen:**

- Doelworkspace, geldige Fabric-capaciteit, rechten, authenticatie en relevante
  tenantinstellingen moeten beschikbaar zijn; secrets blijven buiten Git.
- Geen vaste dev-item-ID's in runtimecode; itemreferenties moeten per doel
  worden opgelost. Runtime-/packageversies en metadatarelease zijn traceerbaar.
- Tabellen en metadata krijgen veilige, versieerbare setup/migraties;
  herdeploy mag bestaande audit of historie niet ongemerkt wissen.
- Het publiceren van definities is niet het kopieren van landingbestanden,
  Delta-historie, credentials of alle workspace-instellingen.
- Geen automatische verwijdering van orphan-items als standaard.

**Acceptatie:** deployment vanuit dezelfde projectversie naar een lege,
voorbereide testworkspace, gevolgde setup en smoke test, plus een tweede
uitrol naar dezelfde workspace zonder onbedoeld dataverlies of duplicaten.
Nog niet uitgevoerd; er bestaan nog geen Fabric-deployscripts of
geexporteerde Fabric-items in dit project.

**Volgende stap:** foundation-aanmaak en export/deploymentroute samen uitwerken,
zodat de drie gekozen Lakehouses en Environment niet alleen in de UI bestaan.

### Aanscherping van het einddoel: herbruikbaar ETL-template

Op 10 oktober 2026 verduidelijkt de opdrachtgever: "mijn doel is een template
etl metadata project dat herbruikbaar kan blijven".

**Bevestigd doel:** een herbruikbaar metadata-gedreven ETL-template, niet alleen
een eenmalige Contoso-migratie. Contoso Sales blijft de bevestigde eerste
proefscope en dient als referentie-implementatie voor het template.
Ondersteuning van andere uitvoerplatformen dan Fabric is nog niet afgesproken.

| ID | Eis | Status |
|---|---|---|
| F-08 | Het ETL-project blijft als template herbruikbaar voor volgende projecten; generieke verwerking wordt gescheiden van projectspecifieke metadata en omgevingsbindingen | Doel bevestigd; ontwerp en bewijs nog open |

**Aanleiding en waarom:** alleen opnieuw naar een andere workspace deployen
bewijst nog geen herbruikbaarheid. Een nieuw project moet andere bronnen,
mappings, kwaliteitsregels en doelmodellen kunnen configureren zonder de
generieke laagverwerking te kopieren of Contoso-specifieke runtimecode te
moeten aanpassen.

**Ontwerpvoorstel:**

1. **Generiek framework:** ingestiecontracten, metadata-validatie, gates,
   audit, runcontext, Quality/Reject, dependencyplanning en laadpatronen.
2. **Projectconfiguratie:** bronobjecten, mappings, DQ-regels, afhankelijkheden,
   Vault-/Gold-modellen en business-SQL. Contoso wordt een voorbeeldpakket,
   niet de impliciete standaard voor ieder nieuw project.
3. **Platform- en omgevingsbindingen:** Fabric-itemreferenties, workspace,
   lakehouses, schema's, identities en connections via expliciete configuratie.
   Credentials blijven buiten Git.
4. **Deployment en versiebeheer:** ondersteunde itemdefinities, setup-/
   migratiescripts, frameworkversie, metadata-contractversie en per-project
   releasefingerprint.
5. **Onboarding en acceptatie:** een gedocumenteerde route om een nieuw project
   te initialiseren, configureren, valideren, deployen en testen.

**Alternatieven en afweging:** een Contoso-project kopieren is snel, maar
vergroot de kans op verborgen Contoso-defaults en uiteenlopende frameworkkopieen.
Een template met gedeelde, versioneerbare frameworkcode en aparte configuratie
past beter bij het doel. Een zelfstandig package versus een volledig
zelfstandige templatekopie moet nog worden besloten. Evenmin is besloten een
algemene multi-platformabstractielaag te bouwen.

**Functionele grens:** metadata-gedreven betekent niet dat iedere willekeurige
bron of transformatie zonder programmeerwerk wordt ondersteund. Nieuwe
connectortypen of laadpatronen kunnen een expliciete frameworkuitbreiding
vereisen; nieuwe configuraties binnen ondersteunde patronen niet.

**Voorgesteld bewijs van herbruikbaarheid:**

- Contoso Sales draait als eerste referentieproject.
- Een tweede, klein referentieproject met andere namen, bronobjecten en
  mappings draait zonder wijzigingen aan de generieke frameworkcode.
- Beide projecten hebben eigen metadata, audit en omgevingsconfiguratie.
- Een herdeploy behoudt bestaande data en historie volgens het contract.
- Onboarding, configuratiefouten en ondersteunde uitbreidingspunten zijn
  gedocumenteerd en getest.

**Beslissing versus voorstel:** het herbruikbare template is het gevraagde
einddoel. Bovenstaande componentindeling en bewijsaanpak zijn architectuuradvies,
nog geen goedgekeurd detailontwerp.
**Uitvoering:** doel en ontwerpeisen vastgelegd; geen code, projectnaam,
repositorystructuur of Fabric-item gewijzigd.
**Vervolg:** eerst het platformbereik van het template bevestigen, daarna de
templategrenzen en configuratiecontracten uitwerken voordat foundation-items
als definitieve projectstructuur worden aangemaakt.

**Platformbereik bevestigd:** de opdrachtgever kiest vervolgens "Eerst
Microsoft Fabric; Contoso is het voorbeeldproject". Hiermee is B-FAB-008
vastgelegd. Platformonafhankelijke kernlogica blijft waar zinvol behouden,
maar ondersteuning van Databricks als tweede template-runtime wordt niet
gebouwd of gecertificeerd in de eerste scope.

### Scope-uitbreiding B-FAB-009: alle ontsloten bronnen testen en accepteren

Op 10 oktober 2026 stelt de opdrachtgever voor alle bronnen die ook in
Databricks ontsloten zijn hier te ontsluiten, testen en accepteren. Dit wordt
als aanscherping van het gewenste eindresultaat vastgelegd. B-FAB-004 blijft
de eerdere keuze voor de eerste Sales-proef beschrijven, maar is geen
uitsluiting van aanvullende bronnen uit de uiteindelijke acceptatie.
Een gefaseerde start met Sales is een uitvoeringsadvies, geen beperking van
de nieuwe eindscope.

**Waarom:** het template moet aantoonbaar werken voor bestands-, HTTP JSON-,
HTTP CSV- en JDBC-bronnen, en zowel Vault- als referentiedataroutes.
Alleen Sales testen zou onvoldoende bewijs leveren voor die breedte.

**Bewijsbasis en onzekerheid:** de lokale seed bevat SALES, CBS, ECB, NAGER
en FABRIC_SALES als actieve bronsystemen. SHAREPOINT en de twee bijbehorende
objecten zijn inactief. Deze configuratie bewijst niet welke bronnen
daadwerkelijk succesvol in Databricks draaien. Bestaande runresultaten,
broncontracten en verwachte datasets moeten de definitieve bronlijst en
acceptatiebaseline onderbouwen.

| Kandidaatbron | Ontsluiting en route in de configuratie | Te bewijzen bij Fabric-acceptatie |
|---|---|---|
| SALES | Bestandsleveringen; vijf objecten; Raw/Business Vault en Gold | Leveringsgate, samenhang, historie, deletes waar van toepassing en herstart |
| CBS | HTTP JSON; Jeugdzorg Wijk 2025; Vault-route | Paginering, volledige extractie, typering, sleutel-/hash- en Gold-uitkomsten |
| ECB | HTTP CSV; wisselkoersen; referentiedata | CSV-contract, datums/decimalen, SCD2 en Gold-uitkomsten |
| NAGER | HTTP JSON; Nederlandse feestdagen; referentiedata | Volledige jaarlevering, sleutelcontract, historisatie en Gold-uitkomsten |
| FABRIC_SALES | JDBC; orderregels uit Fabric SQL Database; referentiedata | Bronauthenticatie, consistente extractie, historisatie en Gold-uitkomsten |
| SHAREPOINT | Price Agreements en Sales Budgets; momenteel inactief | Eerst bevestigen of daadwerkelijk ontsloten in Databricks; geen connectorwerking veronderstellen |

**Alternatieven:** alleen Sales accepteren is sneller, maar dekt het gevraagde
eindresultaat niet. Alles tegelijk bouwen bemoeilijkt foutisolatie. Advies:
de gedeelde foundation eerst bewijzen, vervolgens bronnen in begrensde
end-to-end stappen toevoegen en per bron een acceptatiebewijs bewaren.

**Voorgesteld acceptatiecontract per bron:**

1. Bevestigde Databricks-baseline: bronobjecten, contractversie, referentielevering,
   succesvolle run en verwachte uitkomsten.
2. Fabric-extractie en ingestie: volledigheid, herkomst, manifests,
   schema-afwijkingen, foutregistratie en veilige retries.
3. Quality/Reject: aantallen en recordidentiteiten reconciliëren; expliciete
   bronfilters en drempels verklaren.
4. Vault of referentiedata: toepasselijke hashes, keys, historie, deletes en
   idempotentie vergelijken met de afgesproken referentie.
5. Gold: betekenisvolle kolommen en businessuitkomsten vergelijken; technische
   timestamps/run-ID's niet blind op gelijkheid toetsen. Het gekozen
   publicatie-/consumercontract bewijzen.
6. Operationeel: falen, herstel, monitoring, toegangsrechten en afgesproken
   SLA's testen; run-ID's en resultaten registreren.
7. Acceptatie: per bron PASSED/FAILED/open bevindingen vastleggen; technische
   tests onderscheiden van expliciete acceptatie door de opdrachtgever.

Voor veranderlijke publieke API's moeten beide platforms dezelfde vastgelegde
referentielevering of hetzelfde afgesproken peilmoment gebruiken. Twee live
extracten op verschillende momenten vormen geen betrouwbare gelijkheidsproef.
Welke referentiedata mag worden opgeslagen en hoe lang, volgt uit governance.

**Functioneel:** alle werkelijk ontsloten bronnen vallen onder het gewenste
Fabric-resultaat, inclusief brongebonden downstreamproducten.
**Technisch:** generieke ingestie-/controlcontracten plus geteste connectoren
en verwerking per bron; geen hardcoded bronlogica in het framework toevoegen.
**Project:** bronlijst, toegang, referentiegegevens en acceptatiecriteria
worden prerequisites voor de betreffende bronmigratie. Nog geen planning of
succespercentage toegekend.
**Uitvoering:** scope en voorgestelde bewijsaanpak vastgelegd; geen bron
aangesloten, code gewijzigd of test uitgevoerd in deze stap.
**Vervolg:** vaststellen of de inactieve SharePoint-bronnen bij de operationele
Databricks-scope horen, daarna de bron-/acceptatiematrix definitief maken.

**Scopebevestiging:** de opdrachtgever bevestigt vervolgens dat SharePoint
nog niet ontsloten is en kiest de vijf actieve bronsystemen. De overeengekomen
migratie-/acceptatiescope is daarmee **SALES, CBS, ECB, NAGER en FABRIC_SALES**
(negen actieve bronobjecten in de huidige seed). SHAREPOINT valt buiten scope.
Deze bevestiging bepaalt de bronlijst; de daadwerkelijke Databricks-uitkomsten
moeten nog als vergelijkingsbewijs worden verzameld.

### Productierijpheid als harde acceptatie-eis B-FAB-010

Op 10 oktober 2026 bevestigt de opdrachtgever de aanpak en benadrukt dat de
oplossing productierijp moet worden, met v2 als referentie. Dit is het gewenste
eindniveau; het is geen verklaring dat de nieuwe Fabric-oplossing of ieder
onderdeel van v2 al technisch gecertificeerd is.

| ID | Eis | Status |
|---|---|---|
| F-09 | Het herbruikbare Fabric-template wordt alleen productiegeschikt verklaard na bewezen functionele, technische en operationele acceptatie van de afgesproken scope | Bevestigd; bewijs en acceptatie nog open |

**Waarom:** een succesvolle eerste run bewijst geen correcte historie bij
herstart, concurrente publicatie, toegangsafscheiding, herstel of duurzame
deployment. De kernprincipes van v2 worden behouden als ontwerpbaseline,
maar platformafhankelijke garanties worden opnieuw in Fabric getoetst.

**Alternatieven en beslissing:** uitsluitend een demo/PoC opleveren voldoet
niet aan het gevraagde eindresultaat. Gefaseerde ontwikkeling met een proef
blijft zinvol voor foutisolatie, maar de proef is een tussenstap naar
productieacceptatie, niet het eindproduct.

### Voorgestelde productie-releasegates

De productierijpheidsdoelstelling is bevestigd; onderstaande invulling is
het architectuurvoorstel voor concrete gates. Numerieke SLA's, volumes,
RPO/RTO en bedrijfsacceptatie moeten nog met de opdrachtgever worden vastgesteld.

| Gate | Verplicht te onderbouwen | Bewijs voor vrijgave |
|---|---|---|
| P-01 Functionele juistheid | Alle vijf bronnen en negen bronobjecten, inclusief afgesproken downstreamproducten | Brongebonden referentieleveringen, reconciliaties, verklaarde afwijkingen en expliciete gebruikersacceptatie |
| P-02 Historische correctheid | Keys, hashes, tijdsemantiek, deletes en toepasselijke SCD2-/Vault-historie | Meerleveringstests en vergelijkingen met vastgelegde Databricks-uitkomsten |
| P-03 Herstel en idempotentie | Falen op schrijf-/state-/publicatiegrenzen, retries en replay | Failure-injectiontests zonder onbedoeld verlies of duplicaten; gedocumenteerde herstelprocedure |
| P-04 Concurrency en publicatie | Exclusieve claims, lease-expiratie, chronologie en consistente Gold-consumptie | Parallelle writers, actieve readers en slothergebruik getest via de werkelijke consumerroute |
| P-05 Metadata- en schemacontract | Gevalideerde releases, contractversies, schema drift en veilige wijzigingen | Ongeldige configuratie blokkeert deployment/run; goedgekeurde migraties zijn herhaalbaar en traceerbaar |
| P-06 Beveiliging | Least privilege, identities, secrets en afgescheiden omgevings-/laagtoegang | Positieve en negatieve toegangstests via relevante SQL-, Spark- en opslagroutes; geen secrets in Git |
| P-07 Deployment en upgrade | Reproduceerbare uitrol, targetbindingen, package-/runtimeversies en veilige schema-upgrades | Uitrol naar voorbereide lege workspace plus herdeploy/upgrade zonder onbedoeld dataverlies; aantoonbare terugvalroute |
| P-08 Operationeel beheer | Monitoring, alerts, SLA-controles, dead letters en verantwoordelijkheden | Opzettelijke storingen leiden tot zichtbare fouten en alerts; uitvoerbare runbooks en eigenaar per proces |
| P-09 Capaciteit en performance | Realistische volumes, parallelisme, looptijd en kosten | Metingen tegen vooraf afgesproken belasting/SLA; duurzame capaciteit voor productie, niet uitsluitend een trial |
| P-10 Retentie en continuiteit | Bewaarbeleid, onderhoud, herstelbaarheid en afgesproken RPO/RTO | Getest restore/replay-proces en onderhoud binnen vereiste herstelvensters; geen onbewezen backupgarantie |
| P-11 Herbruikbaarheid | Een ander project binnen de ondersteunde patronen zonder wijziging van de generieke kern | Tweede configuratie-/referentieproject, eigen metadata/audit en geslaagde onboarding/deployment |
| P-12 Go-live en overdracht | Open bevindingen, cutover, terugvalcriteria en beheeracceptatie | Go/no-go-besluit met bewijsreferenties; geen onopgeloste releaseblokkerende fouten |

**Functionele gevolgen:** scope blijft de vijf bevestigde bronsystemen;
acceptatie omvat fouten en herstel, niet alleen de happy path.
**Technische gevolgen:** productie-eisen sturen het templatecontract vanaf
het begin. Persistente runcontext, expliciete fouten, metadata-validatie,
audit en veilige deployment zijn geen uitgestelde productie-extra's.
**Projectgevolgen:** de trial ondersteunt ontwikkeling en validatie, maar
duurzame productiecapaciteit, beheerrollen en omgevingsscheiding zijn
go-livevoorwaarden. Geen SLA's of kostenbedragen verzonnen; die blijven open.
**Uitvoering en validatie:** eisen en gates opgeslagen; geen implementatie,
test of productiecertificering uitgevoerd in deze stap.
**Vervolg:** templatecontract met configuratiegrenzen, identities, fout-/
herstelmodel en deploycontract uitwerken; gates vertalen naar concrete tests
en verantwoordelijken voordat productieacceptatie wordt geclaimd.

## 10. Uitvoeringsroute naar een productiegeschikt template

Op 10 oktober 2026 vraagt de opdrachtgever welke stappen worden doorlopen.
Onderstaande route concretiseert de eerdere fasering; het is een
uitvoeringsvoorstel, geen kalenderplanning of bewijs van gerealiseerde stappen.
Productiegates P-01 t/m P-12 blijven leidend.

| Fase | Werk en deliverables | Voorwaarde om verder te gaan |
|---|---|---|
| 1. Baseline en acceptatiecontract | Bevestigde vijf bronnen/negen objecten, broncontracten, Databricks-referentieruns, dataclassificatie, volumes, SLA's en hersteldoelen; bevindingen V-01 t/m V-06 reproduceren waar relevant | Beschikbare en toegestane referentiedata; meetbare acceptatiecriteria en bekende baseline-afwijkingen |
| 2. Templatecontract en doelarchitectuur | Scheiding framework/projectmetadata/omgeving, configuratie- en metadata-contractversies, bron-/laadpatronen, schema's en namespaces, persistente runcontext, fout-/herstelmodel en Gold-consumptie | Beslissingen over ingrijpende alternatieven bevestigd; ondersteunde patronen en grenzen expliciet |
| 3. Project- en deploymentstructuur | Generieke naamgeving, Contoso-voorbeeldconfiguratie, branch/repositoryafspraken, dependencyversies, Fabric-itemdefinities en voorgestelde fabric-cicd-route; geen broncode of historie ongeautoriseerd vervangen | Project lokaal valideerbaar; targetbindingen en secrets gescheiden; deploymentontwerp herleidbaar |
| 4. Fabric foundation | Drie besloten schema-enabled Lakehouses, expliciete Environment, identities/connections/rechten, metadata-/audit-DDL en versieerbare setup; minimale setupdeployment vanuit project | Items en definities persistent; setup herhaalbaar; runtime/SQL en toegangsgrenzen gecontroleerd |
| 5. Generieke ingestie en control plane | Manifest-/bestandsregister, schema-contract, delivery-gate, context, audit-events, claims/leases en retries; eerste Sales-object als verticale smoke test | Incomplete levering blokkeert; fouten zichtbaar; herstart en registratie correct; geen volledige ketenclaim |
| 6. Complete Sales-keten | Alle vijf Sales-objecten, Quality/Reject/reconciliatie, Raw/Business Vault, toepasselijke PIT/historie, Gold Historisch en het gekozen publicatiecontract | Meerleveringstests, referentievergelijking en kritische fout-/hersteltests geslaagd; Sales technisch aantoonbaar |
| 7. Aanvullende bronroutes | CBS HTTP JSON, ECB HTTP CSV, Nager HTTP JSON en Fabric Sales JDBC; per bron downstreammodellen en acceptatiebewijs | Alle negen afgesproken objecten getest; verschillen verklaard; brongebonden acceptatie door opdrachtgever vastgelegd |
| 8. Productiehardening en templatebewijs | Monitoring/alerts, remediation/Control Room naar afgesproken scope, onderhoud/retentie, concurrency, restore/replay, load-/kostentests en tweede klein referentieproject | Releasegates aantoonbaar; tweede project binnen ondersteunde patronen zonder wijziging van generieke kern |
| 9. Release-, upgrade- en deploymentacceptatie | Uitrol naar voorbereide lege testworkspace, herdeploy, schema-/metadata-upgrade, terugvalproef, versie-/runreferenties en beheerhandleidingen | Geen onbedoeld verlies/duplicaten; deployment en herstel reproduceerbaar; geen releaseblokkerende bevindingen |
| 10. Productieovergang en overdracht | Duurzame capaciteit, dev/tst/prd-afspraken, dataovername/replay, parallelvergelijking, cutover-/rollbackplan, eigenaarschap en go/no-go | Expliciete functionele en beheeracceptatie; productievoorzieningen en herstelroute beschikbaar |

### Waarom deze volgorde?

- Baseline en contracten voorkomen dat verschillen pas na het bouwen als
  onduidelijke migratiefouten zichtbaar worden.
- Foundation en een kleine verticale ingestietest isoleren namespace-, SQL-,
  authenticatie- en parameterproblemen voor de volledige keten wordt toegevoegd.
- Sales bewijst de complexere leverings-/Vault-/publicatieketen; aanvullende
  bronnen bewijzen connector- en referentiedataherbruikbaarheid.
- Beveiliging, audit, fouten en deployment worden vanaf het begin meegenomen.
  Fase 8 verdiept en bewijst ze; zij worden niet tot die fase uitgesteld.
- Een herdeploytest en een tweede project zijn verschillende bewijzen:
  reproduceerbaarheid is niet hetzelfde als functionele herbruikbaarheid.
- Productieovergang volgt pas na meetbare acceptatie en duurzame capaciteit.

### Besluiten, status en verslaglegging

Reeds uitgevoerd: broncode gecloned, eerste inventarisatie vastgelegd,
scope/template-/productiedoelen bevestigd, trialcapaciteit toegewezen en
workspace-runtime read-only gecontroleerd. De drie-Lakehouse-indeling is
besloten, maar de items zijn nog niet aangemaakt.

**Huidige positie:** start van fase 1; de eerste inventarisatie is beschikbaar,
maar baseline-runs, referentiedata en numerieke acceptatiecriteria ontbreken.
Fase 2 kan op bekende feiten worden voorbereid; ontbrekende baseline-eisen
mogen niet stilzwijgend als ingevuld worden beschouwd.

Per uitvoeringsstap worden keuze, alternatieven, besluit, wijzigingen,
verwacht/waargenomen resultaat, bewijsreferenties, hersteloptie en open acties
bijgehouden volgens B-FAB-006. Een blokkerende gate leidt tot herstel of een
expliciet gewijzigd besluit, niet tot een succesmelding.

**Direct volgende werkzaamheden:** de bron-/acceptatiebaseline concretiseren
en het templatecontract uitwerken. Daarna wordt de eerste foundationdeployment
gebouwd. Er zijn voor deze roadmap geen code of Fabric-objecten gewijzigd.

### Fase 1: uitvoering onder mandaat

Het [fase-1-baseline- en acceptatiecontract](10_fabric_baseline_acceptatie.md)
legt de concrete bronnen, sleutels, strategieen, bestaande freshness-/retentie-
instellingen, historische bewijsreferenties en criteria AC-01 t/m AC-12 vast.

Uitgevoerd op 10 oktober 2026:

- Bestaande pytest-suite: **133 passed in 2.78s**, lokale Python 3.13.3.
  VS Code ontdekte geen tests; daarom rechtstreeks pytest gebruikt.
- Metadatareleasecheck geslaagd, fingerprint
  `40b0fbb42f32c19b0ca4417f41b561af631d7d22a2e6730133c9f0f895a2694a`.
- V-01 gereproduceerd met gecontroleerde klok: gelijke batch/delivery levert
  verschillende `load_date` bij aparte factoryaanroepen.
- Dubbel JSON-veld gevonden bij Customers: freshness 26 en 24. Geparste
  baseline is 24; oorspronkelijke seed niet aangepast. Templatevalidatie
  moet zulke ambiguiteit weigeren.
- Databricks CLI v1.7.0 beschikbaar; alle gevonden auth-profielen melden
  Valid NO. Geen live run-, data- of auditverificatie uitgevoerd.
- V2-documentatie bevat expliciet resterende amber/rode productiepunten;
  deze zijn onderdeel van de referentie, geen afgeronde garanties.

**Besluiten en waarom:** oorspronkelijke ETL-baseline intact houden;
geen niet-gereproduceerde runtimefouten speculatief repareren. Technische
acceptatiecriteria nu concreet vastleggen, terwijl productie-SLA's/RPO/RTO
zonder feitelijke bedrijfsbasis open blijven.

**Status:** lokaal uitvoerbare inventarisatie en criteria gereed;
fase 1 blijft in uitvoering wegens ontbrekende live referentiedata en
productie-/hersteldoelen. Documenten persistent opgeslagen; geen dependencies
geinstalleerd, geen bron-/Fabric-data gewijzigd, geen externe jobs gestart,
geen commit of push uitgevoerd.

### Correctie fase-1-bewijsbasis: Databricks-omgeving weg

Op 10 oktober 2026 meldt de opdrachtgever dat de Databricks-omgeving weg is.
De eerdere vervolgstap om CLI-authenticatie te herstellen vervalt. Er worden
geen verdere liveverificatie- of aanmeldpogingen voor die omgeving gedaan.

Onder het fase-1-mandaat is B-FAB-013 genomen. Het
[acceptatiecontract](10_fabric_baseline_acceptatie.md) bevat de gewijzigde
bewijsstrategie: repositorybaseline, historisch documentatiebewijs,
synthetische golden leveringen met onafhankelijke verwachtingen en werkelijke
Fabric-tests. Eventueel bewaarde exports zijn nog niet aangetoond.

**Waarom:** opnieuw bouwen van Databricks herstelt niet automatisch de oude
data-/runtimestaat; alleen counts uit verslagen zijn onvoldoende bewijs.
Productierijpheid vereist nog steeds alle afgesproken correctheids-, herstel-,
beveiligings-, deployment- en beheertests.
**Grens:** geen exacte gelijkheid met verdwenen Databricks-uitkomsten claimen
zonder herleidbaar bewaard input-/outputbewijs.
**Volgende stap:** generators/fixtures inventariseren en golden scenario's
voor de negen objecten concretiseren. Geen code of data gewijzigd in deze
correctiestap; fase 1 blijft in uitvoering.

### Actuele doelstelling na B-FAB-014

De opdrachtgever verduidelijkt dat aansluiting op de oude Databricks-omgeving
niet nodig is. Deze instructie vervangt eerdere eisen voor platformpariteit,
oude referentieruns en reconstructie van historische verwachte datasets.
De historische beschrijving van die voorstellen blijft behouden, maar is
niet meer het actuele uitvoeringsplan.

**Waarom:** het gewenste eindproduct is een zelfstandig goed functionerend
Fabric-template. Oude platformuitkomsten zijn geen maatstaf voor vrijgave.
V2 levert ontwerpinput; noodzakelijke verbeteringen mogen bewust afwijken.

**Blijvende scope:** SALES, CBS, ECB, NAGER en FABRIC_SALES, met de negen
afgesproken objecten. Kwaliteit, historie, herstel, beveiliging, deployment,
beheer en herbruikbaarheid blijven te testen eigenschappen.
**Vervallen werk:** ophalen van Databricks-bewijs, historische vergelijkingen,
reconstructie van oude golden datasets en een verplichte parallelcutover.
**Nieuwe acceptatiebasis:** eigen versioneerbare Fabric-contracten en
controleerbare tests van het nieuwe systeem; uitgewerkt in sectie 9 van het
[acceptatiecontract](10_fabric_baseline_acceptatie.md).

**Fase-1-gevolg:** lokale inventarisatie en technische acceptatiebasis zijn
beschikbaar; oude referentiedata zijn geen blocker meer. Numerieke
productie-/hersteldoelen blijven open voor productieacceptatie.
**Vervolg:** templatecontract en foundationontwerp uitwerken; gerichte
testdata tijdens implementatie toevoegen in plaats van een historisch
reconstructieproject te starten.
**Uitvoering:** documenten aangepast; geen code of Fabric-data gewijzigd.

### Expliciet end-to-end laagcontract B-FAB-015

De opdrachtgever bevestigt dat opnieuw een Raw-laag nodig is en de hele
verwerking tot en met Gold Current, inclusief alle tussenlagen.

Hier betekent **Raw/Landing** de oorspronkelijke bronleveringen; dit is
iets anders dan de **Raw Vault** met hubs, links en satellites.

```text
Bronextractie / aangeleverde bestanden
                  |
                  v
Raw / Landing: originele, herleidbare leveringen + manifest
                  |
                  v
Bronze: bronrecords + technische herkomst
                  |
                  v
Delivery-gate: volledigheid en verwerkingsvolgorde
                  |
                  v
Quality ----------------------> Reject
                  |
                  v
Reconciliatie: invoer en uitkomsten verantwoord
                  |
         +--------+---------+
         v                  v
     Raw Vault       Referentiedataroute
         |                  |
         v                  |
   Business Vault <---------+
         |
         v
Gold Historical: dimensionele historie
         |
         v
Gold Current: laatste complete succesvolle publicatie
```

Metadata, runcontext, audit, foutafhandeling en monitoring besturen en
begeleiden de volledige keten. Reject is een zijroute met foutcontext,
geen stap waarin alle geslaagde records worden verwerkt.
Referentiedatabronnen mogen de daarvoor bedoelde Business Vault-route volgen;
zij hoeven niet kunstmatig hubs/links te krijgen. De Vault-lagen blijven
wel daadwerkelijk onderdeel van het template en de toepasselijke bronketens.

| Laag | Vereist resultaat |
|---|---|
| Raw / Landing | Originele bronlevering bewaard, herleidbaar en gesloten verklaard; concrete bewaartermijnen nog vast te stellen |
| Bronze | Bronrecords met bestands-/bron-/deliveryherkomst, zonder verborgen businessfiltering |
| Quality | Getypeerde en gevalideerde records volgens projectmetadata |
| Reject | Afgekeurde records met payload, redenen en traceerbare status |
| Reconciliatie | Geen onverklaard verlies of duplicatie; expliciete filters apart verantwoord |
| Raw Vault | Toepasselijke hubs, links en satellitehistorie |
| Business Vault | Afgeleide businesslogica, referentiedata en toepasselijke PIT/bridgefunctionaliteit |
| Gold Historical | Historiserende dimensies en feiten volgens het eigen modelcontract |
| Gold Current | Consistente actuele publicatie van de laatste volledig geslaagde businessload |

De besloten Lakehouse-indeling blijft geldig: Raw/Landing en de verwerkings-
lagen in processing, metadata/audit in control, Gold-lagen in gold.
Dit is een logisch contract; fysieke schema's en toegangsgrenzen worden
in het foundationontwerp uitgewerkt.

**Waarom:** herbruikbaarheid moet de volledige metadata-gedreven keten omvatten,
niet alleen bronextractie of een snelle mart.
**Grens:** er zijn nog geen lagen ingericht of end-to-end runs uitgevoerd.
Kleine verticale tests zijn tussentijdse validaties, geen vervanging van
het volledige eindresultaat.

### Commandogestuurde uitvoering en feedbackloop

De opdrachtgever vraagt of Fabric vanuit VS Code actief kan worden aangestuurd
met commando's, terwijl het proces via een feedbackloop wordt bewaakt.

**Capability:** ondersteunde Fabric-items kunnen via REST/deploymenttools
worden gepubliceerd en via Job Scheduler worden gestart en gevolgd.
**Actuele status:** browsertoegang is bewezen; Entra-authenticatie voor
commandoregel/API, itemdeployment, jobstart en status-/auditophaling zijn nog
niet ingericht of getest. Browseraanmelding alleen bewijst geen CLI-toegang.

**Voorgestelde loop:**

1. Lokale metadata/code/deploymentvalidatie.
2. Deploy van expliciete projectversie naar de bedoelde workspace.
3. Provisioningoperaties tot eindstatus volgen; HTTP 202 is geen eindresultaat.
4. Setup- of ETL-job starten met expliciete parameters en instance-ID bewaren.
5. Jobstatus begrensd volgen met deadline en Retry-After/backoff; na een
   onzekere startresponse eerst de bestaande run onderzoeken, niet blind
   dezelfde job opnieuw starten.
6. Bij voltooiing laag-/deliveryaudit, reconciliatie en publicatiestatus
   controleren; jobsuccess is niet hetzelfde als een geslaagde businessload.
7. Fouten analyseren, gerichte wijziging maken, lokaal testen en gecontroleerd
   opnieuw uitvoeren. Geen onbeperkte retries of ongeautoriseerde dataverwijdering.
8. Versie, run-ID, waarnemingen en resultaat in het verslag vastleggen.

**Productiegrens:** interactieve assistentie kan deze loop uitvoeren tijdens
een actieve sessie, maar is geen permanente 24/7-monitor. Productiebewaking,
alerts en toegestane automatische retries moeten duurzaam in pipelines,
monitoring en CI/CD worden ingericht. Codewijzigingen in productie worden niet
automatisch op basis van een alert gepubliceerd.

Officiele bronnen, geraadpleegd op 10 oktober 2026:

- [Run on-demand item job](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/run-on-demand-item-job).
- [Get item job instance](https://learn.microsoft.com/en-us/rest/api/fabric/core/job-scheduler/get-item-job-instance).
- [Long-running operations](https://learn.microsoft.com/en-us/rest/api/fabric/articles/long-running-operation).

Geen API-aanmelding of commandogestuurde job uitgevoerd in deze verkenning.

### Operatorinterface: alternatieven voor Streamlit

De opdrachtgever vraagt naar een Streamlit-alternatief. Voor de bestaande
Control Room moeten twee functies apart worden beoordeeld: read-only inzicht
en gecontroleerde operatoracties. Een dashboard alleen vervangt geen
remediationinterface.

| Optie | Passend gebruik | Afweging |
|---|---|---|
| Power BI op audit/controlgegevens | Delivery-, DQ-, SLA- en Gold-publicatieoverzichten | Microsoft-stackintegratie; refreshlatency en autorisatie testen; geen volledige actieconsole op zichzelf |
| Fabric Monitoring | Platformstatus van uitvoeringen | Aanvulling op eigen businessaudit, geen vervanging van delivery-/publicatiecontracten |
| Power Apps plus beveiligde actiebackend | Formulieren en gecontroleerde operationele opdrachten | Licenties, connectoren, identiteit en fout-/runfeedback expliciet onderzoeken |
| Streamlit behouden met passende externe hosting | Bestaande Python-console aanpassen naar Fabric | UI-hergebruik mogelijk, maar hosting/authenticatie en Databricks-SQL-/SDK-backend moeten worden vervangen |
| Eigen webapp, bijvoorbeeld React met API-backend | Volledige controlroom en fijnmazige runfeedback | Meer ontwikkel- en beheerwerk |

**Advies, nog niet besloten:** eerst eigen audit en API-/jobfeedback bouwen;
begin read-only met Power BI/Fabric Monitoring. Kies daarna de operator-UI
op basis van benodigde acties, licenties en hosting. Als bestaande functionaliteit
snel behouden moet worden, kan Streamlit blijven met een Fabric-backend en
passende hosting; een productwissel is geen doel op zichzelf.

Operatoracties horen achter een geautoriseerde uitvoerroute met reden,
uitvoerder, audit en begrensde retries. Rechtstreekse audit-/pointermutaties
vanuit een dashboard zijn geen standaardontwerp.
Geen hosting, UI-tool, licentie of connector is in deze stap ingericht of
technisch gevalideerd. Geen keuze voor vervanging van Streamlit genomen.

### Formele afronding fase 1: M-009 / B-FAB-016

Op verzoek van de opdrachtgever is fase 1 afgesloten zonder nieuwe
akkoordvraag. De actuele doelstelling is een zelfstandig Fabric-template
met alle lagen en vijf bronsystemen, niet een vergelijking met Databricks.

De [fase-exitcontrole en overdracht](10_fabric_baseline_acceptatie.md)
bevatten de resultaten, redenen voor vrijgave en open punten per vervolgfase.
Lokale regressie is opnieuw uitgevoerd: **133 passed in 0.50s**.
De metadatareleasecheck slaagt met dezelfde fingerprint als de eerste controle.

**Statusbesluit:** inventarisatie en technische acceptatiebasis gereed;
fase 1 afgerond, overgang naar fase 2. Eerdere tekst over "fase 1 in uitvoering"
blijft het historische verloop beschrijven en is niet de actuele status.
**Functioneel:** volledige Raw-tot-Gold-Current-scope blijft verplicht.
**Technisch:** geen nieuwe runtime of Fabric-uitvoering gecertificeerd;
de tests betreffen de bestaande lokale code.
**Projectmatig:** ontbrekende productieparameters zijn overgedragen als
voorwaarden voor latere productieacceptatie, niet stilzwijgend ingevuld.
**Uitvoering:** alleen documenten/status gewijzigd; geen commit/push,
itemaanmaak, ETL-job of datamutatie.
**Vervolg:** fase 2, eigen template- en doelarchitectuurcontract uitwerken.

### Start fase 2: templatearchitectuur versie 0.1

Op 10 oktober 2026 geeft de opdrachtgever opdracht met fase 2 door te gaan.
Het [template- en architectuurcontract](11_fabric_template_architectuur.md)
is opgesteld, met generieke/project-/omgevingsscheiding, drie Lakehouses,
metadatareleasepinning, manifests, deterministische context, herstelmodel,
Vault-/historiecontracten en deploymentfeedback.

**Belangrijke voorstellen en onderbouwing:**

- Batchtijd eenmaal persisteren; technische attempts apart van businessbatch.
- Datawrites idempotent en state herstelbaar; geen batchpurge als retrybasis.
- Metadatarelease pinnen om gemengde configuratie tijdens een run te voorkomen.
- Bronmultipliciteit behouden; zakelijke duplicaten niet stil in Bronze wissen.
- Gold Current bouwen als immutable productreleases met releasepinning voor
  consumers; geen ongecontroleerd slothergebruik bij actieve readers.

Deze voorstellen zijn niet automatisch als opdrachtgeverbesluiten behandeld.
SQL-/Power BI-consumptie heeft invloed op publicatie, latency en kosten en
blijft een expliciete ontwerpkeuze. Referentiedata behouden hun passende
Business Vault-route; volledige laagscope is niet gereduceerd.

**Uitvoering:** nieuw ontwerpdocument en README-verwijzing opgeslagen.
Geen runtimecode, Fabric-item, workspace-instelling of data gewijzigd.
**Validatie:** lokale documentlinks en statusregistratie controleren;
runtime-/concurrencybewijzen horen bij implementatie en zijn niet geclaimd.
**Fase-exit:** fase 2 niet afgerond; belangrijke voorstellen/consumercontract
nog bevestigen en technische details concretiseren.

### Consumerkeuze: voorkeur zonder Import

Op 10 oktober 2026 geeft de opdrachtgever aan bij voorkeur geen Power BI
Import te gebruiken. Het eerdere Import-advies is ingetrokken als primair
uitgangspunt. DirectQuery en Direct Lake worden onderscheiden in het
[architectuurcontract](11_fabric_template_architectuur.md).

**Waarom:** consumerontwerp moet aansluiten op de gewenste toegang zonder
volledige Import-kopie. Geen Import gebruiken betekent niet automatisch
Direct Lake en garandeert geen consistente release over meerdere queries.
**Advies:** DirectQuery als eerste route voor SQL-viewgebaseerd Gold Current;
Direct Lake als alternatief met expliciet Delta-/framingcontract.
**Open keuze:** primaire leesroute nog bevestigen; geen semantic model,
Fabric-item of runtimecode gewijzigd.

**Vervolgens bevestigd:** opdrachtgever kiest DirectQuery via het SQL-endpoint
zonder Import. B-FAB-017 is vastgelegd en het architectuurcontract aangevuld.
T-SQL-views worden apart gedeployed; Delta-data worden via Spark geschreven.
Querylatency, toegangsgrenzen, SQL-zichtbaarheid en consistentie over
meerdere queries blijven implementatie-/acceptatietests, geen bewezen
eigenschappen van dit besluit.

### M-011 - Vervolgmandaat, templatekern, foundation en volledige redeploy-eis

**Opdracht:** doorgaan tot het einde. Aanvullend wil opdrachtgever aan het
projecteinde aan de assistent kunnen vragen alle stappen opnieuw uit te rollen.
Dit is vastgelegd als B-FAB-018/019. Een redeploy moet vanuit versiebeheer en
doelbindingen kunnen gebeuren, niet vanuit alleen browserhandelingen of
onthouden chatcontext.

**Afweging en beslissing:** nieuwe platformonafhankelijke `fabric_etl`-kern
naast de v2-baseline. Een bulkrename van de oude package zou platformdefaults,
UC-namespaces en onbewezen retrygedrag meenemen. De eerste kern bevat daarom
strikte JSON-/bindingcontracten, expliciete hashconventie, herstelbare
batchcontext, gesloten-manifestvalidatie en een conservatieve chronologiegate.
De kern is onafhankelijk van PySpark; daadwerkelijke Spark-/OneLake-adapters
en het volledige metadatamodel zijn nog vervolgwerk.

**Uitvoering:** een foundationrunner gebruikt ondersteunde Azure CLI-
authenticatie en de publieke REST API. Hij bewaakt schema-enabled inrichting,
identiteit, provisioning en deadlines. GET-retries zijn begrensd; POST wordt
niet blind herhaald. Redirects/afwijkende origins worden geweigerd. Geen
DELETE, datareset of automatische wijziging van omgevingsbindings.

**Live waarneming:** de CLI gebruikt een user-identiteit; zowel `az rest` als
de nieuwe runner krijgen HTTP 401 / UserNotLicensed. Geen browsertokens
uitgelezen en geen betaalde licentie/capaciteit aangekocht. Met de bestaande
aangemelde browser zijn wel drie lege schema-enabled Lakehouses ingericht.
Tables/dbo, Files en automatisch aangemaakte SQL-endpoints zijn zichtbaar.
Item-ID's staan in de [dev-binding](../environments/contoso.dev.json).
Deze browserbootstrap is expliciet geen bewijs van commandoredeploy.

**Verificatie:** 69 nieuwe tests; volledige regressiesuite **202 passed in
0.60s**. De oorspronkelijke metadatarelease blijft geldig met fingerprint
`40b0fbb42f32c19b0ca4417f41b561af631d7d22a2e6730133c9f0f895a2694a`.
Geen editorerrors in de vijf nieuwe implementatiemodules. De fake-API-test
bewijst twee foundationdeploys met gelijke IDs, precies drie creates en
geen deletes; het live API-bewijs is juist een expliciete blokkade.

**Beheer-/acceptatiegevolg:** alle toekomstige stappen moeten uiteindelijk
via een volledige releaserunner uitgerold en nogmaals gedeployed worden,
inclusief metadata, package/Environment, notebooks, pipelines, migraties en
Gold/DirectQuery-contracten. Redeploy en replay zijn verschillende acties.
Zie het [deployment- en redeploycontract](12_fabric_deployment_beheer.md) voor
werkende commando's, bewijs, herstelgrenzen en resterende eindacceptatie.
Geen volledige ETL-run, vijfbronacceptatie of productiecertificering geclaimd.

### M-012 - Tenantcorrectie en echte commandofeedback bewezen

**Aanleiding:** interactieve Azure CLI-login afgerond, maar de Fabric API
bleef UserNotLicensed melden. Herhaald aanmelden alleen loste dat niet op.
Opdrachtgever bevestigde hetzelfde account als de Fabric-browser te gebruiken;
CLI-accountvergelijking bevestigde dit en de browser toonde Pro.

**Onderzoek en bewijs:** twee toegankelijke tenant-ID's vastgesteld. De
standaard CLI-tenant `5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad` gaf de fout.
Expliciete Fabric-tokenaanvraag voor
`f9350d85-6f0e-42fd-8385-a59c2f09ec1a` gaf succesvolle read-only toegang tot
de workspace-items. Tokens zijn alleen gebruikt in procesgeheugen en niet
in bestanden, browserextractie of rapportage opgenomen.

**Root-causefix:** doelbinding bevat verplicht tenant_id, contractversie 1.1.
AzureCliTransport gebruikt altijd de geconfigureerde tenant in tokenacquisitie.
Geen globale CLI-subscriptionwissel en geen impliciete fallback. Oude 1.0-
bindingen falen tot tenant en versie expliciet zijn aangepast (B-FAB-021).

**Echte uitvoering:** deploy-foundation tweemaal gestart met de dev-binding.
Beide resultaten SUCCEEDED, scope foundation only. Alle drie Lakehouses
verified, schemas aanwezig en SQL-endpoints Success. Dezelfde Lakehouse- en
endpoint-ID's; geen nieuwe items, deletes of datawrites. Dit bewijst
commandogestuurde herhaalde verificatie/reconciliatie, niet een volledige
lege-workspace-uitrol of Raw-naar-Gold-runtime.

**Regressie:** 74 gerichte tests; volledige suite 207 passed in 0.55s.
Metadatareleasecheck geldig met actuele fingerprint
`d4746f655f74448732ab5d552d80b9cf591d38976e9204fbae196cfc44a2da60`.
Deze wijkt af van de eerder gerapporteerde waarde. Extra vergelijking van
alle seed-JSON met Git HEAD bevestigt gelijke inhoud en dezelfde actuele
fingerprint voor HEAD. Geen oorspronkelijke v2-code/seeds gewijzigd;
oorzaak van de oudere rapportwaarde niet vastgesteld.

**Vervolg:** API-authenticatieblokkade opgeheven; foundationdata/control-
migraties, Environment, ETL-items en volledige redeploy kunnen nu met een
echte commandofeedbackloop worden uitgewerkt. Zie
[deploymentcontract](12_fabric_deployment_beheer.md) voor bewijsgrenzen.

### M-013 - Fase 2 afgerond onder expliciet mandaat

**Opdracht:** opdrachtgever geeft op 10 oktober 2026 expliciet toestemming
door te gaan totdat fase 2 klaar is. Deze stap sluit de architectuurfase;
geen nieuwe ETL-runtime uitgerold tijdens deze afsluiting.

**Aanleiding:** de eerdere architectuur had nog open alternatieven voor
volledige projectmetadata, type-/bronrecordidentiteit, veilige ownership,
historieherstel en rapportbrede DirectQuery-consistentie. Deze zijn nu
vastgelegd in het [normatieve ontwerpcontract](13_fabric_ontwerpcontract_fase2.md).

**Besluiten en onderbouwing:**

- Projectmetadata als volledige gefingerprinte release met expliciete
  collecties, schema's, logische targets, DAG, business-SQL en policies.
  Geen los muterende actieve seedtabellen tijdens een batch.
- Typecanonicalisatie vastgelegd, inclusief decimalen, UTC/DST en de
  bewuste case-insensitive hash-v1-grens. Raw-recordidentiteit via JSON/CSV-
  locators en Parquet-multisets behoudt echte bronduplicaten.
- Eén writer per project in de eerste versie. Bronparallelisme is
  afgewezen omdat verschillende bronnen dezelfde downstreamtargets kunnen
  wijzigen. Vooraf bestaande Delta-owner-rij met conditional UPDATE/readback;
  geen runtime lock-INSERT en geen automatische TTL-overname.
  Echte concurrency-/stopproeven blokkeren operationele writer-vrijgave.
- Immutable committed historie-events in plaats van batchpurge of een
  kwetsbare authoritative UPDATE-then-INSERT-SCD2. Intervallen afleiden;
  PIT en as-of-joins expliciet modelleren.
- General Gold Current SQL-views bieden queryconsistentie. Power BI
  DirectQuery gebruikt release-specifieke views en een gepind model/rapport
  naast de vorige consumerrelease. Een dynamische pointer per visual is
  afgewezen: die biedt geen rapportbrede releasegarantie.
  Consumerartifactretentie, quota en deployment worden expliciet getest.
- Redeploy blijft gescheiden van replay. Zakelijke SLA/RPO/RTO, retentie en
  betaalde productiecapaciteit blijven verplicht voor go-live, zonder
  verzonnen waarden of automatische aankoop.

**Uitvoering:** ontwerpcontract toegevoegd; bestaande architectuur,
deploymenthandleiding, README, besluitregister en actuele fasestatus
bijgewerkt. Eerdere historische verslagen blijven behouden; de normatieve
fase-2-specificatie heeft voorrang op eerdere voorstellen.

**Verificatie en vrijgavegrens:** ontwerp-exitmatrix en vijftien expliciete
bewijsopdrachten T-01 t/m T-15 opgesteld, met passcriteria en relevante
vervolgfasen. Platformdocumentatie bevestigt Delta OCC en variabele SQL-
endpointsync; zij bewijst niet onze nog te bouwen writer of consumers.
De reeds aangetoonde 207 tests en tweemaal echte foundationreconciliatie
blijven bestaande bewijzen, geen nieuw volledige-ketenbewijs.
Deze documentatiestap wijzigt geen v2-code/seeds of Fabric-data.

**Fase-exit:** fase 2 afgerond (B-FAB-026). Klaar voor fase 3:
projectcontractvalidator, Contoso-projectconversie, packaging en versieerbare
deploymentstructuur. Runtime-, full-redeploy- en productieacceptatie blijven
in fasen 4 t/m 10 verplicht. Onder deze opdracht wordt niet zelfstandig
verder gebouwd voorbij de fase-2-afsluiting.

### M-FAB-013 - fase 3 projectrelease, packaging en deploymentvoorbereiding

**Datum:** 10 oktober 2026. **Status:** lokale fase-3-deliverables afgerond.
**Opdracht:** doorgaan tot en met het einde van fase 3.

Volledige onderbouwing, alternatieven, gevonden fouten, herstelkeuzes,
identiteiten en runtimebewijsgrenzen staan in het
[fase-3-acceptatieverslag](14_fabric_fase3_release_acceptatie.md).

**Uitgevoerd:** volledige structurele projectvalidator, expliciete Contoso-
conversie met vijf bronnen/negen objecten, volledige Gold-kolomdeclaraties,
standalone fabric-etl-wheel 0.1.0, Environment-/Notebookdefinities,
namespace- en control-DDL, geordend lokaal plan en voorbereidingscommando.
De originele v2-code, seeds en packaging zijn ongewijzigd.

**Bewijs:** 284 regressietests slagen. Tweemaal voorbereiding geeft identieke
wheelbytes en plan-ID. Geisoleerde import komt daadwerkelijk uit de wheel,
zonder v2-package. Runtimevrijgave faalt expliciet met 70 geregistreerde
gates. Geen remote deployment of ETL-datawrites uitgevoerd in deze fase.

**Exit en vervolg:** lokale fase-3-release/deploymentstructuur afgerond.
Volgende fase is 4: echte Environment-/Notebookdeployment, controlmigraties,
schema-reconciliatie, persistente controladapter en ownershipproeven.
De volledige Raw-tot-Gold-Current-keten, DirectQuery-consumer en volledige
redeployacceptatie blijven vervolgwerk. Onder dit mandaat stopt uitvoering
na fase 3; er wordt niet impliciet gestart met fase 4.

## Vervolgmandaat: autonome fase 4

De opdrachtgever gaf daarna expliciet toestemming: **"ga zelfstandig door
tot einde fase 4"**. Fase 4 is gestart met echte REST-deployment,
Environment-publicatie en Spark-feedback; de beperkingen van fase 3 blijven
historisch correct en worden niet achteraf als runtimebewijs herbenoemd.

De voortgang, concrete job-/artifact-ID's, geobserveerde API-afwijkingen,
negatieve feedback en nog open control-/rechtenbewijzen staan in het
[fase-4-platformrapport](15_fabric_fase4_platformbewijs.md).
Er is nog geen fase-4-exit zolang vereiste platformbewijzen ontbreken.

### T-13: consumeridentity en rechtenbewijs

Op 10 oktober 2026 gaf de opdrachtgever expliciet toestemming om voor T-13
een afzonderlijke Entra-service principal te maken voor de dev-Gold-consumer.
De eerste `az ad app list/create`-pogingen gebruikten de actieve CLI-
tenantcontext `5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad` en werden geweigerd met
`Insufficient privileges to complete the operation`. De dev-binding wees
echter naar Contoso-tenant `f9350d85-6f0e-42fd-8385-a59c2f09ec1a`.
Een expliciete Microsoft Graph-controle in die doeltenant bevestigde
`bobstoelinga@4kjwn2.onmicrosoft.com` als Member en vond geen appregistratie
met de afgesproken naam.

De tenantgerichte aanmaak is daarna geslaagd:

| Identiteit | Object-ID | Client-/app-ID | Status |
|---|---|---|---|
| Entra-app `contoso-fabric-dev-gold-consumer-t13` | `efd3474a-c62f-46bc-96f0-7ae1d27d763d` | `05b4a703-823e-47bf-9cda-bdd35e6a03bd` | Single tenant; teruggelezen |
| Service principal `contoso-fabric-dev-gold-consumer-t13` | `653455eb-7bb1-4000-8330-9aeef97e10ca` | `05b4a703-823e-47bf-9cda-bdd35e6a03bd` | Enabled; teruggelezen |

Er is nog geen certificaat, client secret, API-permissie, workspace-rol,
Lakehouse-/SQL-grant of OneLake-securityrol uitgegeven of toegekend. Er is
dus nog geen authenticatie- of toegangstest uitgevoerd. T-13 blijft open:
Gold-lezen moet met deze identiteit slagen, Raw/control en credentialtoegang
moeten worden geweigerd, zowel via SQL als OneLake/Spark. Er wordt geen brede
workspace-memberrol als shortcut toegekend. De volledige opdracht-,
tenantdiagnose-, Graph-uitvoer-, validatie- en herstelregistratie staat in het
[fase-4-platformrapport](15_fabric_fase4_platformbewijs.md).

Vervolgens zijn met de aangemelde gebruikersidentiteit alleen read-only
Fabric REST-controles uitgevoerd. De workspace-role assignment-response
bevestigde Bob als Admin; dit geeft geen uitsluitsel over directe itemshares.
Alle drie Lakehouses retourneerden via OneLake Data Access Security uitsluitend
de platformrol `DefaultReader`. Dit endpoint is evenmin een volledige
inventaris van itemdeling.

De gedeelde Fabric-portal toont op dit moment alleen de publication-tabellen
`active_product`, `consumer_catalog` en `product_release` in Gold; het contract
beschrijft deze als release-/consumerpointermetadata, niet als business-
datamart. De service principal werd in de Share-ontvangerspicker herkend, maar
er is bewust niet op Grant gedrukt: SQL- en Spark-ReadAll zouden Lakehouse-
breed zijn en er bestaat nog geen goedgekeurd business-Goldobject voor de
positieve consumerproef. Er zijn geen rechten veranderd, credentials
uitgegeven of T-13-toegangstests uitgevoerd.

Details, portalwaarnemingen, API-bewijs, documentatiebronnen en toetsstappen
staan in sectie 9 van het
[fase-4-platformrapport](15_fabric_fase4_platformbewijs.md). Formele T-13-exit
blijft geblokkeerd tot een expliciet testobject is goedgekeurd en positieve/
negatieve SQL- en Spark/OneLake-tests met effectieve least-privilege-grants
daadwerkelijk zijn bewezen.

#### Uitvoeringsupdate 11 oktober 2026: fixture en aanvankelijke shareblokkade

De opdrachtgever keurde de synthetische fixture goed. `publication.t13_gold_read_test`
is aangemaakt in Gold en bevat uitsluitend één testrecord; een Spark SQL-
SELECT bevestigde de fixture. De OneLake-rol `T13GoldTestRead`
(`42373883-c2c4-4bc2-8ce2-e4a4baff5fa0`) is opgeslagen en via Fabric REST
teruggelezen als `Read` op uitsluitend
`/Tables/publication/t13_gold_read_test` voor de T-13-service principal.
De rol beschermt niet zelfstandig tegen de Lakehouse-`DefaultReader`-rol:
die geeft `Read` op `*` aan principals met `ReadAll`, zodat brede Spark-
toegang bewust niet is toegekend.

De eerste Lakehouse-sharepoging blokkeerde **Grant** met “ReShare permission
required to do the share.” De latere readback en proefresultaten staan in de
aanvullende uitvoeringsupdate hieronder; dit blok legt alleen de aanvankelijke
waarneming vast.

#### Aanvullende uitvoeringsupdate 11 oktober 2026: OneLake-consumerproef

Via **Manage permissions > Add user** is daarna basis-`Read` verleend op Gold;
Direct access bevestigde de T-13-service principal met `Read`. Er zijn geen
rechten verleend op Control of Processing, geen workspace membership,
SQL-/Spark-`ReadAll` of **Execute Apache Spark**.

Met een tijdelijk, niet-exporteerbaar certificaat verkreeg de service
principal een Storage-audience-token. De credential is na afloop uit de
appregistratie verwijderd en het certificaat met private key is lokaal
verwijderd. De directe OneLake DFS-proeven met deze identiteit gaven:

| Proef | Resultaat |
|---|---|
| Gold `publication.t13_gold_read_test`-pad listen | HTTP 200 |
| GET van het synthetische fixture-Delta-logbestand | HTTP 200 |
| Control `Tables`-pad listen | HTTP 403 |
| Processing `Tables`-pad listen | HTTP 403 |
| PUT naar uniek tijdelijk Gold-`Files`-pad | HTTP 403; bestand niet aangemaakt |

Dit bewijst de OneLake DFS-pad-/bestandsleesgrens voor deze service principal,
inclusief geweigerde Control-/Processing-reads en schrijven op Gold. De
Lakehouse-brede item-`Read`-grant blijft aanwezig; brede SQL-/Spark-ReadAll is
niet gegeven. De aanvankelijke SQL-poging in deze registratie liep vast vóór
login op de pre-login-handshake. Zie de latere uitvoeringsupdate voor de
geslaagde, herhaalde SQL-proef en het fase-4-exitbesluit.

#### Laatste uitvoeringsupdate 11 oktober 2026: SQL en fase-4-exit

De SQL-proef is herhaald met Microsoft.Data.SqlClient 6.1.3. De consumer kon
verbinden met Gold. Een expliciete `GRANT SELECT` op uitsluitend
`publication.t13_gold_read_test` was nodig; de permission-readback bevestigde
deze ene objectgrant voor `contoso-fabric-dev-gold-consumer-t13`. De
service-principalquery retourneerde precies één rij:
`T13_SYNTHETIC_FIXTURE` / `fixture_id = 1`.

Met dezelfde identiteit werden verbindingen naar de SQL-endpoints van Control
en Processing geweigerd (SQL-fout 18456: authenticatie geslaagd, database-
toegang onvoldoende). In combinatie met de OneLake-resultaten hierboven
(Gold HTTP 200; Control/Processing en write HTTP 403) is de consumergrens
positief en negatief bewezen voor SQL en OneLake. Een SQL-INSERT-probe kon
geen autorisatieconclusie opleveren: dit Lakehouse-endpoint ondersteunt DML
op deze tabel niet (fout 24559); de transactie is gerold back en er is geen
data gewijzigd. De write-weigering is bewezen door de afzonderlijke OneLake
PUT-probe.

De SQL-evaluatie `HAS_PERMS_BY_NAME` onder de consumeridentiteit bevestigde
voor de Gold-fixture `SELECT = 1` en `INSERT`, `UPDATE`, `DELETE` en `ALTER`
elk `= 0`. De Direct access-readback toont alleen basis-`Read`; er is geen
`Reshare`- of workspace-role-toekenning aan deze consumer.

De tijdelijke certificaatcredential is ingetrokken en het lokale certificaat
verwijderd; readback vond nul tijdelijke T-13-credentials. Er is geen
workspace-role, SQL-/Spark-ReadAll of **Execute Apache Spark** verleend. Een
aparte notebookrun als service principal is bewust niet gestart: de
Fabric-notebook Job Scheduler-documentatie vereist hiervoor een workspace-
rol Admin, Member of Contributor, wat de afgesproken least-privilege-scope
zou verbreden. De directe OneLake DFS-read is de uitgevoerde OneLake-
consumerproef.

**Besluit:** de controlfoundation en T-13 voldoen aan de fase-4-acceptatie;
fase 4 is afgerond. Volledige business-ETL, broningestie, ketenredeploy en
DirectQuery-consumers blijven vervolgwerk buiten deze fase. Er is geen
Databricks-worktree, Access Connector, storage-accountconfiguratie of
Databricks-container uit de afzonderlijke Databricks-dev-heruitrol gewijzigd.

### Aanvulling: bestaande bron- en tabelmetadata

De repository bevat al gedetailleerde bronobject-, connector-, kolommapping-,
quality-, Data Vault 2.0- en Gold-doeltabelmetadata plus SQL-transformaties.
Deze artefacten zijn de baseline voor het bouwen van de ETL-laag en voor het
reproduceren van de vijf bronnen/negen objecten; ze zijn geen bewijs dat de
brondata of business-tabellen al in Fabric aanwezig zijn. SALES is als
Contoso ERP-export met vijf Parquet-objecten beschreven; CBS, ECB en NAGER
zijn externe HTTP-bronnen; FABRIC_SALES is een aparte JDBC-route. De
SHAREPOINT-bronnen zijn in de projectfixture uitgesloten.

Een deel van de seedmetadata bevat nog Databricks Volume/catalog-paden en
moet daarom naar goedgekeurde Fabric/OneLake- of Blob-doelbindingen worden
vertaald. De onderzochte repository bevat geen oorspronkelijke SALES-
Parquetleveringen en er is geen aparte Blob-account/shortcut ingericht.
Synthetische fixtures worden niet als originele Contoso-data aangemerkt.
De volledige inventaris en bewijsgrenzen zijn toegevoegd aan sectie 10 van
het [fase-4-platformrapport](15_fabric_fase4_platformbewijs.md).

### Read-only controle Azure Blob Storage-account

Op 10 oktober 2026 is op verzoek read-only gecontroleerd of
`bobstoelinga@4kjwn2.onmicrosoft.com` een Azure Blob Storage-account kan
aanmaken. Azure CLI toont voor de Contoso-tenant
`f9350d85-6f0e-42fd-8385-a59c2f09ec1a` alleen een tenant-level account en geen
subscription. De enige zichtbare subscription
(`9fe6cc02-a83b-4130-8d57-5e8247ce2565`) is gekoppeld aan een andere tenant,
`5e17ad2f-962c-4bf3-bdd5-a1972e4c73ad`. Een directe RBAC-query op die andere
subscription vond geen roltoewijzing voor het aangemelde object; eventuele
groepsgeërfde rechten zijn daarmee niet uitgesloten. Er is niets aangemaakt
of gewijzigd. **Huidige uitkomst: provisioning onder de Contoso-tenant is
niet mogelijk met de zichtbare context, omdat daar geen subscription
beschikbaar is.** Eerst moet een subscription en passende resource-
Contributor-toegang in de goedgekeurde doeltenant worden bevestigd, of moet
expliciet worden besloten dat de andere tenant/subscription de bedoelde
doelgrens is. Zie sectie 11 van het
[fase-4-platformrapport](15_fabric_fase4_platformbewijs.md) voor de
gedetailleerde waarnemingen.

### Vervolg: Azure dev-Raw-opslag in dezelfde regio als Fabric

De opdrachtgever koos vervolgens expliciet de subscription onder
`stoelingabob@gmail.com` voor de opslag en keurde de dev-configuratie goed.
Fabric REST bevestigde dat de workspace-capacity
`103e56d3-ccdb-42d8-a56d-ff3d5a6c92f8` in **North Europe** draait. Daarom zijn
resource group `rg-contoso-raw-dev-neu` en ADLS Gen2-account
`stcontosorawdev9fe6cc02` in Azure `northeurope` aangemaakt.

De subscriptiongerichte identity was daadwerkelijk de Gmail-account
(object `03667848-cd1f-48b4-bf68-cba53d7eeafc`) met effectieve
managementrechten; niet het eerder gecontroleerde Bob-gastobject.
Standard LRS, HNS, HTTPS/TLS 1.2, verboden shared-key/anonieme blobtoegang,
private containers `raw`/`test-fixtures` en zeven dagen blob-/container-
soft-delete zijn teruggelezen. Het netwerkendpoint is publiek bereikbaar
met Entra-authenticatie; er is geen private endpoint, locked WORM-retentie
of automatische verwijdering ingericht.

Per container zijn de vijf bronsysteemdirectories SALES, CBS, ECB, NAGER en
FABRIC_SALES aangemaakt en via DFS teruggelezen: tien directories, nul
datafiles. De deploymentuser kreeg storage-account-scoped
`Storage Blob Data Contributor`; Fabric- of consumerrechten zijn niet
veranderd. Gegenereerde Contoso-data zal expliciet synthetische fixturedata
zijn en apart worden gehouden van echte bronleveringen.

De nieuwe [Azure Raw dev-binding](../environments/contoso.raw.dev.json) en
het [provisioningbewijs](../deploy/evidence/raw-storage-dev-20261010.json)
leggen de configuratie en read-back vast. Sectie 12 van het
[fase-4-platformrapport](15_fabric_fase4_platformbewijs.md) bevat alle
identiteiten, instellingen, folder-/deliveryafspraken en bewijsgrenzen.
Datageneratie, closed manifests, cross-tenant Fabric-toegang en ETL-proeven
zijn nog niet uitgevoerd; de opslagaanmaak is geen volledige fase-4-exit.

### Bevestigde bronverdeling na opslagaanmaak

De opdrachtgever bevestigde dat **alleen SALES** wordt gegenereerd als
metadata-conforme synthetische dev-testdata voor CUSTOMERS, PRODUCTS, ORDERS,
RETURNS en EMPLOYEES. CBS, ECB en NAGER worden uit hun echte externe API's
opgehaald. FABRIC_SALES wordt geextraheerd uit de bestaande SQL-database in
Fabric, met de huidige source-query op
`SalesLT.vw_databricks_sales_order_line`; bereikbaarheid en schema moeten nog
worden geverifieerd. Er is geen synthetische vervanging voor deze vier bronnen
afgesproken.

SALES-fixtures worden apart onder `test-fixtures/SALES` voorbereid; echte
API-/SQL-extracten volgen hun eigen `raw`-deliveryroute. De overige lege
fixturefolders zijn geen opdracht om die bronnen te genereren. Sectie 13 van
het [fase-4-platformrapport](15_fabric_fase4_platformbewijs.md) legt de
bronverdeling, provenance en uitvoeringsgrenzen vast. Deze verduidelijking
heeft nog geen datageneratie, API-ophaling of SQL-extract uitgevoerd.

### Afstemming 11 oktober 2026: parallelle Databricks-dev-heruitrol

De opdrachtgever stemt de Fabric-migratie parallel af met een andere AI-
assistent die de Databricks-dev-heruitrol uitvoert in de aparte worktree
`D:\Contoso-deploy`. Deze assistent werkt uitsluitend aan de Fabric-kant:
Fabric-status, contractgrenzen en dit migratieverslag. Er is geen gedeelde
codewerkmap of gedelegeerde wijzigingsbevoegdheid tussen beide assistenten;
informatie over de andere werkstroom wordt hier alleen als door de
opdrachtgever gerapporteerde afstemming vastgelegd. De assistenten wijzigen
niet elkaars worktree of resources.

De opdrachtgever meldde dat de Databricks-dev-heruitrol vanuit een aparte
Git-worktree `D:\Contoso-deploy`, gebaseerd op `origin/main` / commit
`bd8936d`, is uitgerold naar workspace
`adb-7405617094653375.15.azuredatabricks.net`. Deze voortgang is door de
opdrachtgever gerapporteerd; zij is niet door dit Fabric-traject technisch
geverifieerd.

Volgens de afstemming gebruikt deze Databricks-dev-inrichting het bestaande
storageaccount `stcontosorawdev9fe6cc02`, maar uitsluitend de eigen containers
`landing` en `contoso-dev-managed`. De bestaande containers `raw` en
`test-fixtures` blijven buiten deze Databricks-inrichting. De Access Connector
`ac-contoso-databricks-dev` heeft volgens de melding Blob Contributor uitsluitend
op de eigen containers. Acht `*_dev`-catalogi, de credential en external
locations zijn aan de nieuwe Databricks-workspace gebonden. De setup was op
het moment van afstemming nog bezig; een kleine SALES-smoketest was de
volgende stap en was nog niet als geslaagd gemeld.

**Grensbewaking:** het Fabric-traject heeft geen wijzigingen aangebracht in
`D:\Contoso-deploy`, de Databricks-workspace, de Access Connector, de
Databricks-containers of de Databricks-catalogi. De Fabric-containers `raw`
en `test-fixtures` zijn niet aangepast. Gedeeld-accountbeheer, zoals
accountbrede netwerk-, lifecycle- of beveiligingsinstellingen, valt buiten
deze gescheiden containerscope en moet bij eventuele wijzigingen vooraf
worden afgestemd. Er is op dit moment geen gemeld conflict tussen de
container-scoped Databricks-toegang en de Fabric-inrichting.

### Afgesproken werkwijze: Databricks als SALES-referentie voor Fabric

De opdrachtgever heeft ingestemd met een parallelle vergelijking om de
Fabric-migratie concreter en toetsbaar te maken, zonder dat beide assistenten
dezelfde werkmap of resources aanpassen. Op 11 oktober 2026 om 09:01 CEST
meldde de opdrachtgever dat de Databricks-dev-baseline functioneel is
gevalideerd. De details hieronder zijn overgenomen uit die melding; deze
Fabric-werkstroom heeft de Databricks-runs en SQL-uitvoer niet zelfstandig
geopend of herhaald.

1. Setup-run `530326121228620` is als `SUCCESS` gemeld.
2. Sales-pipeline-run `960091284575444` is als succesvol gemeld voor alle 13
   taken, met een looptijd van ongeveer 19 minuten en 17 seconden.
3. De opdrachtgever meldde zes actuele Gold-views bevraagd via SQL en hun
   resultaatrijaantallen: customers 2, products 2, employees 2, dates 4018,
   sales 2 en returns 1. De gemelde delivery is `SALES|2026-09-01` en de
   batch-ID is `27f58746-4ebf-47ae-9573-d013d1995743`.
4. Deze Databricks-uitkomst kan als functionele vergelijkingsbaseline dienen.
   De Fabric-werkstroom verwerkt dezelfde afgesproken SALES-fixture volgens
   hetzelfde contract en legt haar resultaten apart vast.
5. Zodra beide bewijzen beschikbaar zijn, vergelijken we de uitkomsten en
   registreren we verschillen en resterend migratiewerk.

**Databricks-spoor en implementatiegrens:** de deployment-worktree
`D:\Contoso-deploy` bevat volgens de opdrachtgever uitsluitend wijzigingen
van het Databricks-werkspoor. Voor dat spoor zijn het oorspronkelijke project
en Fabric niet aangepast, met uitzondering van de op verzoek bijgehouden
registratie in de Contoso-documentatie. De andere AI blijft eigenaar van
Databricks-uitvoering en -deployment; deze Fabric-werkstroom claimt geen
onafhankelijke verificatie van die wijzigingen of runs.

**Seedbug en regressietests:** de opdrachtgever meldt dat een bug bij fresh
setup reproduceerbaar is aangetoond en hersteld. De seed liet per-record
booleanvelden ontbreken (de effectieve waarde was `false`), gebruikte route
`RAW_VAULT`, gebruikte legacy Gold-source `SALES` en had
`publish_status=READY`. Voor de fix zijn **139 tests groen** gemeld in het
Databricks-werkspoor; die testuitvoering is niet in deze Fabric-worktree
herhaald.

**Toegang en nog niet uitgevoerde extracten:** voor de gemelde Databricks-
storage is `Storage Blob Delegator` op accountniveau noodzakelijk naast
`Storage Blob Data Contributor`, beperkt tot de eigen landing- en
`contoso-dev-managed`-containers. Dit wordt hier als gerapporteerde
toegangsvoorwaarde vastgelegd; er zijn in deze stap geen Azure-rechten
gewijzigd. API-, JDBC- en Fabric-extract zijn volgens de opdrachtgever niet
gestart.

Deze Databricks-resultaten vormen een **functionele vergelijkingsbaseline**,
maar bewijzen geen Fabric-pariteit. Een daadwerkelijke vergelijking vraagt
om dezelfde herleidbare fixture en vergelijkbare, afzonderlijk vastgelegde
uitvoer per platform. De Fabric-end-to-end-keten en de runtime-/autorisatie-
acceptatie zijn nog niet voltooid. Wijzigingen in elkaars worktrees of
gedeelde Azure-accountinstellingen blijven buiten deze werkwijze en worden
vooraf afgestemd.

### Fase 5 gestart — P5-01 Raw-delivery- en Bronze-kern

**Datum:** 11 oktober 2026. **Status:** lokale gate en ingestiekern uitgevoerd;
gerichte tests geslaagd en zelfstandige wheel gebouwd; daadwerkelijke
Fabric/Spark-runtimeacceptatie nog open.

**Aanleiding en beslissing:** de fase-3-acceptatie liet delivery-completeness
expliciet open als runtimegate voor fase 5. Omdat voor dit Fabric-spoor nog
geen gesloten bronlevering als runtimebewijs is vastgelegd, is eerst de lokale,
niet-destructieve contractgrens versterkt. Een levering mag pas aan ingestie worden vrijgegeven
als het manifest `CLOSED` is, de exacte verplichte objectscope bevat, het de
oudste chronologisch verwerkbare `READY`-delivery is, en de lokale bestanden
qua bestaan, grootte en SHA-256 overeenkomen. Een oudere open delivery of een
latere manifestlevering blokkeert.

**Uitvoering:** `fabric_etl.delivery.validate_ready_delivery_manifest`
combineert de bestaande strikte manifestvalidatie, chronologiegate en lokale
bestandsverificatie; de bestandsinventaris moet exact met het manifest
overeenkomen. `fabric_etl.ingestion` bevat de Fabric/Spark-kern voor een
expliciete metadata-gedreven schema-inlezing, object-/bestands-scoping,
multisetgebaseerde record-ID's (inclusief bestandfingerprint en occurrence
voor identieke dubbelen), Delta-Bronze read-back/reconciliatie en een
owner-gebonden commitmanifest. De schrijftarget is beperkt tot het gebonden
Processing-Lakehouse en het `bronze`-schema. Lokale tests gebruiken
synthetische testbytes; projectmetadata, bestaande bronbestanden en echte
brondata zijn niet aangepast.

**Validatie:** `tests/test_fabric_contracts.py` — **57 passed** (0,61 s);
volledige projectsuite — **359 passed** (3,20 s). Pylance meldde geen
diagnostics in de gewijzigde kern- en testbestanden. De Pylance-interpreter is
Python 3.13 uit de project-`.venv`; de pytest-run gebruikte
`C:\Python313\python.exe` (3.13.3). De lokale testdata zijn geen echte Parquet-
levering en deze resultaten bewijzen de Spark/Delta-functies nog niet.

**Runtimegrens:** lokaal is PySpark 4.2.0 geïnstalleerd, maar geen Java-runtime;
de lokale Spark-gateway kon daardoor niet starten. In Fabric is een aparte
notebookkopie `contoso_phase5_raw_bronze_dev` gemaakt en aan de bestaande
Processing- en Control-Lakehouses gekoppeld. De Fabric-sessie werd uiteindelijk
`Session ready` op Fabric Runtime 2.0 / Spark 4.1 / Delta 4.2. Een geslaagde,
read-only diagnostische cel meldde dat `fabric_etl` aanwezig was,
`fabric_etl.ingestion` nog niet geïnstalleerd was, de owner-status `FREE` met
generation 7 was en er geen tabellen in `lh_contoso_processing.bronze` waren.
De daaropvolgende smoke-testcellen faalden met `IndentationError` vóór hun
testcode werd uitgevoerd. Die twee misvormde cellen zijn daarna verwijderd
uit de geïsoleerde notebookkopie; de oorspronkelijke cel, geslaagde
read-only diagnose en lege cel zijn behouden. De Fabric-sessie is inmiddels
gestopt/verlopen. Er is geen Bronze-tabel of andere Lakehouse-data door de
smoke-test geschreven; er is dus nog geen ingestie-runtimebewijs.

**Publicatiegate:** de bestaande `contoso_etl_dev`-Environment bevat volgens
de Fabric-inventaris de eerder gepubliceerde `fabric_etl-0.1.0`-wheel. De
geslaagde read-only cel bevestigde dat daarin `fabric_etl.ingestion` ontbreekt.
Het repository-releasecommando valideerde project en binding, maar produceerde
alleen `PLANNED_NOT_DEPLOYED` binnen scope
`PHASE3_FOUNDATION_PREPARATION`, met 70 runtimeblokkades en een open
`etl_environment_item_id`-binding. Dat plan is geen geschikt deployplan voor
de fase-5-module; de Environment is daarom niet aangepast. De lokale wheel
`dist/fabric_etl-0.1.0-py3-none-any.whl` heeft SHA-256
`86c761bd0332e56541ee0561673a5a16c9223e9854868376fc3623997e75e95e`.
Deze wheel heeft dezelfde naam en interne versie als de eerder gepubliceerde
wheel, maar andere bytes; daarom is zij niet via een ongetoetste
overschrijfroute gepubliceerd.
Het niet-uitgevoerde plan heeft ID
`6830a1898ae112cf6dd4e6cbe11f0d0723da4a96290ec936dab9a02f7635db80`.

**Vervolg:** definieer een expliciete fase-5-package/publicatiegate, met
controleerbare packageversie/checksum en een bewezen Python/Spark-ID-hashvector,
voordat de bestaande dev-Environment wordt bijgewerkt. De notebookkopie is
na heropenen bevestigd opgeslagen met de twee foutieve cellen verwijderd.
Daarna is de Fabric-samenwerkingsverbinding verbroken en de Spark-sessie
verlopen; er is niet offline verder bewerkt of uitgevoerd. Hervat na herstel
van de Fabric-verbinding; publiceer gecontroleerd en voer daarna uitsluitend
synthetische SALES-fixtures in Processing/`bronze` uit om manifestinventaris,
exacte objectscope, retries, dubbelen, read-back en gecontroleerd committen te
bewijzen. De bevestigde Databricks-load blokkeert deze Fabric-proeven niet;
fixture- en resultaatgegevens zijn alleen nodig voor de afzonderlijke
pariteitsvergelijking. Fase 5 is in uitvoering, niet afgerond.

### Architectuuroptie onderzocht: één Lakehouse met schema-isolatie

Op 11 oktober vroeg de opdrachtgever of de drie Lakehouses kunnen worden
geïntegreerd en gescheiden via schemas, met behoud van strakke autorisatie.
Microsoft documenteert schema-enabled Lakehouses, schema-shortcuts en
OneLake security op schema-/tabel-/folderniveau. Dit maakt een inrichting met
één Lakehouse en onder meer `control`, `processing` en `gold`-schemas technisch
mogelijk. OneLake security is grant-only: er zijn geen deny-rollen. De
workspace/itembasisrechten en het endpointtoegangstype blijven dus deel van
het beveiligingsontwerp. Workspace Admin/Member/Contributor hebben verhoogde
toegang en worden niet door OneLake RLS/CLS beperkt. Voor SQL OneLake-
handhaving is User identity access mode nodig; het gedocumenteerde standaard-
endpoint gebruikt Delegated identity mode, waarin SQL-permissions gelden.
Een modewissel kan tijdelijk alle SQL analytics endpoints in de workspace
onbeschikbaar maken.

**Architectuurbeoordeling:** consolidatie kan notebookbinding, itembeheer en
cross-item-rechten vereenvoudigen en behoudt domeinscheiding via schemas.
Dit maakt Delta-transacties echter niet multi-table-atomisch; de commit-
manifest-/reconciliatieaanpak blijft nodig. De bestaande drie-Lakehouse-
fundering en T-13-bewijzen zijn al uitgevoerd, dus samenvoegen zou een
architectuurwijziging zijn en T-13/schema-autorisaties opnieuw laten bewijzen.
Er is nog geen consolidatie, schema-shortcut, rolwijziging of
endpointmoduswijziging uitgevoerd. Aanbeveling is eerst een niet-destructieve
autorisatieproef op de bestaande schema-enabled dev-items uit te voeren,
inclusief een least-privilege Viewer/consumer over SQL én Spark; pas na
geslaagde negatieve tests een migratiebesluit nemen. De Databricks-
implementatie/worktree blijft hiervan gescheiden.

Documentatie: [Lakehouse schemas](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-schemas),
[OneLake security access control model](https://learn.microsoft.com/en-us/fabric/onelake/security/data-access-control-model),
[Spark support for OneLake security](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-onelake-security),
[SQL analytics endpoint OneLake security](https://learn.microsoft.com/en-us/fabric/onelake/security/sql-analytics-endpoint-onelake-security)
en [Delta Lake interoperability](https://learn.microsoft.com/en-us/fabric/fundamentals/delta-lake-interoperability).

### Lakehouse versus Warehouse beoordeeld voor het ETL-template

Op 11 oktober 2026 is ook beoordeeld of de oplossing beter naar een Fabric
Warehouse kan uitwijken. **Advies: houd Lakehouse als primaire ETL- en
medallion-opslag; migreer fase 5 nu niet naar Warehouse.** De huidige kern is
Python/Spark, manifeste bestandsingestie, ruwe bestanden en Delta-tabellen,
met metadata-gedreven Raw/Bronze/Vault/Gold-verwerking. Dit sluit direct aan op
de door Microsoft beschreven Lakehouse-rol voor Spark/data-engineering en
medallion-architecturen. De Lakehouse-SQL analytics endpoint kan Gold-data
read-only met T-SQL ontsluiten.

Warehouse heeft een wezenlijk voordeel als een concrete use-case atomisch
meerdere tabellen moet publiceren: expliciete T-SQL-transacties kunnen de
ondersteunde wijzigingen over meerdere Warehouse-tabellen als één commit
behandelen. Dat voordeel geldt niet automatisch voor Spark-writes naar Delta
of voor de huidige Lakehouse-targets. Voor die route zou ook de publicatie-
implementatie naar ondersteunde Warehouse-T-SQL-transacties moeten worden
verplaatst en end-to-end opnieuw worden getest. Warehouse is daardoor een
mogelijke latere bestemming voor SQL-first marts/serving of een bewezen
multi-table transactievraag, maar geen drop-in oplossing voor de Spark-
gebaseerde Raw-to-Bronze- en Vault-verwerking.

**Besluitstatus:** advies vastgelegd; geen Warehouse aangemaakt, data
verplaatst, endpoint gewijzigd of migratiebesluit uitgevoerd. De drie
bestaande Lakehouses blijven de fase-5-proefbasis. Een eventuele combinatie
(Lakehouse voor ingestie/transformaties, Warehouse voor geselecteerde
SQL-first consumptie) vereist eerst een concreet consumer-/transactiecontract
en een aparte vergelijking van beheer, beveiliging en herstel.

Documentatie: [What is a Lakehouse? — vergelijking met Warehouse](https://learn.microsoft.com/en-us/fabric/data-engineering/lakehouse-overview),
[Warehouse-transacties](https://learn.microsoft.com/en-us/fabric/data-warehouse/transactions)
en [What is Fabric Data Warehouse?](https://learn.microsoft.com/en-us/fabric/data-warehouse/data-warehousing).

### Fase 5 voortgang — Databricks-load bevestigd; Fabric-smoke-test nog niet uitgevoerd

**Datum:** 11 oktober 2026. **Status:** Databricks-load tot en met Gold Current
door opdrachtgever bevestigd; Fabric-runtime-ingestie en
autorisatieacceptatie blijven open.

**Correctie op de eerdere registratie:** de opdrachtgever bevestigt expliciet
dat de Databricks-restore is afgerond en dat de load succesvol doorliep tot en
met Gold Current. De vorige formulering dat alleen een restore beschikbaar
was en dat SALES-/Gold-uitvoering nog moest worden vastgesteld, was onjuist
en is hiermee vervangen. De Databricks-implementatie en die uitvoering horen
bij het aparte werkspoor van de andere AI. Dit Fabric-spoor heeft geen
Databricks-run, tabellen of uitvoer onafhankelijk geopend; er worden hier
geen extra aantallen, kwaliteitsresultaten, autorisatiebewijzen of exacte
pariteitsclaims aan toegevoegd.

**Relatie tussen de twee AI-werksporen:** de andere AI heeft de afzonderlijke
Databricks-restore/load uitgevoerd; dit werkspoor werkt aan Fabric. Er is
geen wijziging aangebracht aan de Databricks-worktree, workspace, containers
of load. De door de opdrachtgever bevestigde end-to-end-uitkomst is nuttige
voortgang voor het totale project, maar is geen Fabric-runtimebewijs.

Bij hervatting van de Fabric-smoke-test was de Spark-sessie in de notebook
als `Session ready` zichtbaar. Pogingen om testcode via de interactieve
Monaco-editor in de geïsoleerde notebookkopie in te voeren werden door de
editor onbedoeld opnieuw ingesprongen. De twee testuitvoeringen stopten met
`IndentationError` vóór uitvoering. Er is dus geen bewijs van een geslaagde
Spark-transformatie, Delta-write, owner-claim of autorisatieproef en er is
geen Lakehouse-data door deze pogingen geschreven. De twee foutieve testcellen
zijn inmiddels verwijderd en de notebook is na heropenen als opgeslagen met
drie cellen bevestigd; verdere bewerking is gestopt nadat de
samenwerkingsverbinding wegviel.

**Beveiligingsacceptatie:** blijft een harde vrijgavegate. De owner-lock in de
lokale kern is geen vervanging voor Fabric-identiteit, OneLake-/SQL/Spark-
autorisatie of een least-privilege negatieve test. Geen rol-, endpoint-,
Lakehouse- of security-instelling is gewijzigd. De bestaande T-13-platform-
basis is geen bewijs dat de nieuwe Raw-to-Bronze-write op Fabric is
geautoriseerd en correct afgeschermd.

**Packaging-validatie:** de zelfstandige `fabric-etl`-wheel is lokaal gebouwd
en de inhoudscontrole bevestigt dat `fabric_etl/ingestion.py` is opgenomen.
De gerichte contractsuite is opnieuw uitgevoerd: **57 passed**. Dit bewijst
packaging en lokale contracttests, niet de Spark-/Delta-runtime. De wheel is
niet naar Fabric gepubliceerd.

**Vervolg:** herstel eerst de Fabric-samenwerkingsverbinding. Beproef daarna
een expliciete fase-5-package/publicatiegate en de Python/Spark-ID-hashvector;
pas vervolgens alleen via die gecontroleerde route de bestaande
dev-Environment aan. Voer pas daarna een synthetische Bronze-smoke-test uit
met owner-claim, read-back, retry/duplicatecontrole en opruimen van uitsluitend
het eigen testtarget. De Databricks-load tot Gold Current is door de
opdrachtgever bevestigd; een eventuele fixture-/baselinevergelijking vereist
vergelijkbare, herleidbare uitvoergegevens. Fase 5 is niet functioneel of
technisch opgeleverd zolang Fabric-runtime-, beveiligings- en end-to-end-gates
niet zijn bewezen.

### Aanvulling 11 oktober 2026 — fase-5-wheel, publicatie en bundle-route

**Doel en keuze.** De fase-5-ingestiekern mag niet stilzwijgend de al
gepubliceerde `fabric-etl==0.1.0` vervangen door andere bytes met hetzelfde
versienummer. Daarom is de kern als afzonderlijke
`fabric-etl-phase5==0.1.0`-wheel verpakt. De add-on controleert bij import
dat `fabric-etl==0.1.0` aanwezig is. Dit houdt de bestaande basisdistributie
gescheiden, maar is alleen een werkbare technische tussenstap: de echte
Fabric-Spark-import en API-compatibiliteit moeten nog in de gepubliceerde
Environment worden bewezen.

**Lokale artefacten en checks.**

| Controle/artefact | Uitkomst | Wat de uitkomst wel en niet bewijst |
|---|---|---|
| Add-onwheel `dist/fabric_etl_phase5-0.1.0-py3-none-any.whl` | Gebouwd; SHA-256 `081953584d6aadd7d32528ff2932ba0fdd749f8539e63c38a897d484863270a1` | Identificeert de bytes die voor de Fabric-upload zijn gebruikt; bewijst niet dat Fabric de wheel al importeert. |
| Lokale basiswheel `fabric_etl-0.1.0-py3-none-any.whl` | Gebouwd; de build heeft niet de eerder in Fabric gepubliceerde wheelbytes vervangen | De lokale installatieproef met die build controleert package-resolutie, niet byte-identieke compatibiliteit met de bestaande remote library. |
| Geïsoleerde lokale packageproef | `fabric_etl_phase5` en `fabric_etl_phase5.ingestion` importeren naast `fabric-etl==0.1.0`; de compatibiliteitsmodule is aanwezig in de lokale build | Geen PySpark/Fabric-runtime, Environment-cache of remote package-resolutionbewijs. |
| Golden Raw-ID-contract | Verwachte SHA-256 `90d2b85ced455a8fee9cb1a2644b73a8e5c20ccd4f95e55786f0871937038810`; test controleert bovendien alfabetische veldvolgorde | Bewijst de Python-contractvector. De overeenkomst met Spark `to_json(struct(...))` is nog niet bewezen. |
| Gerichte suite | `tests/test_fabric_contracts.py`: **58 passed**; meest recente herhaling op 11 oktober: **58 passed in 0.32 s** | Lokale contracttests, geen Fabric-job. |
| Volledige suite | **360 passed in 3.05 s** na de laatste codewijziging | Lokale regressiecontrole van de toenmalige repositorystatus; geen platformacceptatie. |
| Pylance | Geen diagnostics voor beide ingestion-modules en `test_fabric_contracts.py` | Editoranalyse, geen runtimebewijs. |

**Environment-publicatie.** De wheel is via Fabric Environment
`contoso_etl_dev` > **Custom libraries** > **Upload to Full mode** toegevoegd.
De bestaande `fabric_etl-0.1.0-py3-none-any.whl` is niet vervangen. Na een
eerste publicatiepoging bleef de interface tijdelijk `Publishing` tonen; een
latere controle op 11 oktober toonde de add-on én de bestaande basiswheel als
`Success` met publicatietijd `10/11/26, 09:19:34 AM` en melding **Publish
succeeded**. Daarmee is de library-configuratie gepubliceerd. Dit bewijst
nog niet dat een notebook een nieuwe Spark-sessie met beide libraries
succesvol kan starten.

**Read-only notebookproef en actuele blokkade.** De geïsoleerde notebookkopie
`contoso_phase5_raw_bronze_dev` heeft een bestaande diagnosecel die alleen
controleert of `fabric_etl` en `fabric_etl.ingestion` importeerbaar zijn, de
ownerstatus leest en tabellen in Processing/Bronze opsomt. De laatst
opgeslagen diagnose-output is ouder dan de nieuwe publicatie en luidde:
framework aanwezig, ingestion-module afwezig, owner `FREE` (generation 7),
geen Bronze-tabellen. Dat is nadrukkelijk geen post-publicatie-uitkomst.
Een nieuwe run via de browserknop kon niet worden gestart: de
samenwerkingsserver meldde dat het notebook locked was voor edits/saves en de
Spark-session meldde timeout. De notebook toonde `Not connected`; de
interactieve `Run all`-actie liep vast op de UI-interactie. Er is daarom nog
geen post-publicatie Fabric-import, Spark-hashvector, Delta-write/read-back,
retry- of securitybewijs. Geen Bronze-data of rollen zijn door deze poging
gewijzigd.

**Bundle/asset-bundle-onderzoek en besluit.** Op 11 oktober zijn de publieke
documentatie van Fabric Automation Bundles en Microsoft Fabric naast de
repositoryreleasecode gelegd. De tool `fab-bundle` noemt zichzelf **Public
Preview** op de eigen projectdocumentatie en wordt gepubliceerd vanuit de
communityrepository `dereknguyenio/fabric-automation-bundles`; het is geen
Microsoft-product of door Microsoft gegarandeerde Fabric-previewfunctie.
De geraadpleegde projectpagina's zijn bovendien niet volledig consistent over
het aantal ondersteunde resource types (30 versus 45). De featureclaims zijn
daarom als hypothesen behandeld, niet als acceptatiebewijs. De tool heeft
ook `destroy`- en rollbackachtige mogelijkheden; geen package is in de
project-`.venv` geïnstalleerd en er is geen derdepartij-CLI met tenantrechten
uitgevoerd.

De repository heeft al een eigen declaratieve basis:
`fabric/deployment.json` registreert items, afhankelijkheden, migraties,
recovery-instructies en open gates; `plan-release` bouwt lokaal een
fingerprinted plan; `deploy-items` gebruikt de eigen Fabric REST API-client,
een ownership-journal, Environment-publicatie en export/read-back-verificatie.
De scope van de bestaande manifest/plan is echter expliciet
`PHASE3_FOUNDATION_PREPARATION`, niet een complete fase-5-release. De planner
controleert momenteel de basiswheel; de itemrunner ondersteunt begrensd
Environment- en Notebookbeheer. De reeds geüploade sidecar-wheel, een
fase-5-smoke-notebook, volledige Raw-to-Bronze-operaties en platform-
autorisatie zijn niet als één fase-5-bundle opgenomen.

**Besluit onder het bestaande projectmandaat:** maak de repository-eigen
bundle/runner stapsgewijs af en gebruik de community-preview hoogstens
side-by-side als experimentele referentie. Geen bestaande
deploymentsemantiek, ownershipguard, herstelgedrag of verbod op dataverlies
wordt vervangen door previewgedrag. De eerste eigen bundleproef blijft
klein: gevalideerde desired state, leesbare `plan`/diff, één geïsoleerde
phase-5 Environment/notebook-scope, expliciete packagechecksums, journal en
read-back. Eerst `validate`/`plan`, pas na controle `deploy`; `destroy`,
orphan cleanup, DDL-mutaties en ETL-replay zijn in deze proef uitgesloten.
Waar API-/Environmentgedrag niet gegarandeerd is, proberen we het bewust op
dev en registreren we request, response/status, remote item/job-ID en
herstelactie; een mislukte previewactie wordt niet als succes geïnterpreteerd.

**Volgende bewijsstappen.**

1. Gebruik na de geslaagde Full-mode-publicatie een niet-interactieve,
   bestaande CLI/API-route om de read-only diagnosecel eenmaal uit te voeren;
   noteer job-ID, status, exitwaarde en volledige diagnose-output.
2. Als de Environment-libraries geladen zijn, voer één synthetische Spark-ID-
   vector uit en vergelijk exact met de hierboven vastgelegde Python-hash.
3. Ontwerp een afzonderlijk fase-5-bundlecontract; hergebruik de huidige
   fingerprint-, dependency-, ownership-, timeout-, journal- en
   read-back-helpers. Breid de foundationmanifest-scope niet stilzwijgend uit.
4. Laat de bundle eerst een lokaal plan tonen en verifieer de exacte
   workspace, itemnamen, librarybytes en verwachte wijzigingen.
5. Probeer daarna een geïsoleerde, niet-destructieve Fabric-deploy en
   synthetische ingestie; controleer read-back/retry en ruim uitsluitend
   aantoonbaar eigen testartefacten op.
6. Voer negatieve least-privilege-tests en de rest van de fase-5-keten uit.
   Pas na Fabric-runtime-, security- en end-to-endbewijs kan fase 5 worden
   afgerond.

### Bundle-experiment 11 oktober 2026 — eigen phase-5-plan en runtimeprobe

**Probleemonderzoek: notebookjob is niet hetzelfde als aantoonbaar succes.**
De eerste niet-interactieve run van de bestaande read-only diagnose is via
`fabric-etl run-notebook` ingediend. Fabric gaf notebookjob
`e1679966-5cd5-4c0d-817e-ce243d5f9e87` terug. De runner eindigde met foutcode 1
en `Completed notebook has no explicit exit value; runtime proof missing`.
Bij de eerste implementatie zocht de runner alleen naar
`detailed.properties.exitValue`. De actuele Microsoft Notebook Public API-
documentatie toont `exitValue` als top-level veld in **Get Item Job Instance**.
De runner is daarop aangepast om het gedocumenteerde top-level veld te
lezen, met geneste `properties.exitValue` als expliciete compatibiliteits-
fallback. Lokale regressietest simuleert beide antwoordvormen.

Daarna is dezelfde bestaande jobreceipt opnieuw gelezen; er is geen tweede
diagnosejob gestart. De runner bleef zonder exitresultaat. Een aparte
`az rest`-opvraag onder de toen actieve CLI-identiteit kreeg `401
UserNotLicensed`; die response bevatte geen jobpayload. Een uitlezing via
de runnertransport kreeg wel antwoord: jobstatus `Completed`,
`failureReason: null`, geen top-level `exitValue`, en onder `properties`
een `exitValue` zonder stringwaarde (`null`). De oude interactieve
diagnosecel bevatte een `print` maar retourneerde geen JSON via
`notebookutils.notebook.exit(...)`. Daarmee is de job als voltooid gezien,
maar is er geen machineleesbaar resultaat waarmee succes of de diagnose-
inhoud vanuit de job-API kan worden bewezen. Dit is geen bewijs dat de
Sparkcode faalde; het is evenmin bewijs dat die code succesvol was.

**Eigen bundlecontract en beperkte scope.** Omdat het toegestane
`fab-bundle` een community-preview is en de repository al een eigen
gecontroleerde releasekern bevat, is geen externe package geïnstalleerd.
Er is een eerste intern contract `fabric/phase5-runtime-bundle.json`
gemaakt met exact één scope:
`PHASE5_RUNTIME_PROBE`. Het contract bindt:

- workspace `4be37544-10b9-450d-80f0-f9b3d793b83b`;
- bestaande gepinde dev-Environment `7266a872-dd91-4aa6-a0b4-d4cc5b862643`;
- add-onwheel `dist/fabric_etl_phase5-0.1.0-py3-none-any.whl`;
- vastgelegde SHA-256 `081953584d6aadd7d32528ff2932ba0fdd749f8539e63c38a897d484863270a1`;
- alleen notebook `contoso_phase5_runtime_probe_dev`.

Nieuwe `fabric_etl.bundle.build_phase5_plan` valideert contractversie en
scope, Environment-binding, veilige repositorypaden, exacte wheelbytes,
wheel-METADATA (distributienaam/versie en vereiste modules), notebookbron en
de workspace-/Environmentreferentie. Het resultaat is
`PLANNED_NOT_DEPLOYED` met fingerprint; een afwijkende bestaande planfile
wordt niet overschreven. `deploy_phase5_plan` valideert fingerprint en scope,
controleert remote Environment-ID, workspace, `publishDetails.state ==
Success` en SHA-256 van de via `getDefinition` geëxporteerde custom library,
en roept daarna de bestaande `deploy_items` aan. De hergebruikte itemrunner
schrijft vóór verificatie het journal, gebruikt de gepinde Environment-ID
voor notebookdependency, weigert bestaande niet-owned gelijknamige items en
controleert na deployment de uitgevoerde notebookdefinitie. Er is geen
delete-, orphan-cleanup-, migratie-, ETL-run- of datawritepad in deze
bundle-scope.

CLI:

```powershell
$env:PYTHONPATH = Join-Path $PWD 'src'
python -m fabric_etl plan-phase5-bundle fabric\phase5-runtime-bundle.json `
  --repository $PWD --output $env:TEMP\contoso-phase5-runtime.plan.json
python -m fabric_etl deploy-phase5-bundle `
  $env:TEMP\contoso-phase5-runtime.plan.json `
  --journal $env:TEMP\contoso-phase5-runtime-bundle-journal.json
```

De deploystap is alleen uitgevoerd nadat het lokale plan is gecontroleerd.
Fabric-readback vóór deploy bevestigde de gepinde Environment en de exacte
wheelchecksum. De runner heeft vervolgens één notebook aangemaakt:

| Bewijs | Waarde |
|---|---|
| Planstatus/scope | `PLANNED_NOT_DEPLOYED` / `PHASE5_RUNTIME_PROBE` |
| Planfingerprint | `7f4010d8434c52c3013289f4595792ef50d2d4ddaafae512730518b06979c55c` |
| Remote notebook-ID | `13355c3f-c59a-471e-b96e-754460df1c53` |
| Itemnaam | `contoso_phase5_runtime_probe_dev` |
| Itemactie | `created`, `verified: true` |
| Definition fingerprint | `095de6f6877eebac0db779f09e4f4cd9b2a4cab9ddca33349a6bbd34cecb2b95` |
| Export-hash notebook | `affe497bd8d1d8c4f36368f5215d74e0502812c03b32e9d0f4086729c2dc5a14` |
| Export-hash platformmetadata | `e0d692799f18b6f46b6793f2959eb270b4a77116441c7cdacc26f61fa1799556` |

De lokale Fake-API-suite bewijst planvalidatie, wielchecksumgate, notebook-
export/readback, ownershipjournal en idempotentie: **17 gerichte bundle- en
itemtests passed in 0.35 s**. Negatieve gevallen omvatten checksumdrift,
onveilig pad, gepubliceerde librarychecksum mismatch en Environment-status
anders dan `Success`. De volledige suite daarna: **365 passed in 3.20 s**.
Dit zijn lokale tests; het remote aanmaken/readback van de ene notebook is
afzonderlijk hierboven als Fabric-waarneming geregistreerd.

**Eerste Fabric-runtimeprobe: fout gevonden en concreet hersteld.** De eerste
probe (`a093384f-3682-4294-a0cd-39f34184e44d`) eindigde na 2 minuten 37
seconden als `Failed`, `System_Cancelled_Session_Statements_Failed`. De
hoog-niveau Job Instance API gaf geen celstacktrace, maar de
Fabric-execution-snapshot toonde de exacte Sparkfout:
`UNRESOLVED_COLUMN.WITH_SUGGESTION` op `version`. De Python-fixture bevatte
acht velden; het gedeclareerde Spark-contract `RAW_IDENTITY_FIELDS` bevat
negen. De Python-helper voegt `"version": "fabric-raw-record-v1"` zelf toe
aan de canonieke hashinput, terwijl de Spark-struct die key ook expliciet
moet bevatten. De failure was dus in de probe-fixture, niet in een
Lakehouse-write of in de library-publicatie.

De probe bouwt nu de Spark-row met exact alle `RAW_IDENTITY_FIELDS` en
verwijdert alleen het interne `version`-veld uit de kwargs die aan
`raw_record_identity` worden doorgegeven; dat houdt Python en Spark
vergelijkbaar zonder de publieke helper-signature te veranderen. Een
AST-regressietest vergelijkt de fixturekeys met het gedeclareerde contract.
Gerichte lokale bundle- en itemtests na deze wijziging: **18 passed in
0.29 s**; Pylance-problemen voor de gewijzigde testfile: geen; de probe-
source is opnieuw syntactisch gevalideerd.

Een nieuw fingerprinted plan met ID
`53d65d4758705afa44c6fa34250722cc4f19d4c922db0ef742c03c1c7b49f224` heeft
de eerder door deze bundle beheerde notebook bijgewerkt, niet gedupliceerd:
zelfde item-ID `13355c3f-c59a-471e-b96e-754460df1c53`, actie `updated`,
`verified: true`; nieuwe definition fingerprint
`b46299c3a95ffea96c63c22374e5fb9d5a4b49a6877ab1bf19ea4722964b52a5`,
notebook-export SHA-256
`b7d6a74309f56e8946e77e4d61d50f5482a49e5f4217bfbeb84cdb0093c18fb1`.
Platformmetadata bleef onveranderd
(`e0d692799f18b6f46b6793f2959eb270b4a77116441c7cdacc26f61fa1799556`).

**Herstelprobe geslaagd in de echte Fabric-runtime.** De nieuwe uitvoering
is ingediend met Fabric-job-ID
`de6b9870-38d4-4b8d-80f6-6e2e4113b474`. Het terminale jobdetail meldt
`Completed`, `failureReason: null`, rootActivityId
`9508ae36-6dad-4ebf-aa34-89cbcc3f3be6`, start `2026-10-11T07:46:10.9923226`
en einde `2026-10-11T07:49:10.4823689`. De geparste expliciete
`notebookutils.notebook.exit`-waarde is:

```json
{
  "framework_version": "0.1.0",
  "phase5_version": "0.1.0",
  "raw_record_id": "90d2b85ced455a8fee9cb1a2644b73a8e5c20ccd4f95e55786f0871937038810",
  "scope": "PHASE5_PACKAGE_AND_SPARK_ID_VECTOR",
  "status": "SUCCEEDED"
}
```

Dit is direct Fabric-runtimebewijs voor beide package-imports en voor
gelijkheid van het door de package-Pythonfunctie berekende ID met het
onafhankelijke Fabric-Spark-ID; de waarde matcht de bestaande lokale
golden-vector. De probe is read-only gebleven: geen DataFrame-write,
Delta-tabel, Lakehouse-file of controlmutatie. Daarmee is blocker
`POST_PUBLICATION_FABRIC_IMPORT_AND_SPARK_HASH` afgetekend. De
planbuilder-runtime_blockers zijn overeenkomstig bijgewerkt en testen nu
dat alleen `BRONZE_INGESTION_AND_SECURITY_ACCEPTANCE` open blijft voor deze
slice. **Deze probe bewijst geen Bronze-write, retry/dedup, quality/reject,
vault/gold, ketenpariteit of beveiligingsacceptatie; fase 5 blijft dus open.**

### Tweede eigen bundlescope — synthetische Raw-to-Bronze

Na het geslaagde runtimecontract is een tweede, afzonderlijk begrensde
bundlescope toegevoegd; de oorspronkelijke read-only probe blijft
afzonderlijk beheerd. De exacte IDs zijn `contoso_phase5_bronze_probe` en
`PHASE5_BRONZE_PROBE`; de scope kan door de planner/deployer niet als
algemene release of andere notebook worden gebruikt. Dit is nog geen
Fabric-data-uitvoeringsbewijs.

De nieuwe bundle bestaat uit:

- `fabric/phase5-bronze-bundle.json`: dev-Environment-ID, gepubliceerde
  `fabric-etl-phase5==0.1.0`-wheel/SHA-256 en precies één notebook;
- `fabric/items/phase5_bronze_probe.Notebook/notebook-content.py`: expliciet
  Environment-contract, pinned synthetische klantdata en de fase-5-
  read/write-/commithelpers;
- wijzigingen in `src/fabric_etl/bundle.py`: gesloten allowlist met twee
  afzonderlijke `(bundle_id, scope, notebook_id)`-combinaties; voor de
  Bronze-notebook injecteert de planner de exacte Environment-binding in
  het notebook en berekent hij een fingerprinted desired-state-plan;
- `tests/test_fabric_bundle.py`: planbinding, wheelpin, fixturehash en
  fake-API-deploy van de nieuwe scope.

De enige input is twee identieke JSONL-records, elk
`customer_id=PHASE5-PROBE-001`, `display_name=Synthetic customer`; grootte
142 bytes, vaste SHA-256
`d21c87b4686069725b2147364b438bc2fdb7e9c0af43c28f475718969120eed8`.
De notebook vergelijkt die input met de vastgelegde checksum vóór het
schrijven. Zij maakt onder
`abfss://Contoso_lakehouse_ontwikkel@onelake.dfs.fabric.microsoft.com/lh_contoso_processing.Lakehouse/Files/phase5-proof/bronze-probe-v1`
alleen `customers.json` en `_manifest.json`; beide moeten bij een retry
byte-voor-byte overeenkomen, extra bestanden en subdirectories blokkeren
de run. Het manifest is `CLOSED`, bevat exact één object `PROBE.CUSTOMERS`
en één bestand, en wordt na schrijven met de fysieke OneLake-inventory
gecontroleerd. Het bron-ID `PHASE4_PROOF` houdt de proof-context geïsoleerd
van de businessbronnen; delivery ID is `PHASE5_BRONZE_PROBE|V1`.

De notebook bouwt een expliciet Spark-schema, roept `read_manifest_records`
aan, controleert twee records met twee verschillende `raw_record_id`-s
ondanks identieke payloads en gebruikt `write_bronze_delivery` voor
idempotente Delta-append plus exacte readback. Het enige Delta-target is
`Contoso_lakehouse_ontwikkel.lh_contoso_processing.bronze.phase5_probe_customers`.
Voor control schrijft zij uitsluitend een geïsoleerde, deterministische
`PHASE4_PROOF`-batchcontext en het idempotente `BRONZE`-commitmanifest.
Iedere uitvoering claimt en verifieert eerst de bestaande project-owner,
houdt die voor writes vast en geeft hem in `finally` vrij. Geen metadata-
activatie, SALES/brondata, Quality/Reject, Vault/Gold of productieobjecten
zijn in deze bundlescope opgenomen. Er is geen delete/cleanup; de
synthetische testfile, Delta-rijen en proof-controlrecords zijn
geïdentificeerde persistente testartefacten.

Gerichte validatie van het nieuwe pad: **19 bundle- en itemtests passed in
0.35 s**; volledige repositorysuite **367 passed in 3.12 s**. Pylance vond
geen fouten in de gewijzigde Python-modules/tests en de notebooksource is
syntactisch gevalideerd. Nieuw plan:
`f05ae5627123c9946d20612ef21c0733fee240584db76d7ee18f6949d2b3a1e3`,
status `PLANNED_NOT_DEPLOYED`. Daarna is de deploy afzonderlijk uitgevoerd:
notebook `contoso_phase5_bronze_probe_dev` aangemaakt als Fabric-item
`83b67667-6a42-4108-ac1b-ee5f911a8248`, actie `created`, `verified: true`,
definition fingerprint
`4a519e0077bd741dccbe5731d189f6f090c96a3d263d06411e7b70b37237d44e`,
export SHA-256 `notebook-content.ipynb` =
`f35ac65be4adb6e1e14bd0a474c6042eb8a40feb489639860562fdeb2d04913a`,
`.platform` =
`823a2feb42a534625108edb242d6e4810f493d553bc90886fe294bcc7200ad41`.
Dit is Fabric-item create/readbackbewijs, nog geen bewijs dat de notebookcode
of Delta-write geslaagd is.

**Eerste echte synthetische Raw-to-Bronze-run geslaagd.** Fabric-job
`ffbec3fb-b55e-48e1-bb35-ab8d8ed21d6f`, item
`83b67667-6a42-4108-ac1b-ee5f911a8248`, status `Completed`,
`failureReason: null`, rootActivityId
`ae06e6a1-a688-4129-8714-d3ac6445346b`, start
`2026-10-11T07:57:56.1882091`, einde `2026-10-11T08:01:54.9465086`.
De expliciete notebook-exitwaarde:

```json
{
  "batch_id": "ea2c0b10-0205-5f29-90b0-108e8ef44c58",
  "bronze_commit_status": "COMMITTED",
  "delivery_id": "PHASE5_BRONZE_PROBE|V1",
  "framework_version": "0.1.0",
  "inserted_count": 2,
  "output_fingerprint": "d2e6c43b9dcad779c7b482c8647a356c178504e0efd1f5a2ac69d7d1e89e527d",
  "owner_released": true,
  "phase5_version": "0.1.0",
  "record_count": 2,
  "retry_count": 0,
  "scope": "PHASE5_SYNTHETIC_RAW_TO_BRONZE",
  "status": "SUCCEEDED",
  "target_table": "`Contoso_lakehouse_ontwikkel`.`lh_contoso_processing`.`bronze`.`phase5_probe_customers`"
}
```

De notebook-exit vindt pas plaats na de volgende opeenvolgende controles:
owner claim en proof-only batchcontext; OneLake-folder zonder onverwachte
files/subdirectories; exacte bestandsgrootte en byte-readback van zowel het
142-byte-inputbestand als `_manifest.json`; CLOSED-manifest en exacte Raw-
fileinventory; expliciet schema en Raw reader; exact 2 input records,
verschillende Raw-ID's voor de dubbele payloads; targetschema, append en
volledige Delta-readback door `write_bronze_delivery`; fingerprint over
alle readbackrecords; stabiel COMMITTED-record in het Bronze commitmanifest;
vrijgeven en readback van de project-owner. Het resultaat bewijst de
synthetische manifest-to-Bronze-runtime en het eerste commitpad. Het bewijst
niet dat echte businessbrondata, Quality/Reject, Vault of Gold al doorlopen.

**Retry/dedup over afzonderlijke Fabric-jobs geslaagd.** De tweede run met
dezelfde delivery, manifest, bestandsbytes en batchcontext was job
`73f5ef04-98a5-4393-976f-1f66889f0cc0`, status `Completed`,
`failureReason: null`, rootActivityId
`b586f47c-7218-40cd-a568-fa363ff24946`, start
`2026-10-11T08:02:16.5654076`, einde `2026-10-11T08:06:16.7515379`.
De notebook-exitwaarde bevestigt batch
`ea2c0b10-0205-5f29-90b0-108e8ef44c58`, `record_count=2`,
`inserted_count=0`, `retry_count=2`, dezelfde outputfingerprint
`d2e6c43b9dcad779c7b482c8647a356c178504e0efd1f5a2ac69d7d1e89e527d`,
commit `VERIFIED`, `owner_released=true`. Daarmee blijven na een echte
separate-job retry exact twee Silver-independent Bronze Raw-records bestaan;
er is geen verdubbelde append en de persisted commitmanifest is herkend.
Beide jobreceipts zijn terminal `Completed` en beide notebook-exitresultaten
zijn `SUCCEEDED`.

Daarmee zijn `POST_PUBLICATION_FABRIC_IMPORT_AND_SPARK_HASH` en de
afgebakende `SYNTHETIC_RAW_TO_BRONZE_WRITE_READBACK_RETRY`-gate geslaagd.
De planner rapporteert voor de volgende fase-slice nu
`QUALITY_REJECT_VAULT_GOLD_AND_SECURITY_ACCEPTANCE`. Dat is geen claim dat
kwaliteit, Vault, Gold of least-privilege al zijn bewezen.

**Werkelijke gewijzigde paden in deze iteratie:**

- `src/fabric_etl/bundle.py` — versie- en wheelvalidatie, plan en begrensde
  deployadapter voor read-only-runtime- en synthetische Bronze-probe;
- `src/fabric_etl/items.py` — remote gepubliceerde-librarychecksumcontrole,
  plan-Environmentbinding, en job-exitwaarde op root plus fallback in
  `properties`;
- `src/fabric_etl/__main__.py` — `plan-phase5-bundle` en
  `deploy-phase5-bundle`;
- `fabric/phase5-runtime-bundle.json` — desired state van deze probe;
- `fabric/items/phase5_runtime_probe.Notebook/notebook-content.py` — import-
  en Python/Spark-hashprobe met expliciete exitwaarde;
- `fabric/phase5-bronze-bundle.json` en
  `fabric/items/phase5_bronze_probe.Notebook/notebook-content.py` — gesloten
  desired state en afzonderlijke synthetische Raw-to-Bronze-probe;
- `tests/test_fabric_bundle.py` en `tests/test_fabric_items.py` — positieve,
  negatieve, idempotentie- en response-envelope-tests;
- dit verslag.

Geen Databricks-worktree, projectseed, bronlevering, Lakehouse-data,
autorisatierol of Environment-libraryconfiguratie is door het bundle-experiment
gewijzigd. De eigen bundle bestaat nu als eerste operationeel geteste,
strikt beperkte plan/deployslice; hij is nadrukkelijk nog geen complete
fase-5-release. Vervolg na probe-uitkomst: hergebruik dit bundlecontract voor
synthetische Raw-to-Bronze, quality/reject en vault/gold; voor iedere scope
eerst desired-stateplan, expliciete target en readback, daarna retries en
least-privilege-negatieve tests. Fase 5 blijft open totdat de hele keten
remote aantoonbaar werkt.

### 11 oktober 2026 - Fase 5: deterministische Quality/Reject-slice

**Opdracht en beslisruimte.** De opdrachtgever bevestigt om 10:56 CEST:
"ga maar door totdat het geslaagd is, fase 5". De eerder gegeven toestemming
voor eigen bundels, dev-experimenten en technische beslissingen blijft gelden.
Deze toestemming is geen bewijs van businessacceptatie, privacyclassificatie
of een daadwerkelijk uitgevoerde negatieve autorisatietest.

**Onderzoek en keuze.** De oorspronkelijke
`src/contoso_lakehouse/quality.py` gebruikt delivery-scoped DELETE gevolgd
door append, willekeurige reject-ID's en actuele verwerkingstijden. Dat
wordt niet zonder meer naar Fabric gekopieerd: het vastgestelde Fabric-
contract verlangt deterministische recorduitkomsten en herstel zonder
purgen van committed uitvoer. Nieuwe module
`src/fabric_etl_phase5/quality.py` gebruikt de Bronze `raw_record_id` als
unieke uitvoerkey. Iedere rij krijgt precies FILTERED, PASSED of REJECTED;
meerdere foutredenen blijven in een array op dezelfde rejected rij.
Warnings annoteren passed records en tellen niet als extra terminale rij.
Een NULL-regeluitkomst is een violation; een NULL-filter is een fout omdat
de presence-semantiek dan niet expliciet is. Niet-BOOLEAN-expressies falen.

**Drempelcontract.** Denominator is POST_FILTER. De threshold faalt alleen
bij `actual > configured`, niet bij gelijkheid. Een lege populatie heeft
nul violations. Thresholdacties FAIL_BATCH en QUARANTINE_BATCH geven een
expliciete `QualityBlocked` voordat targetwrites/Quality-commit plaatsvinden.
CONTINUE registreert de overschrijding zichtbaar. Deze slice bewijst het
blokkeren, niet een volledige duurzame quarantaine-/work-itemworkflow.

**Write- en herstelcontract.** Nieuwe `write_immutable_output` valideert de
gebonden rol/schema, alle inputscopewaarden, unieke niet-NULL keys en de
targetschema. Bestaande output die afwijkt van deterministische input wordt
geweigerd. Alleen ontbrekende records worden appended; bidirectionele
`exceptAll`-readback controleert de complete multiset vóór een fingerprint
wordt gerapporteerd. Iedere write controleert projectownership. De laagcommit
legt alle outputs plus accounting in één controlmanifest vast. Een bestaande
batch/layer/entity met andere input/output wordt geweigerd in plaats van een
tweede schijnbaar succesvolle commit aan te maken.

**Afgebakende fixture en doelen.** De nieuwe Quality-bundel heeft scope
`PHASE5_QUALITY_PROBE`, één notebook
`contoso_phase5_quality_probe_dev` en uitsluitend synthetische data:

- vijf JSONL-records, 368 bytes, SHA-256
  `bd1c9c264db11ebbbd411dd2d8ec66d0625179bc0e9412c72b6ecddaa806afed`;
- twee identieke geldige records die afzonderlijke Raw-ID's moeten houden;
- één geldige businesskey met lege naam (WARNING, niet REJECTED);
- één NULL-businesskey met twee foutredenen op één REJECTED-record;
- één expliciet gefilterd record;
- verwacht accounting: `5 = 1 FILTERED + 3 PASSED + 1 REJECTED`;
- verwachte rule-populatie: vier; één keyviolation geeft exact 25 procent;
  25 procent is toegestaan, 24,99 procent moet expliciet blokkeren;
- Raw-map `Files/phase5-proof/quality-probe-v1`;
- targets `bronze.phase5_quality_customers`,
  `quality.phase5_probe_customers`, `reject.phase5_probe_customers`,
  `quality.phase5_probe_filtered_customers` in uitsluitend Processing;
- geïsoleerde `PHASE4_PROOF`-batch, geen echte SALES- of API-data.

**Versionering en publicatie.** De add-on krijgt versie 0.2.0; de eerder
bewezen 0.1.0-wheel wordt lokaal behouden. Nieuwe wheel:
`dist/fabric_etl_phase5-0.2.0-py3-none-any.whl`, SHA-256
`5b98ea1092fb580cb925fb2d0ba771b89b878cdcb4e101e2a9d85d9bff2dddc4`.
De gepubliceerde basiswheel `fabric-etl==0.1.0` wordt NIET herbouwd of
vervangen. REST-preflight las exact de bestaande basiswheelchecksum
`b9da29e98d1d06440b3a04515c32d71a60913ce108d50a705033156b2bab0dbb`
en Spark-settingschecksum
`878607076a0add4ac6d6ee77aa6a30e104b39ddb2103c6950c7db2eea7560593`.
`deploy/publish-phase5-quality.py` bewaart deze bytes, verifieert exclusieve
setup en vervangt uitsluitend de eigen add-on in het volledige Environment-
definitioncontract. Publication/readback-receipt:
`deploy/evidence/phase5-quality-runtime-upgrade.json`.
De oude runtime-/Bronze-plannen zijn historische 0.1.0-pins; na een upgrade
horen zij op deze Environment bewust checksum/version-drift te weigeren.
Geen bestaande Bronze-commit of data wordt door de upgrade aangepast.

**Lokale verificatie vóór runtime.** 27 gerichte Quality-/bundletests
geslaagd in 0,52 s: thresholdgelijkheid, 24,99-grens, lege populatie,
beide blokkadeacties, zichtbare CONTINUE-breach, ongeldige waarden,
duplicaatrules, immutable commitconflict en no-write commitretry, exacte
fixturebytes, Environmentbinding en idempotente notebookdeployment.
Pylance meldt geen errors in de gewijzigde modules/tests/deployscript en
notebook. Lokale PySpark is 4.2.0, maar Java staat niet op PATH; dit is geen
lokale Spark-executieproef. De remote Fabric-runtime wordt apart uitgevoerd.
**Status op dit registratiepunt:** lokaal geïmplementeerd en getest;
Environmentpublicatie en notebookuitvoering worden afzonderlijk gevolgd.

**Publicatie-resultaat.** Upgrade succesvol: `PUBLISHED_READBACK_VERIFIED`;
Fabric-publicatie `Success`, targetVersion
`917d2046-69c9-4cd7-ba28-587237f4cd50`, start
`2026-10-11T09:21:04.2816418Z`, einde
`2026-10-11T09:25:07.3780221Z`. Basiswheel en Spark-settings zijn na
publicatie ongewijzigd teruggelezen, de add-onchecksum klopt.
Quality-plan `0f313a6d322255727d6f25cb6eebd59e0cef2fc5c2906daf962dd93304b1a09b`
is deployed/readback-verified als Notebook
`c60846b1-4d12-4ecc-9cde-518f98faa7da`.
Eerste Quality-job `e1c35265-c767-47bc-99c2-38c2c6932b17` is éénmaal
ingediend; receipt `deploy/evidence/phase5-quality-v1-receipt.json` wordt
door de runner bijgewerkt bij terminale feedback. Geen tweede writer
gestart zolang deze job niet terminal is.
Volledige regressiesuite na de Quality-uitbreiding: **388 passed in 3,21 s**.

**Vervolgproef voorbereid.** Vierde gesloten bundelscope
`PHASE5_CHAIN_PROBE`, `fabric/phase5-chain-bundle.json`, gebruikt dezelfde
gepubliceerde 0.2.0-wheel. Alleen synthetische customers/segmenten in drie
immutable leveringen, fixturefingerprint
`00d63936aacd2cbdcd5cd535036a31f98b3c132111e35ebef8097434e51d3ff9`.
De eerste levering bevat één echte dubbele bronrij; de entityprojectie
voegt alleen identieke businesspayloads expliciet samen na DQ/accounting.
Twee verschillende payloads voor één key/delivery blokkeren. Levering twee
wijzigt één klantnaam en segment; levering drie bevat een expliciete soft-
deleteflag. De andere klant blijft alle drie leveringen ongewijzigd.
Verwacht: 2 customerhubs, 2 segmenthubs, 3 links en 4 satellite-events;
historische Gold-versies per productbuild 2/3/4, actieve currentklanten
2/2/1. Satellites zijn append-only; intervallen worden met LEAD afgeleid,
een tombstone blijft in historie maar niet in current. PIT legt twee
expliciete as-of eventverwijzingen per batch vast. Goldbuilds zijn per
inputcommitset gepind, niet in-place overschreven; oude builds blijven intact.
De notebook gebruikt de geverifieerde generieke immutable-output- en
commithelpers, maar de fixturetransformaties zijn testharnesscode, geen
claim dat alle 18 echte Vault-/3 Business Vault-/19 Gold-entities werken.
Doelen zijn uitsluitend eigen `phase5_chain_*`-tabellen; geen business-
productpromotie, SQL-consumerbinding of governancegoedkeuring.
28 gerichte tests geslaagd in 0,76 s inclusief de nieuwe chain-fixture en
fake deployment; runtimebewijs moet nog volgen.

**Werkelijke projectrisico's tijdens onderzoek.** De bestaande
`GH_FCT_SALES.sql` bevat current-joins voor order- en BV-orderlinedata,
terwijl het historische contract halfopen as-of-joins vereist. De
Business Vault-mapping voor tenure_band gebruikt `current_date()`, wat bij
latere retry een andere classificatie kan geven. Deze zijn niet
goedgekeurd of als Fabric-pariteit afgevinkt. De echte projectrelease
behoudt de 70 blockers totdat schemas, temporele expressions,
implementaties, bindings en policies aantoonbaar zijn afgehandeld.

**Quality/Reject runtime-resultaat en afzonderlijke retry.**

| Bewijs | Eerste job | Retryjob |
|---|---|---|
| Fabric job-ID | `e1c35265-c767-47bc-99c2-38c2c6932b17` | `0e4ecd88-759d-4702-8098-13fc8eceb092` |
| Terminale status / exitresultaat | `Completed` / `SUCCEEDED` | `Completed` / `SUCCEEDED` |
| Start UTC | `2026-10-11T09:27:44.8579517` | `2026-10-11T09:33:04.7117528` |
| Einde UTC | `2026-10-11T09:32:49.1204318` | `2026-10-11T09:37:23.5435495` |
| Batch | `40c5a8d2-e8ee-572b-9aeb-61b1b7f3ecab` | dezelfde batch |
| Bronze inserts / retries | 5 / 0 | 0 / 5 |
| Passed inserts / retries | 3 / 0 | 0 / 3 |
| Rejected inserts / retries | 1 / 0 | 0 / 1 |
| Filtered inserts / retries | 1 / 0 | 0 / 1 |
| Bronze / Quality-commitstatus | `COMMITTED` / `COMMITTED` | `VERIFIED` / `VERIFIED` |
| Owner vrijgegeven | true | true |

Beide jobs hebben `failureReason=null`. Accounting is in beide exact
`5 = 1 FILTERED + 3 PASSED + 1 REJECTED`; denominator vier, één violation
per keyrule = exact 25 procent, `breached=false` bij threshold 25.
Het verwachte expliciete threshold-foutresultaat bij 24,99 procent is
`Quality thresholds blocked: key_present:FAIL_BATCH`. De notebook controleerde
ook dat de NULL-keyrij beide redenen op één rejected record behoudt en
precies één passed record de WARNING-array heeft.

Alle outputfingerprints zijn gelijk tussen de jobs:

| Output | Fingerprint |
|---|---|
| Bronze | `c4e6be1207f140d89bd8b4bff32c2a067a81649b36f4c656190a18d6276884ac` |
| Passed | `0805bc92cd3f3d10d31e72bed448e3d9e4bfef848480491172c225cb6a450d7d` |
| Rejected | `37a68e86b86beeed4e680387df0111e54d450a2d9d05e58eec00fb641604310a` |
| Filtered | `c8ec6b1cb8bfecbb318bcbdccc224c9b5d7194af3d227ef580915d974258fe27` |

Bronze-commit-ID
`e085ca73f0d021bf58dcb3043402257d45aff233b9940efe5e87aaa2f32c6b55`
en Quality-commit-ID
`539fae8c54e89b41275dea518f3100c85fd5f8372290df72a0a0883ed49d40a1`
blijven eveneens identiek. Daardoor is deze synthetische Quality/Reject-
slice inclusief separate-jobretry afgetekend; niet de hele Contoso-DQ-set.

**Automatische bewijscontrole en duurzaamheid.** Nieuw
`deploy/verify-phase5-proofs.py` verlangt terminale succesvolle jobs met
vrijgegeven owner, exacte counts/distributie/thresholdpopulatie en bij retry
zero inserts, gelijke fingerprints/context/commit-ID's en `VERIFIED`.
Het weigert een eigen eerste job als "aparte retry" te accepteren.
Het controleert daarnaast de drie-leveringen-chaincontracten en weigert
een synthetische proof die productieacceptatie claimt.
41 gerichte tests geslaagd in 0,66 s na toevoeging van negatieve
receiptverificatietests; volledige suite vóór die extra tests:
389 passed in 3,17 s. Verificatie van de echte Quality-receipts:
`deploy/evidence/phase5-quality-retry-verification.json`, status `VERIFIED`,
`separate_job_retry_verified=true`, `production_release_accepted=false`.
Het bundleownershipjournal en de twee nieuwe plannen zijn duurzaam
gekopieerd naar `deploy/evidence/phase5-bundle-journal.json`,
`phase5-quality-v1.plan.json` en `phase5-chain-v1.plan.json`, zodat recovery
en redeploy niet van een later opgeschoonde TEMP-map afhankelijk zijn.

**Chain-deployment.** Plan
`e55098c4d5046ff5c68f337cf0162cf990479af67ae5e56e3361428c2e566cce`
is readback-verified deployed als Notebook
`0c46a801-39f3-4d28-aafe-570e88649f24`. De chainwriter is pas na de
geverifieerde terminale Quality-retry gestart. Receipt:
`deploy/evidence/phase5-chain-v1-receipt.json`. Runtime-uitkomst en retry
worden apart vastgelegd; geen voorafgaande succesclaim.

**Concrete configuratiegrens.** De Fabric REST-inventaris toont in deze
workspace uitsluitend de drie Lakehouses/SQL-endpoints, de eigen
Environment en ontwikkelnotebooks. Er is hier geen apart Sales-bronitem
zichtbaar. Het project verwijst naar connectionalias `fabric_sales_source`,
maar bevat geen geverifieerde echte verbinding daarvoor. Dit sluit een
bron in een andere workspace niet uit; het bewijst juist niet dat die
binding operationeel bestaat. De vijf bronpolicies zijn nog DEVELOPMENT
(SALES/FABRIC_SALES CONFIDENTIAL; CBS/ECB/NAGER PUBLIC). Ze zijn niet
automatisch APPROVED gemaakt.

### F5-CHAIN-RETRY en F5-RECOVERY - terminal bewijs, 11 oktober 2026

**Opdracht en keuze.** De opdrachtgever gaf opnieuw toestemming door te
gaan tot fase 5 slaagt, zonder nieuwe technische akkoordvragen. Daarom
zijn de reeds gestarte jobs gevolgd, niet opnieuw ingediend. REST,
gepinde bundles en automatische receiptcontrole blijven de uitvoeringsroute;
geen langzame browserklikreeks en geen handmatige succesinterpretatie.

**Drie-leveringenketen.** Eerste job
`28b53341-4468-4f15-94b9-020f023968c3` liep van
09:37:50.1237387 tot 09:50:39.2455319 UTC. Aparte retryjob
`1ec5cd9f-824d-4639-b248-3847b9d4045b` liep van
09:51:17.7923991 tot 10:01:58.7435229 UTC. Beide:
`Completed`, notebook `SUCCEEDED`, `owner_released=true`.
De retry duurt ongeveer 10m41: dit is werkelijk gemeten jobtijd, geen
claim dat een grotere productiejob even snel zal zijn.

| Resultaat per levering 01/02/03 | Eerste uitvoering | Aparte retry |
|---|---|---|
| Bronze/Quality records | 3 / 2 / 2 | 0 inserts; 3 / 2 / 2 bestaande records |
| Nieuwe customerhubs | 2 / 0 / 0 | 0 / 0 / 0 |
| Nieuwe segmenthubs | 2 / 0 / 0 | 0 / 0 / 0 |
| Nieuwe customer-segmentlinks | 2 / 1 / 0 | 0 / 0 / 0 |
| Nieuwe customer-satellite-events | 2 / 1 / 1 | 0 / 0 / 0 |
| BV-satellite-events | 2 / 1 / 1 | 0 / 0 / 0 |
| PIT records per levering | 2 / 2 / 2 | 0 inserts; 2 / 2 / 2 bestaande records |
| Historische Gold-versies | 2 / 3 / 4 | dezelfde snapshots/fingerprints |
| Actieve Gold-klanten | 2 / 2 / 1 | dezelfde snapshots/fingerprints |
| Laagcommits | 15 COMMITTED | dezelfde 15 IDs, VERIFIED |

De drie vastgelegde batchtijden blijven exact
`2026-10-11T09:40:39.062308+00:00`,
`2026-10-11T09:43:55.815568+00:00` en
`2026-10-11T09:46:57.043768+00:00`.
Dit bewijst in deze fixture multipliciteitsbehoud tot Quality, expliciete
entiteitdeduplicatie daarna, wijzigingsevents, segmentwijziging,
historische intervallen, PIT en uitsluiten van de expliciet soft-deleted
laatste klantversie uit Current. Geen impliciete absence-delete.
Volledige fingerprints, contexten en commit-ID's staan in
[eerste chainreceipt](../deploy/evidence/phase5-chain-v1-receipt.json) en
[retryreceipt](../deploy/evidence/phase5-chain-v1-retry-receipt.json).
De [automatische verificatie](../deploy/evidence/phase5-chain-retry-verification.json)
bevestigt `separate_job_retry_verified=true`,
`production_release_accepted=false`.

**Negatieve hersteltests.** Notebook
`e828095e-9274-4098-aacf-557f0ea336d3`, job
`c9397e71-b6c8-4081-9de2-2286fb243c1f`, 10:02:15.9668993 tot
10:06:30.4565036 UTC, is `Completed/SUCCEEDED`.
Alleen kandidaatframes zijn gewijzigd; er is geen gecommitte data
beschadigd om een conflict te simuleren.

| Negatief pad | Exact ontvangen contractfout |
|---|---|
| Payloadconflict | `Stored immutable output conflicts with deterministic input` |
| Verouderde owner | `OWNERSHIP_LOST: runtime writes prohibited` |
| Input buiten scope | `Immutable input contains rows outside its declared scope` |
| Dubbele outputkey | `Immutable output key is duplicated` |
| NULL outputkey | `Immutable output key cannot be NULL` |
| Conflicterende laagcommit | `Layer commit conflicts with an existing batch/entity` |

Na de zes blokkades: `target_unchanged=true`, `commit_unchanged=true`,
passed count 3, fingerprint
`0805bc92cd3f3d10d31e72bed448e3d9e4bfef848480491172c225cb6a450d7d`,
owner vrijgegeven. [Receipt](../deploy/evidence/phase5-recovery-v1-receipt.json)
en [verificatie](../deploy/evidence/phase5-recovery-verification.json)
zijn duurzaam bewaard. `platform_least_privilege_proven=false` is bewust:
frameworkownership/validatiefouten zijn geen platformautorisatietest.

**Correctie eerdere beveiligingsstatus.** De openingszin dat
least-privilege-negatieve tests nog helemaal onbewezen waren, was te breed.
Fase-4 T-13 bewees al SQL SELECT op de specifieke Gold-testtabel, geen
INSERT/UPDATE/DELETE/ALTER, geweigerde Control/Processing-login, OneLake
Gold-read 200, Control/Processing-read 403 en Gold-write 403.
Zie [T-13-platformbewijs](15_fabric_fase4_platformbewijs.md).
De huidige Gold-role-readback bevestigt `T13GoldTestRead` op alleen
`/Tables/publication/t13_gold_read_test`. In deze iteratie zijn geen rollen,
grants, apprechten of consumerbindings gewijzigd. Acceptatie van nieuwe
businessproductinterfaces blijft een afzonderlijk eindcriterium.

**Regressie en open scope.** Na de laatste verifier/recoverywijzigingen:
403 tests geslaagd in 3,28 s. Geen businessrelease geactiveerd. De
chainnotebook is een bounded synthetische harness, niet de implementatie
van de 18 echte Vault-entities, drie BV-definities of 19 Gold-contracten.
De 70 preflightblockers verdwijnen niet door deze geslaagde platformproef.
Een aanvullend Quality-aandachtspunt is vastgesteld in de code: windowregels
worden nu berekend voordat filtered rows zijn uitgesloten. Dit moet met
een reproduceerbare windowfixture en een nieuwe packageversie worden
opgelost voordat echte metadatakwaliteitsregels worden geaccepteerd.
De gepubliceerde 0.2.0-wheel wordt niet onder hetzelfde versienummer
overschreven.

### F5-SOURCE-SCHEMA - echte Fabric Sales-bron bereikbaar, 11 oktober 2026

**Aanleiding en hergebruik Databricks.** In de actuele Fabric-itemlijst
staat geen Sales-bronitem. De Databricks-deploymentseed bevat echter een
concrete Fabric SQL JDBC-server/database en de view
`SalesLT.vw_databricks_sales_order_line`; dezelfde binding staat in de
oorspronkelijke repositoryseed. Deze referentie is alleen gelezen,
niet gewijzigd; Databricks-secret-scopes zijn niet benaderd.
De Fabric connectioninventory bevat twintig zichtbare connections,
waarvan vier zonder displayName. De zichtbare `FabricSql bobstoelinga`
toont geen verifieerbare server/databasekoppeling en is daarom niet
automatisch als `fabric_sales_source` geactiveerd.

**Beslissing.** Eerst de concrete bron controleren met een schema-only
query, geen SELECT met bedrijfsrecords, geen bronwrites of grants.
Nieuw [inspectiescript](../deploy/inspect-fabric-sales-source.ps1) verlangt
precies het bekende seedobject en querycontract. Het gebruikt de bestaande
Microsoft.Data.SqlClient 6.1.3-cache, tenantgebonden Azure CLI SQL-token
alleen in geheugen, encryptie, certificaatvalidatie, `Application Intent=ReadOnly`,
30 s connect-/querytimeout en `SELECT TOP (0) * FROM
SalesLT.vw_databricks_sales_order_line`.
Een bestaande receipt wordt niet overschreven.

**Uitvoering en fouten.** Eerste poging strandde lokaal op
`ReflectionTypeLoadException`: `Add-Type` enumereerde tevens typen met
Microsoft.Identity.Client/Azure.Core-afhankelijkheden. Die authenticatie-
klassen zijn niet nodig voor een expliciet access token. Gewijzigd naar
`Assembly.LoadFrom`, zonder ontbrekende pakketten blind te installeren.
De [eerste foutreceipt](../deploy/evidence/phase5-fabric-sales-source-first-loader-failure.json)
is duurzaam gearchiveerd. Daarna bleek de PowerShell dictionaryadapter
`DataSource` niet als geldige connectionstringkeyword te behandelen:
gecorrigeerd naar de canonieke indexerkey `Data Source` en overeenkomstige
spaced keys. Vervolgens vroeg de client om
`System.Runtime.CompilerServices.Unsafe, Version=4.0.4.1`; de aanwezige
package 4.5.3 levert die assemblyversie, niet package 6.0.0. De cache is
hergebruikt. Geen nieuwe SQL-driver/pakketinstallatie uitgevoerd.
Een receiptpad werd aanvankelijk relatief aan de .NET-processdirectory
opgelost in plaats van de PowerShell-werkdirectory. Dat is gecorrigeerd
met de PowerShell-providerresolver; de eerste foutreceipt is uit de
verkeerde parentmap naar de deployment-evidencemap verplaatst.

**Werkelijk resultaat.** De succesvolle schema-only poging liep van
10:16:44.5026827 tot 10:17:12.2394850 UTC en gaf
`SOURCE_SCHEMA_VERIFIED`, **33 kolommen**, nul bedrijfsrecords.
[Volledige schemareceipt](../deploy/evidence/phase5-fabric-sales-source-schema.json)
bevat namen, SQL-typen, nullability en gerapporteerde precision/scale.
SQL-providerwaarden zoals scale 255 bij niet-decimale typen zijn onbewerkt
bewijs, geen Spark-decimaldefinitie.
Gelezen kolommen omvatten orderdetail/order/product/customer-ID's,
quantity, unit price/discount, order/due/ship dates, status/online,
subtotal/tax/freight, customernamen, bill/ship-adreskenmerken,
productkenmerken, `source_last_modified_at` en `is_deleted`.
De Quality-projectie bevat acht doelkolommen; dat is niet dezelfde
inventaris als de 33 bronkolommen. Afgeleide velden en casts moeten nog
via de echte mappinglaag worden geverifieerd.

**Betekenis en grens.** Dit is concreet voordeel van de Databricks-
referentie: de bron hoeft niet via de UI te worden geraden.
De aangemelde ontwikkelaar kan de bekende view/schema werkelijk lezen.
Dit bewijst niet automatisch een service-principalbinding, geplande
Fabric-notebookauthenticatie, incrementele extractie of een geactiveerde
alias. `connection_alias_activated=false` en
`business_rows_extracted=false` blijven expliciet in de receipt.
Daarom is de formele releasegate niet blind verwijderd.

### F5-QUALITY-WINDOW - correctie en nieuwe immutable package

**Root cause.** In 0.2.0 werden regels op alle parsed rows berekend en
pas daarna filtered rows uit de thresholdpopulatie verwijderd. Daardoor
kon een filtered duplicate een eligible record onterecht laten falen op
`count(*) OVER (PARTITION BY customer_id) = 1`.
De bestaande scalarprobe detecteerde dit niet. De oorspronkelijke
Databricks-engine filtert wel voordat regels worden berekend; dat principe
wordt overgenomen, maar de onveilige DELETE/append-/uuid-/current_timestamp-
retrystrategie niet.

**Implementatie.** Parsed/filtervalidatie blijft gelijk; filtered en
eligible frames worden nu eerst gesplitst. Alleen eligible records krijgen
regels/thresholds. Filtered outcomes worden daarna met lege error-/warning-
codes terug verenigd, met behoud van alle Raw-ID's en terminal accounting.
Metadataregel-ID's zoals `DQ-CUST-001` zijn toegelaten als gecontroleerde
identiteiten; interne Sparkkolommen gebruiken ordinalen, niet zulke IDs.
Ongeldige ID's met whitespace/backticks of zonder geldige beginletter
worden expliciet geweigerd.

**Versiebeheer en plan.** Nieuwe `fabric-etl-phase5==0.3.0`-wheel:
`86b8b102e8ca59feaa12a364e287ca0525d7b1e5b98502c26e35669c4e59b0a7`.
0.2.0 en bijbehorende historische plans/receipts blijven behouden.
Nieuwe gesloten read-only scope `PHASE5_QUALITY_WINDOW_PROBE`, plan
`c92a05c8ef3d131c06ebcc72620fb0fe2552f33b90d8e137f4da5c7cfdb5f685`.
Het publisher-script accepteert expliciet de stap 0.2.0 naar 0.3.0,
controleert de exacte predecessor-SHA en bewaart de basiswheel/settings.
37 gerichte Quality/bundletests geslaagd in 0,41 s; geen editorproblemen
in gewijzigde code/notebook. Publicatie en live resultaat worden pas na
readback/jobuitkomst afgetekend, niet op grond van deze lokale tests.

**Exacte nieuwe fixture.** Vier Raw-records: eligible A, filtered A,
eligible B, eligible B. Verwacht `4=1 FILTERED+1 PASSED+2 REJECTED`,
windowpopulation 3 en violations 2. Een tweede evaluatie moet bij threshold
66 procent expliciet blokkeren; een all-filtered evaluatie moet population
0, violations 0 en vier FILTERED-records met lege codes opleveren.
Geen businessdata-/controldatawrites in deze notebook.

**Publicatie/readback.** Upgrade naar 0.3.0 is geslaagd, targetVersion
`594fc936-a3c5-422a-b579-d3467400e4d3`, van
10:19:44.6074277 tot 10:23:47.5104051 UTC. Zowel Spark Libraries als
Spark Settings melden `Success`; basiswheel/settings zijn byte-identiek
teruggelezen. [Upgrade-receipt](../deploy/evidence/phase5-quality-window-runtime-upgrade.json)
meldt `PUBLISHED_READBACK_VERIFIED`. Notebook
`3da778b8-2774-4f54-9632-b71d66d02ab4` is via het duurzame journal
aangemaakt en exported readback verified. De bestaande vijf scopes zijn
niet opnieuw uitgevoerd of automatisch naar 0.3.0 omgepind.
Nieuwe job `6cfc8b12-0828-40bc-a597-d133b5cd7d9f` is ingediend;
zijn receipt blijft de bron voor terminale opvolging. De nieuwe verifier
controleert per Raw-ID de verwachte uitkomst, exacte populations/counts,
66-procentblokkade en all-filtered accounting. Read-only windowproofs
vereisen geen writerowner; bestaande writerproofs vereisen die nog wel.
59 gerichte tests geslaagd in 0,41 s; volledige suite daarna
**420 passed in 3,22 s**.

**Nieuw concreet mappingrisico uit bronschema.** De acht Fabric Sales-
Qualitymappings bevatten voor `total_due_amount` de expressie
`cast(coalesce(total_due_amount, subtotal_amount + tax_amount + freight_amount)
as decimal(18,4))`. De werkelijk teruggelezen 33-kolommenview bevat wel
subtotal/tax/freight, maar geen `total_due_amount`. Coalesce is geen
schema-presenceguard: Spark moet de ontbrekende kolom alsnog resolven.
Dit is een concrete kandidaat voor een compilefout, geen al uitgevoerde
mappingjob. Het maakt duidelijk waarom schema-only bereikbaarheid of
synthetische DQ-success geen echte metadataacceptatie is. De mapping moet
bij de echte bronprojectie worden gecorrigeerd en gecompileerd. De
oorspronkelijke Databricks-seed en werkende deployment zijn niet gewijzigd;
de huidige Fabric-release is niet geactiveerd. De genoemde 70 blockers
zijn de statische preflightinventaris, geen garantie dat er daarbuiten
geen functionele compile-/temporale fouten bestaan.

**Live windowresultaat.** Job
`6cfc8b12-0828-40bc-a597-d133b5cd7d9f` liep van
10:26:15.5615823 tot 10:29:08.8341142 UTC, `Completed/SUCCEEDED`.
Het resultaat is exact het verwachte `4=1+1+2`. Record-0 A is PASSED,
record-1 A is FILTERED met lege codes; beide B-records zijn REJECTED met
`DUPLICATE_CUSTOMER`. Metrics: population 3, violations 2,
66.66666666666667 procent. Bij threshold 66 is expliciet
`Quality thresholds blocked: DQ-CUST-001:FAIL_BATCH` ontvangen.
All-filtered case: vier FILTERED, population 0, violations 0, geen codes.
[Receipt](../deploy/evidence/phase5-quality-window-v1-receipt.json) en
[verificatie](../deploy/evidence/phase5-quality-window-verification.json)
zijn duurzaam bewaard, `production_release_accepted=false`.

**Behoud bestaande Quality-output.** Om niet alleen de nieuwe windowcase
te bewijzen, is vervolgens de oorspronkelijke vijf-record-Qualityfixture
op de nieuwe gepubliceerde package gestart. De notebookdefinitie en oude
0.2.0-plannen zijn niet omgepind; uitsluitend de expliciete uitvoerparameter
`expected_phase5_version=0.3.0` is vastgelegd in een eigen parameterreceipt.
Job `69748535-0ad8-438d-8fd5-cc6f8e62a59f` wordt tegen de oorspronkelijke
0.2.0-first-receipt vergeleken. Vereist: zero inserts, identieke vier
outputfingerprints, batchcontext en twee commit-ID's/VERIFIED-statussen.
Dit is een nieuwe cross-versionregressie, geen overschrijving van oud bewijs.

### F5-SOURCE-SPARK - native SQL-bronroute onderzoeken

**Keuze op grond van officiële documentatie.** Een webzoekantwoord stelde
ten onrechte de Warehouse-connector `synapsesql` gelijk aan de connector
voor een SQL database. Dat antwoord is niet als productbewijs gebruikt.
De rechtstreeks gelezen Microsoft-documentatie onderscheidt:
[SQL database connector](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-sql-connector)
gebruikt `import com.microsoft.sqlserver.jdbc.spark` en
`spark.read.option("url", url).option("query", query).mssql()`, met
automatisch doorgegeven Entra-identiteit; de
[Warehouse-connector](https://learn.microsoft.com/en-us/fabric/data-engineering/spark-data-warehouse-connector)
gebruikt `synapsesql` voor Warehouse/Lakehouse SQL analytics endpoints.
Daarom wordt de native SQL-connector geprobeerd in plaats van handmatig
tokens in notebookparameters te stoppen of interactieve JDBC-login te forceren.

**Afgebakend experiment.** Nieuwe
[sourcebundle](../fabric/phase5-sales-source-bundle.json), scope
`PHASE5_SALES_SOURCE_PROBE`, dezelfde gepubliceerde 0.3.0-wheel, plan
`1d38871138b9b841b5fb03b1635013335ba24f24941497b6fdc08b47b510e21c`.
De notebook gebruikt uitsluitend de bekende server/database/view en
`SELECT TOP (0)`, 30 s timeouts, TLS/certificaatvalidatie en read-only intent.
Hij verlangt exact de onafhankelijk teruggelezen 33 bronkolommen, nul
records, de verwachte oorspronkelijke Spark-compilefout op het ontbrekende
`total_due_amount`, en de acht correcte projectietypen nadat dat veld uit
subtotal/tax/freight is afgeleid. Geen bron-/business-/controldatawrites,
geen service-principalsecrets, geen actief product.
De gewijzigde expressie is hier nog testharnesscode, niet stilzwijgend
een goedgekeurde wijziging van de projectrelease.
Beschikbaarheid/authenticatie van de native connector wordt pas na
werkelijke uitvoering afgetekend. Lokale planning is geen bronverbinding.

**Cross-versionregressie: juiste provenanceblokkade gevonden.** De eerder
genoemde vijf-record-job `69748535-0ad8-438d-8fd5-cc6f8e62a59f`
eindigde van 10:30:30.7653563 tot 10:34:49.9448896 UTC als `Failed`.
De generieke API-fout gaf geen concrete oorzaak. List Livy Sessions leverde
Livy-ID `b413c3f6-8d18-42f0-ba05-359a864e36c6`; via de execution snapshot
is `ContractError: Layer commit conflicts with an existing batch/entity`
bij `commit_layer` vastgesteld. De notebook neemt de packageversie op in
de gecommitte inputfingerprint. Het proberen te committen van 0.3.0 onder
de bestaande 0.2.0-batch/commit is daarom terecht geblokkeerd.
Dit is geen reden runtimeprovenance te verwijderen of de commitcheck
te versoepelen om een groene job te krijgen.
[Duurzame foutanalyse](../deploy/evidence/phase5-quality-v3-regression-failure-analysis.json).

De eerdere verwachting van een succesvolle cross-versioncommitretry is
hiermee expliciet gecorrigeerd. De juiste regressie is **read-only
outputequivalentie**, niet een nieuwe runtime als oude runtime committen.
Daarom wordt de windowprobe in een nieuwe planrevisie uitgebreid: lees de
oorspronkelijke vijf Bronze-records; evalueer met 0.3.0; vergelijk de drie
outputfingerprints en accounting exact met 0.2.0; lees de bestaande drie
targets en twee originele commit-ID's/statussen terug zonder writes.
De eerdere geslaagde windowreceipt en zijn oorspronkelijke plan blijven
behouden. De nieuwe verifieroptie `--require-scalar-regression` verlangt
dit aanvullende bewijs en weigert een provenanceversoepeling.

**Native source-deployment.** De sourceprobe is na de terminale writerfout
gedeployed/readback verified als Notebook
`44b6ef5a-f8d4-46e8-ba35-34876c8a9851`. De sourcejob is gestart met een
eigen receipt; er worden geen writers parallel gestart.
De bestaande Environment is niet opnieuw gepubliceerd voor deze notebook.

**Native bronproef geslaagd.** Job
`6946f2e2-edcd-4bc3-8d32-d12c8e5db57a` liep van
10:40:43.478278 tot 10:43:10.3773483 UTC en is
`Completed/SUCCEEDED`, daarna automatisch `VERIFIED`.
`authentication=FABRIC_AUTOMATIC_ENTRA_EXECUTING_IDENTITY`.
De 33 werkelijke kolomnamen komen overeen met de onafhankelijke lokale
SQL-readback; nul bedrijfsrecords. De oorspronkelijke mapping faalde
daadwerkelijk met `UNRESOLVED_COLUMN.WITH_SUGGESTION`; de gecorrigeerde
projectie compileert tot exact:

| Doelkolom | Werkelijk Spark-type |
|---|---|
| sales_order_detail_id | bigint |
| order_quantity | int |
| unit_price | decimal(18,4) |
| unit_price_discount | decimal(9,6) |
| total_due_amount | decimal(18,4) |
| order_status_code | string |
| source_last_modified_at | timestamp |
| is_deleted | boolean |

[Sourcejobreceipt](../deploy/evidence/phase5-sales-source-v1-receipt.json)
en [verificatie](../deploy/evidence/phase5-sales-source-verification.json).
Dit is een echte Spark-bronverbinding, geen alleen lokale JDBC-test.
Het vereist geen handmatige tokenparameter of nieuwe package-installatie.
Nog niet bewezen: toekomstige unattended identiteit, echte extraction/
watermarks/closed manifests en data-quality op de werkelijke records.

**Werkelijke releasekandidaat gecorrigeerd.** Na het live compilerbewijs
is mapping `M-QA-FAB-05` gewijzigd naar
`cast(subtotal_amount + tax_amount + freight_amount as decimal(18,4))`.
De converter bevat dezelfde expliciete Contoso-correctie, met controle
op mapping-ID, bronobject, doelkolom, datatype en de exact bekende oude
expressie. Afwijkingen worden niet generiek herschreven maar blokkeren
voor een nieuwe review. Alle andere Qualitymappings blijven ongewijzigd.
De oorspronkelijke seed en Databricks-deployment zijn niet aangepast.

Vorige kandidaat is duurzaam bewaard in
[release vóór de correctie](../deploy/evidence/phase5-project-release-before-sales-mapping-fix.json),
bestand-SHA256
`a2432c7a6db139742f3b555ae49184a4ed3cca9a97f354398b01de9b179d9a32`,
release-ID `caac34a0cf503bdb14ebe9bc101cf6c789220f4f78a603394805585c38c36705`.
Nieuwe kandidaatrelease-ID:
`c9beb08ca216ac512739c00a6dde127190bd7e53713327080abe59a10b5406f4`.
Conversie is opnieuw identiek/herhaalbaar getest; 120 gerichte
project/plan/bundle/receipttests geslaagd in 2,59 s. Projectvalidator geeft
`VALID_CANDIDATE` met dezelfde 70 statische blockers. De bronaliasgate is
nog niet omzeild; native toegang wordt nu bewezen, maar moet nog via de
volledige runner/bindings worden aangehaakt. Geen schema's mechanisch
DECLARED gemaakt of 20 expressies zonder review APPROVED gezet.
Volledige suite daarna: **432 passed in 3,24 s**, geen editorproblemen in
converter/tests.

**Tweede regressieprobe: fingerprintalgoritmen niet verwarren.**
Read-only job `4d0d0b55-6920-4192-9e91-fe7e050be5e1`, van
10:45:08.9782974 tot 10:48:11.616529 UTC, faalde op
`RuntimeError: Original committed scalar Bronze fixture changed`.
Execution snapshot bevestigt die precieze assert.
Codeonderzoek toont een fout in de probevergelijking:
Bronze gebruikt zijn oorspronkelijke Raw-ID-bucketprefix en canonical
`bucket/count/sha256`-structuur; de algemene Quality-fingerprinter hasht
de key opnieuw en gebruikt een andere bucketstructuur. Hun uitkomsten
mogen niet met dezelfde gepinde hash worden vergeleken.
Dit is geen bewijs van beschadigde data.
[Foutanalyse](../deploy/evidence/phase5-quality-window-v2-failure-analysis.json).

De probe gebruikt nu de **bestaande gepubliceerde Bronze-helper**
`ingestion._output_fingerprint` voor de Bronze-pin en de bestaande
`quality.frame_fingerprint` voor Quality. Geen nieuwe fingerprintdefinitie
of bibliotheekversie, geen targetwijziging. Een AST/source-regressiecontrole
voorkomt opnieuw de verkeerde helper voor Bronze.
41 gerichte bundle/receipt-tests geslaagd in 0,65 s.
Nieuwe planrevisie
`1886a1a2e817a36c583d5b6384cd282882d89fbe1f76a6a33bffe02f22611c9b`
is afzonderlijk opgeslagen; oorspronkelijke plannen/receipts blijven
behouden. De nieuwe job wordt met `--require-scalar-regression`
geverifieerd; geen succesclaim voordat die terminal is.

**Gecorrigeerde scalarregressie nu geslaagd.** Job
`f1b2c50a-c34f-4da1-b06f-f6176be61e72` liep van
10:51:04.1404904 tot 10:54:22.5175997 UTC.
`Completed/SUCCEEDED`, automatische verifier inclusief
`--require-scalar-regression`: `VERIFIED`.
De nieuwe package levert op de oorspronkelijke vijf Bronze-records exact
`5=1 FILTERED+3 PASSED+1 REJECTED`; alle drie regels behouden population
4, één violation en 25 procent. De beide errorregels bij threshold 25
blijven niet breached; de warningregel bij 100 evenmin.

De nieuwe computed outputs en bestaande targets behouden exact:

| Uitvoer | SHA256 |
|---|---|
| PASSED | `0805bc92cd3f3d10d31e72bed448e3d9e4bfef848480491172c225cb6a450d7d` |
| REJECTED | `37a68e86b86beeed4e680387df0111e54d450a2d9d05e58eec00fb641604310a` |
| FILTERED | `c8ec6b1cb8bfecbb318bcbdccc224c9b5d7194af3d227ef580915d974258fe27` |

Ook de gepinde originele Bronze-fingerprint is met zijn eigen algoritme
geverifieerd. Beide oorspronkelijke commits blijven `COMMITTED` met
dezelfde IDs. `targets_unchanged=true` en
`new_runtime_committed_as_old_runtime=false`.
[Eindreceipt](../deploy/evidence/phase5-quality-window-v3-receipt.json) en
[eindverificatie](../deploy/evidence/phase5-quality-window-scalar-verification.json).
Dit sluit de regressie van de packagefix aantoonbaar af; eerdere gefaalde
proeven blijven zichtbaar en zijn niet als succesvolle retries gelabeld.

**Eindcheckpoint van deze bewijsronde.** Laatste volledige lokale suite:
**433 passed in 3,23 s**. De sourceverifier is opnieuw op de echte receipt
uitgevoerd en verlangt nu ook alle 33 exacte kolomnamen, niet slechts een
count en drie essentiële velden; `VERIFIED`.
Geen editorproblemen in de gewijzigde converter, verifier of windownotebook.
Alle gestarte jobs uit deze ronde zijn terminal; geen ontbrekende achtergrond-
uitkomst of onterecht open run. Bundles, ownershipjournal, oude/nieuwe
plannen, receipts, fouten en readbacks zijn duurzaam bewaard.
Geen geheimen opgeslagen, geen Databricks-writes, geen nieuwe platformgrants,
geen businessrecords geëxtraheerd en geen bedrijfsrelease geactiveerd.

**Wat nog werkelijk nodig is voor einde fase 5.** De afgebakende
platform-/frameworkproeven zijn geslaagd, maar de volledige metadataruntime
voor vijf systemen/negen objecten is nog niet geïmplementeerd en
geaccepteerd. Prioriteit verschuift nu van extra losse platformproeven naar
de generieke mapping-/Qualityadapter voor alle 33 regels, de 18 Vault-
entities, drie BV/PIT-definities en 19 Gold-contracten, inclusief de
historische as-of-/determinismeproblemen. De werkende native SQL-route
moet onderdeel worden van een echte environmentbinding en closed-delivery-
extractrunner. Governance/retentie, absence-delete, productpublicatie en
nieuwe consumeracceptatie blijven expliciete gates. De technische
uitvoeringspermissie is niet gebruikt om ontbrekende feiten of
bedrijfseigenaarsgoedkeuringen te verzinnen.

### F5-METADATA-01 - Echte metadata-Quality via de eigen assetbundel

**Datum/status:** 11 oktober 2026; implementatie, publicatie, deployment en
eerste Fabric-uitvoering geslaagd. Aanvullende afzonderlijke herstart
gestart op verzoek om fase 5 voort te zetten; resultaat wordt afzonderlijk
geverifieerd. Dit is geen activatie of volledige bedrijfsacceptatie.

**Aanleiding en keuze.** Losse synthetische frameworkproeven bewezen nog
niet dat de daadwerkelijke Contoso-mappings en kwaliteitsregels op Fabric
werken. Daarom is een generieke adapter toegevoegd in
`src/fabric_etl_phase5/mapping.py`: alle negen source projections en alle
33 metadatakwaliteitsregels worden uit de gevalideerde kandidaatrelease
gecompileerd. Geen afzonderlijke hardcoded implementatie per bron; de
notebook bevat uitsluitend gecontroleerde fixtures en assertions.

**Semantische correcties en onderbouwing.**

- De legacy ECB-filter beschrijft KEEP; de Quality-kern beschrijft EXCLUDE.
  De adapter inverteert de raw-filter expliciet. Ongeldige `OBS_VALUE`
  wordt FILTERED, niet door de latere typed regels afgekeurd.
- `holiday_types` is een geserialiseerde JSON-string. De exact herkende
  Nager-regel wordt daarom `size(from_json(..., 'array<string>')) > 0`;
  een afwijkende legacy-expressie wordt niet stilzwijgend herschreven.
- `current_date()` in kwaliteitsregels bindt aan de persistente
  `load_date_utc`, zodat dezelfde levering dezelfde beoordelingsdatum houdt.
- Expliciete nested CASTs worden quote-/parenthesis-aware naar TRY_CAST
  vertaald. Iedere inner cast krijgt een eigen validity guard, zodat
  `coalesce(..., false)` een ongeldige boolean niet kan verbergen.
  Verplichte outputkolommen krijgen bovendien expliciete nullcontroles.
- Referentiele regels gebruiken alleen tijdelijke PASSED-views uit dezelfde
  project-, bronsysteem-, delivery-, batch- en releasecontext. Legacy
  catalogusverwijzingen worden gecontroleerd gebonden en views opgeruimd.
  Een duurzaam geverifieerde referentiecommit moet de latere writer leveren;
  de read-only adapter zelf bewijst zo'n commit nog niet.
- De bestaande Quality-kern is hergebruikt via `evaluate_typed_quality`.
  Population en alle ruleviolations gebruiken samen een aggregate;
  terminale aantallen gebruiken een groupBy. Dit vermindert losse
  Spark-acties zonder de threshold- of accountingcontracten te wijzigen.

**Immutable publicatie en deployment.** Add-on
`fabric_etl_phase5-0.4.0-py3-none-any.whl`, SHA-256
`16f322fd0e4020d31dd6f3f6fa6d7557b4e5e5f22c2f5f6d8733a1bfb068c1eb`,
is via de eigen REST-publisher gepubliceerd; de exacte 0.3.0-voorganger
werd vooraf gecontroleerd. Basiswheel 0.1.0 en Spark-settings bleven
byte-identiek. Publicatie liep van 11:01:11 tot 11:06:44 UTC en eindigde
`PUBLISHED_READBACK_VERIFIED`; targetVersion
`ce9817c5-c13d-4d5f-a727-f9a3b6439355`.
De bundelplanner valideert de release inclusief filefingerprints en embedt
de kandidaat, zonder deze te activeren. Scope:
`contoso_phase5_metadata_quality_probe`.
Notebook `861b9c7f-115c-40eb-b135-e50195620674` is via REST gedeployed,
geregistreerd in het ownershipjournal en teruggelezen.

**Werkelijk runtimebewijs.** Job
`1fa2b802-3ea0-4b55-a09d-7835a281ba65` liep van 11:09:42 tot 11:13:11 UTC,
`Completed/SUCCEEDED`. De exacte receiptverifier geeft `VERIFIED`:

- SALES Customers, Products, Employees, Orders en Returns: respectievelijk
  4, 3, 2, 9 en 6 daadwerkelijke metadataregels.
- CBS, ECB en Nager: ieder 2; Fabric Sales: 3; samen exact 33.
- Per bronfixture: 1 PASSED, 0 REJECTED, 0 FILTERED en nul ruleviolations.
- ECB-negatieve case: 2 input = 1 PASSED + 1 FILTERED; population 1.
- Ongeldige `is_deleted`: 1 REJECTED met expliciete INVALID_CAST-code.
- De daadwerkelijke duplicateklantregel blokkeert de negatieve fixture.
- Herhaalde klantevaluatie binnen dezelfde job behoudt de fingerprint.

**Lokale validatie.** Eerdere volledige baseline 449 tests; laatste
gerichte adapter/bundle/receipt-suite 53 tests. Na alle laatste wijzigingen
is de volledige suite opnieuw uitgevoerd: **460 passed in 4.70 s**.
De VS Code-testtool ontdekte geen tests (0 uitgevoerd); dit is niet als
bewijs gebruikt. De bestaande pytest-suite via de geconfigureerde
Python 3.13.3-interpreter leverde het bovenstaande resultaat.

**Evidence.** [Runtimepublicatie](../deploy/evidence/phase5-metadata-quality-runtime-upgrade.json),
[plan](../deploy/evidence/phase5-metadata-quality-v1.plan.json),
[eerste receipt](../deploy/evidence/phase5-metadata-quality-v1-receipt.json)
en [verificatie](../deploy/evidence/phase5-metadata-quality-verification.json)
zijn persistent. Gepubliceerde wheelbytes worden niet overschreven.

**Beperkingen en vervolg.** Geen businessdata geschreven, geen Databricks-
writes en geen releaseactivatie. De fixturetest is geen end-to-end
bedrijfsrun. Afzonderlijke jobretry moet de volledige exit_result exact
herhalen. Duurzame metadata-Quality/Reject-outputs, committed referenties,
expliciete empty snapshots en aanvullende negatieve arithmetic-/datumcases
zijn nog nodig. CAST-guards bewijzen niet dat iedere impliciete arithmetic
cast veilig wordt afgekeurd. De bestaande 70 preflightgates zijn niet
mechanisch verwijderd; Vault/BV/Gold-runtime en acceptatie blijven open.

**Afzonderlijke metadata-herstart afgerond.** Job
`a845fc07-33e2-4683-bac4-23073be15a24` liep van 15:27:44 tot 15:31:08 UTC
(17:27-17:31 CEST) en eindigde `Completed/SUCCEEDED`. De volledige
inhoudelijke exit_result is exact gelijk aan de eerste job: alle negen
bronfingerprints, alle daadwerkelijke en gegenereerde regelmetrics en
negatieve cases. De verifier geeft `separate_job_retry_verified=true`;
de regressietest gebruikt nu beide echte receipts in plaats van een
gekopieerde eerste uitkomst.
[Retryreceipt](../deploy/evidence/phase5-metadata-quality-v1-retry-receipt.json)
en [cross-jobverificatie](../deploy/evidence/phase5-metadata-quality-retry-verification.json).
Dit bewijst determinisme in afzonderlijke Spark-jobs, niet durable nul-inserts.

### F5-METADATA-02 - Duurzame Customers-slice met werkelijke metadata

**Datum:** 11 oktober 2026, vervolg na de geslaagde afzonderlijke
metadataherstart. **Doel:** niet nog een losse engineproef, maar dezelfde
werkelijke SALES.CUSTOMERS-projecties en regels duurzaam naar Quality en
Reject schrijven, met controleerbare commits en een afzonderlijke nul-insertretry.

**Beslissing en afbakening.** Eerst een reference-free Customers-slice
volledig afmaken voordat Orders/Returns met committed referenties worden
aangesloten. Dit beperkt foutdiagnose zonder de generieke adapter te
dupliceren. De writer `write_metadata_quality` is toegevoegd in
`src/fabric_etl_phase5/mapping.py`. Alleen afgebakende proofdata wordt
geschreven: `PHASE4_PROOF`, delivery
`PHASE5_METADATA_WRITE|CUSTOMERS_V1`, tableprefix `phase5_metadata_`.
Businessmode eist de actieve goedgekeurde release; er is geen activatie
of bedrijfsgoedkeuring uit technische toestemming afgeleid.

**Duurzaam inputcontract.** Voor writes controleert de writer de exacte
raw-kolommen, een nonempty/non-NULL context, sourceidentity, persistente
batchvelden en batchtijd, een COMMITTED BRONZE-marker met juiste batch/
layer/entity, een target in de processing Bronze-namespace, volledige
row-equivalentie tussen aangeboden input en duurzame targetreadback en
de oorspronkelijke Bronze-fingerprint. Geen louter caller-supplied
successmarker. Referentieafhankelijke sources blokkeren voor writes tot
committed-reference loading is geimplementeerd.

**Outputs en retrycontract.** Bestaande `write_immutable_output` schrijft
PASSED, REJECTED en FILTERED apart, bewaakt ownership, vergelijkt schema's
en bestaande inhoud en verifieert readback. Daarna legt `commit_layer`
de QUALITY-commit vast. De inputfingerprint bevat Bronze-commit-ID,
kandidaatrelease, bron, rawschema, projecties, regels, filter en runtimeversie.
Insert-/retryaantallen worden niet in de stabiele outputmanifest opgenomen.
Een afzonderlijke retry moet dezelfde batchtijd, hashes en commits behouden
en nul nieuwe rows schrijven. Cachecleanup gebeurt ook bij writeconflicten.

**Proeflevering.** Twee immutable JSON-records: geldig MC1 en MC2 met
ongeldige `is_deleted`. De bestaande closed-manifestreader leest de
bytes en maakt de stabiele Raw-ID's; geen eigen tweede reader. Raw-
inventory, inhoud en bestandsgrootten worden teruggelezen. Verwacht:
Bronze 2, Quality PASSED 1, Reject 1 met expliciete INVALID_CAST-code,
FILTERED 0. Targets staan alleen in processing:
`bronze.phase5_metadata_customers_v1`,
`quality.phase5_metadata_customers_v1`,
`reject.phase5_metadata_customers_v1` en
`quality.phase5_metadata_customers_v1_filtered`.

**Publicatie/deployment.** Nieuwe immutable wheel 0.5.0, SHA-256
`a49d9a84594f12fa6411a876438f57fb75d433442bfa8e84c3443486c3709b66`.
Publicatie: 15:35:27-15:39:31 UTC, targetVersion
`33617b19-e2fc-4678-a086-790bb76a2a01`, `PUBLISHED_READBACK_VERIFIED`.
Basiswheel en Spark-settings byte-identiek behouden.
De originele publicatieplanrevisie blijft bewaard. Voor deployment is
een tweede notebookplan gemaakt met expliciete terugleesassertion op de
opgeslagen Reject-codes; dezelfde wheelbytes, geen runtime-rebuild.
Deploymentplan `727c3b583817a1efad8034f18f633ddb2a77dbac1aa88de93f0add8359e66e17`.
Notebook `9df60181-44b6-4863-b2e6-d4041a13091b`, definition fingerprint
`46f2845abc61d1f3d11feb986076fb247ac1549f8d99a8b417a9472d0c5dca42`,
is geregistreerd in het bestaande ownershipjournal en export/readback-
verified. Andere items in het journal zijn niet opnieuw door deze scope
gedeployed; de console toont hun historische journalregistraties.

**Validatie tot deployment.** Gerichte writer/receipt/bundle-suite:
89 geslaagd. Volledige projectsuite na aanvullende negatieve guards:
**484 passed in 3.85 s**. Negatieve tests bewaken NULL/empty context,
release/source/batchtijd, pending/wrong-layer/wrong-entity commits,
veranderde Bronze-inhoud/pin, verkeerde targetnamespace, business-
releasegate, ongebonden referenties, proofscope en cleanup bij conflicts.
Dit zijn lokale unit-/contracttests, geen lokale Spark-uitvoering.
Geen editorproblemen in writer, bundle, verifier of notebook.

**Uitvoering gestart.** Eerste job
`ab0409c9-aa69-453e-8b46-292e48cb9dcd` is ingediend met persistente receipt;
completion en afzonderlijke retry worden hieronder geregistreerd.
[Publicatie](../deploy/evidence/phase5-metadata-write-runtime-upgrade.json),
[eerste plan](../deploy/evidence/phase5-metadata-write-v1.plan.json),
[deploymentplan](../deploy/evidence/phase5-metadata-write-v2.plan.json)
en [jobreceipt](../deploy/evidence/phase5-metadata-write-v1-receipt.json).

**Beheerimpact.** De dev Environment is nu 0.5.0. Oudere notebooks met
0.4.0 of eerdere pins weigeren terecht een verkeerde runtime. Gebruik
het notebook dat bij de huidige publicatie hoort; verander geen
expected-versionparameter om dit te omzeilen. Historische receipts en
oude immutable wheels blijven behouden.

**Eerste duurzame uitvoering geslaagd.** Job
`ab0409c9-aa69-453e-8b46-292e48cb9dcd` liep van 15:41:55 tot 15:47:08 UTC
(17:41-17:47 CEST), `Completed/SUCCEEDED`. De exacte verifier geeft
`VERIFIED`. Alle verwachte targets zijn geschreven en teruggelezen:
2 Bronze-inserts, 1 Quality-insert, 1 Reject-insert, 0 FILTERED-inserts.
De opgeslagen Reject-rij heeft exact `INVALID_CAST_11`. Alle werkelijke
Customers-regels hebben population 2 en nul violations; uitsluitend
de gegenereerde boolean-castguard heeft 1 violation (50%, CONTINUE,
maar row-level ERROR dus REJECTED).

Persistente batch `2387b849-3b44-5f62-ae6a-67fec0902f7d`, load_date
`2026-10-11T15:44:28.448883+00:00`.
Bronze-commit:
`b724a0d5b6fc850f1f96da846b894e14ac2bd62f35e45faef18029b54e50d317`.
QUALITY-commit:
`9eeccf55c978f7a679e04519cb79f1437a490b0846a7e29baa2166b6aea1c0b1`.
Beide `COMMITTED`. Fingerprints:
Bronze `76a7c2d31fc59676adcee453ed15fba523fc7b69706f3a538d2afa8c865f20dc`,
PASSED `528694d241ba986e65da82c075920c9b02c67309b39b6f02f2530aa94aa741f0`,
REJECTED `acd7e83957e7d42d8459b18be4253d28975c90732b8be00ddc50929fccfdfd55`.
[Eerste verificatie](../deploy/evidence/phase5-metadata-write-verification.json).
De owner is vrijgegeven; geen mutaties in Databricks of businessrelease.

**Afzonderlijke durable retry gestart:** job
`4d4ef134-6e83-4c61-aa43-f45a549fe8ec`, eigen
[retryreceipt](../deploy/evidence/phase5-metadata-write-v1-retry-receipt.json).
Geen overlappende writer: de eerste job was terminal voordat deze werd
gestart. Het processing-Lakehouse is voor de opdrachtgever in de gedeelde
browser geopend; zichtbaarheid van de Explorer-preview is geen vervanging
voor de reeds geverifieerde Spark-/Delta-readback.

**Afzonderlijke duurzame retry geslaagd.** Job
`4d4ef134-6e83-4c61-aa43-f45a549fe8ec` liep van 15:47:35 tot 15:52:33 UTC
(17:47-17:52 CEST), `Completed/SUCCEEDED`. De exacte cross-jobverifier
geeft `VERIFIED`, `separate_job_retry_verified=true`.
Bronze, PASSED, REJECTED en FILTERED: alle inserts **0**; retryaantallen
respectievelijk 2, 1, 1, 0. De volledige inhoudelijke uitkomst is gelijk
aan de eerste run, uitgezonderd de verwachte insert-/retrytellingen en
commitstatus `VERIFIED`. Batch-ID, oorspronkelijke load_date, alle
targetfingerprints en beide commit-ID's zijn behouden. De owner is
opnieuw vrijgegeven.
[Cross-jobverificatie](../deploy/evidence/phase5-metadata-write-retry-verification.json).
Beide echte receipts zijn als permanente regressietest opgenomen.

**Explorerobservatie.** De gedeelde browser navigeerde naar processing,
maar de Lakehouse-iframe laadde niet volledig; netwerkrequests werden
afgebroken. Een enkele reload is geprobeerd, zonder de succesvolle
Spark-/Delta-uitkomst afhankelijk te maken van de interface. Er wordt
daarom niet geclaimd dat de Explorer-preview al is gecontroleerd.
De opdrachtgever kan de vier geregistreerde tabellen in processing
bekijken; dit zijn geisoleerde testtabellen, geen Gold-producten.

**Eindstand van deze stap.** Publicatie, deployment, eerste write en
afzonderlijke retry zijn terminal en verified. Geen achtergrondwriter
uit deze ronde blijft open. De Customers-slice is nu duurzaam bewezen;
fase 5 als geheel blijft actief. Volgende implementatie: committed
Quality-referenties en overige bronobjecten, expliciete empty snapshots
en daarna de daadwerkelijke Vault/BV/Gold-definities.

**Laatste regressiecontrole.** Na opname van beide echte durable receipts
gaf de volledige suite eerst 484 passed/1 failed: een negatieve
commitmutatieassertion was bij het toevoegen van de receipt-test in de
verkeerde testfunctie terechtgekomen en probeerde een veld op het
receipt-envelope te lezen. De assertion is teruggezet in de bestaande
synthetische negative-verifiertest; geen runtime-, wheel-, receipt- of
commitwijziging. Volledige rerun: **485 passed in 3.88 s**. Geen
editorproblemen. Dit is de actuele testbaseline van deze bewijsronde.

### F5-EXIT-01 - Formele exitcontrole generieke ingestie en control plane

**Opdracht en datum.** Op 11 oktober 2026 om 18:00 CEST vraagt de
opdrachtgever de exitcontrole af te ronden. De controle is uitgevoerd
tegen het uitgewerkte tienfasenplan, het vastgestelde fase-2-contract
en de bestaande fase-4-afsluiting. **Controle afgerond; fase-5-exit
geblokkeerd; geen vrijgave van fase 6.** Een geslaagde Customers-slice
is niet hetzelfde als een complete generieke ingestie-/controlruntime.

**Correctie van eerdere scope-uitleg.** De oorspronkelijke globale
fasentabel in sectie 8 is een historische eerste planning. Het later
uitgewerkte tienfasenplan en het vastgestelde ontwerpcontract zijn de
basis voor deze review. Fase 5 omvat generieke ingestie en control:
manifest/bestandsregister, schema-/deliverygate, persistente context,
audit, ownership en veilige retries, met een verticale Sales-smoketest.
Complete Sales Quality/Vault/BV/PIT/Gold hoort bij fase 6; aanvullende
bronroutes bij fase 7. Voorbereidend bewijs uit die latere fasen mag
bestaan zonder die fasen als afgesloten te verklaren.

**Uitgevoerde verificatie, geen aannames.**

1. Code gelezen van de closed-manifestvalidator, chronology-selector,
   shared Spark-reader, Bronze-writer/commit, ControlStore, metadatawriter
   en recovery-/Customers-notebooks; gekoppeld aan de contracttests.
2. Acht bestaande receiptcontracten opnieuw read-only uitgevoerd:
   Qualityretry, chainretry, recovery, window, scalarregressie,
   SQL-bronschema, metadataretry en durable metadataretry: alle `VERIFIED`.
   Geen nieuwe Fabric-job gestart en geen platform-/bedrijfsdata gewijzigd.
3. De vier actuele runtimebestanden `ingestion.py`, `quality.py`,
   `mapping.py` en `__init__.py` byte-voor-byte vergeleken met de
   gepubliceerde 0.5.0-wheel: gelijk. Wheel-SHA-256 blijft
   `a49d9a84594f12fa6411a876438f57fb75d433442bfa8e84c3443486c3709b66`.
   De reeds opgeslagen publicatie/readback en deploymentownership vormen
   het platformbewijs; dit was geen nieuwe live Environment-export.
4. De volledige bestaande suite opnieuw uitgevoerd via de geconfigureerde
   Python 3.13.3-interpreter: **485 passed in 4.04 s**.
   Een eerste snippetcall miste de vereiste workspaceRoot; met expliciete
   workspaceRoot werkte de read-only bewijscontrole. Geen inhoudelijke
   gate op basis van die toolfout aangepast.

**Exitmatrix en werkelijk ontbrekende bewijsstukken.**

| Criterium | Oordeel | Bewijs en beperking |
|---|---|---|
| Versiegepinde probe-uitrol en readback | Geslaagd binnen scope | Immutable wheel, eigen journalitems, notebook-/Environment-readback; geen volledige releaseredeployclaim |
| Persistente batchtijd, ownerfencing, Bronze en aparte retry | Geslaagd binnen scope | Fase-4-controlbewijs en actuele Customers-receipts; PHASE4_PROOF, geen geactiveerde bedrijfslevering |
| T-03 JSON/CSV/Parquet-retry en schema-afwijkingen | Geblokkeerd, P5-G01 | JSON-fixtures en hashvectors bewezen; exacte live CSV/Parquet-multisets onder andere partitionering en negatieve schemajobs ontbreken |
| Gesloten levering, duurzame volgorde en bestandsintegriteit | Geblokkeerd, P5-G02 | Pure/local validator getest; nog geen generieke OneLake-gate gekoppeld aan duurzaam delivery-/bestandsregister |
| Expliciete complete lege snapshot | Geblokkeerd, P5-G03 | Manifest/reader ondersteunen declaratie; durable empty commit/retry ontbreekt, metadatawriter blokkeert terecht zonder expliciete context |
| T-06 ingestiecrashgrenzen en work-status/auditrepair | Geblokkeerd, P5-G04 | Conflict-/ownerherstel bewezen; geen daadwerkelijke onderbreking op iedere ingestiegrens en geintegreerde work-/audittransities |

**P5-G01 - Bestandsformaten en schema.** De reader heeft JSON/CSV/Parquet-
code en de tests controleren schema-opbouw en Raw-ID-vectors. Dat is
geen bewijs dat dezelfde fysieke bestanden na andere Spark-partitionering
exact dezelfde Raw-ID-multiset inclusief duplicates opleveren. Voer alle
drie formaten in Fabric door de shared reader, vergelijk exacte
multisets en bewijs verkeerde kolommen/Parquet-types en malformed input
als expliciete fouten voordat targets veranderen. Geen nieuwe reader
per probe toevoegen.

**P5-G02 - Duurzame deliverygate en bytes.** `next_delivery` en
`validate_ready_delivery_manifest` gebruiken caller-supplied geregistreerde
deliveries; de lokale validator controleert echte lokale SHA-256/size.
De OneLake-probes controleren specifieke fixturebytes, maar de generieke
Spark-reader bindt zijn filehash aan de manifestwaarde en verifieert die
waarde niet zelf tegen de fysieke bytes. Controlmigraties declareren
delivery/work_item, maar ControlStore bevat nog geen runtime
deliveryregistratie/statusovergangen. Een bestaande tabel of gezonde
batchcontext is geen bewijs van die koppeling. Nodig: registratie en
conflictreadback van scope/sequence/manifest en per-file hashes, echte
OneLake-inventory/size/hashcontrole voor READY, en blokkade van een nieuwere
READY-levering als de oudere nog niet afgerond is. Scope voor de Sales-gate
blijft het daadwerkelijk vereiste objectmanifest; geen volledige levering
afleiden uit een enkele Customers-fixture.

**P5-G03 - Lege levering.** Expliciet `complete_snapshot=true`,
`empty_snapshot=true`, nul files is toegestaan; ontbreken zonder die
verklaring blokkeert. De reader bouwt lege frames, maar die dragen geen
observeerbare contextrow. De metadatawriter weigert lege input momenteel
expliciet. Nodig: context uit persistente batch plus geverifieerd manifest,
zero-count commit/audit en afzonderlijke retry, zonder een dummyrecord,
silent success of absence-delete af te leiden. Dit is een open implementatie-
en runtimebewijs, niet slechts ontbrekende documentatie.

**P5-G04 - Onderbrekingen, workstatus en audit.** De zes bestaande
recoverytests proberen conflicten tegen een reeds gecommitte target;
zij simuleren niet een crash na append maar voor commit, of na commit
maar voor status/audit. ControlStore kan audit-events schrijven, maar
de actuele ingestie-/Customers-notebook roept deze work-/auditovergangen
niet aan. Nodig: persistente attempts/status/errorcodes en audit,
gecontroleerde crash-/retryproeven op de Raw/Bronze/manifest/statusgrenzen,
commit als authoritative herstelbron en blokkade van downstream bij
onvolledige output. Latere Vault/Gold-crashgrenzen blijven fase 6-8.

**Niet als fase-5-blocker opgevoerd.** De 70 kandidaat-runtimegates blijven
zichtbaar voor businessreleaseactivering, maar worden niet allemaal
fase-5-exitvoorwaarden: complete Sales-Vault/BV/PIT/Gold volgt fase 6,
echte aanvullende bronacceptatie fase 7, consumer/publicatie/hardening
en zakelijke SLA/retentie fase 8-10. De fysieke pipeline en volledige
release-/upgraderedeploy moeten later aantoonbaar worden opgeleverd.
Fase 1-4 worden niet opnieuw uitgevoerd vanwege deze ingestiegaten.

**Besluit en vervolg.** Geen cosmetische fase-afsluiting of verschuiving
van deze vier gaten naar fase 6. Eerst P5-G01 t/m P5-G04 technisch sluiten,
bewijzen vastleggen en dezelfde exitcontrole herhalen. Geen nieuw
gebruikersakkoord nodig voor die technische uitvoering binnen het
bestaande fase-5-mandaat. Deze review zelf is afgerond; de ontbrekende
implementaties/proeven zijn daarmee nadrukkelijk niet uitgevoerd.
Het machineleesbare
[exitrapport](../deploy/evidence/phase5-exit-review.json)
legde toen `review_status=COMPLETED`, `phase_status=ACTIVE_EXIT_BLOCKED` en
`phase6_released=false` vast.

### Vergelijkingsaanpak Databricks/Fabric - identieke bronleveringen

Op 11 oktober 2026 om 18:13 CEST stelt de opdrachtgever voor gelijke
bronbestanden aan beide ketens aan te bieden. Dit wordt als acceptatie-
aanpak vastgelegd: een centrale immutable Sales-levering met vijf
samenhangende objectbestanden, manifest, zakelijke sleutels en SHA-256;
exact dezelfde bytes naar geisoleerde landinglocaties op beide platforms.
Gelijktijdig starten mag, maar is geen voorwaarde voor inhoudelijke
gelijkwaardigheid. Levering, configuratie en vaste beoordelingsdatum
moeten gelijk zijn; technische run-ID's/timestamps mogen verschillen.

Vergelijk per laag de zakelijke sleutels, relevante waarden en totalen,
afkeuringen, ontbrekende/extra records, historie en Current-selecties.
Gebruik minimaal een beginlevering, een wijzigings-/soft-deletelevering
en een identieke retry. Geen absence-delete afleiden. Toets beide
uitkomsten ook aan vooraf bepaalde verwachtingen: dezelfde fout in
beide implementaties is geen acceptatie. Rapporteer gelijkwaardig,
bewust verschillend, afwijkend of niet bewezen.

Voor HTTP/SQL-bronnen kan een vastgelegde extractie downstream-pariteit
testen; connectorvolledigheid/authenticatie vereist afzonderlijk bewijs.
Nu zijn geen Databricks-bestanden, tabellen of pipelines gewijzigd.
De vergelijkingsroute volgt de fase-5-exit en de herkenbare Sales-
implementatie in fase 6; fixtures blijven expliciet testdata.

### F5-EXIT-02 - Implementatie open ingestie-/controlgaten

Na opdracht om de vier exitgaten te sluiten is een gedeelde
`fabric_etl_phase5.delivery`-runtime toegevoegd, zonder een tweede
reader/writer te bouwen. Closed manifest plus per-file identities worden
in de bestaande control.delivery-rij als canonieke scope_json en
manifestfingerprint opgeslagen. OneLake-inventory, fysieke manifestinhoud
en werkelijke file size/SHA-256 worden geverifieerd; SHA-256 wordt door
Spark binaryFile over bytes berekend, niet over parsed records.
De bestaande shared reader en immutable Bronze-writer worden hergebruikt.

De generieke orchestratie leest de leveringsvolgorde uit durable control,
registreert batchcontext, workattempt/status en deterministische audit-
events en schrijft pas na alle targetreadbacks een INGEST-commit.
Alleen daarna volgen succesvolle status en audit; replay reconcileert
data en herstelt ontbrekende status/audit zonder purge. Expliciete lege
manifesten krijgen via persistente context zero-count outputs/commit,
zonder dummyrecord of afgeleide deletes.

Immutable add-on 0.6.0 is gebouwd, SHA-256
`6c2c2a2679dd1435e4b3c6904c3d9f16ac532719315f757c9c1a5a41b5611d9c`.
Gerichte gate-/manifesttests: 71 passed. Volledige suite voor publicatie:
499 passed in 3.95 s. Tien gesloten bundlescopes behouden eerdere pins.
Nieuwe exitnotebook test JSON/CSV/Parquet met duplicates bij partitions
1 en 7, verkeerde kolommen/types, incomplete manifests, gewijzigde
fysieke filehash/missing files, oudere-leveringsblokkade, empty snapshot
en expliciete onderbrekingen op registered/raw/bronze/commit/status.
Een levering wordt na Bronze bewust ongecommit achtergelaten en in de
volgende afzonderlijke Fabric-job hersteld. Nieuwe code/test is nog geen
geslaagd live bewijs; de oorspronkelijke exitreview blijft BLOCKED tot
de werkelijke receipts zijn geverifieerd.

**Publicatieblokkade en toestemming.** Eerste publicatie werd door de
bestaande exclusieve-setupguard geweigerd: er liep een Lakehouse Operations-
preview Spark-sessie `a668555e-44a6-4843-b6e6-19146de8b0d5` op processing.
De opdrachtgever gaf expliciet toestemming alleen die preview te stoppen.
De publieke cancel-API retourneerde 202; de daaropvolgende publicatie
passeerde dezelfde guard en werd ingediend. Geen willekeurige Spark-jobs
gestopt, geen data verwijderd en exclusiviteitsguard niet versoepeld.
Publication/readback en runtimebewijs worden hieronder apart vastgelegd.

**Publicatie bewezen, exit nog geblokkeerd.** De 0.6.0-publicatie is
`PUBLISHED_READBACK_VERIFIED`, targetVersion
`911c0f5c-31dd-455a-ab69-ea744a9d1890`, publicatie van
16:09:26.9016827 tot 16:14:00.39497 UTC. Basiswheel en settings zijn
ongewijzigd. Het owned exitnotebook is
`783d4f39-d323-4804-a7ab-dc33a961fff3`.

Eerste live job `73e687df-8d3d-4dfb-bbd7-a21a43430acb`
(16:17:56.0393979 tot 16:21:48.6872381 UTC) is FAILED, niet geaccepteerd.
De execution snapshot meldt `ContractError: OneLake file bytes differ
from manifest: parquet.parquet`. De REST-fout over een geannuleerde
Spark-sessie was alleen de omhullende fout; de inhoudelijke fout is via
de execution snapshot vastgesteld.

De aparte diagnostische job `1add94d9-d90a-4f47-a7a5-e035472fab27`
bevestigt: JSON 72 bytes en CSV 22 bytes hebben exact dezelfde hashes
als hun manifest. Bij Parquet klopt de lengte 928 bytes, maar de
DataFrame-hashprojectie rapporteert
`e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
(SHA-256 van lege inhoud), terwijl de SQL-projectie bij fixture-inventory
`e01e6b5b4c477666f7da534f99516b63469c064a551485c397b2f3a48fe0fad7`
rapporteerde. Dit is een reproduceerbare discrepantie, nog geen bewezen
verklaring of geslaagde oplossing. Er wordt nu ook de gematerialiseerde
inhoud onafhankelijk met hashlib vergeleken. Geen checksumgate
uitgeschakeld, geen bestaande fixtures overschreven en de gepubliceerde
0.6.0-wheel niet gewijzigd.

Zes aanvullende lokale runtimecontracttests bewijzen expliciete
onderbrekingsgrenzen, FAILED-work/status/audit met hergooien van de fout,
en een zero-record commit. Volledige lokale suite: 514 passed in 3.82 s.
Dit zijn unit-/contracttests, niet het ontbrekende Fabric-exitbewijs.
Fase 6 blijft niet vrijgegeven; de vier oorspronkelijke exitgaten blijven
open totdat de volledige live eerste uitvoering plus aparte recovery
en de betreffende inhoudelijke controles zijn geverifieerd.

**Diagnose aangescherpt en patch gebouwd.** Job
`df8b3cf0-f9c5-4330-a619-e1dd92413ca8` bevestigt dat ook een expliciet
gematerialiseerde binaryFile.content nul bytes bevat, terwijl length
928 meldt. hashlib en een herhaalde SQL-hash leveren dan eveneens de
lege hash. Het probleem is dus niet uitsluitend de DataFrame-sha2-
expressie: het herhaalde binaryFile-leespad levert geen volledige inhoud.
De technische interne platformoorzaak is niet vastgesteld.

Patch 0.6.1 leest voor de fysieke filegate daarom een nieuwe Hadoop-
inputstream, telt werkelijk verwerkte bytes en berekent SHA-256 via
DigestInputStream. Geen volledige bestanden meer als Spark binaryFile-
rows naar de driver collecten; streams worden ook bij fouten gesloten.
Manifestgelijkheid, inventory, grootte en hash blijven harde gates.
De bestaande immutable manifesten worden niet vervangen: de patch moet
de eerder vastgelegde werkelijke hashes reproduceren.
Wheel-SHA-256:
`7211506d031508eb7942f2b6a95a10bc7679fea5f90513604ce809bf792097a9`.
De publisher verlangt exact de gepubliceerde 0.6.0-voorganger.
Lokale streaming-/fouttests: 30 passed; volledige suite 516 passed
in 3.98 s; editor meldt geen fouten in runtime, tests en exitnotebook.
Publicatie en live bewijs van deze patch zijn afzonderlijke vervolggates,
niet uit deze lokale resultaten afgeleid.

### Dagafsluiting 11 oktober 2026 - pauze op verzoek opdrachtgever

Op 11 oktober vroeg de opdrachtgever om die dag af te ronden. Geen nieuwe jobs,
retry, aanvullende failureproef of fase-6-activiteiten meer starten.
De reeds ingediende exitjob wordt niet geannuleerd en mag uitlopen.

**Duurzaam vastgelegde stand:**

- Patch 0.6.1 is `PUBLISHED_READBACK_VERIFIED`, targetVersion
  `796e43f4-2989-4f6b-b74d-81c38806745b`; publicatie
  16:37:43.7573208 tot 16:42:16.9328909 UTC.
- Basiswheel en Spark-settings zijn byte-identiek gebleven.
- Exitnotebook is uitgerold met plan
  `5b7fed9e38daa48320a25e9d7fff48893e96dafc1852450d0bf1a729db94b5d2`.
- Lopende job: `adea8361-6f4d-4fa2-add4-aeb65044a77f`.
  [Receipt](../deploy/evidence/phase5-exit-streaming-v1-receipt.json)
  staat bij dagafsluiting op SUBMITTED; nog geen terminale exitclaim.
  De lokale monitor is `exit-stream-first`. Bij een later gestopte lokale
  monitor hetzelfde receipt hervatten, niet een duplicaatjob indienen.
- De verifier controleert nu ook de concrete bound Bronze-targets,
  volledige filepaths/positieve groottes/SHA-256 en positieve workattempts.
  Gerichte receipt-/runtimecontracttests: 34 passed in 0.31 s.
  Laatste volledige suite voor deze verifieraanscherping: 516 passed
  in 3.98 s. De aangescherpte verifier verandert de deployed wheel niet.
- Fase 5 bleef op 11 oktober ACTIVE_EXIT_BLOCKED; fase 6 was niet vrijgegeven.
  Geen Databrickswrites, businessreleaseactivatie of nieuwe grants.

**Eerstvolgende hervatstap:** terminale status van deze bestaande job
ophalen en, bij succes, de exacte exitreceipt controleren. Daarna pas
een afzonderlijke recoveryjob op hetzelfde gepinde notebook indienen
en de twee receipts vergelijken. De eerste succesvolle proef laat
bewust een geisoleerde Bronze-levering zonder INGEST-commit achter;
dat is testopzet, geen vrijgegeven downstreamdata. Resterend live
failure-/auditrepairbewijs en de vier formele exitcriteria inhoudelijk
beoordelen voordat de fase wordt afgesloten. Bij een fout eerst de
execution snapshot onderzoeken; geen bestaande immutable wheel,
manifest of gecommitte output overschrijven.

### F5-EXIT-02 - Volledige fase-5-exitproef en aparte recovery (12 oktober 2026)

De reeds ingediende eerste job
`adea8361-6f4d-4fa2-add4-aeb65044a77f` is hervat via de opgeslagen receipt,
niet opnieuw ingediend. Fabric rapporteerde `Completed`; de expliciete
exitwaarde was `SUCCEEDED` met scope `PHASE5_EXIT_INGESTION_CONTROL`.
De eerste job bewees de gedeelde JSON/CSV/Parquet-reader, exacte
identity-multisets over partitioneringen, negatieve schema-/integriteits-
gevallen, duurzame deliverygate en volgorde, expliciete lege snapshot en
gecontroleerde onderbrekingen op de geregistreerde/Raw/Bronze/commit/status-
grenzen. De opzettelijke cross-job-stop liet de delivery `PROCESSING` met
Bronze-output maar zonder INGEST-commit achter; downstream werd niet
vrijgegeven.

De afzonderlijke recoveryjob
`7e0c31c7-86ca-45d0-8c11-a552a08e2fa7` heeft die toestand hersteld. De
recoveryreceipt toont dezelfde context en outputfingerprints, nul dubbele
inserts, geverifieerde bestaande commits, herstel van de ontbrekende
cross-job commit en een vrijgegeven owner. De exitverifier controleerde beide
receipts samen en schreef
[de verificatie](../deploy/evidence/phase5-exit-streaming-v1-verification.json)
met `status=VERIFIED`, `separate_job_retry_verified=true` en
`production_release_accepted=false`. De
[bijgewerkte exitreview](../deploy/evidence/phase5-exit-review.json) sluit
P5-G01 t/m P5-G04 als `PASS_SCOPED`.

De gerichte suite voor receipt- en deliveryregressies slaagde met
**34 passed in 1.91 s**. Bewijs betreft geïsoleerde synthetische
`PHASE4_PROOF`-fixtures; er is geen businessdata geschreven of
productierelease geaccepteerd. Fase 5 is afgerond binnen de generic
ingestion/control-plane-scope. Op dit exitmoment was fase 6 niet gestart; de businessruntime-
gates, aanvullende bronacceptatie en latere productie-/consumercriteria
blijven expliciet vervolgwerk.

### F6-SALES-01 - Start Sales-keten en gecommitte Quality-referenties (12 oktober 2026)

De opdrachtgever heeft opdracht gegeven fase 6 zelfstandig voort te zetten
tot de fase werkelijk gereed is. De exit blijft de volledige SALES-keten:
Bronze, Quality/Reject, Raw Vault, Business Vault, PIT, historisch Gold en
Gold Current. Een succesvolle Quality-proef sluit fase 6 niet af.
Outputacceptatie omvat bedrijfswaarden, sleutels, relaties, multipliciteit,
historie en deletegedrag; groene jobs en gelijke aantallen zijn onvoldoende.
Vergelijking vereist dezelfde bronbytes, leveringsvolgorde en businesscutoff,
met onafhankelijke verwachte uitkomsten naast een Databricks-baseline.

De nieuwe [Sales-extensie](../src/fabric_etl_phase6/quality.py) leest uitsluitend
exacte INGEST- en QUALITY-commits en verifieert targetbinding, context,
recordaantallen en outputfingerprints. De vijf Sales-objecten worden in
metadata-afhankelijkheidsvolgorde verwerkt. ORDERS gebruikt gecommitte
CUSTOMERS, PRODUCTS en EMPLOYEES uit dezelfde levering; RETURNS gebruikt
volgens de werkelijke DQ-metadata PRODUCTS en EMPLOYEES, niet ORDERS.
Vault-relaties naar ORDERS moeten afzonderlijk worden bewezen.
De gepubliceerde fase-5-runtime is niet aangepast. Bronze-proofnamen blijven
bewust `phase5_exit_phase6_sales_*` om aan de bestaande ingestionguard te
voldoen; de Quality-/Reject-proofnamen beginnen met `phase6_sales_*`.

De eerste [proofnotebook](../fabric/items/phase6_sales_quality.Notebook/notebook-content.py)
verwerkt drie onveranderlijke synthetische vijf-objectleveringen: initiele
records, customer-/productwijziging en expliciete soft deletes van de drie
dimensies. Alle vijftien getypeerde Quality-payloads worden tegen afzonderlijk
vastgelegde verwachte waarden gecontroleerd, inclusief decimalen. Dit is
een kleine synthetische proof, niet de oorspronkelijke Contoso-bedrijfsdata
of volledige fase-6-testdekking.

**Publicatie en uitvoering:**

- Sales-wheel 0.1.0 is gepind op SHA-256
  `92e2864a067b8a1a79e32dff1d7af5bebc140c3658fd7953528a115f2cbe4671`.
- [Environmentpublicatie](../deploy/evidence/phase6-sales-quality-runtime-publication.json)
  is `PUBLISHED_READBACK_VERIFIED`, targetVersion
  `2ac99a6d-3e49-4ac4-8ca7-c1ec6c294bb1`; de basiswheel, ingestionwheel en
  Spark-settings zijn byte-identiek behouden.
- [Bundleplan](../deploy/evidence/phase6-sales-quality-v1.plan.json):
  `62ce9df5264d2840acdcff476cc710fe287e9c7a9e3695eda537f5a85f38d1ae`.
  Notebook-ID: `0c31f59c-0bfd-4a14-8200-2a0e3fa58610`.
- Eerste job: `11e0a31c-bd0a-431c-9f83-6110808eb614`.
  [Receipt](../deploy/evidence/phase6-sales-quality-v1-receipt.json) is
  `COMPLETED`, met Fabric-status `Completed`, exitstatus `SUCCEEDED` en
  vrijgegeven owner. De receipt vermeldt start `2026-10-12T05:54:38.047239`
  en einde `2026-10-12T06:20:30.2025649`.
  De inhoudelijke [verificatie](../deploy/evidence/phase6-sales-quality-v1-verification.json)
  is `VERIFIED`: alle drie leveringen, vijftien Quality-uitkomsten,
  fysieke inputidentiteiten, achttien layercommits en onafhankelijke
  verwachte bedrijfswaarden zijn binnen deze proofscope gecontroleerd.
- De aparte retryjob `5a18b1e3-d23c-48a5-8b09-21e1a3da1f9c` is eenmaal
  ingediend met dezelfde gepinde notebook en Environment. De
  [retryreceipt](../deploy/evidence/phase6-sales-quality-v1-retry-receipt.json)
  is duurzaam opgeslagen. Bij de eerste readback was de job `InProgress`,
  gestart `2026-10-12T06:21:21.868514`, zonder failureReason.
  De latere terminale uitkomst en gezamenlijke verificatie staan in
  F6-SALES-02 hieronder.
- De [Sales-receiptverifier](../deploy/verify-phase5-proofs.py) vereist
  drie onderscheiden batches met oplopende UTC-tijden, achttien verschillende
  commits, exacte referentiecommits en een retry zonder extra inserts of
  gewijzigde context. Negatieve tests blokkeren onvolledige en
  success-shaped receipts. Gerichte suite na toevoeging van de Vault-
  historyguards, commit-readback, dependency-order en business-datebinding:
  **90 passed in 2.21 s**; editoranalyse geeft geen fouten
  in de gewijzigde Python-bestanden.

De [Vault-uitwerking](../src/fabric_etl_phase6/vault.py) is lokale,
niet-gepubliceerde vervolgcode. De metadata-specificaties voor zestien Raw
Vault-entiteiten en twee BV-satellieten compileren in lokale contracttests;
PIT kan niet via een generieke satellitefallback worden overgeslagen.
De lokale writer vereist expliciete proofscope, een werkelijk gepersist
proofbatch met dezelfde releasereferentie en load timestamp, en de exacte
QUALITY-/Raw-parentcommitinventaris. De historyreader controleert ontbrekende,
dubbele, vreemde en onvolledige voorafgaande commitinventarissen voordat
historische data gelezen wordt. Deze grenscontroles zijn lokaal getest;
de Spark-transformaties zelf daarmee nog niet.
De writer reconstrueert de getypeerde Quality-projectie vanuit het
authoritative PASSED-target en vergelijkt die volledig met de staged
Vault-payload. Parentmanifesten en hun actuele plus eerdere gecommitte
keys worden gelezen, zodat een relatie niet alleen een geldig commit-ID
maar ook een bestaande parentkey vereist. BV-tenure bindt `current_date()`
aan de gepersiste batchdatum; SQL-literals, gequote identifiers en andere
functienamen worden daarbij niet gewijzigd. Deze deterministische
projectievariant wordt expliciet in de Vault-inputfingerprint opgenomen;
ze is geen impliciete goedkeuring van de oorspronkelijke Databricks-
wall-clockberekening.
Deze code zit nadrukkelijk niet in de gepubliceerde 0.1.0-wheel. Spark-
eventselectie, history-readback, PIT en Gold zijn nog niet live bewezen.
De huidige historische sales-SQL bevat current-only joins; deze mogen niet
zonder temporele beoordeling als historische pariteit worden geaccepteerd.

**Vervolg bij dit checkpoint:** de bestaande retryjob afwachten en beide
receipts gezamenlijk inhoudelijk verifiëren. Vervolgens de volledige gecommitte
Vault/BV/PIT/Gold-keten en aanvullende wijzigings-, reject-, late-arrival-
en herstelgevallen uitvoeren. Recordniveauvergelijking met dezelfde
Databricks-input/output blijft open; er zijn geen Databrickswrites uitgevoerd.

Op de vraag naar extra Spark-capaciteit is bevestigd dat dit mogelijk is
binnen pool- en Fabric-capacitylimieten. De huidige gepinde configuratie
blijft driver/executor 4 cores en 28 GB, dynamic allocation 1-2 executors.
Er is geen compute-upgrade, capacitywijziging of aanvullende grant uitgevoerd.

### F6-SALES-02 - Afzonderlijke Quality-retry inhoudelijk geverifieerd (12 oktober 2026)

**Aanleiding en doel:** aantonen dat de vijf-object-Sales-Quality-keten niet
alleen in een eerste job correct werkt, maar ook bij een afzonderlijke
Fabric-uitvoering dezelfde gecommitte data herkent zonder duplicatie of
wijziging van referentiecontext. Dit toetst reproduceerbaarheid en veilig
herhalen als eigenschappen van een productiewaardige testarchitectuur;
het is geen productie-ingebruikname.

**Uitvoering en waarneming:** de reeds ingediende retryjob
`5a18b1e3-d23c-48a5-8b09-21e1a3da1f9c` is via de bestaande monitor en
[retryreceipt](../deploy/evidence/phase6-sales-quality-v1-retry-receipt.json)
opgehaald, niet opnieuw ingediend. De receipt vermeldt start
`2026-10-12T06:21:21.868514` en einde
`2026-10-12T06:43:25.1784092` UTC. Fabric-status is `Completed`,
receiptstatus `COMPLETED`, exitstatus `SUCCEEDED`; de owner is vrijgegeven.
Runtime, inputfixtures, notebook en Environment bleven dezelfde gepinde
0.1.0-proofvariant als in F6-SALES-01.

**Verificatie:** de bestaande receiptverifier is uitgevoerd met scope
`sales-quality`, de eerste receipt en de retryreceipt via `--retry`.
De [gezamenlijke verificatie](../deploy/evidence/phase6-sales-quality-v1-verification.json)
is duurzaam opgeslagen met `status=VERIFIED`,
`separate_job_retry_verified=true` en beide verschillende job-ID's.
De verifier vergelijkt de volledige receiptinhoud en staat uitsluitend de
verwachte overgang naar geverifieerde bestaande commits en retrycounters toe.
Een aparte samenvatting van de daadwerkelijke retryreceipt bevestigt:

- drie dezelfde gepersiste batches en laadtijden;
- dezelfde fysieke inputidentiteiten en fixturefingerprints;
- dezelfde vijftien getypeerde Quality-uitkomsten, accounting,
  outputtargets en outputfingerprints;
- dezelfde referentiecommit-ID's binnen iedere levering;
- achttien bestaande INGEST-/QUALITY-commits met status `VERIFIED`;
- som van alle Bronze- en Quality/Reject/Filtered-insertcounters: **0**;
- `owner_released=true` en `phase6_complete=false`.

**Interpretatie en beperking:** de drie-leveringenketen tot committed
Sales Quality plus afzonderlijke retry is nu live en inhoudelijk bewezen
binnen de kleine synthetische proofscope. Dit bewijst nog geen Raw Vault,
Business Vault, PIT, historische/current Gold of recordniveaupariteit met
Databricks. De lokale Vault-vervolgcode is niet door deze job uitgevoerd
en blijft buiten de gepubliceerde 0.1.0-wheel. Productieacceptatie blijft
`false` als expliciete niet-verleende claim, niet als verplichting tot een
productie-uitrol.

**Vervolg:** de gecommitte Quality-uitkomsten gebruiken als gecontroleerde
input voor de volledige Sales-Vault/BV/PIT/Gold-keten, met aanvullende
wijzigings-, reject-, late-arrival- en herstelgevallen. Daarna dezelfde
testcontracten en broninputs met het Databricks-referentiespoor vergelijken.
Fase 6 blijft in uitvoering.

### F6-SALES-03 - Gepinde Vault/BV-opvolger en voorbereiding live bewijs (12 oktober 2026)

**Doel en keuze:** de reeds geverifieerde, onveranderlijke Quality-output
wordt hergebruikt voor zestien Raw Vault-entiteiten en twee BV-satellieten.
De nieuwe notebook schrijft geen nieuwe Bronze-/Quality-leveringen en
embedt de eerste succesvolle Quality-receipt in het deploymentplan.
Alle inputs en bestaande historie worden bij gebruik opnieuw tegen
authoritative commits gelezen en op inhoudelijke fingerprints gecontroleerd.

De [Vault-notebook](../fabric/items/phase6_sales_vault.Notebook/notebook-content.py)
doorloopt de werkelijke metadata-parentvolgorde. De
[onafhankelijke verwachte payloads](../src/fabric_etl_phase6/proof.py) leggen
alle achttien businessrijvormen expliciet vast, niet via heruitvoering van
de productiemappings. De verwachte eventaantallen zijn 18, 2 en 3 voor de
drie leveringen: hubs/links worden niet opnieuw toegevoegd, de customer-
en productwijzigingen maken nieuwe events, expliciete deletes maken
customer-/product-/employee-events, ongewijzigde orders/returns/BV niet.
De technische historisatie gebruikt uitsluitend de gepersiste laadtijden.
Halfopen satellite-intervallen worden afgeleid uit append-only events;
eerdere events worden niet gepurged of overschreven.

**Versiebeheer en deployment:** Sales-runtime 0.2.0 is afzonderlijk gebouwd
met SHA-256 `9010fd01a37b3864ae65f0984aeceee283a7544425d604247d25c24568b18934`.
De oorspronkelijke 0.1.0-wheel blijft lokaal onveranderd en beschikbaar.
Het [Vault-bundleplan](../deploy/evidence/phase6-sales-vault-v1.plan.json)
heeft identiteit
`66e5f4fc795786135221910d99fcdb5ee39c944ed9ca5f664184cb87a1b9aab5`.
De gedeelde Environment mag onder exclusieve setup uitsluitend de exact
gepinde 0.1.0-Sales-wheel vervangen door 0.2.0; niet beide versies van
dezelfde distributie tegelijk installeren. Basisframework, ingestionruntime
en Spark-settings blijven op hun bestaande remote bytepins. De oude
Quality-notebook eist nog 0.1.0 en wordt daarom na deze opvolger niet opnieuw
gestart; historische receipts blijven geldig. Heruitvoering van die oude
notebook vereist een expliciet gecontroleerde versieherstelpublicatie.

**Lokale validatie:** 106 gerichte tests slagen in 2.52 s, inclusief
independent payload-/eventverwachtingen, alle Sales-identityparents,
complete historie-inventarissen, harde proofguards, bundlepins, exact
receiptinput en publicatie-/driftgevallen. Dit is geen live Vault-bewijs.
Nieuwe publisher-tests faalden aanvankelijk door een verkeerd geneste
mock-definitie; die is gecorrigeerd naar het daadwerkelijke `decode_parts`-
contract. Daarna bleek de lokale basiswheel/configuratie niet bytegelijk
aan de historisch gepubliceerde remote pins. De remote pins zijn niet
versoepeld of vervangen: de mocktests gebruiken expliciete eigen fixturepins,
terwijl echte publicatie de onveranderde remote pins blijft eisen.

**Status:** gebouwd en gepland; Environmentpublicatie en live Vault-
uitvoering volgen. PIT, Gold, uitgebreidere scenarios en Databricks-
outputvergelijking blijven open en blokkeren volledige fase-6-acceptatie.

### F6-SALES-04 - Vault-publicatie, live start en lokale Spark-validatie (12 oktober 2026)

**Publicatie:** de
[0.2.0-publicatiereceipt](../deploy/evidence/phase6-sales-vault-runtime-publication.json)
is `PUBLISHED_READBACK_VERIFIED`, targetVersion
`f0479b11-d756-4ad4-9fa2-20509bd355c4`. Publicatie liep van
`2026-10-12T06:56:45.7493144Z` tot `2026-10-12T07:02:11.7835155Z`.
Libraries en Spark-settings zijn beide `Success`; de remote basiswheel,
fase-5-ingestionwheel en computeconfiguratie zijn exact behouden.
De 0.1.0-Sales-distributie is gecontroleerd opgevolgd door 0.2.0.

**Deployment en live start:** de
[notebookjournal](../deploy/evidence/phase6-vault-bundle-journal.json)
bevestigt creatie en exportreadback van notebook
`37424361-d6b3-430a-8d51-c5a09087a32c`, naam
`contoso_phase6_sales_vault_dev`, definitionfingerprint
`7e55beac48e7e4073a55dcace12589834cacd652bc2c5ecd3da58a75946d81f3`.
Job `915bc15a-e7bf-4d85-8fa6-13c22357ae36` is eenmaal ingediend met
duurzame [receipt](../deploy/evidence/phase6-sales-vault-v1-receipt.json).
De eerste API-check gaf `NotStarted`, de latere `InProgress`, zonder
failureReason. De lokale monitor stopte vervolgens door een Azure CLI-
tokenacquisitietimeout van 60 seconden. Dit is geen bewijs dat de remote
Spark-job mislukte. Dezelfde job-URL is via dezelfde receipt hervat;
er is geen tweede notebookjob ingediend of output verwijderd.

**Receiptcontract:** een nieuwe `sales-vault`-verifier verlangt alle
achttien entiteiten over drie leveringen, 54 onderscheiden commits, exacte
Quality-/parentprovenance, complete voorafgaande historie, 18/2/3 verwachte
nieuwe events en stabiele outputbindingen/fingerprints. Een aparte retry
moet exact dezelfde toestand opleveren met nul inserts. De negatieve
Vault-/Quality-receipttests slagen gezamenlijk: 30 passed in 2.55 s.

**Lokale Spark-infrastructuur:** een echte lokale Spark-start faalde met
`JAVA_GATEWAY_EXITED`: Java ontbrak en JAVA_HOME was niet ingesteld.
Na deze concrete ontbrekende dependency is een Temurin Java 21 JRE alleen
in de sessieopslag uitgepakt, zonder systeeminstallatie, globale PATH-/
JAVA_HOME-wijziging of repositorydependencywijziging. De download is tegen
de leverancierschecksum geverifieerd:
`d35f31e712f0fcf6ac5a093edc90204fbff22f720ba3950bd09d331d5e621636`.
Lokale PySpark-versie is 4.2.0; lokale Spark-uitkomsten vervangen daarom
niet het bewijs op de gepinde Fabric-runtime 2.0.

**Eerste echte Spark-test en gevonden grensfout:** de volledige metadata-
projectie/eventselectie over drie leveringen voor zestien Raw- en twee
BV-entiteiten, hashdiffs, halfopen intervallen en PIT-pointerselectie
slaagde lokaal. Een tweede negatieve test faalde: een NULL customer-
businesskey werd door de hubprojectie niet geblokkeerd. De werkelijke
hubmapping verklaart die kolom `nullable=true`; vertrouwen op alleen die
mappingguard bleek daarom onvoldoende. De identityguard is in lokale
vervolgcode aangescherpt: iedere identiteitcomponent moet non-NULL zijn,
en STRING-identiteiten mogen niet leeg zijn na trimmen. Deze correctie
wordt niet in de onveranderlijke reeds gepubliceerde 0.2.0-wheel teruggeschreven.
De eerste run eindigde met 1 passed, 1 failed in 503.67 s; de gerichte
heruitvoering, nu ook met historische/current Gold, loopt als afzonderlijke
lokale validatie. De waarschuwing over ontbrekende Arrow-optimalisatie
betreft de standaard PySpark-UDF-uitvoering, niet een andere hashsemantiek.

**PIT-/Gold-vervolgcode, nog niet gepubliceerd:** een expliciete customer-PIT
legt zowel Raw- als BV-event-ID's en eventtijden vast op de gepersiste
as-of-cutoff; ontbrekende/ambigue parents en verkeerde tijdzones blokkeren.
Gold leest elf gepinde metadata-SQL-expressies uitsluitend via geverifieerde
Vault-stateviews. Een expliciete, fingerprinted temporele overlay vervangt
de historische current-only salesjoins door halfopen as-of-joins; customer-
en saleslinetimelines nemen ook BV-/order-only wijzigingen mee.
De oorspronkelijke kandidaatrelease en SQL-checksums blijven onveranderd.
Relationshipwisselingen zonder gemodelleerde effectivity worden expliciet
geblokkeerd in plaats van door een fan-out te worden vermenigvuldigd.
Dit blijft een open scope-uitbreiding voor volledige wijzigingsdekking.
PIT-/Gold-contracttests: 32 passed in 2.58 s; Spark-Goldvalidatie loopt.

De lokale Gold-publicatie-uitwerking bouwt eerst alle immutable targets,
verifieert daarna het complete Gold-commitmanifest en schakelt pas dan
een geisoleerde `PHASE6_SALES_MART`-pointer. SQL-/consumeracceptatie wordt
expliciet niet geclaimd. Een oudere retry mag de actieve nieuwste cutoff
niet terugzetten. Deze publicatiecode is nog niet live bewezen.

### F6-SALES-05 - Exacte Gold-verwachtingen en gevonden runtimegrenzen

**Tweede echte Spark-run:** 1 passed, 1 failed in 592.69 s. De negatieve
NULL-/conflict-/terugdateertest slaagde na de identityguardcorrectie.
De ketentest bereikte Gold en stopte expliciet bij `GH_DIM_CUSTOMER`:
de geregistreerde Gold-schema's bevatten runtime-auditvelden die de
geregistreerde business-SQL niet selecteert. De lokale vervolgcode
onderscheidt nu exact de businesskolommen en het toegestane technische
contract: historisch `_batch_id`/`_loaded_at`; current
`_as_of_delivery_id`/`_as_of_timestamp`/`_batch_id`.
De schrijver vult deze uitsluitend uit de gepersiste batch, niet uit
een nieuwe wandkloktijd. Onbekende technische velden blijven een fout.
Deze correctie verandert geen oorspronkelijke kandidaatrelease of
reeds gepubliceerde 0.2.0-wheel.

**Onafhankelijke outputverwachtingen:** de lokale
[proofmodule](../src/fabric_etl_phase6/proof.py) beschrijft nu alle
businesskolommen en waarden voor vijf historische en zes current
Gold-entiteiten, inclusief alle 4.018 kalenderdagen van 2020 tot en met
2030. De [Spark-test](../tests/test_fabric_sales_spark.py) vergelijkt
beide richtingen met `exceptAll`, zodat ontbrekende waarden, extra
records en verkeerde multipliciteit niet achter gelijke tellingen
verdwijnen. Een afzonderlijke test bouwt Vault-toestanden uit onafhankelijke
verwachtingen; daarmee hoeft de al geteste, relatief trage UDF-projectie
niet bij ieder Gold-experiment opnieuw te draaien. Dit is uitdrukkelijk
geen nieuw bewijs voor de ingestie- of Vaultschrijver.

**Windows-/tijdzonegrenzen in de test:** de eerste uitvoering van deze
nieuwe Gold-test faalde na 67.82 s bij het construeren van de verwachte
open einddatum: Windows `mktime` accepteerde de naive timestamp in jaar
9999 niet. Het expliciet aanleveren als UTC voorkomt deze lokale
conversiegrens. Daarna toonde de vergelijking een afwijking van twee uur:
de onafhankelijke teststates gebruikten nog naive Python-datetimes;
PySpark interpreteerde die volgens de lokale tijdzone, ondanks een UTC
SQL-session. De fixtures leveren nu expliciete UTC-tijden. De writer
geeft ook de uit Spark verzamelde UTC-naive batchtijd expliciet als UTC
door aan Gold-auditkolommen en het cutoffreceipt. Er wordt geen verschil
weggenormaliseerd in de daadwerkelijke businessvergelijking.

**Productpointercontract:** 23 tests passed in 0.67 s; onafhankelijke
verwachtingen, Gold- en publicatietests gezamenlijk 39 passed in 1.06 s
voor de laatste Spark-fixturecorrecties. Twee eerdere mockfailures bleken
te zoeken naar gewone SQL-stringliterals, terwijl het bestaande
controlcontract UTF-8-hex via `decode(unhex(...),'UTF-8')` gebruikt.
De tests volgen nu de bestaande `literal()`-helper. De publicatiecode
maakt de UTC-naive Spark-cutoff expliciet UTC voordat zij SQL genereert.
SQL-endpoint-/consumerbewijs blijft `NOT_TESTED`/`PROOF_ONLY_NOT_PUBLISHED`.

**Actuele grens:** de live 0.2.0-Vaultjob en de gecorrigeerde lokale
exacte Gold-test zijn nog niet als geslaagde eindbewijzen geregistreerd.
PIT, Gold en productpointer staan lokaal, niet in de gepubliceerde
0.2.0-distributie. Volledige fase 6 is daarom nog open.

### F6-SALES-06 - Eerste live Vault bewezen; Gold-opvolger voorbereid

**Live Vault-resultaat:** job `915bc15a-e7bf-4d85-8fa6-13c22357ae36`
is `Completed`, zonder failureReason, van
`2026-10-12T07:05:34.532271Z` tot `2026-10-12T07:44:49.8858943Z`.
De oorspronkelijke monitor-timeout was dus inderdaad lokaal; de
hervatting bewaakte de oorspronkelijke job en diende niets opnieuw in.
De [Vault-verificatie](../deploy/evidence/phase6-sales-vault-v1-verification.json)
is `VERIFIED`: achttien Raw-/BV-entiteiten, drie leveringen, 54 verschillende
commits, onafhankelijke volledige businesswaarden, exacte Quality-/
parentinputs, volledige append-only historie en 18/2/3 nieuwe events.
`separate_job_retry_verified=false` blijft waar totdat het aparte
retryreceipt gezamenlijk is geverifieerd.

**Aparte retry:** job `d06a3f32-8246-4601-95ba-670faf851620` is
eenmaal ingediend met eigen
[retryreceipt](../deploy/evidence/phase6-sales-vault-v1-retry-receipt.json).
Een eerste lokale CLI-aanroep werd afgewezen wegens niet-bestaande
monitoropties; zij bereikte de API niet en startte geen Spark-job.
Na raadpleging van de bestaande CLI-help is de aanroep uitgevoerd met
`--timeout-seconds 5400`. De Environment blijft tijdens deze uitvoering
op de onveranderlijke Sales-versie 0.2.0; er wordt geen Gold-runtime
gepubliceerd terwijl de retry actief is.

**Exacte lokale Gold-vergelijking:** na de UTC-fixturecorrectie passeerde
de Spark-test de beide `exceptAll`-vergelijkingen voor alle elf volledige
businessoutputs, inclusief de 4.018 kalenderrecords. Ook de tweede
Gold-projectie met order-only en BV-only historie bereikte de readback.
De totale test faalde alsnog na 456.02 s toen Windows/PySpark bij
`collect()` de open einddatum in jaar 9999 via `fromtimestamp` probeerde
te converteren (`OSError: [Errno 22] Invalid argument`).
De onderzochte historische grenzen worden nu binnen Spark als strings
geselecteerd; alle businesskolommen en typed timestamps waren daarvoor
al exact binnen Spark vergeleken. Onnodige jaar-9999-collecties zijn ook
uit de oorspronkelijke ketentest verwijderd. De volledige nieuwe
Gold-/multistream-/PIT-test wordt opnieuw uitgevoerd; een gedeeltelijk
gepasseerde test wordt niet als een geslaagde totale run geboekt.

**Lokale opvolger 0.3.0:** PIT-, Gold-, proof-publicatie- en identity-
correcties zijn in een nieuwe wheel gebouwd met bestaande `pip wheel`,
zonder dependency-installaties of computeaanpassingen. Een eerste
bouwpoging via `python -m build` mislukte doordat die module niet aanwezig
was; de bestaande pip/setuptools-route bouwde wel succesvol.
SHA-256: `fe1156550d249b4302d01903c5af86e8207d3e560fab964a278a0789ade5dd05`.
De reeds gepubliceerde 0.2.0-wheel is opnieuw gehasht en blijft exact
`9010fd01a37b3864ae65f0984aeceee283a7544425d604247d25c24568b18934`.

De [Gold-bundle](../fabric/phase6-sales-gold-bundle.json) en
[notebook](../fabric/items/phase6_sales_gold.Notebook/notebook-content.py)
pinnen de geslaagde live Vault-input en de oorspronkelijke elf
metadata-SQL-bestanden met hun releasechecksums.
Het [Gold-plan](../deploy/evidence/phase6-sales-gold-v1.plan.json)
is `PLANNED_NOT_DEPLOYED`, plan-ID
`85e332b9531a4f0813bd0a861cea3df41ace27b86aa9325c6e83f43357aa0aa1`.
De publisher kent uitsluitend de gecontroleerde overgang van de
gepinnde 0.2.0-Vaultwheel naar deze 0.3.0-Goldwheel en behoudt alle
andere remote runtime-/computebytes. Een actieve job blokkeert publicatie.

**Gold-proofcontract:** per levering schrijft de notebook een PIT-
snapshot met exacte Raw-/BV-eventpointerreadback en een volledige
elf-entiteiten-Goldbuild. Zij vergelijkt alle businessrecords en de
batchgebonden auditwaarden voordat de geisoleerde productpointer
wordt veranderd. In levering 2 volgt een bewuste onderbreking na
`GH_DIM_CUSTOMER`: er mag nog geen compleet Gold-commit bestaan en de
vorige goede pointer moet onveranderd blijven. Daarna volgt herbouw
met immutable readback en gecontroleerde activatie. Een aparte oudere
retry mag de actieve laatste cutoff niet terugzetten.
De nieuwe `sales-gold`-verifier verlangt de exacte Quality- en Vault-
inputreceipts, alle elf outputs per levering, zes PIT-/Gold-commits,
cutoffs, querypins, herleidbare productidentiteiten, werkelijk
onderbrekings-/herstelbewijs en nul extra inserts in de aparte retry.

**Validatie:** 102 Vault-/PIT-/Gold-/proof-/pointercontracttests passed
in 3.71 s; Gold- en Vault-receipttests gezamenlijk 45 passed in
12.77 s; de gecombineerde opvolger-/bundle-/Gold-/PIT-/proof-/pointer-
suite 127 passed in 17.35 s. Gewijzigde Pythonbestanden hebben bij de
editorcontrole geen diagnostics. Dit zijn lokale contractbewijzen,
geen live PIT-/Gold-/consumerbewijs.

**Open:** aparte live Vault-retry, volledige lokale Spark-heruitvoering,
Gold-Environmentpublicatie/deployment/live run/retry/herstel, uitgebreidere
meerrij-/sleutel-/reject-/late-arrival-/relationshipscenarios en een
gelijk-input Databricks/Fabric-outputvergelijking. De productiewaardige
testdoelstelling blijft gelden zonder productiecutover te verlangen.
Volledige fase-6-acceptatie is nog niet bereikt.

### F6-SALES-07 - Volledige lokale Gold-/historie-/PIT-test geslaagd

De gerichte echte Spark-heruitvoering
`test_real_spark_gold_full_business_outputs_and_multistream_history_from_independent_states`
is volledig geslaagd: **1 passed, 2 deselected in 469.40 s**.
De twee overige tests waren bewust niet geselecteerd; dit resultaat is
geen nieuwe uitvoering van de volledige ingestie-/Vaultketentest.

Het resultaat bewijst binnen lokale PySpark 4.2.0:

- alle businesskolommen en exacte recordmultipliciteit van elf
  Gold-entiteiten, vergeleken in beide richtingen met onafhankelijke
  verwachtingen, inclusief alle 4.018 kalenderrecords;
- historische salesversies bij een order-only statuswijziging,
  aaneensluitende halfopen grenzen en precies de juiste currentmarkering;
- customerhistorie bij een BV-only wijziging, zonder een nieuw
  Raw-customer-event te vereisen;
- PIT-selectie van de juiste Raw- en BV-event-ID's op de cutoff en
  expliciete blokkering van een orphan-satellite.

De Windows-readbackcorrectie behoudt de oorspronkelijke timestampwaarden:
alle volledige typed outputs worden binnen Spark vergeleken; alleen
de aanvullende gerichte grensasserties lezen de timestamps als strings
uit. De waarschuwing over een toekomstige `toJSON`-returntypewijziging
veranderde de huidige testuitvoering niet.

**Volgende afhankelijkheid:** de aparte live Vault-retry moet eerst
terminaal en gezamenlijk geverifieerd zijn voordat de gedeelde Fabric
Environment naar de voorbereide 0.3.0-Goldruntime mag overgaan.
Er is nog geen live PIT-/Gold-/herstel-/consumerbewijs toegevoegd.
De lokale succesvolle test vervangt niet de vereiste uitvoering op
Fabric-runtime 2.0 of de Databricks-outputvergelijking; fase 6 blijft open.

### F6-SALES-08 - Aparte live Vault-retry bewezen en Gold-publicatie gestart

De aparte job `d06a3f32-8246-4601-95ba-670faf851620` eindigde
`Completed`, zonder failureReason, van `2026-10-12T07:47:07.2994446Z`
tot `2026-10-12T08:19:10.361086Z`. De gezamenlijke
[verificatie](../deploy/evidence/phase6-sales-vault-v1-verification.json)
is `VERIFIED` met `separate_job_retry_verified=true`.
Alle 54 entity-/deliverycommits zijn `VERIFIED`; de retry voegde nul
records toe en behield exact dezelfde outputs, historie-inventarissen,
Quality-/parentinputs en fingerprints. Dit sluit het afzonderlijke
committed-Vault/BV-fixturebewijs af, niet de volledige fase.

Pas na deze terminale retry en gezamenlijke verificatie is de
gecontroleerde 0.3.0-Environmentpublicatie gestart. De
[publicatiereceipt](../deploy/evidence/phase6-sales-gold-runtime-publication.json)
registreert de voortgang; basiswheel, fase-5-wheel en remote
computeconfiguratie moeten bytegelijk blijven. Publicatiesucces en
live Gold worden nog niet geclaimd.

**Laatste preflightcorrectie:** de bestaande foundationmigratie maakt
`gold.historical` en `gold.release_data`, maar niet `gold.current`.
De Gold-proofnotebook maakt daarom onder geclaimd eigenaarschap de
exact gebonden `gold.current`-namespace aan voordat immutable
`phase6_gc_*`-outputs worden geschreven. Bestaande foundationmigraties
en hun checksums worden niet achteraf aangepast; business-release-
tabellen en consumerpointers blijven buiten deze proofscope.
Deze notebook-only correctie wijzigt de 0.3.0-wheel niet.

De gerichte bundle-/receipt-/publishervalidatie slaagt: 63 passed in
11.74 s. Het opnieuw plannen naar de oorspronkelijke planlocatie is
terecht geweigerd (`Refusing to overwrite a different phase-five plan`).
De oorspronkelijke plan blijft bewaard. Het
[opvolgende notebookplan](../deploy/evidence/phase6-sales-gold-v2.plan.json)
heeft ID `fb8a0aaaaf85f8d597a1ffdcc78051740790be2970331b6dbfa24d51d5a2ffa2`
en dezelfde exacte wheelpin. De lopende Environmentpublicatie verwijst
nog naar het oorspronkelijke plan; dat plan beschrijft dezelfde
librarybytes. Deployment moet het opvolgende notebookplan gebruiken,
zodat de namespacecorrectie in de geexporteerde notebookdefinitie zit.

**Volgende stappen:** terminale publicatie met library-/settingsreadback,
deployment/readback van het opvolgende notebookplan, daarna eenmalige
live Gold-uitvoering met eigen duurzame jobreceipt. De notebook zal
PIT, alle elf exacte outputs, auditwaarden en bewuste
onderbreking/herstel toetsen; een aparte Gold-retry volgt daarna.

### F6-SALES-09 - Gold-runtime gepubliceerd, notebook geverifieerd en live proof gestart

**Environmentpublicatie:** de
[publicatiereceipt](../deploy/evidence/phase6-sales-gold-runtime-publication.json)
is `PUBLISHED_READBACK_VERIFIED`, targetVersion
`6922adfb-3f96-41bc-990e-6346f1a3a980`. Publicatie liep van
`2026-10-12T08:20:39.3806647Z` tot `2026-10-12T08:25:03.7718447Z`.
Libraries en Spark-settings zijn beide `Success`. De Sales-wheel is
exact de gepinde 0.3.0-distributie; basiswheel, fase-5-ingestionwheel en
computeconfiguratie zijn bytegelijk behouden. De 0.2.0-Sales-runtime is
gecontroleerd opgevolgd; de oude Vault-notebook wordt niet opnieuw
uitgevoerd tegen deze nieuwe runtime.

**Deployment:** het
[Gold-notebookjournal](../deploy/evidence/phase6-gold-bundle-journal.json)
registreert notebook `2e56d882-1b62-4cc8-959f-2b240c75bc4c`, naam
`contoso_phase6_sales_gold_dev`, als `created` en `verified=true`.
De deployment gebruikte het opvolgende plan
`fb8a0aaaaf85f8d597a1ffdcc78051740790be2970331b6dbfa24d51d5a2ffa2`,
inclusief de gecontroleerde current-namespaceaanmaak.
Definitionfingerprint:
`d4ab9578ed5aa568836822a017111e592076c7ce705316f4a954542545c4210a`.
De geexporteerde notebookdefinitie is teruggelezen en geverifieerd.

**Eenmalige live start:** job
`f6e44e1f-d76e-40f0-ba96-dade531a0708` is ingediend met duurzame
[Gold-receipt](../deploy/evidence/phase6-sales-gold-v1-receipt.json).
De monitor bewaakt dezezelfde job met een timeout van 7.200 seconden;
een eventuele lokale monitorfout vereist hervatten/reconciliatie van
deze receipt, niet opnieuw indienen.

De notebook gebruikt uitsluitend de gepinde geslaagde Vault-input en
checksummed metadata-SQL. Zij moet drie PIT-snapshots, alle elf
historische/current Gold-outputs per levering, exacte onafhankelijke
businesswaarden en batch-auditvelden bewijzen. Levering 2 bevat een
bewuste immutable-writeonderbreking, behoud van de vorige goede
proof-productpointer en herstel naar een compleet Gold-commit.

**Statusgrens:** publicatie en deployment zijn bewezen; de live
PIT-/Gold-/hersteljob is gestart, niet als geslaagd beoordeeld.
Pas het terminale receipt en de exacte `sales-gold`-verifier kunnen
dit uitvoeringsbewijs opleveren. Een aparte Gold-retry, uitgebreidere
scenarios en de Databricks/Fabric-outputvergelijking blijven nodig.
Er is geen business-consumerpublicatie of volledige fase-6-acceptatie.

### F6-SALES-10 - Eerste Gold-run gefaald, snapshotdiagnose en gerichte correctie

**Werkelijke remote fout:** job `f6e44e1f-d76e-40f0-ba96-dade531a0708`
is `Failed`, van `2026-10-12T08:29:18.2087093Z` tot
`2026-10-12T08:33:58.0947066Z`. De oorspronkelijke
[receipt](../deploy/evidence/phase6-sales-gold-v1-receipt.json) blijft
ongewijzigd als foutbewijs. Job Instance meldt uitsluitend
`System_Cancelled_Session_Statements_Failed`, niet de oorzaak.
De terminale Spark-session is `30c415a0-f5cb-4da3-bfa8-53394b04a739`,
application `application_1791793817034_0001`, runtime 2.0.
Dit is een echte uitvoeringsfout, niet een lokale monitortimeout.

**Diagnosepad:** de eerste losse `az rest`-inventoryaanroep gebruikte
niet expliciet de bestaande Fabric-tenant en gaf `UserNotLicensed`.
Met de bestaande tenant-scoped tokenroute lukte de read-only
sessioninventory wel; er zijn geen rechten, licenties of accounts
veranderd. Het gedocumenteerde notebook-jobendpoint met `beta=True`
leverde de execution-snapshot-, Spark-UI- en driverloglinks.
De driverloglink leverde in deze HTTP-context slechts Spark-UI-HTML;
de geteste publieke statement-/snapshotroutes gaven `EntityNotFound`.
Geen van deze antwoorden is als runtimeoorzaak opgevat.
De bestaande ingelogde Fabric-browser kon uiteindelijk de
[execution snapshot](https://app.fabric.microsoft.com/groups/4be37544-10b9-450d-80f0-f9b3d793b83b/synapsenotebooks/2e56d882-1b62-4cc8-959f-2b240c75bc4c/snapshots/f6e44e1f-d76e-40f0-ba96-dade531a0708?experience=power-bi)
read-only openen.

**Exacte fout:** `AnalysisException`,
`[UNRESOLVED_COLUMN.WITH_SUGGESTION] ... project_id cannot be resolved`,
SQLSTATE `42703`, in de query naar het bestaande Gold-commit.
De recovery-preflightquery gebruikte
`project_id ... AND batch_id ... AND layer ... AND entity_id ... AND status ...`,
terwijl het geregistreerde `commit_manifest` geen `project_id`-kolom heeft.
De bestaande context wordt al via de projectgebonden controltabel en
de geverifieerde proofbatch bepaald; de query gebruikt nu uitsluitend
de werkelijke manifestkolommen `batch_id`, `layer`, `entity_id`, `status`.
Er is geen fout weggecatcht, fallbackcommit gemaakt of output verwijderd.
De fouttrad op na de eerste PIT-write/readback en voor de eerste
Goldbuild; de nieuwe uitvoering moet die bestaande PIT-output exact
verifieren in plaats van dupliceren.

**Correctie en regressie:** uitsluitend de notebookquery is aangepast;
de gepubliceerde 0.3.0-wheel en Environment blijven bytegelijk.
Een nieuwe regressietest evalueert de daadwerkelijke notebook-
predicate en eist exact de geregistreerde manifestkolommen, zonder
`project_id`. Bundle-/Gold-receipttests: 53 passed in 6.78 s.
Het [nieuwe plan](../deploy/evidence/phase6-sales-gold-v3.plan.json)
heeft ID `20cd8db6093e972a26204b6da5181d3259b25ec1aa773bba9cfa3fd4c3f1999c`.

**Ownership en deployment:** een eerste deployment met een leeg nieuw
journal werd terecht geweigerd: `Existing item is not owned by this
deployment journal`. De ownershipguard is niet versoepeld. Het
oorspronkelijke journal is bewaard en gekopieerd naar een afzonderlijk
[opvolgend ownershipjournal](../deploy/evidence/phase6-gold-v3-owned-bundle-journal.json).
Daarmee is dezelfde aantoonbaar beheerde notebook gecontroleerd
bijgewerkt en de export geverifieerd. Nieuwe definitionfingerprint:
`0398346423d01cf74cfe7554ea5b7f4433b3c83f5b18fd5f619d2e0a56593316`.

**Gecorrigeerde uitvoering:** pas na de bevestigde terminale oude job
is een nieuwe job `da26810a-a9bc-4170-826d-03e6963eee51` gestart met
eigen [receipt](../deploy/evidence/phase6-sales-gold-v2-receipt.json).
Dit is een gewijzigde-hersteluitvoering, nog niet het afzonderlijke
succes-na-succes retrybewijs. De oorspronkelijke foutreceipt, plannen
en deploymentjournal blijven beschikbaar. Live Gold-succes en volledige
fase-6-acceptatie blijven onbewezen totdat de nieuwe job terminal is
en de exacte verifier slaagt.

### F6-SALES-11 - Productregistratie-SQL gefaald; immutable runtimepatch 0.3.1

**Tweede live Gold-fout:** job `da26810a-a9bc-4170-826d-03e6963eee51`
eindigde `Failed`, van `2026-10-12T08:46:05.9813655Z` tot
`2026-10-12T08:56:57.799051Z`. Het
[foutreceipt](../deploy/evidence/phase6-sales-gold-v2-receipt.json)
blijft bewaard. De
[execution snapshot](https://app.fabric.microsoft.com/groups/4be37544-10b9-450d-80f0-f9b3d793b83b/synapsenotebooks/2e56d882-1b62-4cc8-959f-2b240c75bc4c/snapshots/da26810a-a9bc-4170-826d-03e6963eee51?experience=power-bi)
toont een andere concrete fout dan de eerste run:
`AnalysisException`, `INVALID_INLINE_TABLE.CANNOT_EVALUATE_EXPRESSION_IN_INLINE_TABLE`,
SQLSTATE `42000`: de expressie `decode(unhex(636f6e746f736f), UTF-8)`
kan niet worden geevalueerd in een inline table.

**Oorzaak en prior art:** de nieuwe productrelease-/pointerregistratie
gebruikte `INSERT ... VALUES` met de bestaande veilige `literal()`-
expressies. De bestaande controlwriters gebruiken juist `INSERT ...
SELECT` voor dergelijke expressies. Lokale mocktests controleerden
registratievolgorde/readback, maar evalueerden deze INSERT-expressies
niet daadwerkelijk in Spark. De twee nieuwe INSERTs volgen nu de
bestaande SELECT-aanpak; encoded literals blijven behouden.
De eerste pointerinsert typeert NULL expliciet als STRING en generatie
1 als BIGINT. Conditional UPDATE en alle immutable output-/manifest-
checks blijven ongewijzigd. Er worden geen goede outputs of commits
verwijderd: de volgende uitvoering moet bestaande writes opnieuw
exact verifieren.

**Nieuwe runtime, geen overschrijving:** doordat deze correctie in de
runtime zit, is versie 0.3.1 gebouwd in plaats van de gepubliceerde
0.3.0-wheel te vervangen. SHA-256 van 0.3.1:
`c1d0f7385849a2f4508f2273f9d01028dd20391f5868f397185c61fa50c98423`.
De oude 0.3.0-wheel blijft exact
`fe1156550d249b4302d01903c5af86e8207d3e560fab964a278a0789ade5dd05`.
De [recoverybundle](../fabric/phase6-sales-gold-recovery-bundle.json)
pint de patch afzonderlijk; de publisher accepteert daarvoor alleen
de exacte gepubliceerde 0.3.0-voorganger of een bytegelijke reeds
gepubliceerde 0.3.1 voor hervatting. Andere versies, gewijzigde
voorgangerbytes en gelijktijdige oude/nieuwe wheels worden geblokkeerd.

**Validatie:** 87 publicatie-/receipt-/bundletests passed in 9.83 s.
Een nieuwe echte Spark-test voert de SELECT-expressies uit die
`publish_gold()` daadwerkelijk genereert voor productrelease en
eerste pointer, inclusief de gecodeerde project-/productliterals:
1 passed, 3 deselected in 8.69 s. Dat bewijst expressie-evaluatie,
niet Delta-DML/readback op Fabric. Vijf extra patchsuccessioncases
testen behoud van overige runtimes, exacte hervatting en drie
driftblokkades; de bundel-/publishersuite: 39 passed in 1.91 s.

Het [patchplan](../deploy/evidence/phase6-sales-gold-v4.plan.json)
heeft ID `213a5a89839ffab116c856a5e5f5b35db216624016284207b166502a5218c318`.
De notebookversie wordt bij planning exact aan de librarypin gebonden;
de nieuwe live verifier vereist 0.3.1 en blijft volledig fail-closed.
De oorspronkelijke plannen/receipts worden niet herschreven.

**Volgende stap:** na de terminale gefaalde job is de exclusieve
0.3.1-publicatie gestart met
[eigen receipt](../deploy/evidence/phase6-sales-gold031-runtime-publication.json).
Na succesvolle publicatie/readback moet het patchplan via een kopie
van het bestaande ownershipjournal worden gedeployd en een nieuwe
hersteljob met eigen receipt worden gestart. Publicatie is nog niet
als voltooid geboekt; live Gold-/pointer-/herstelacceptatie en fase 6
blijven open.

### F6-SALES-12 - Runtimepatch gepubliceerd en derde Gold-uitvoering gestart

De [0.3.1-publicatiereceipt](../deploy/evidence/phase6-sales-gold031-runtime-publication.json)
is `PUBLISHED_READBACK_VERIFIED`, targetVersion
`4cef4452-5e5f-41f4-8776-5294985dfc3c`.
Publicatie liep van `2026-10-12T09:00:14.5283905Z` tot
`2026-10-12T09:04:19.4627838Z`; libraries en Spark-settings zijn
beide `Success`. Basiswheel, fase-5-ingestionwheel en computeconfiguratie
blijven bytegelijk aan de bestaande remote pins.

Het [opvolgende deploymentjournal](../deploy/evidence/phase6-gold-v4-owned-bundle-journal.json)
is gestart vanuit een kopie van het vorige bewezen ownershipjournal;
de oude journals blijven bewaard. Dezelfde notebook
`2e56d882-1b62-4cc8-959f-2b240c75bc4c` is bijgewerkt en de export
is geverifieerd tegen patchplan
`213a5a89839ffab116c856a5e5f5b35db216624016284207b166502a5218c318`.
Definitionfingerprint:
`af0209b04b13433a9bbc8fd76aa5dbe64511cadc2d8cdb405c535fce86d5bbdc`.
De notebook verlangt expliciet de gepubliceerde versie 0.3.1.

Een nieuwe live hersteljob `bd24d09f-f7f9-409a-a0f0-05c6e1fb2a5b`
is eenmaal ingediend met
[eigen receipt](../deploy/evidence/phase6-sales-gold-v3-receipt.json).
De monitor bewaakt deze job; de twee eerdere foutreceipts worden
niet hergebruikt of overschreven. Bestaande immutable PIT-/Goldoutputs
worden opnieuw exact getoetst in plaats van verwijderd.

**Status:** patchpublicatie en notebookdeployment zijn bewezen.
De derde Gold-uitvoering is gestart, niet als geslaagd beoordeeld.
Na terminal succes volgt de exacte `sales-gold`-verificatie met de
Quality- en Vault-inputreceipts, daarna de aparte succes-na-succes
retry. Volledige fase 6 en Databricks-outputgelijkheid blijven open.

### F6-SALES-13 - Live PIT/Gold en bewuste onderbreking/herstel geverifieerd

Job `bd24d09f-f7f9-409a-a0f0-05c6e1fb2a5b` is `Completed`,
zonder failureReason, van `2026-10-12T09:07:24.4520348Z` tot
`2026-10-12T09:33:18.9517218Z`, met Sales-runtime 0.3.1.
Het [terminale receipt](../deploy/evidence/phase6-sales-gold-v3-receipt.json)
is door de exacte verifier getoetst met de oorspronkelijke geslaagde
Quality- en Vault-inputreceipts. De
[Gold-verificatie](../deploy/evidence/phase6-sales-gold-v3-verification.json)
is `VERIFIED`; `separate_job_retry_verified=false` blijft expliciet
staan totdat een andere succesvolle job gezamenlijk is geverifieerd.

**Live bewezen scope:** drie customer-PIT-snapshots met correcte
committed Raw-/BV-eventpointers en cutoff, zes verschillende
PIT-/Gold-deliverycommits, alle elf businessoutputs per levering met
exacte waarden/multipliciteit, volledige 4.018-recordkalender,
historische/deleted dimensionversies en de batchgebonden Gold-
auditwaarden. Productrelease-identiteiten, querychecksums en volledige
parent-/historycommitinventarissen sluiten aan op de gepinde inputs.
De eerste levering herverifieerde al aanwezige PIT-/Goldoutputs uit
de eerdere gefaalde uitvoering: nul nieuwe Gold-records, bestaande
PIT-/Gold-commits `VERIFIED`, en daarna de eerste succesvolle
proof-productpointeractivatie. Er is niets verwijderd om de test
groen te maken.

**Werkelijke bewuste onderbreking en herstel:** bij levering 2,
batch `035cffd2-9ee0-51db-9162-129bbc0a2ce7`, trad de gevraagde
onderbreking op na immutable `GH_DIM_CUSTOMER`. Readback bewees
`incomplete_commit_absent=true` en `active_pointer_unchanged=true`.
De vorige goede release bleef
`b4f8cd69444a29191ef0ec30f5d83d51dd53911e4a0d8a087e3e54da565b1db7`.
Dezelfde delivery is vervolgens volledig herbouwd en hersteld naar
Gold-commit
`b9eb9e1d7e796d170b02031ac0085a46887561aba07f425ac0fb1dc6392f6a7c`
en release
`a1eeb784ec2a099a44be83aae26a9404e68b4590cec2f26dd2bd33706b983ac9`.
Alle drie complete releases zijn na exacte outputcontrole
geactiveerd binnen het geisoleerde proofproduct, met generations 1/2/3.
Dit is geen business-consumerpublicatie.

**Aparte succes-na-succes retry gestart:** nieuwe job
`ccea86cd-247a-46f4-9f71-a0d46ff1f7d1`, met
[eigen retryreceipt](../deploy/evidence/phase6-sales-gold-v3-retry-receipt.json),
voert dezelfde gepinde notebook/runtime opnieuw uit. Zij moet nul
extra PIT-/Goldinserts, identieke manifesten/waarden/auditcontext en
het behoud van de nieuwste actieve pointer bewijzen: leveringen
1/2 mogen de cutoff niet terugzetten, levering 3 moet dezelfde
pointer verifieren. De actieve Environment wordt niet gewijzigd.

**Acceptatiegrens:** het volledige drie-leveringen SALES-fixturepad
is nu live tot en met PIT/Gold en controlled recovery bewezen.
De aparte Gold-retry is nog niet terminaal. Uitgebreidere meerrij-/
sleutel-/reject-/late-arrival-/effectivityscenarios, SQL-/consumerchecks
en gelijk-input Databricks/Fabric-outputvergelijking zijn nog geen
geaccepteerde eindbewijzen. Fase 6 blijft daarom open.

### F6-SALES-14 - Aparte live Gold-retry geverifieerd; parallelle referentieafstemming

De aparte retryjob `ccea86cd-247a-46f4-9f71-a0d46ff1f7d1`
is `Completed`, zonder failureReason, van
`2026-10-12T09:33:46.4209228Z` tot `2026-10-12T09:54:52.5851462Z`.
Het [retryreceipt](../deploy/evidence/phase6-sales-gold-v3-retry-receipt.json)
is gezamenlijk met de geslaagde herstelrun en de exacte Quality-/
Vault-inputs getoetst. De
[Gold-verificatie](../deploy/evidence/phase6-sales-gold-v3-verification.json)
is `VERIFIED`, nu met `separate_job_retry_verified=true`.

**Aparte-jobbewijs:** nul extra inserts voor alle drie PIT-snapshots
en alle 33 Gold-outputbindings; alle zes PIT-/Gold-commits `VERIFIED`;
exact dezelfde outputs, businesswaarden, recordmultipliciteit,
querypins, parent-/historyinventarissen, release-identiteiten en
batch-auditcontext. Leveringen 1/2 zijn `VERIFIED_SUPERSEDED`, zonder
de nieuwste actieve release terug te zetten. Levering 3 is `VERIFIED`
tegen dezelfde actieve release; pointergeneration blijft 3.
De retry voert de eerdere bewuste onderbreking niet opnieuw uit:
`recovery.attempted=false`, terwijl het oorspronkelijke daadwerkelijke
onderbrekings-/herstelbewijs in het eerste succesvolle receipt blijft
staan. De eigenaar is vrijgegeven.

Hiermee is het afzonderlijke drie-leveringen-fixturebewijs door
Bronze, committed Quality, Raw Vault, BV, PIT en historische/current
Gold inclusief afzonderlijke retries en Gold-writeherstel rond.
Dit is geen acceptatie van uitgebreidere scenario's of platformgelijkheid.

**Mens-AI-afstemming:** de gebruiker stelde op 12 oktober voor nu
een tweede sessie in het Databricks-project te openen, zodat beide
AI-sessies het referentiebewijs kunnen afstemmen. Dit is aanbevolen
nu het Fabric-fixturepad tot Gold aantoonbaar werkt.
De voorgestelde startopdracht verlangt eerst read-only inventarisatie,
een gezamenlijk onveranderlijk-input-/leveringsvolgorde-/businessregel-/
cutoffcontract en gescheiden schrijvers per omgeving.
Vergelijking moet volledige businesswaarden, sleutels, relaties,
multipliciteit, historie, deletes, rejects en herstelgedrag omvatten;
platformhashes en technische tijden mogen semantisch worden gemapt,
maar verschillen mogen niet stil worden weggefilterd.
Onafhankelijke verwachtingen blijven nodig naast platformvergelijking.

Een sessie-inventaris toont een bestaande oudere Databricks-projectsessie,
maar nog geen nieuw aangewezen referentietestsessie. Er is daarom
geen oude sessie stilzwijgend herbestemd en geen Databricks-uitvoering
of write gestart. Zodra de gebruiker de bedoelde sessie heeft geopend
en herkenbaar gemaakt, kan gerichte sessieafstemming volgen.

**Resterende fase-6-scope:** uitgebreide meerrij-/sleutel-/reject-/
late-arrival-/effectivityscenarios, overeengekomen SQL-/consumerchecks
en daadwerkelijke gelijk-input Databricks/Fabric-outputvergelijking.
De productiewaardige testdoelstelling vereist geen productiecutover.
Volledige fase-6-acceptatie blijft open.

### F6-COLLAB-02 - Eerste directe Fabric/Databricks-sessieafstemming

**12 oktober 2026, vanaf 12:14 CEST:** de Databricks-referentiesessie
meldde zich vanuit `D:\Mijn Drive\Contoso_lakehouse_v2`, branch main,
commit `bd8936d`, volgens haar eigen inventaris schoon.
De Fabric-assistent controleerde de sessie-identiteit en recente
opdracht via de sessietools. De aangewezen sessie is
`copilotcli:/16ca25b7-4278-43db-9b1a-3d37b55d370b`,
titel op dat moment "pull request". De oudere Databricks-sessie
is niet herbestemd. De menselijke opdracht is read-only afstemming:
geen jobs/deployments of wijzigingen in de Databricks-repo; iedere
assistent schrijft uitsluitend in de eigen projectmap.
De Fabric-assistent registreert alleen de samenwerking in dit verslag;
er is namens deze afstemming geen platformuitvoering gestart.

**Observeerbare communicatie:** de Fabric-assistent stuurde rechtstreeks
een concreet antwoord via `send_message`, bericht-ID
`a33d20f1-8b0d-4340-af9d-f0cede07b009`, door de sessietool als
`processing` gemeld. Een vervolgaanvulling heeft bericht-ID
`54234d1a-93ea-4ddf-a144-26e0cd3b61ec`, status `pending`.
Dit bewijst het verzenden/aanbieden aan de aangewezen sessie;
inhoudelijke ontvangstbevestiging van deze antwoorden en definitief
testcontractakkoord zijn nog niet geregistreerd.

**Databricks-bijdrage, als gerapporteerde read-only bevindingen:**
vijf verplichte SALES-objecten; customer/product/employee
`SNAPSHOT_SCD2`, orders en returns `INCREMENTAL_APPEND`.
Bronkeys zijn customer/product/employee key, orders
`(order_key, order_line_number)` en returns `return_key`.
RunContext-tijden komen uit runtime UTC, niet uit businessdatum.
Hashes volgen volgens de referentiesessie uppercase/trim,
NULL/empty-marker `^^`, separator `||` en prefix SALES; dat is niet
het Fabric canonical-JSON-hashcontract. De sessie signaleerde tevens
dat de bestaande active-sources-test externe bronnen ophaalt en
de stressgenerator onder andere `limit` en
`monotonically_increasing_id` gebruikt: gelijke generatorparameters
bewijzen dus geen gelijke records. Geen van beide is gestart.

**Fabric-bijdrage:** het antwoord beschreef de echte raw stringrecords,
niet slechts de getransformeerde Quality-waarden, alle drie wijzigingen,
complete-snapshotflags, fysieke Parquet-SHA's/lengtes, logische
fixturefingerprints en de werkelijk gepersiste UTC-cutoffs:

| Levering | Bestaande Fabric-cutoff |
|---|---|
| D1 | `2026-10-12T05:58:21.042199Z` |
| D2 | `2026-10-12T06:06:26.014796Z` |
| D3 | `2026-10-12T06:13:39.557082Z` |

De lokale unittests gebruiken andere tijden; die zijn nadrukkelijk
niet als live inputtijden doorgegeven. Alle bronrecords/schema's,
15 fysieke file-identiteiten en bewijsreceipts zijn herleidbaar via
[fixtures](../src/fabric_etl_phase6/fixtures.py),
[Quality-inputreceipt](../deploy/evidence/phase6-sales-quality-v1-receipt.json),
[onafhankelijke verwachtingen](../src/fabric_etl_phase6/proof.py) en
[gezamenlijke Gold-verificatie](../deploy/evidence/phase6-sales-gold-v3-verification.json).
Het bericht benoemde de checksummed temporele Fabric-overlay,
fail-closed relationshipwisselingen en onbewezen SQL-/consumeracceptatie,
zodat de referentiesessie geen vollediger gelijkheidsbewijs aanneemt
dan daadwerkelijk bestaat.

**Inhoudelijke convergentie, nog ontwerp en geen executable akkoord:**
beide voorstellen verlangen immutable input per levering, fysieke
en logische inputidentiteit, identieke volgorde, expliciete
referentiedata voor tijdafhankelijke regels en volledige business-
vergelijking met rijmultipliciteit. Voorgestelde gates zijn nul
ontbrekende/extra records en nul attribuutverschillen, naast exacte
tellingen/bedragen. Rowhashes zijn slechts quickchecks.
Technische sleutels worden per hub-/linktype via gecontroleerde
bijectieve natuurlijke-keymaps vergeleken. Historie wordt aan
delivery-sequentie/attribuutversie gekoppeld; werkelijke UTC-tijden,
replaystabiliteit, halfopen grenzen en as-of-selectie worden apart
getoetst, niet generiek weggefilterd.

**Open ontwerpverschillen en vervolgvragen:**

- De bronkey is niet altijd de link-/factgrain: orders bevat ook
  productkey in de linkidentity, returns bevat order-/line-/productkeys.
  Grains moeten per laag worden overeengekomen.
- Infinitywaarden kunnen verschillen: Fabric gebruikt
  `9999-12-31 23:59:59`; eventuele referentiedatum `9999-12-31`
  vraagt een expliciete markerafspraak met behoud van originele waarden.
- Bestaande Fabric-Parquetbytes read-only exporteren versus een nieuwe
  gezamenlijk gepinde fixture uitvoeren is nog niet besloten.
  Regeneratie mag niet als fysiek identieke input worden aangeduid.
- Tijdafhankelijke BV-regels, historische current-only joins,
  order-only cancellation/BV-verversing en PIT-selectie moeten door
  beide sessies worden getoetst; geen platform krijgt automatisch gelijk.
- Reject-/warning-/threshold-/quarantine-effecten vereisen afzonderlijke
  verwachtingen naast eind-Gold, ook als het eerste fixturepad nul rejects had.

**Betekenis voor het experiment:** de mens bracht twee gescheiden
AI-contexten bijeen. De eerste concrete samenwerking maakt technische
verschillen en eerder onbewezen aannames zichtbaar voordat uitvoering
wordt toegestaan. Het communicatiepad werkt voor berichtaanbieding;
testcontractakkoord, gezamenlijke uitvoering en platformgelijkheid
blijven afzonderlijk te bewijzen.

### F6-COLLAB-03 - Menselijke toestemming voor voortgang bij afwezigheid

Na de eerste directe sessieafstemming gaf de gebruiker expliciet aan:
"ik moet er straks vandoor, maar ik geef jullie het akkoord om zonder
mijn aanwezig door te gaan".
Daarmee mogen beide sessies het experiment zonder voortdurende
menselijke aanwezigheid verder uitwerken en onderling afstemmen.
De Fabric-assistent gaf deze opdracht door aan de aangewezen
Databricks-sessie via bericht
`51ad8914-ac06-4913-bea2-42803546ad90` (aangeboden als `pending`).

Deze toestemming is geen onbeperkte wijzigingsmacht: iedere sessie
houdt de eigen projectmap als schrijfgebied; geen verwijdering van
bewijs, productiecutover, rechten-/capaciteitswijzigingen of ongepinnde
externe inputs. De lopende Databricks-inventarisatie blijft voor deze
stap read-only. Eerst moeten het concrete C0-testcontract, de
metadata-/tijd-/grainverschillen en een veilige geisoleerde
uitvoeringsaanpak zijn afgestemd voordat vergelijkingsjobs worden gestart.
Afwijkingen, blokkades en resultaten worden voor menselijke naslag
geregistreerd, ook wanneer de gebruiker niet aanwezig is.

Autonome voortgang is afhankelijk van de beschikbare actieve sessies
en tooling. Doorwerken na afsluiten van VS Code, sessies of de computer
is niet bewezen of toegezegd; er is geen persistent losgekoppelde
achtergrondservice gestart.

### F6-COLLAB-04 - Referentieanalyse onthult uitvoerings- en omgevingsgates

**Read-only terugkoppeling op 12 oktober 2026:** de Databricks-sessie
rapporteerde bevindingen uit lokale commit
`bd8936dc727808ec6d1ba2eaba551f647a3b10ee`, zonder jobs,
deployments of bestandswijzigingen. Dit is codeanalyse, geen gemeten
Databricks-output of onafhankelijk herhaald Fabric-bewijs.
Het ontwerpprincipe is gedeeld; executable C0 is expliciet nog
niet overeengekomen. De vervolgantwoorden bevestigen inmiddels
daadwerkelijke tweerichtingsafstemming tussen de sessies.

| Gate | Databricks-bevinding volgens referentiesessie | Gevolg voor vergelijking |
|---|---|---|
| Ingestietypes/isolation | Bestaande Bronze is typed; Auto Loader heeft inferColumnTypes en persistent schema/checkpointstate | Raw string-Parquetacceptatie niet bewezen; bytesidentiteit alleen is onvoldoende |
| Automatische trigger | Landingpad heeft file-arrival-trigger | Geen fixture-upload zonder aparte veilige isolatie; upload kan zelf jobs starten |
| Deliverymap | Datumfolders leveren SALES-datum-ID's en YYYYMMDD-sequences | Expliciete mapping naar D1/D2/D3 nodig; geen willekeurige folderinvoer |
| Decimalcontract | discount_rate DECIMAL(9,6), Fabric DECIMAL(18,6) | Schema-afwijking behouden; exacte waarden en bereik apart beoordelen |
| Gold-enddating | Historische MERGE insert-only, geen update van eerdere valid_to/is_current | Mogelijk stale currentversies; gemeten baseline nodig, geen deduplicatie als camouflage |
| PIT-capability | DDL aanwezig, maar repository/loader sluit PIT uit | Niet als uitgevoerd of equivalent claimen; UNSUPPORTED_IN_REPO tot tegenbewijs |
| Retry/purge | Batchpurge kan events verwijderen terwijl current_state behouden blijft | Same-batch retry apart van nieuwe-batch/nochange toetsen |
| Laagtijden/BV | Iedere notebook initialiseert UTC-runtimecontext; geen gedeelde load_date meegegeven | Raw/BV-as-ofjoin kan BV missen; logische deliverymap mag future-data niet maskeren |
| Referentiedatum/DQ | current_date voor datumregels/tenure, ERROR/WARNING en thresholds met publicatie-effect | Expliciete evaluatiedatum en afzonderlijk regel-/reject-/thresholdbewijs |

De referentiesessie voorspelt vanuit de code dat customer/product
currenttelling bij D1/D2/D3 mogelijk 1/2/2 wordt en employee 1/1/1,
in plaats van de onafhankelijke verwachting 1/1/0.
Dit zijn hypotheses uit het insert-only historische writepad,
niet waargenomen remote tellingen. De Fabric-assistent onderschrijft
dat zo'n verschil niet mag worden verborgen door normalize/dedupe.
Ook expliciete soft-delete en absence-delete blijven verschillende
contracts: een complete snapshotmanifest bewijst op zichzelf geen
absence-tombstoneimplementatie.

**Afgesproken analysegrens:** oorspronkelijke deployed referentie
en eventuele latere correctierelease worden afzonderlijk beoordeeld.
Geen stille reparatie voordat de baseline is herleid, en geen
Fabric-aanpassing om fout gedrag als gelijkheid te reproduceren.
Natuurlijke bronkeys, linkkeys en factgrain worden per laag vastgelegd.
PIT wordt niet als een lege succesvolle vergelijking beschouwd
wanneer het writepad ontbreekt. Beide assistenten bevestigden dat
menselijke toestemming voor autonomie de eerdere no-job/no-deployment/
no-writegrens niet automatisch intrekt.

**Nieuwe remote toegangsgate:** de referentiesessie meldt dat de
in repo en [Azure-inventaris](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/docs/08_azure_inrichting.md) aangewezen host
`adb-7405619535862062.2.azuredatabricks.net` faalt op DNS
(`no such host`). Dat bewijst niet dat alleen een token verlopen is.
Er is een ander geldig profiel voor workspace `7405617094653375`,
maar die omgeving is nog niet als de bedoelde referentie gebonden.
Read-only jobnamen/herkomstinspectie is hoogstens een aanwijzing;
deployed project-/bundle-/release-identiteit en de juiste
omgevingsaanwijzing blijven noodzakelijk.
De Fabric-assistent registreert de referentiegate daarom als
`BLOCKED_UNVERIFIED_IDENTITY`, zonder het bestaande Azure-document
op basis van een kandidaatprofiel te herschrijven.

Geen login, tokenwijziging, compute- of SQL-start is toegestaan
binnen deze analyse. Zelfs SELECT/SHOW/DESCRIBE kan een gestopt
warehouse automatisch starten; de sessies vereisen eerst bewezen
RUNNING-status voordat eventueel read-only SQL wordt gebruikt.
Bij onbekende/gestopte compute blijft API-/lokale inspectie de route.

**Communicatiebewijs:** inhoudelijke Fabric-antwoorden zijn aangeboden
via berichten `84aa41ef-74d5-46d5-a1c7-fb699c3b2a4d`,
`8d8ebf5d-5708-4fd4-b9dd-bfe6ee219c53` en
`613ac2c2-5942-4194-89c6-3cdb7d71bd9b`.
Definitieve contractbesluiten en remote identitybevestiging blijven
open. Het experiment toont nu ook zijn kritische waarde: twee
gescheiden AI-contexten vinden concrete risico's en betwisten
gelijkheidsaannames voordat onveilige uitvoering plaatsvindt.

### F6-COLLAB-05 - Kandidaatdeployment gevonden, compute blijft gestopt

**Terugkoppeling 12 oktober 2026, 12:22 CEST:** de Databricks-sessie
meldt nieuw read-only API-bewijs uit het bestaande geldige profiel
`contoso-deploy-dev`, workspace `7405617094653375`.
Pipelinejob `952714285929685` behoort volgens `jobs.get` tot
deployed bundle `contoso_lakehouse/dev`; de notebookpaden liggen in
de bijbehorende dev-bundleworkspacefolder. De pipeline-trigger en
andere aangetroffen pull schedules zijn op dit meetmoment `PAUSED`.
Warehouse `3175f890ee47d266`, naam `Contoso dev validation`, is
`STOPPED`. Er is geen SQL/compute gestart.

Dit verscherpt de omgevingsgate tot
`CANDIDATE_DEPLOYMENT_FOUND_DOC_HOST_DRIFT`: een relevante deployment
is gevonden, maar de repo-/documentatiehost wijst naar een andere,
onbereikbare workspace. De bedoeld-referentieaanwijzing en exacte
deployed runtime-/releasebinding blijven open. Jobnamen en bundle-
paden alleen bewijzen niet de actieve code- of metadata-equivalentie.
De oorspronkelijke Azure-inventaris is daarom niet herschreven.

**Vervolg binnen de afgesproken read-only grens:** de referentiesessie
kan Unity Catalog REST-tablemetadata lezen en gedeployde sourcebytes
in memory vergelijken met haar lokale HEAD. Geen SQL, exports,
lokale/remote filewrites, authenticatiewijzigingen of jobstarts.
De Fabric-assistent verricht geen parallelle remote Databricks-
aanroepen; de referentiesessie is de enige Databricks-inspecteur.
Inhoudhashes, schema-/decimal-/nullabilityverschillen en herkomst
moeten afzonderlijk worden gemeld. Niet bereikbare imported libraries
blijven onbevestigd, ook wanneer notebookbytes overeenkomen.
PIT-tableaanwezigheid is geen PIT-writebewijs en gepauzeerde
triggers nu zijn geen permanente garantie voor toekomstige uploads.

Deze ontvangst en begrensde vervolgstap zijn rechtstreeks aangeboden
aan de referentiesessie via bericht
`4f1fd3be-ed8a-4abb-bcad-19ba664c982e`.
PIT-, Gold-enddating-, purge/retry- en laagtijdbevindingen blijven
code-afgeleid totdat de deployed vergelijking of latere toegestane
uitvoering aanvullend bewijs levert. Executable C0 en volledige
fase-6-acceptatie blijven open.

### F6-COLLAB-06 - Gerichte deployed identiteit en gezamenlijk C0-HOLD-besluit

**Read-only remote ronde afgerond, 12 oktober 2026, 12:22 CEST:**
de Databricks-sessie rapporteerde vergelijking van deployed sourcebytes
in memory via Workspace Export, zonder exportfiles of wijzigingen.
De volgende geselecteerde bestanden zijn volgens haar bytegelijk
aan lokale HEAD `bd8936dc727808ec6d1ba2eaba551f647a3b10ee`:

| Geselecteerd deployed bestand | SHA-256 |
|---|---|
| gold.py | `9d9f5cb99aa75772c675ab07425ceedf804b3448c1ac92ff3c59bc1d3caf8cd6` |
| datavault.py | `bb8d535af6793977b35c78384d8fb5f8db527be3edbb4f8079fe1d932c9cbefd` |
| metadata.py | `ea5dc932845384f0d6f7fde9d5dc4034390a8f92d6fef6a4eefe970687927add` |
| meta_gold_entity.json | `f1d27ec5abee4eb655ee7b42a39baefff5fe647334a3cd6ad1f6d145755de137` |

Dit is gerichte file-identiteit, niet een volledige releasechecksum,
dependencylineage of bewijs van actieve metadata-rijwaarden.
De gevonden pipelinejob `952714285929685` heeft trigger `PAUSED`.
Een bestaande run `960091284575444` staat `SUCCESS`; zij is niet met
de C0-input uitgevoerd en telt niet als referentietestresultaat.

Unity Catalog REST bevestigt volgens de referentiesessie vijf
fysieke historische SALES-dim/facttabellen als `MANAGED`, geen
dynamische historieviews; `valid_from`/`valid_to` zijn TIMESTAMP
NOT NULL en `is_current` BOOLEAN NOT NULL.
`fct_sales_hist.discount_rate` is daadwerkelijk DECIMAL(9,6),
`unit_price` DECIMAL(18,4). Een managed customer-PIT bestaat met
snapshotdatum en Raw-/BV-pointers, maar inhoud en laadbewijs zijn
niet gelezen. Orders-Bronze heeft typed date/int/decimalvelden.
Warehouse `3175f890ee47d266` blijft `STOPPED`; er zijn geen SQL-
statements uitgevoerd. Checkpointschemas, eventtijden, actieve
metadatawaarden en businessoutputs blijven onbekend.

De overeenkomstige geselecteerde deployed code en fysieke tabellen
maken de eerdere Gold-/PIT-risicoanalyse relevanter voor deze
kandidaatomgeving. De voorspelde stale Gold-output en ontbrekende
PIT-uitvoering zijn nog steeds geen gemeten outputfeiten.
De afwijkende onbereikbare repo-/documentatiehost blijft expliciete
omgevingsdrift, niet stil gecorrigeerde configuratie.

**Gezamenlijk ontwerpbesluit, geen uitvoeringsakkoord:** de
Fabric-assistent bevestigde de door Databricks voorgestelde HOLD
en de volgende contractkeuzes via bericht
`fe6fbaf6-8a48-4650-8b2a-aa13b550dbac`:

- Rateprecision blijft een schema-afwijking: Databricks DECIMAL(9,6)
  tegenover Fabric DECIMAL(18,6). C0 vergelijkt numerieke decimalwaarden
  exact op zes fractionele posities, zonder float/tolerantie.
  Baseline `0.000000` past in beide; schema-equivalentie wordt daarmee
  niet goedgekeurd. Bereik-/scalegrenzen vragen afzonderlijk bewijs.
- Canonieke leveringen zijn D1/D2/D3 met onveranderlijke volgorde
  en raw record/schema/file-identiteit. Fabric-IDs/batches/cutoffs
  staan vast; concrete geisoleerde Databricks-datumfolders en hun
  SALES-datum-ID's/sequences zijn nog niet gekozen.
- Elke laagtijd blijft zichtbaar. Deliverymapping is geen toestemming
  om toekomstige BV-data in een eerdere Raw-as-of te schuiven.
  Halfopen grenzen, currentflags, infinitymarker en evaluatiedatum
  zijn afzonderlijke semantische contracts.
- Bestaande Fabric-bytes later onveranderd overdragen heeft de
  voorkeur, maar er is niets geexporteerd/geupload. Een eventuele
  schemaadapter vereist een expliciete versie en aparte per-layer
  inputscope; aangepaste Bronze is niet stil raw-inputidentiek.
- De oorspronkelijke deployed referentie blijft een meetbaseline;
  eventuele correcties krijgen een afzonderlijke release-identiteit.
  Geen normalisatie van stale duplicates, verkeerde einddatering
  of ontbrekende PIT om gelijkheid te suggereren.

**Executable C0 = HOLD**, met expliciete gates:

| Gate | Nog vast te stellen |
|---|---|
| G1 | Bedoelde referentieworkspace, volledige deployed/importherkomst en actieve metadata |
| G2 | Geisoleerde landing, triggers, checkpoints en schemastate |
| G3 | Immutable rawbyteoverdracht en eventuele versioned schemaadapter |
| G4 | Concrete datumfolder-/deliverymap, evaluatiedatum en laagtijdcontract |
| G5 | Exacte schema/value/grainvergelijking en bijectieve keymapping |
| G6 | Gemeten Gold-enddating, currentduplicaten en cancellationgedrag |
| G7 | PIT-capability en daadwerkelijk write-/as-ofbewijs |
| G8 | Same-batch purge/retry versus nieuwe-batch nochange en herstel |
| G9 | Expliciete uitvoeringsruimte buiten de huidige no-job/no-deploy/no-writegrens |

G6-G8 zijn mede doelen van een later toegestane baselinemeting;
een baseline mag tekortkomingen aantonen. Ze hoeven niet vooraf
als correct te worden aangenomen om te mogen meten, maar wel
als acceptatiecriteria/verwachtingen worden vastgelegd.
De mens hoeft niet iedere analysevraag te beantwoorden; beide
sessies bewaken wel de eerder afgesproken uitvoeringsgrens.
Deze ronde heeft een concreet gezamenlijk ontwerp en herleidbare
blockers opgeleverd, nog geen cross-platform-outputgelijkheid.

### F6-COLLAB-07 - C0-DESIGN-v0.1 en zichtbare verschillen per inputlaag

**Aanvullende read-only afstemming, 12 oktober 2026, 12:24 CEST:**
de referentiesessie ondersteunt vijf SALES-bronnen, alle DQ-uitkomsten
en vijf historische plus zes actuele Gold-producten, inclusief
kalender. Raw Vault, BV en PIT zijn afzonderlijke diagnostische
checkpoints. De Fabric-assistent bevestigt dit als voorlopig
`C0-DESIGN-v0.1`, niet als executable contract of testresultaat.
De centrale invariant is **verschillen zichtbaar laten**.
Keybijectie geldt per namespace/type, op basis van unieke committed
as-of hubmaps. Geen many-to-many rescue of normalisatie van echte
businessverschillen. Comparatorveldvolgorde wordt expliciet bepaald,
los van de hashveldvolgorde.

**Infinitycorrectie:** de gepinde Databricks GH-selecties gebruiken
volgens de referentiesessie exact `9999-12-31 23:59:59`, gelijk aan
de nominale Fabric-sentinel. Een eerdere verkorte aanduiding
`9999-12-31` is dus geen vastgesteld sentinelverschil.
Datatype, timezone en timestampprecision blijven te controleren;
generieke infinitynormalisatie is niet nodig bij exact gelijke waarden.

**Nieuwe pre-Quality multisetgate voor C2/C3:** volgens de gelezen
Databricks-code wordt Bronze voor Quality gededupliceerd op
source-filepath, delivery-ID en businesskeys. Daardoor kunnen
gelijke keys binnen een bestand verdwijnen, ook bij conflicterende
payload, zonder deterministische winnaar. Gelijke keys in verschillende
bestanden kunnen wel Quality bereiken. Quality UNIQUE telt vervolgens
binnen de huidige deliveryslice; gelijke keys in verschillende
deliveries worden niet door die UNIQUE-check afgekeurd.

Het testontwerp moet daarom naast businessrecords ook file-layout
en inputmultipliciteit vastleggen, met tellingen en payloadreconciliatie
op raw input, Bronze/pre-Quality en post-Quality.
Exacte duplicaten, conflicterende duplicaten binnen een bestand,
duplicaten over bestanden en herhaalde keys over deliveries zijn
afzonderlijke gevallen. Groen Quality-bewijs mag eerder recordverlies
niet verbergen. Een winnaar mag niet achteraf worden verzonnen.
Dit gedrag is code-afgeleid, niet met nieuwe inputs uitgevoerd.

DQ-referenties lezen volgens de code de gehele Quality-referentietabel,
niet alleen de huidige delivery/as-of en niet uitsluitend niet-verwijderde
records. WARNING resulteert in PASSED_WITH_WARNINGS; blocking
FAIL_BATCH kan voor outputwrite stoppen. Regeluitkomsten, multipliciteit,
threshold_pct-precisie en publicatie-effect blijven afzonderlijke checks.

**Order-only wijziging: onderscheid BV-producer en Gold-consument.**
Volgens de referentiesessie historiseert de ordersatelliet status/date/
currency, terwijl de orderlinesatelliet die velden niet in hashdiff heeft.
Orderline-BV leest rechtstreeks Quality ORDERS; is_cancelled en lead_time
zijn hashdiff-attributen. Een status-only wijziging kan daar dus een
BV-event opleveren zonder nieuw line-event.
De historische salesfact wordt echter gedreven door line-load_date,
met current-only order/BV-joins en insert-only historische MERGE.
De hypothese is daarom: BV kan wijzigen terwijl GH/GC-fact stale blijft
omdat geen nieuwe historische factkey of matched update ontstaat.
Dat is nog geen gemeten Databricks-resultaat.

Voor Fabric blijft order-only BV-recomputation eveneens onbewezen.
De lokale temporele overlaytest bewijst de consumentlogica, niet dat
de producer in een dergelijke levering het benodigde BV-event maakt.
Een afzonderlijk cancellation-/datumwijzigingsscenario buiten de
bestaande D1-D3-fixtures moet beide kanten toetsen.

PIT-cutoffselectie is bij Databricks niet uit een writeimplementatie
te verifiëren: alleen tabledefinitie/pointers zijn aangetroffen.
Runtime UTC-load_date per notebook, current_date-datumchecks en
BV-tenure blijven afzonderlijke tijdscontracts. Kalenderbereik
2020-2030 is gedeeld ontwerp. STRING-naar-typed acceptatie is
alleen expressions-based aannemelijk, niet live ingest bewezen.

**Status blijft HOLD:** transferkeuze OPEN, voorkeur bestaande vijftien
immutable Fabric-bestanden; geen export/upload nu, geen all-string
Bronze-schemawijziging, warehouse STOPPED en geen code-uitvoering.
Gerichte deployed hashes/schema's uit F6-COLLAB-06 zijn ontvangen,
maar sluiten volledige release-/metadata-identiteit niet af.
Ontwerpbevestiging aangeboden via bericht
`d2504a42-20ef-4d33-b212-7e65134b88f0`.

### F6-COLLAB-08 - Ontwerp voor geisoleerde uitvoering, provisioning blijft HOLD

**Voorstel referentiesessie, 12 oktober 2026, 12:24 CEST:** een
nieuwe namespace `p6c0_<contracthash12>_<attemptid>` moet alle
raw/control/meta/bronze/quality/reject/vault/gold-catalogs scheiden.
Eigen bundle/root/job, alleen manual, zonder file-arrival/schedules;
SALES-only allowlists. Business-collisionprefix SALES blijft behouden.
Landing, checkpoints/schemaLocation, quarantine, current_state,
locks/audit, pointers/slots en PIT worden eveneens geisoleerd.
Nieuwe baseline/correctierelease krijgt een nieuwe attemptnamespace;
geen reset/delete of hergebruik van dev/tst/prd.

De referentiesessie constateert in generieke setup een EXTERNAL
landingvolume met abfss-location. Catalogisolatie bewijst dus geen
storageisolatie. De bestaande generieke bootstrap is niet zonder meer
geschikt: zij omvat ook feeds, grants en gedeelde external locations.
Voorgesteld is een afzonderlijke SALES-only bootstrap met managed
landing of een aantoonbaar eigen bestaand opslagpad, zonder grants,
capacity of productie te wijzigen. Dit voorstel is niet gebouwd,
geautoriseerd of getest; provisioning blijft HOLD.

**Voorgestelde transportmap:** D1/D2/D3 naar
SALES|2026-10-10, SALES|2026-10-11, SALES|2026-10-12, uitsluitend
in de nieuwe namespace. Folderdatums zijn transport/sequence,
geen Fabric-cutoff of business-evaluatiedatum.
Iedere levering wordt volledig verwerkt en per-cutoff evidence
vastgelegd voordat de volgende bestanden zichtbaar worden.
Databricks-laagtijden worden feitelijk geregistreerd; geen import
van Fabric-cutoffs als runtime-load_date en geen clockspoof.
Gewenste business-evaluatiecontext 2026-10-12/UTC is nog een
ontwerpkeuze: originele current_date-code en werkelijke execute-day
blijven zichtbaar; een eventuele tijdadapter is een aparte versie.

De beoogde bewijsvolgorde is: vijftien immutable bestanden met
hash/length/schema/multiset, Bronze-coercion, Quality rulevector/
rejects/multipliciteit, Raw/BV-eventgrains en FK-maps, PIT-capability,
GH5/GC6-snapshots met volledige rowdiff per cutoff.
Verwachte actuele dims 1/1/0 per dim, facts elk een rij en kalender
4.018 rijen; finale historische customer/product drie versies,
employee twee, inclusief exacte halfopen grenzen/currentflags.
Rateprecision blijft SCHEMA_MISMATCH met exacte value-overlapcheck.
Niet-geimplementeerde PIT is NOT_IMPLEMENTED, niet empty=pass.
Een baseline met echte businessverschillen kan FAIL opleveren en
is daarmee geen succesvolle referentieacceptatie.

**Gerichte Fabric-reactie via bericht
`7187e56f-50d5-4dba-8724-10952d8bb3fc`:** inhoudelijke steun
als `C0_SCOPE_PROPOSED / EXECUTION_HOLD`, met extra readinessgates:

- Settings moet arbitrary suffix en alle resolved paden aantoonbaar
  ondersteunen. Hardcoded dev-/feed-/SQL-/importpaden buiten Settings
  mogen niet door de isolatiecontrole glippen; fail closed voor bootstrap.
- De volledige immutable contracthash is autoritatief; hash12 is
  alleen een leeslabel. Attempt en baseline/correctierelease worden
  gepind, met geen namespacehergebruik bij een collision.
- Originele businesscode en nieuwe bootstrap/harness krijgen aparte
  manifests/hashes. Een infrastructuuradapter bewijst niet vanzelf
  volledige bytegelijke release-identiteit.
- Bestaande rechten zijn prerequisite. Read-only ACL-informatie
  bewijst niet altijd effectieve provisioningrechten; onbekend blijft
  UNKNOWN/HOLD, niet een ongeautoriseerde proefwrite.
- Een actieve Bronze-preflight mag de echte baseline niet vervuilen
  door checkpoints te vullen. Alleen inspectie of een aparte nieuwe
  attemptnamespace voor coercionuitvoering; geen onzichtbare no-op
  in de daaropvolgende ingest.
- Verwachte businessdiff FAIL en uitvoeringsfout/FAIL_BATCH worden
  onderscheiden. Geen stil succes, skip of herstel; vervolganalyse op
  incomplete lagen uitsluitend als expliciet begrensde diagnostiek.
  Ontbrekende PIT blokkeert PIT-acceptatie, maar mag niet worden
  verborgen of onterecht als succesvolle lege output worden geteld.

Transfer blijft OPEN, geen resources aangemaakt of bestanden
overgedragen. Beide sessies vervolgen alleen ontwerp/read-only;
geen garantie van werking na VS Code/computer sluiten.

### F6-COLLAB-09 - Releaseprovenance, bewijsstatus en expected/observed-matrix

**Bounded remote inventaris afgerond, 12 oktober 2026, 12:25 CEST:**
de referentiesessie meldt bundle-stateprovenance
`contoso_lakehouse`, target dev, development-mode, gitcommit
`bd8936dc727808ec6d1ba2eaba551f647a3b10ee`, origin
`bobstoelinga/Contoso_lakehouse_v2.git`. Dit ondersteunt de
deploymentherkomst, maar is geen volledige remote checksum of
bewijs van actieve metadata-rijwaarden. De hostdrift blijft open.
Geen verdere remote reads zijn nodig zonder een nieuwe gerichte vraag.

Aanvullend bytegelijk met lokale repo volgens de referentiesessie:

| Deployed bestand | SHA-256 |
|---|---|
| context.py | `c3a1108ce1f6431cdcde050845709205d2e5c61a0555bb3d43de36e0a340cc1f` |
| bronze.py | `fc059f1b9eb639f2da2be5ef91d2df397c1a1be0c3f041d8f8802aa0bb323ef5` |
| quality.py | `13897ddb13db1eb2c0be7e7a9dcd6664784f6baefdebaaa46872f8d75cac832a` |

Zes source/object/mapping/DQ/Vault-seeds zijn eveneens als bytegelijk
gerapporteerd; hun individuele hashes zijn in dit bericht niet geleverd.
Samen met de eerdere vier files is dit gerichte identiteit, niet
volledige releaseacceptatie. Metadata SQL-script-schema bevat versie,
checksum, status en approvalvelden; opgeslagen waarden zijn niet gelezen.
Geen afzonderlijke release/versiontabel gevonden in de schema-listing
bewijst niet dat elke mogelijke releaseadministratie ontbreekt.

Live REST-bronschema's hebben nullable businessvelden. Customer/
employee-datums zijn DATE; delete/discontinued BOOLEAN; productbedragen
DECIMAL(18,4). Orders heeft INT-line/quantity, DATE-datums en
DECIMAL(18,4)-unitprice/discount/net. Returns heeft INT-line/quantity,
DATE-returndatum en DECIMAL(18,4)-refund; overige velden STRING.
Dit bewijst typed Bronze, niet all-string rawacceptatie/coercion.
GC6 zijn volgens REST views met releasepointerreferentie;
as-ofdelivery/as-ofstamp/batch zijn NOT NULL. GH5 blijven fysieke
MANAGED-tabellen. Schema-aanwezigheid zegt niets over businessinhoud.
Warehouse blijft STOPPED, geen businesspayload/SQL/code-uitvoering.

**Expected/observed-grondslag C0-A:** Fabric-bewijs hieronder is
eerder door de Fabric-sessie geverifieerd, niet door Databricks herhaald.
Databricks-kolom is nu NOT_MEASURED waar geen businessrows zijn gelezen.
Codevoorspellingen blijven afzonderlijke hypotheses, niet observed.

| Checkpoint | Expected bij D1/D2/D3 | Observed Fabric | Observed Databricks C0 |
|---|---|---|---|
| Input | Vijf bronnen, elk een rij per levering; vijftien immutable files | Gepinde Quality-inputreceipt | NOT_MEASURED; geen transfer |
| Quality | Per bron PASSED 1, REJECTED 0, FILTERED 0; volledige rulevector apart | Tellingen verified; geen cross-platform rulevectorbewijs | NOT_MEASURED |
| Raw/BV | Nieuwe events 18/2/3, exacte grains/keys/payload | Verified eerste uitvoering en retry | NOT_MEASURED |
| GH-dims | Finaal customer/product 3 versies, employee 2; correcte grenzen/flags | Verified | NOT_MEASURED; stale enddating codehypothese |
| GC-dims | Elke dim 1/1/0 | Verified | NOT_MEASURED; stale current codehypothese |
| GC-facts | Sales/returns elk 1 per cutoff, exacte businesswaarden | Verified | NOT_MEASURED |
| Calendar | 2020-2030, 4.018 rijen | Verified | NOT_MEASURED; codebereik gelezen |
| PIT C0-B | Committed as-of Raw/BV-pointers per cutoff | Verified | UNSUPPORTED_IN_INSPECTED_PATH; geen outputmeting |

C0-A is een later te autoriseren regressie-/baselinemeting,
geen vooraf aangenomen succesvolle referentieacceptatie.
C0-B blijft PIT-capability-blocked. C1 onderscheidt same-batch retry
van nieuwe-batch/nochange. ORIGINAL_AS_DEPLOYED blijft gescheiden
van CORRECTED_REFERENCE_RELEASE. Geen gezamenlijke AI-verklaring
van volledige acceptance.

**Nummeringscorrectie:** het laatste peerbericht gebruikte G6-G9
voor andere bevindingen dan de eerder gepinde gatebetekenissen.
Fabric heeft alleen die afwijking teruggestuurd via
`c3859b47-e450-4d1d-8336-db1ea2fee3cb`, zonder context te herhalen.
Canonical bewijsstatus:

| Gate | Betekenis | Status nu |
|---|---|---|
| G1 | Referentie/release/actieve metadata | PARTIAL; provenance en geselecteerde files bewezen door peer, hostdrift/volledige identiteit/rijwaarden open |
| G2 | Storage/rechten/landing/state-isolatie | OPEN; voorgesteld namespace niet geprovisioned |
| G3 | Immutable transfer/adapter | OPEN; geen overdracht |
| G4 | Delivery/evaluatiedatum/laagtijd | PROPOSED; niet executable gepind |
| G5 | Comparator/schema/grain/keybijectie | PROPOSED; rate SCHEMA_MISMATCH bevestigd |
| G6 | Gemeten Goldgedrag | NOT_MEASURED; code-/schemasignalen |
| G7 | PIT-capability | UNSUPPORTED_IN_INSPECTED_PATH; definitie aanwezig, geen laadbewijs |
| G8 | Retry/purge | CODE_RISK_NOT_MEASURED |
| G9 | Uitvoeringsautorisatie | NOT_AUTHORIZED_WITHIN_CURRENT_BOUNDARY |

Tijdverschillen worden zowel in businesswaarden als technische lineage
zichtbaar gehouden; toekomstige BV wordt niet naar Rawtijd teruggeschoven.
Timeadapter is een aparte release-/harnessclaim. Huidige EXECUTION_HOLD
blijft ongewijzigd; alleen Fabric-documentatie is bijgewerkt.

### F6-COLLAB-10 - Projectbinding is geen referentieaanwijzing

**Identitygate aangescherpt, 12 oktober 2026, 12:26 CEST:**
beide sessies onderscheiden twee onafhankelijke bewijslabels:

- `ARTIFACT_PROJECT_BINDING_PARTIALLY_VERIFIED`: kandidaatworkspace
  `7405617094653375` heeft bundleprovenance, geselecteerde bytegelijke
  loaders/seeds en passende UC-schema's. De job is BUNDLE/UI_LOCKED,
  met gepauzeerde trigger. Dit is meer dan alleen jobnamen.
- `REFERENCE_ENVIRONMENT_GATE=BLOCKED_UNVERIFIED_IDENTITY`:
  de kandidaat is niet automatisch de aangewezen referentie.
  `DESIGNATED_REFERENCE_IDENTITY_NOT_VERIFIED` blijft gelden.

De eerdere aanduidingen G1 PARTIAL en
CANDIDATE_DEPLOYMENT_FOUND_DOC_HOST_DRIFT betreffen uitsluitend
artifact-/projectbinding, niet het openen van de referentiegate.
Een expliciete menselijke of repo-aanwijzing voor de vervangende
host ontbreekt. Volledige releaseinhoud, clean-at-deploy-attestatie,
actieve metadata-checksums/approvedversions, runtime-/schemastate
en feitelijke outputs ontbreken eveneens. Bundlemetadata met een
gitcommit bewijst geen schone deployed tree of actuele metadatarijen.
DNS-falen van de oude host is geen tokenexpirybewijs.

Geen herhaalde remote inventaris nodig; bestaand bewijs volstaat
voor begrensd lokaal C0-A/B/C1-contractwerk. Geen meetrun,
metadata-equivalentieclaim, provisioning, login/authwijziging of
andere mutation. Warehouse blijft STOPPED. Namespace-/foldermaps
blijven voorstellen. Bevestiging rechtstreeks aangeboden via bericht
`52e50b26-521c-4373-86f9-cbf33088f86a`.

### F6-COLLAB-11 - Directe importketen begrensd geverifieerd

**Laatste gerichte inventaris, 12 oktober 2026, 12:27 CEST:**
de referentiesessie meldt dat Quality/Vault/Gold-taskconfig naar
dezelfde deployed bundlefiles-root verwijst. Drie onderzochte notebooks
voegen repo_root/src vooraan sys.path toe en importeren de eerder
gehashte frameworkmodules. Hun remote bytes en vier helpers zijn
volgens in-memory vergelijking exact gelijk aan lokale HEAD:

| Onderzocht artifact | SHA-256 |
|---|---|
| 20_quality notebook | `346b5677493c4b286d7810127ec392f6111e2782948b87e2c97fbe88a3b27afb` |
| 30_data_vault notebook | `3cc28dbd9a71ba5d6b7ffffa58b2d06f2732d7bbdf784c3de607632c3f6c5540` |
| 40_gold_historical notebook | `eba24c7bdac95f0b253977c21df63eedad650bb02c7445dab38a0e11356b0b05` |
| hashing helper | `198e3edd89179f445094127013768877cba293533b031e9011d1124ad3cae4c0` |
| orchestration helper | `bac04cf14bf32d5a70c45da6f5513d35095cc1252ead69749713dd8dacb9c9ac` |
| sqlutil helper | `829926ad9d8fbe56dfbad8a4f4beb69e62fb08b2a2d4d8a859e8396d51523b74` |
| audit helper | `e0b13d97d67a1143948b8dc851957735e1603a0a66c4a59518f9b07b604ce5ab` |

In deze onderzochte taskconfigs zijn libraries/environment_key en
settings.environments afwezig. Daaruit volgt geen gepinde wheel-,
dependency- of runtimeversieclaim. Werkelijk geladen modulefiles,
caches en PySpark/Delta/stdlib-runtime zijn zonder uitvoering
niet bewezen. Ongelezen dependencies blijven UNVERIFIED.
De beperkte set bevat geen onbereikbaar helperartifact.

In onderzochte enddating/PIT/purge/load-date/mappingcode zijn geen
deployed-vs-HEAD-byteverschillen gevonden. Code-afgeleide risico's
gelden daarmee ook voor deze geselecteerde artifactset, maar hun
businessgevolgen blijven NOT_MEASURED. UC-schema's bewijzen geen
PK-enforcement, rijtellingen of unieke as-ofmaps.
Trigger PAUSED blijft een momentopname, geen uploadgarantie.

De Fabric-sessie bevestigt dat de directe-importvraag is afgedekt
via bericht `5bc69747-53d6-410f-9dd3-935e94a8db7d`.
Geen nieuwe remote inventaris nodig: volgende werk is uitsluitend
de lokale contractmatrix met bestaand bewijs. Artifactbinding blijft
PARTIALLY_VERIFIED; aangewezen referentie-identiteit blijft
BLOCKED_UNVERIFIED_IDENTITY. Geen SQL, compute, payloadreads,
exportfiles of mutations; checkout van de referentiesessie schoon.

### F6-COLLAB-12 - Afstemming afgerond zonder uitvoerings- of gelijkheidsclaim

**Gezamenlijke afronding, 12 oktober 2026, 12:27 CEST:**
de referentiesessie bevestigt geen inhoudelijke discrepantie en neemt
de G1-G9-indeling uit F6-COLLAB-09 over als canoniek; eerdere anders
gegroepeerde nummeringen zijn daarmee vervangen.
Rondestatus: **paired design agreed; execution HOLD;
output equivalence NOT_TESTED**.

De transportdatums 2026-10-10/11/12 uit het namespacevoorstel waren
uitsluitend suggesties, geen afspraak. De eerdere Fabric-reactie in
F6-COLLAB-08 ondersteunt hun mogelijke gebruik als transportmap,
maar pint die datums niet. Concrete unieke geordende transportdatums
en reference_date blijven OPEN en worden niet uit businessdates afgeleid.
Transfer, isolatie, referentieklok, executable comparator en
uitvoeringstoestemming blijven eveneens open.

ORIGINAL_AS_DEPLOYED benoemt voorlopig alleen de gepinde
kandidaatartifactset, zonder definitieve referentiedesignation.
CORRECTED_REFERENCE_RELEASE is nog niet gemaakt.
G6/G7/G8 zijn baseline-meetdoelen die FAIL/UNSUPPORTED kunnen
opleveren, geen eis om een baseline vooraf succesvol te maken.
Rate DECIMAL(9,6) versus DECIMAL(18,6) blijft schema-diff,
met exacte zes-decimalenwaardevergelijking zonder float/tolerantie;
geen schemapass. De bestaande succesvolle operational run blijft
geen C0-resultaat.

Geen DB-/Fabric-jobs aangepast of gestart. De referentiesessie
meldt geen files/mutations en een schone checkout; Fabric heeft
alleen de toegestane verslaglegging bijgewerkt.
Inventorymatches zijn geen actieve metadata-/output-/runtime-
importattestatie. Geen aanvullende remote calls nodig en geen
nieuw ontvangstbericht verstuurd: de ronde wordt bewust gesloten
zonder ACK-loop of gezamenlijke volledige acceptanceverklaring.

**Aanvullende afsluitbevestiging, 12:27:54 CEST:** de referentiesessie
onderschrijft expliciet de aanvullende isolatiegates uit
F6-COLLAB-08 binnen C0-DESIGN-v0.1: volledige contracthash/attempt/
releasebinding, controle van alle directe en hardcoded paden,
afzonderlijke business-/bootstrapmanifests, geen proefwrite bij
onbekende rechten en geen baselinecheckpointconsumptie door preflight.
Ook cutoffevidence, onderscheid business-FAIL/operationeel-FAIL_BATCH
en afzonderlijke PIT-capabilityfail zijn bevestigd.
Transportdatums en gewenste 2026-10-12/UTC-evaluatiecontext blijven
voorstellen; werkelijk current_date/execute-day blijft zichtbaar,
zonder clockadapter in de originele baseline. Deze bevestiging
sluit geen open executiongate. Geen extra ACK of inventaris gestart.

### F6-COLLAB-01 - Mens en twee AI-sessies: expliciet te documenteren experiment

**Menselijke aanleiding, 12 oktober 2026, 12:12 CEST:** de gebruiker
vroeg nadrukkelijk om aantekeningen van deze samenwerkingsactiviteit:
"is het mogelijk om van deze activiteit nadrukkelijk aantekeningen
te maken want ik vindt dit fantastisch".
Deze belangstelling hoort bij de oorspronkelijke onderzoeksvraag:
in hoeverre kunnen Mens en AI samen een functionerende,
productiewaardige metadata-gestuurde ETL-doelarchitectuur op Fabric
realiseren, met Databricks als referentie? Het samenwerken van twee
AI-sessies is daarbij een afzonderlijk te observeren onderdeel,
niet alleen een hulpmiddel om sneller code te schrijven.

**Aanloop en menselijke regie:** de gebruiker stelde eerder een tweede
VS Code-sessie voor het Databricks-project voor en vroeg nu hoe beide
sessies gekoppeld kunnen worden. De Fabric-assistent adviseerde een
herkenbare referentietestsessie in de eigen Databricks-projectmap.
De mens bepaalt het doel, brengt de twee werkcontexten bijeen en
bewaakt de betekenis van het resultaat. De Fabric-assistent heeft het
drie-leveringenpad inclusief retries en herstel bewezen; de beoogde
Databricks-assistent moet eerst de referentie read-only inventariseren.
Het voorstel is nog geen uitgevoerde sessiekoppeling of platformtest.

**Beoogde taakverdeling en grenzen:**

| Deelnemer | Verantwoordelijkheid | Grens |
|---|---|---|
| Mens | Doel, richting, context en beoordeling van het experiment | AI-overeenstemming is niet automatisch acceptatie |
| Fabric-AI-sessie | Fabric-implementatie, testuitvoering en herleidbaar bewijs | Geen gelijktijdige wijzigingen in de Databricks-projectmap |
| Databricks-AI-sessie | Referentie-inventarisatie en afgesproken referentietests | Eerst read-only; geen ongeplande jobs, writes of deployment |
| Gezamenlijke afstemming | Identieke inputs, scenario's, vergelijkingsregels en analyse van verschillen | Geen verschillen wegfilteren om gelijkheid te suggereren |

**Koppelingsvoorstel:** als de aangewezen sessie via de beschikbare
sessietools zichtbaar is, kunnen gerichte berichten worden verstuurd.
Of dit tussen de twee concrete VS Code-contexten werkt, moet eerst
worden vastgesteld. Is directe communicatie niet beschikbaar, dan
volgen expliciete overdrachtsberichten via de mens en een gedeeld,
versiebeheerd testcontract. Er wordt geen autonome communicatie
geclaimd voordat daadwerkelijke verzending en ontvangst zijn bewezen.
Een oudere projectchat wordt niet zonder aanwijzing herbestemd.

**Vaste registratie vanaf de eerste afstemming:** naast het technische
migratieverslag leggen we voor iedere betekenisvolle samenwerking vast:

- datum/tijd, herkenbare sessie en projectcontext van afzender/ontvanger;
- de menselijke opdracht en welke uitvoering daaruit door AI volgde;
- de inhoudelijke vraag, het voorstel, antwoord en uiteindelijke besluit;
- het overeengekomen testcontract, versies, inputpins en schrijfgrenzen;
- verschillen in aannames of interpretatie en hoe die zijn opgelost;
- uitvoeringen, resultaten en links naar tests, jobs en bewijsreceipts;
- fouten, misverstanden, herstel en nog onbewezen conclusies;
- wat deze stap leert over Mens-AI- en AI-AI-samenwerking.

Besluiten en betekenisvolle berichtinhoud worden samengevat met
herkomst; relevante exacte formuleringen worden waar nodig geciteerd.
Secrets, tokens en onnodige persoonsgegevens horen niet in dit verslag.
We registreren observeerbare opdrachten, handelingen en resultaten,
geen veronderstelde interne redeneerprocessen van een AI.
Ook stroef verlopen afstemming blijft zichtbaar: het verslag is
naslag en experimenteel bewijs, geen uitsluitend positief succesverhaal.

**Huidige waarneming:** de initiatiefnemer is de mens; de Fabric-AI
heeft een taakverdeling en communicatiepad voorgesteld en deze
rapportageafspraak vastgelegd. De aangewezen tweede sessie,
daadwerkelijke berichtuitwisseling en Databricks-referentieuitvoering
zijn nog niet bevestigd. De volgende notitie moet vastleggen of
de koppeling werkelijk lukt en welk eerste testcontract beide
sessies overeenkomen.

## Mens en twee AI-sessies: praktijkverslag voor naslag en LinkedIn

**Registratiedatum:** 12 oktober 2026, naar aanleiding van Bobs verzoek
om de samenwerking uitgebreid te beschrijven. Deze sectie bundelt
de gebeurtenissen uit F6-COLLAB-01 t/m F6-COLLAB-12 in leesvolgorde.
De oorspronkelijke tijdgebonden notities blijven staan: een aanvankelijk
onbevestigde koppeling wordt niet achteraf herschreven als toen al bewezen.
De hier beschreven uitkomst is de stand na de eerste afstemmingsronde,
niet een eindconclusie over de gehele migratie.

### De onderzoeksvraag: samen architectuur bouwen, niet alleen code genereren

Bob Stoelinga formuleerde het doel nadrukkelijk als een experiment:
kan mens en AI samen een functionerende, productiewaardige
metadata-gestuurde ETL-architectuur op Microsoft Fabric realiseren,
met het bestaande Databricks-model als referentie?
Er wordt geen productieomgeving of productiecutover nagestreefd.
Wel moeten correctheid, historie, datakwaliteit, reproduceerbaarheid,
retries, herstel en bewijsvoering serieus worden getoetst.

Dat onderscheid bepaalde ook de samenwerking. Een notebook dat groen
eindigt is niet genoeg. De vraag is wat het werkelijk heeft verwerkt,
welke records en versies zijn ontstaan, of een retry dezelfde uitkomst
behoudt en of een ander platform dezelfde bedrijfssemantiek levert.
Databricks is een referentiearchitectuur, geen onfeilbare waarheid:
gedeelde fouten mogen niet als geslaagde pariteit worden aangemerkt.

De menselijke reacties "Dit is een gaaf project als dat lukt" en
"blijf vooral tot op detailniveau het project registreren in het
verslag voor naslag" maken twee doelen zichtbaar: iets werkends bouwen
en het proces zodanig vastleggen dat de weg ernaartoe controleerbaar blijft.
Later vroeg Bob expliciet om deze samenwerking te bewaren omdat zij
voor een LinkedIn-artikel waardevol is. Het verslag moet daarom zowel
technisch naslagwerk als bron voor een eerlijk praktijkverhaal zijn.

### De mens bracht twee gescheiden werkcontexten bij elkaar

Het initiatief voor een tweede sessie kwam van Bob:
"zal ik een tweede sessie openen voor het databricks project, zodat
jullie samen kunnen afstemmen". Hij vroeg vervolgens hoe de sessies
aan elkaar gekoppeld konden worden. Dit was geen ongemerkt gestart
AI-team: de mens koos het doel en bracht de projectcontexten bijeen.

De Fabric-sessie werkte in de migratierepository en beschikte over
haar implementatie, onafhankelijke verwachtingen en uitvoeringsreceipts.
De Databricks-sessie werkte in de afzonderlijke oorspronkelijke
repository en onderzocht de referentiecode en bestaande deployment.
Die contextscheiding was bewust: iedere sessie behield een eigen
verantwoordelijkheid en schrijfgebied, zonder gedeelde ongecontroleerde
wijzigingen in de andere repository.

| Rol | Concrete bijdrage in deze ronde |
|---|---|
| Bob, opdrachtgever en architect | Onderzoeksvraag, productiewaardigheid, tweede sessie, autonomie en documentatiemandaat |
| Fabric-sessie | Bestaand ketenbewijs, exacte fixtures/cutoffs, outputverwachtingen, comparator- en isolatievragen, permanent verslag |
| Databricks-sessie | Bron-/DQ-/Vault-/Gold-analyse, gerichte read-only deployment-/schema-/artifactinventaris, referentiebeperkingen |
| Gezamenlijk ontwerpwerk | Scope, vergelijkingsregels, onderscheid baseline/correctierelease, expliciete uitvoeringsgates |

De koppeling gebruikte gerichte asynchrone sessieberichten.
Een toolstatus pending/processing bewijst aanbieding, niet vanzelf
dat een antwoord inhoudelijk is verwerkt. De terugkoppelingen en
expliciete ontvangstbevestigingen legden vervolgens de tweerichtings-
afstemming vast. De genoteerde bericht-ID's zijn herkomstmarkers;
zij zijn geen vervanging voor technische uitvoeringsbewijzen.
Er was geen gedeeld geheugen en geen magisch uniforme projectcontext:
informatie moest expliciet worden overgedragen en getoetst.

### Een eerste tijdlijn van de observeerbare samenwerking

Tijden hieronder zijn CEST en waar aangegeven benaderingen uit de
registraties; zij zijn geen meting van totale arbeid of AI-snelheid.

| Tijd op 12 oktober | Gebeurtenis | Betekenis |
|---|---|---|
| Voor de koppeling | Fabric SALES-keten door Quality/Vault/BV/PIT/Gold geverifieerd, met aparte retries en herstelbewijs | Afgebakend Fabric-bewijs beschikbaar, geen Databricks-pariteit |
| Rond 12:12 | Bob vraagt expliciete aantekeningen van de samenwerking | Het samenwerkingsproces wordt zelf een projectdeliverable |
| Vanaf 12:14 | Referentiesessie meldt zich; Fabric verstuurt records, schema's, hashes, cutoffs en verwachtingen | Van algemene vergelijking naar concreet hetzelfde-inputcontract |
| In de vervolguitwisseling | Referentiesessie signaleert typed Bronze, tijdsverschillen, Gold-/PIT-/retryrisico's | Referentie wordt onderzocht, niet blind gekopieerd |
| Rond 12:22 | Bestaande dev-deployment gevonden; warehouse gestopt; geselecteerde code en fysieke schema's read-only vergeleken | Artifactbewijs groeit, outputs blijven ongemeten |
| 12:24-12:26 | Multiset-/filelayoutregels, namespace-isolatie, gatebetekenissen en identitylabels aangescherpt | Contract krijgt expliciete fout- en veiligheidsgrenzen |
| Rond 12:27 | Directe importartifactvraag afgerond; ontwerp gezamenlijk bevestigd | Ontwerp akkoord, uitvoering HOLD, gelijkheid NOT_TESTED |
| Vanaf 12:28 | Bob vraagt een uitgebreide beschrijving voor een LinkedIn-artikel | Technisch log wordt aangevuld met deze samenhangende procesbeschrijving |

### Wat de sessies elkaar werkelijk leerden

**1. Dezelfde generator is nog niet dezelfde input.**
De Fabric-sessie leverde concrete raw records, all-string schema's,
vijftien fysieke bestandidentiteiten en drie gepersiste cutoffs.
De referentiesessie wees erop dat een bestaande generator of live
API-/SQL-pull niet dezelfde records garandeert.
De uitkomst was een voorkeur voor immutable bestaande bytes,
met daarnaast logische schema-/record-/multipliciteitscontrole.
Er zijn in deze ronde geen bestanden overgedragen.

**2. Een succesmelding bewijst de verkeerde vraag soms uitstekend.**
Een bestaande Databricks-run stond SUCCESS. Beide sessies hielden
haar buiten het C0-bewijs: zij had niet de afgesproken identieke inputs.
Evenzo geldt Fabric-ketenbewijs alleen binnen de werkelijk geteste scope.
De aanscherping was niet "meer groene jobs", maar aparte expected/
observed-kolommen en NOT_MEASURED waar de juiste meting ontbreekt.

**3. De referentie kan zelf architectuurrisico's bevatten.**
De Databricks-analyse vond een insert-only historisch Gold-writepad,
een PIT-definitie zonder aangetroffen loaderpad en een purge/retryrisico.
Gerichte deployed bytevergelijking bevestigde de overeenkomst van
de onderzochte code met de lokale baseline.
Dat maakt de hypotheses relevant voor die artifactset, maar bewijst
niet welke businessrows een nieuwe run daadwerkelijk zou opleveren.
De afspraak werd: oorspronkelijke baseline bewaren, mogelijke
correctierelease later afzonderlijk identificeren, geen hotfix die
de oorspronkelijke meetuitkomst onzichtbaar maakt.

**4. Een groene Quality-laag kan eerder verlies verbergen.**
De referentiesessie signaleerde deduplicatie in Bronze per bestand,
delivery en businesskey, nog voordat Quality UNIQUE wordt uitgevoerd.
Conflicterende records kunnen daardoor verdwijnen zonder dat
Quality ze afkeurt. De Fabric-reactie maakte raw/Bronze/Quality-
reconciliatie en fysieke filelayout onderdeel van de latere scenario's.
Alleen eindtellingen of rejects vergelijken zou dit risico missen.

**5. Tijd is bedrijfssemantiek, niet een technische kolom om te negeren.**
Fabric gebruikt in dit bewijs authoritative batchcutoffs; Databricks
initialiseert runtime-load_date per notebook. Daardoor kan Rawtijd
voor BV-tijd liggen en een as-ofjoin andere businesswaarden opleveren.
De sessies spraken af toekomstig BV niet via deliverymapping naar
het verleden te schuiven. Een tijdadapter zou een afzonderlijke
versie-/harnessclaim zijn, geen neutrale normalisatie.

**6. Verschillende hashes vereisen aantoonbare identiteit.**
Technische hashes hoeven niet letterlijk gelijk te zijn tussen
platforms met andere hashcontracten. Maar "technische kolommen
uitsluiten" is te ruim. Nodig zijn unieke natuurlijke-keymaps,
bijectie per namespace/type en controle van foreign-keyconsistentie.
Many-to-many joins als reddingsmiddel zijn niet toegestaan.
Hetzelfde geldt voor decimalen: gelijke waarden binnen het overlappende
bereik maken DECIMAL(9,6) en DECIMAL(18,6) nog geen gelijke schema's.

**7. Een catalogsuffix is niet automatisch storageisolatie.**
Het voorstel voor een eigen testnamespace werd aangescherpt toen
generieke setup een gedeeld extern storagepad bleek te gebruiken.
De ontwerpchecks omvatten daardoor ook landing, checkpoints,
schemaLocation, current_state, locks, audit, pointers en hardcoded
paden. Zelfs een Bronze-preflight mag niet ongemerkt de latere
baselinecheckpoints consumeren. Het ontwerp blijft ongeprovisioned.

### Autonomie werkte hier als een begrensd mandaat

Bob gaf toestemming om zonder zijn aanwezigheid verder te gaan:
"ik moet er straks vandoor, maar ik geef jullie het akkoord om zonder
mijn aanwezig door te gaan".
Beide sessies interpreteerden dit als voortzetten van de huidige
analyse en afstemming, niet als intrekken van de eerdere grens
geen jobs/deployments/writes voor de referentieronde.

Dat leverde een concreet veiligheidsbesluit op. Het bestaande
warehouse was STOPPED; zelfs SELECT had auto-start kunnen veroorzaken.
De referentiesessie gebruikte daarom beperkte REST-/artifactinspectie
zonder SQL of compute te starten. De Fabric-sessie deed geen parallelle
remote Databricks-inspectie. Geen tokens werden gewijzigd, geen fixtures
geupload en geen nieuwe testresources aangemaakt.
Alleen de toegestane Fabric-verslaglegging werd bijgewerkt.

Ook identity werd niet op gemak gekozen. De bereikbare deployment
paste bij het project, maar de repo/documentatie wees naar een andere
host met DNS-falen. Artifact-projectbinding is daarmee gedeeltelijk
geverifieerd; de bedoelde referentieworkspace is nog niet aangewezen.
Een gitcommit in bundlemetadata bewijst bovendien geen schone deployed
tree, alle dependencies of actieve metadatawaarden.

Dit is een waarneembaar voorbeeld van begrensde autonomie:
de assistenten konden technische details uitwerken zonder elk punt
aan Bob voor te leggen, maar verruimden hun uitvoeringsmandaat niet zelf.
Er is geen garantie van doorwerken na afsluiten van VS Code of computer.

### Ook de frictie hoort in het verhaal

De samenwerking was niet foutloos of zonder overhead.
Berichten kwamen asynchroon binnen en opeenvolgende aanvullingen
herhaalden soms dezelfde status. G1-G9 werd tijdelijk verschillend
gegroepeerd; de Fabric-sessie corrigeerde dit en de referentiesessie
nam de canonieke nummering over.
Een aanvankelijk verkort genoteerde infinitydatum werd aangescherpt
naar de werkelijke sentinel 9999-12-31 23:59:59.
Voorgestelde transportdatums werden expliciet teruggezet op OPEN
om te voorkomen dat een suggestie als afspraak bleef staan.

Aan het eind besloten de sessies geen extra ontvangstbevestigingen
meer te sturen zonder inhoudelijk verschil. Dit voorkwam een ACK-loop:
meer communicatie is niet automatisch meer voortgang.
Hetzelfde gold voor remote onderzoek: toen de gerichte importvraag
was beantwoord, stopte de inventaris in plaats van dezelfde omgeving
steeds opnieuw te bekijken.

Deze correcties zijn geen voetnoten om weg te poetsen. Zij laten zien
dat afstemming tussen AI-contexten versiebeheer, ondubbelzinnige
statuslabels, stopcriteria en menselijke regie nodig heeft.
Het verslag registreert observeerbare uitwisseling en resultaten,
geen veronderstelde interne gedachten of mensachtige intenties van AI.

### Wat deze ronde wel en niet heeft opgeleverd

| Wel opgeleverd | Niet opgeleverd |
|---|---|
| Observeerbare gerichte afstemming tussen twee projectgebonden AI-sessies | Een bewijs van algemene betrouwbaarheid van multi-agent AI |
| Gedeeld C0-DESIGN-v0.1 met canonieke gates en zichtbare open keuzes | Executable akkoord of geprovisionde testomgeving |
| Gerichte code-/schema-/provenance-inventaris van een kandidaatdeployment | Volledige runtime-/metadata-/referentie-identiteitsattestatie |
| Concrete testvragen over historie, DQ, retries, keymapping en tijd | Nieuwe Databricks-outputmetingen met dezelfde Fabric-input |
| Scheiding oorspronkelijke baseline en eventuele correctierelease | Een al gemaakte correctierelease of volledige fase-6-acceptatie |
| Detailed verslaglegging inclusief fouten, grenzen en onbewezen claims | Gemeten kostenbesparing, snelheidswinst of vergelijking met mens-only uitvoering |

De eindstatus van de ronde blijft:
**paired design agreed; execution HOLD; output equivalence NOT_TESTED**.
De reeds bewezen Fabric-keten is waardevol, maar maakt de nog
open cross-platformvergelijking niet vanzelf geslaagd.

### Bronmateriaal en verantwoorde formuleringen voor LinkedIn

Een feitelijk verdedigbare kern voor een later artikel is:

> Ik bracht twee AI-sessies samen rond een metadata-gestuurde ETL-
> migratie: de ene met Fabric-implementatie en uitvoeringsbewijs,
> de andere met de Databricks-referentie. Ze wisselden niet alleen
> code uit, maar scherpten het bewijscontract aan: dezelfde input,
> zichtbare verschillen en geen groen vinkje zonder de juiste meting.
> De eerste gezamenlijke uitkomst was een gedeeld testontwerp met
> concrete blokkades, niet een voortijdige verklaring dat alles gelijk was.

Een tweede bruikbare observatie:

> De mens hield de regie over doel en toestemming. De AI-sessies
> konden zelfstandig analyseren, maar maakten van toestemming
> om door te gaan geen toestemming om compute te starten,
> fixtures te uploaden of de referentie stil te repareren.

Deze passages zijn redactiesuggesties, geen aanvullende letterlijke
uitspraken van Bob. Het artikel kan de spanning tussen enthousiasme
en bewijsdiscipline laten zien: juist de zichtbare HOLD-status maakt
het verhaal geloofwaardig. Niet claimen dat twee onafhankelijke
menselijke experts werkten, dat alle platformoutputs gelijk zijn,
dat productieacceptatie is bereikt of dat tijd/kostenwinst gemeten is.

Voor openbare publicatie hoeven tenant-/workspace-/job-/sessie-ID's,
interne paden, operationele schermen en persoonsgegevens niet mee.
Gebruik begrijpelijke procesvoorbeelden en geanonimiseerde details;
laat de volledige technische herkomst in het projectverslag.
Controleer screenshots apart op zichtbare accounts en omgevingdetails.

**Naslag voor de feitelijke onderbouwing:**
[Quality-inputreceipt](../deploy/evidence/phase6-sales-quality-v1-receipt.json),
[onafhankelijke outputverwachtingen](../src/fabric_etl_phase6/proof.py)
en [Fabric Gold/retry-verificatie](../deploy/evidence/phase6-sales-gold-v3-verification.json).
De F6-COLLAB-notities hierboven bevatten de berichtmarkers,
gerichte remote hashes, gatecorrecties en afbakening per stap.
Dit praktijkverslag is de leesbare synthese, geen vervanging van die bewijzen.

### F6-COLLAB-13 - Stap-voor-stap mandaat en eerste machineleesbare contractpin

**Menselijke opdracht, 12 oktober 2026, 12:35:40 CEST:** Bob geeft
akkoord voor contract pinnen, referentie bevestigen en geisoleerde
testinrichting voorbereiden, nadrukkelijk stap voor stap.
Dit vervangt geen ontbrekende toestemming voor provisioning, compute,
jobs, transfer, authwijziging of businesscodecorrecties.
De kandidaatworkspace is nog niet expliciet aangewezen.
Deze interpretatie is rechtstreeks aan de referentiesessie doorgegeven;
zij heeft lokaal bounded contractvelden aangeleverd zonder DB-writes.

Het [machineleesbare ontwerpcontract](../projects/contoso/reference-tests/c0-design-v0.1.json)
is nu lokaal gepind als DESIGN_PINNED_EXECUTION_HOLD, met
[externe digest](../projects/contoso/reference-tests/c0-design-v0.1.sha256.json).
Volledige canonieke SHA-256:
`0893cf612d1fd9ca639c23bf8807013d8b2cac198e6dce4a1f7b39cfaba36ae5`.
De digest staat buiten de gehashte contractinhoud.
De bestaande canonical_json-implementatie wordt gebruikt:
UTF-8, recursief gesorteerde objectkeys, compacte separators,
arrayvolgorde behouden, geen NaN, geen Unicode-normalisatie en
geen newline in gehashte bytes. Dit is geen hash van de pretty JSON-file.

Lokale validatie slaagt: JSON zonder dubbele keys, vijf lokale
evidence-/oraclepins, vijftien SHA/length-bestandidentiteiten,
drie cutoff-/batch-/fixturebindings, canonieke roundtrip, eventtelling
en G1-G9/autorisatiegrenzen. Dit is contractconsistentiebewijs,
geen enginecoercion, functionele meting of executable acceptatie.

Open velden zijn expliciet null/status/reason: referentiedesignation,
transportdatums, reference_date, volledige per-layer schemamatrix
en per-product comparatorveldvolgorde, DQ-rulevector/actieve metadata,
shared schemafingerprint, adapter-/transferkeuze, attemptnamespace/
paden/rechten en bootstrap-/harnessmanifests. Zes peer-gerapporteerde
seeds zonder individuele hashes zijn niet als volledige artifactpins
verzonnen. De veertien beschikbare remote artifactdigests zijn
peer-gerapporteerde API-inspectie, niet door Fabric opnieuw opgehaald.

**Bewaring:** contract en digest zijn op schijf opgeslagen.
Dat is nog geen Git-commit of push. De eerdere Git-inspectie toont
veel untracked Fabricbestanden; branch-/commit-/remotebewaring vraagt
een afzonderlijke gecontroleerde stap, zonder blind toevoegen van
lokale environments, credentials of tijdelijke bestanden.

### F6-COLLAB-14 - Seed-evidencecorrectie zonder de contractpin te overschrijven

**12 oktober 2026, 12:40 CEST:** de referentiesessie corrigeert
de Fabric-registratie: zes aanvullende seeddigests waren wel
aangeleverd in inventorybericht
`9519a456-fabe-4763-baae-09329f850cae`.
De eerdere formulering "niet geleverd" in F6-COLLAB-09/-13 en de
basecontractmetadata was dus onjuist; zij is geen bewijs dat de
referentiesessie die digests niet had geregistreerd.
De hashes zijn opnieuw ontvangen zonder nieuwe remote inspectie.

Het [seed-evidenceaddendum](../projects/contoso/reference-tests/c0-design-v0.1-seed-evidence-addendum.json)
bevat alle zes exacte hashes en de herkomstcorrectie, met
[externe digest](../projects/contoso/reference-tests/c0-design-v0.1-seed-evidence-addendum.sha256.json)
`ca65dc18a498c486671da36fe3f75c512356ce93746a9c820ede0aa3202accb5`.
De originele contractpin `0893cf612d1fd9ca639c23bf8807013d8b2cac198e6dce4a1f7b39cfaba36ae5`
is niet herschreven. Canonical_json-v1-validatie bevestigt de
ongewijzigde basehash en zes unieke artifactnamen met geldige digests.
Het addendum corrigeert uitsluitend evidencebeschikbaarheid:
peer-gerapporteerde API-fileinspectie, geen actieve metadata-rijhashes.

De Databricks-sessie heeft de contractinhoud niet onafhankelijk
gelezen/gevalideerd. Ontwerpovereenstemming over de samenvatting
wordt niet als onafhankelijke contractvalidatie opgevoerd.
Execution HOLD en ontbrekende reference designation blijven gelden.
Dit is ook samenwerkingsbewijs: een concrete fout in evidenceoverdracht
wordt gecorrigeerd met behoud van de oorspronkelijke immutable pin.

### F6-COLLAB-15 - Mens wijst kandidaatworkspace expliciet aan

Na "ga maar door" om 12:49 CEST heeft de Fabric-sessie de eerstvolgende
open menselijke gate afzonderlijk voorgelegd. Bob koos expliciet:
"Ja, wijs workspace 7405617094653375 aan als testreferentie".
De [aanwijzingsbeslissing](../projects/contoso/reference-tests/c0-reference-designation.json)
legt dit vast buiten de ongewijzigde C0-DESIGN-v0.1-pin.

Designation is daarmee HUMAN_DESIGNATED_TEST_REFERENCE.
De eerdere BLOCKED_UNVERIFIED_IDENTITY-status wegens ontbrekende
aanwijzing is voor die deelvraag opgelost; volledige release-/metadata-/
runtime-identiteit blijft onbevestigd. Hostdrift en oorspronkelijke
configuratie blijven als historisch bewijs staan. Er is geen
provisioning-, compute-, job-, upload- of correctietoestemming verleend.
Execution blijft HOLD. Volgende werk betreft lokale schemamatrix,
comparatorveldvolgorde en isolatievoorbereiding; datums/reference_date
blijven nog open.

### F6-COLLAB-16 - Lokale productmatrix en correctie van rateprecision

Na Bobs opdracht zelfstandig verder te gaan bij afwezigheid is de
[Fabric-schemavoorbereiding](../projects/contoso/reference-tests/c0-fabric-schema-preparation.json)
vastgelegd, met elf expliciete geordende businesskolomlijsten,
grains en afzonderlijke key-/tijd-/lineagecategorieen.
Typen/nullability verwijzen naar exacte schema-ID's in de gepinde
releasefile, niet naar een onbewezen runtime-schema.
[Externe canonieke digest](../projects/contoso/reference-tests/c0-fabric-schema-preparation.sha256.json):
`7d63676f435fc107132bee97bc5ea33ddf9aeca7e1bffca6f8c92877ab8ea717`.

Hierbij is een eigen eerdere fout ontdekt: de Fabric-release verklaart
discount_rate als DECIMAL(9,6) in BV-mapping/schema en GH/GC sales,
niet DECIMAL(18,6). De lokale release-ID is gecontroleerd gelijk aan
die in de succesvolle Gold-v3-receipt. Bundlecode embedt de
projectmetadata rechtstreeks en Gold gebruikt de gedeclareerde schema's.
De eerdere claims in contract/afstemmingsnotities van een 18,6-vs-9,6
schema-afwijking zijn voor deze gedeclareerde velden dus gecorrigeerd
door dit externe voorbereidingsdocument, zonder de basepin te wijzigen.
Status: NOMINAL_DECLARED_TYPES_MATCH_9_6, geen volledige schemapass
en geen nieuwe remote stored-type-inspectie.
Exacte zes-decimalenvergelijking zonder float/tolerantie blijft gelden.

Lokale validatie bevestigt elf volledige businesskolomlijsten tegen
de schemadeclaraties, geen dubbele kolommen, juiste productscope,
releasefilehash, rateverklaringen, ongewijzigde basepin en beperkt
designationmandaat. Isolatiechecklist is voorbereid; concrete paths,
rechten en bootstrap/harness zijn nog open. Geen cloudresources
gemaakt of jobs gestart.

De Databricks-sessie is gevraagd alleen uit bestaande lokale evidence
haar productmatrix aan te vullen, zonder remote inventaris of writes.
De ratecorrectie is rechtstreeks aangeboden via bericht
`78c0071d-1403-4a36-ba44-2b5290097317`.
Ook dit hoort in het samenwerkingsverhaal: de Fabric-kant wordt
even kritisch gecorrigeerd als de referentie, op basis van eigen codebewijs.

De controles zijn duurzaam opgenomen in
[reference-contractregressies](../tests/test_fabric_reference_contract.py):
**7 passed in 0.48 s**. Ze bewaken externe digests, inputidentiteiten/
cutoffs, elf kolomlijsten, lokale evidencepins en dat referentieaanwijzing
geen uitvoeringsmacht of gekozen transportdatums oplevert.
Editorcontrole meldt geen fouten. De editor-testrunner ontdekte het
nieuwe bestand nog niet; de bestaande pytest-runner is daarom direct
met de geselecteerde interpreter gebruikt. Geen nieuwe tooling toegevoegd.
De eigen matrix is aan Databricks aangeboden via bericht
`eaf38877-d719-4943-b59c-17e208764e6d`; exacte cross-platformmapping
blijft pending totdat de inhoudelijke tegenmatrix is ontvangen/getoetst.

### F6-COLLAB-17 - Tegenmatrix ontvangen en elf declaraties gecombineerd

**12 oktober 2026, 12:53 CEST:** de Databricks-sessie levert de
gevraagde vaste kolomvolgorde, types/nullability en grains voor
GH5/GC6 uit lokale repo en eerder gecachte RESTmetadata.
Geen nieuwe remote calls, writes of runs.
De [cross-platformmatrix](../projects/contoso/reference-tests/c0-cross-platform-matrix.json)
combineert deze met de gepinde Fabric-declaraties.
[Canonieke digest](../projects/contoso/reference-tests/c0-cross-platform-matrix.sha256.json):
`4e5f842048ed8586e707bf719fdeef5103fe6392cf51eae6757ac55779f5e01c`.
De oorspronkelijke contractpin blijft ongewijzigd.

Elk product heeft expliciete business-, temporal- en technical/audit-
kolomgroepen. Fact product/customer/employeekeys zijn comparator-
afleidingen via unieke committed as-of hubmaps, geen fysieke
factkolommen en geen lookup in deleted-filtered GC-dims.
Order-/returnhashes worden ook tegen hun fysieke natuurlijke keys
gecontroleerd. Mappinguniciteit en werkelijk committed payloads
zijn nog onbewezen. A-B-A historie mag niet tot twee waarden worden
gereduceerd. Halfopen grenzen/currentflags en echte laagtijden blijven
aparte verplichte checks.

GH-nullability is peer-gerapporteerd uit gecachte UC-inspectie.
GC-businessnullability is slechts repo-declaratie: CTAS kan die
veranderen. Vergelijking van declaraties is dus geen runtime-
schemaattestatie. Kalendertekst blijft exact; runtime locale onbekend.
De teruggekeerde 18,6-rateclaim in het peerbericht is opnieuw expliciet
gecorrigeerd naar nominale gedeclareerde 9,6 aan beide kanten.

De lokale matrixregressie controleert voor elf producten alle
declaratiekolommen, types, nullableflags, afgeleide keyvelden en
natural grains. Dit bewijst consistente comparatorvoorbereiding,
niet gelijke outputs. Nieuwe DQ-matrix, concrete tijdkeuzes,
isolatiepaden/rechten en uitvoeringsautorisatie blijven open.

**12:55 CEST, expliciete peer-correctie:** de Databricks-sessie trekt
haar eerdere "versus Fabric 18,6"-passage in voor BV/GH/GC
discount_rate. Beide verklaren nominaal DECIMAL(9,6);
Databricks stored GH/GC-type is eerder API-inspected volgens de peer,
Fabric stored type is niet opnieuw remote geinspecteerd.
G5 blijft OPEN voor comparator-/stored-schemabewijs, niet als een
nog geldende rate-SCHEMA_MISMATCH. Deze bevestiging vervangt
uitsluitend de eerdere rate-afwijkingsclaim, niet de volledige
stored-schemaattestatie. De persistente matrix-/pinregressies slagen:
**9 passed in 0.17 s**, editorcontrole zonder fouten.
Geen extra ACK verstuurd, remote reads of uitvoering gestart.

### F6-COLLAB-18 - Inhoudelijke tegencontrole en expliciete relationshipaliases

**12:55 CEST:** de Databricks-sessie heeft de Fabric-preparationfile
en designationfile daadwerkelijk read-only gelezen. Alle elf fysieke
businesskolomlijsten komen volgens haar inhoudelijke toets overeen
met de lokale DB-projecties, ook qua volgorde.
Dit is FILE_CONTENT_INSPECTED/names-order-match, geen volledige
type/nullability-/stored-schema-/outputpass. Zij heeft de
preparationdigest niet onafhankelijk herberekend en de Fabric-releasefile
niet gelezen. Die claims blijven peer-reported aan haar kant.

Het [relationshipaddendum](../projects/contoso/reference-tests/c0-mapped-relationship-addendum.json)
legt de noodzakelijke verfijning vast buiten de bestaande pins:
fysieke businesskolommen blijven de elf oorspronkelijke lijsten;
de vier facts krijgen een afzonderlijke array met
mapped_product_key, mapped_customer_key en mapped_employee_key
aan beide platformkanten. Dit vervangt de onvoorziene onprefixed
afgeleide aliases in de eerste cross-platformmatrix.
Product is deel van de driving grain, customer/employee relaties
zijn te vergelijken attributen. Niet-null FK zonder unieke
committed as-ofmatch wordt MAPPING_ERROR, geen nullfallback.

Alle hashkolommen blijven expliciet onder namespace-/typebijectie
en FK-/linkgraincontroles; de scheiding in de lokale voorbereiding
is geen suffixdrop in de uiteindelijke comparator.
Historische alignment behoudt fysieke key/timestampidentiteit,
multipliciteit, grenzen/currentflags en A-B-A-events.
Volledige comparatorpin is nog PENDING.
[Externe addendumdigest](../projects/contoso/reference-tests/c0-mapped-relationship-addendum.sha256.json):
`504f25f4a0f137c09b5094ccdcf9752651ccb27f8bbd724a78cc53328e1c9e8d`.
Geen oorspronkelijke contract-/preparation-/matrixpin gewijzigd.

**Aanvullend lokaal verificatiebewijs, 12:57 CEST:** de
Databricks-sessie rapporteert een in-memory PowerShell-vergelijking
van de elf preparationlijsten met de top-level projecties in lokale
gepinde Gold-selectSQL, gebonden aan DB HEAD
`bd8936dc727808ec6d1ba2eaba551f647a3b10ee` en preparationdigest
`7d63676f435fc107132bee97bc5ea33ddf9aeca7e1bffca6f8c92877ab8ea717`.
Resultaat: LOCAL_PROJECTION_COMPARISON_VERIFIED, alle elf exact
qua fysieke businessnamen en volgorde. Aantallen:

| Product | GH fysieke businesskolommen | GC fysieke businesskolommen |
|---|---:|---:|
| Customer | 15 | 14 |
| Product | 10 | 9 |
| Employee | 7 | 6 |
| Sales | 18 | 18 |
| Returns | 10 | 10 |
| Date | Niet in GH-scope | 11 |

Dit is externe matrixevidence, geen wijziging van de oorspronkelijke
pins en geen type/nullability-, runtime- of outputpass.
Hash-, temporal- en record_source-velden zijn bij deze vergelijking
expliciet afgescheiden; loader-added auditvelden staan niet in
select_sql en moeten aanvullend via loader/schema worden afgedekt.
De eigen matrix controleert reeds de volledige gedeclareerde
kolomset inclusief die auditvelden, maar daadwerkelijke stored
GC-nullability blijft onbekend.
De peer meldt geen filewrites, remote calls of compute en een schone
main-checkout. Geen aanvullend ACK gestuurd.
Eigen persistente contract-/matrix-/mappingregressies:
**11 passed in 0.52 s**, editorcontrole zonder fouten.

**12:57:38 CEST, verdere inhoudelijke tegencontrole:** de
Databricks-sessie heeft de cross-platformmatrix gelezen en lokaal
getoetst tegen haar eerder gecachte UC-GH-schema's.
GH physical fieldsets/types/nullability matchen volgens deze toets:
customer 22, product 17, employee 14, sales 29 en returns 22 velden.
Drie afgeleide natuurlijke factsleutels zijn uitsluitend buiten de
physical-schemaassertion gehouden, niet buiten businessvergelijking.
Status GH_MATRIX_MATCHES_PREVIOUSLY_API_INSPECTED_DB_SCHEMA;
voor Fabric blijft dit peer-gerapporteerd lokaal verificatiebewijs.
De matrixdigest is door de peer niet onafhankelijk herberekend.
GC-fullnullability en Fabric-releaseinhoud zijn niet aanvullend
door haar geverifieerd; outputs blijven NOT_TESTED.

De peer accepteerde nu ook onprefixed derived aliases in de oorspronkelijke
matrix. Om aliaswisselingen te voorkomen heeft Fabric gericht verduidelijkt
dat het inmiddels gepinde relationshipaddendum voorrang heeft:
de effectieve vergelijking gebruikt aparte mapped_relationship-
kolommen met mapped_product_key/customer_key/employee_key
(alle drie met mapped_-prefix) en mapped_product_key in de factgrain.
De oorspronkelijke matrix blijft onveranderd historisch bewaard;
geen stil terugdraaien van het addendum. Deze enige inhoudelijke
precisering is aangeboden via bericht
`bb805778-2e44-4611-a2e9-544437d2e7b1`.
Volgende voorbereiding richt zich op DQ-rulevector/shared RAW-schemahash
en isolatie-/tijd-/rechten-/bootstrapgates, niet herhaalde Goldinventaris.

### Dagafsluiting - 12 oktober 2026, 16:58 CEST

Bob pauzeert het werk met "we gaan morgen verder".
Geen verdere voorbereiding of uitvoering namens de huidige opdracht.
De aangewezen Databricks-sessie wordt gevraagd eveneens te pauzeren.

**Hervatpunt voor 13 oktober:** DQ-rulevector, gezamenlijk RAW-
schemahashcontract en isolatie-/tijd-/rechten-/bootstrapvoorbereiding.
Gold-productmatrix en relationshipaddendum zijn lokaal vastgelegd;
elf gerichte regressietests slagen. Referentieworkspace is menselijk
aangewezen, maar volledige release-/metadata-/runtime-identiteit
en cross-platformoutputs zijn niet bewezen. Execution blijft HOLD;
geen provisioning, compute, jobs of fixture-overdracht toegestaan.

Bestanden zijn lokaal opgeslagen; Git-commit en push van deze
voorbereidingsronde zijn nog niet uitgevoerd. Gecontroleerde
branch-/commit-/remotebewaring blijft een afzonderlijk aandachtspunt.
Geen autonome voortgang na het sluiten van VS Code toegezegd.

### Git-bewaring op verzoek - 12 oktober 2026

Na de dagafsluiting vraagt Bob expliciet "graag pushen".
De Fabric-voortgang wordt daarom op een afzonderlijke branch
`fabric/phase6-sales-reference-preparation` gecommit en naar origin
gepusht, zonder Databricks-baseline/main samen te voegen of aan te passen.
Scope: Fabric-framework/notebooks/deployment, project-/environment-
configuratie, bewijsreceipts/plannen, tests en documentatie.
Lokale environments, buildoutputs en caches blijven buiten Git.

Vooraf zijn de pending paden, README-diff, bestandsgroottes,
ignore-regels en gerichte credentialpatronen gecontroleerd;
geen matches voor de gecontroleerde secretpatronen.
Dit is een beperkte precommitcontrole, geen volledige securityaudit.
De volledige Fabric-regressiesuite slaagt:
**581 passed, 4 skipped in 19.83 s**. De vier opt-in Sparktests
zijn niet opnieuw gestart; hun eerdere uitvoeringsbewijs blijft
afzonderlijk beschreven. Pushstatus wordt na remote readback bevestigd;
deze registratie op zichzelf bewijst nog geen succesvolle push.

Git waarschuwde bij staging voor automatische LF/CRLF-conversie.
Daarom bewaart de nieuwe .gitattributes de Fabric-runtime/configuratie/
project-/evidencepaden bytegetrouw: release-, SQL- en bewijschecksums
mogen niet veranderen door een Windows-checkout. Geen globale
Git-configuratie gewijzigd. Markdown hard-breakspaties in bestaande
fase-4-notities blijven intact; dit is geen code-whitespacefout.

**Remote bewaring bevestigd:** commit
`4e74fafe19d77a4fc048454597da55bf413940b0` is succesvol gepusht naar
`origin/fabric/phase6-sales-reference-preparation`.
Git ls-remote geeft exact dezelfde SHA terug. De worktree is schoon.
De stagingcontrole bevestigde voor 249 checksumdragende bestanden
exacte bytegelijkheid met de werkbestanden na toepassing van attributes.
Main blijft de oorspronkelijke Databricks-baseline; geen merge,
Databricks-wijziging of platformjob uitgevoerd.
