# Fabric-template: fase 1 baseline en acceptatiecontract

Datum: 10 oktober 2026. Onderdeel van het
[migratieverslag](09_fabric_migratieverslag.md).

## 1. Mandaat, grenzen en status

De opdrachtgever geeft toestemming fase 1 zelfstandig uit te voeren en
binnen die fase besluiten te nemen en te implementeren zonder afzonderlijk
akkoord. Fase 1 legt de baseline, broncontracten en acceptatiecriteria vast.
Dit is geen opdracht om productiegegevens te wijzigen of fase 2-10 stilzwijgend
als goedgekeurd of gerealiseerd te verklaren.

**Actuele status: fase 1 afgerond op 10 oktober 2026.** Inventarisatie,
bron-/laagscope, lokale regressiebaseline, risico's en technische acceptatiebasis
zijn vastgelegd en gecontroleerd. Geen Fabric-implementatie of productieacceptatie.
Oude Databricks-referenties zijn geen fase-exitvoorwaarde. Secties 9 en 10
bepalen de actuele acceptatiebasis en overdracht; eerdere statussen zijn historisch.

## 2. Reproduceerbare lokale baseline

| Onderdeel | Waarneming |
|---|---|
| Broncommit | `bd8936d` op `main`; lokale documentwijzigingen behouden |
| Interpreter voor lokale controle | Python 3.13.3, `C:\Python313\python.exe` |
| Baselinesuite | `python -m pytest -q`: **133 passed in 2.78s** |
| Metadatarelease | `python -m contoso_lakehouse.release_check`: geldig |
| Releasefingerprint | `40b0fbb42f32c19b0ca4417f41b561af631d7d22a2e6730133c9f0f895a2694a` |
| Databricks CLI | v1.7.0; `auth profiles` meldt alle gevonden profielen als niet geldig |
| Fabric | Workspace-runtime 2.0 ingesteld volgens UI; geen uitvoeringsproef |

De VS Code-testrunner ontdekte geen tests. Daarom is dezelfde bestaande
testsuite rechtstreeks met pytest uitgevoerd. Geen dependencies geinstalleerd,
geen externe job gestart en geen data geschreven.
De tests bewijzen geen Spark/Delta-runtimecompatibiliteit met Fabric.
De fingerprint identificeert geparste metadata, niet ieder byteverschil in Git.

## 3. Bevestigd broncontract

Alle negen actieve objecten zijn verplicht in hun levering. Contractversie is
in de seed voor deze objecten `1.0`. De onderstaande waarden komen uit
[bronobjectmetadata](../metadata/seed/meta_source_object.json).

| Object | Natuurlijke sleutel in Bronze | Strategie / route | Driftbeleid | Freshness-uren in geparste seed |
|---|---|---|---|---:|
| SALES.CUSTOMERS | customer_key | SNAPSHOT_SCD2 / RAW_VAULT | Approval | 24 |
| SALES.PRODUCTS | product_key | SNAPSHOT_SCD2 / RAW_VAULT | Approval | 26 |
| SALES.ORDERS | order_key, order_line_number | INCREMENTAL_APPEND / RAW_VAULT | Approval | 26 |
| SALES.RETURNS | return_key | INCREMENTAL_APPEND / RAW_VAULT | Approval | 26 |
| SALES.EMPLOYEES | employee_key | SNAPSHOT_SCD2 / RAW_VAULT | Approval | 26 |
| CBS.JEUGDZORG_WIJK_2025 | Vormen, Wijken, Perioden | SNAPSHOT_SCD2 / RAW_VAULT | RESCUE | 720 |
| ECB.EXCHANGE_RATE | currency_code, rate_date | SNAPSHOT_SCD2 / REFERENCE_DATA | RESCUE | 30 |
| NAGER.HOLIDAYS_NL | date, countryCode, name | SNAPSHOT_SCD2 / REFERENCE_DATA | RESCUE | 2160 |
| FABRIC_SALES.ORDER_LINES | sales_order_detail_id | SNAPSHOT_SCD2 / REFERENCE_DATA | RESCUE | 26 |

SALES-route is een runtime-default; Approval betekent
`ALLOW_NEW_COLUMNS_WITH_APPROVAL`. Quality-sleutels en typen volgen uit de
mappings en kunnen andere kolomnamen hebben dan de Bronze-sleutels.

Customers, Products en Employees hebben soft-delete-flags. Fabric Sales heeft
een soft-delete-flag en `absence_means_delete=true`; ontbreken mag daar alleen
op een bewezen complete snapshot als verwijdering worden behandeld.
Andere actieve bronnen hebben geen absence-deletecontract.
ECB filtert voor Quality op
`try_cast(OBS_VALUE AS decimal(18,8)) IS NOT NULL`; weggefilterde records moeten
apart verantwoord worden, niet als onverklaard verlies of als Reject.

### Governance en productfreshness

De [governanceseed](../metadata/seed/meta_data_governance_policy.json)
classificeert SALES en FABRIC_SALES als CONFIDENTIAL; CBS, ECB en NAGER als
PUBLIC. Zij configureert 2555 retentiedagen. Dit is een bestaande instelling,
geen bewijs van juridische goedkeuring of technisch afgedwongen retentie.
Vertrouwelijke bronpayloads worden niet als testfixture in Git opgenomen.

| Productgroep | Bestaande refresh-SLA in uren |
|---|---:|
| SALES_MART | 24 |
| CBS_JEUGDZORG_MART | 192 |
| ECB_REFERENCE_MART | 30 |
| NAGER_REFERENCE_MART | 800 |
| FABRIC_SALES_MART | 30 |

Bron: [Gold-productmetadata](../metadata/seed/meta_gold_data_product.json).
Productfreshness en objectfreshness zijn verschillende meetpunten.
Deze waarden worden als configuratiebaseline behouden, niet als bewezen
looptijden. CBS/Nager-planning en leveringfrequentie moeten bij het definitieve
SLA-ontwerp consistent worden gemaakt met deze termijnen.

## 4. Bewijsregister

| ID | Bewijs | Niveau en beperking |
|---|---|---|
| E-01 | Lokale suite: 133 tests geslaagd | Uitgevoerd in deze sessie; geen Fabric-/Databricks-runtimeproef |
| E-02 | Metadatarelease geldig, fingerprint in sectie 2 | Uitgevoerd in deze sessie; bestaande validator detecteert niet alle ambiguiteiten |
| E-03 | Sales-runs `138888199822102` en `321288909336916`, deliveries 2026-09-09 en 2026-09-10 | Historisch gedocumenteerd in [besluitenlog](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/docs/00_besluitenlog.md); niet live herbevestigd |
| E-04 | Stressgenerator `291249352899112`: 100k Customers, 50k Products, 10k Employees, 1m Orders, 20k Returns, 140 bestanden | Documentatiebewijs voor gegenereerde levering, geen Fabric-capaciteitsmeting en geen bewijs dat juist deze levering succesvol Gold bereikte |
| E-05 | FABRIC_SALES 2026-09-06: 542 rijen per beschreven laag | Historisch beschreven in [projectverslag](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/docs/06_praktijk_experiment_verslag.md); dataset/run nog niet live gecontroleerd |
| E-06 | Sales-run `775524928887640`: gate SKIPPED | Projectverslag; technische jobsuccess is geen succesvolle businessload |
| E-07 | ECB 2026-09-05 met 262410 actuele records | Historische claim in [artikel](https://github.com/bobstoelinga/Contoso_lakehouse_v2/blob/bd8936d/docs/07_linkedin_artikel.md); ondersteunende run-/datasetreferentie nog verzamelen |
| E-08 | CBS en Nager referentie-uitkomsten | Niet beschikbaar als geverifieerde referentiedataset in deze fase |

Databricks-authenticatie is een externe blocker. Ongeldige CLI-profielen
bewijzen niet dat de workspace of gebruiker geen toegang heeft. Er wordt geen
token omzeild, gepubliceerd of uit een browser gehaald.
Voor livecontrole is een geldige ondersteunde aanmelding nodig.

**Correctie op 10 oktober 2026:** de opdrachtgever meldt dat de Databricks-
omgeving weg is. Authenticatie vernieuwen is daardoor geen zinvolle vervolgstap.
Er worden geen nieuwe CLI-/aanmeldpogingen voor die omgeving gedaan.
Historische bewijsclaims blijven behouden als documentatie, niet als live
controleerbare referentie. Eventueel elders bewaarde datasets of exports zijn
nog niet aangetoond; hun bestaan of afwezigheid wordt niet verondersteld.

## 5. Baselinebevindingen en architectbesluiten

| ID | Feit / status | Besluit en vervolg |
|---|---|---|
| A-01 | Customers bevat `freshness_sla_hours` tweemaal: 26 en 24. De huidige JSON-parser gebruikt 24 | Bestaande seed niet stilzwijgend wijzigen; 24 als geparste baseline vastleggen. Nieuwe templatevalidatie moet dubbele JSON-velden weigeren |
| A-02 / V-01 | Twee `RunContext.create`-aanroepen met dezelfde batch/delivery krijgen verschillende tijden bij een gecontroleerde klok | Gereproduceerd zonder Spark of writes. Persistente gedeelde context verplicht voor Fabric; herstel hoort bij de nieuwe implementatie |
| A-03 / V-02 | Satellitehistorie wordt per batch gewist, state wordt apart bijgewerkt | Failure-injectionproef verplicht; verlieshypothese nog niet in Delta gereproduceerd |
| A-04 / V-03 | Gesloten-manifestfilter staat voor rangschikking van deliveries | Contract: oudste niet-uitgezonderde geregistreerde delivery blokkeert nieuwere bij incompleetheid; runtimeproef nog nodig |
| A-05 / V-04 | Runtimecode gebruikt groepspointer; oude workflowtekst beschrijft viewwissels | Code als baseline; readerconsistentie apart bewijzen |
| A-06 / V-05 | Generieke loader ondersteunt PIT niet als loadtype | Geen PIT-acceptatie op basis van DDL-aanmaak; feitelijke route en noodzaak aantonen |
| A-07 / V-06 | DML-resultaatvorm kan auditcount nul opleveren | Expliciete Fabric-metrics verplicht; geen succesbewijsvorm gebaseerd op een stil nulresultaat |
| A-08 | Eigen v2-projectverslag noemt security, recovery, deletes en performance nog amber/rood | V2 is een referentieontwerp, geen overdraagbaar productiecertificaat |

De contextproef gebruikte klokwaarden 05:00 en 05:01 UTC met batch
`baseline-probe`. Resultaat: batch gelijk, `load_date` ongelijk.
Dit bewijst het factorygedrag; geen externe verwerking is uitgevoerd.

## 6. Meetbaar acceptatiecontract

Deze technische criteria worden onder het fase-1-mandaat als uitgangspunt
besloten. Geen fictieve business-SLA of meetresultaat wordt toegevoegd.

| ID | Criterium | Vrijgavevoorwaarde |
|---|---|---|
| AC-01 | Compleetheid | Iedere vereiste delivery-object/file verklaard; niet-gesloten/onvolledige levering publiceert niets |
| AC-02 | Reconciliatie | Gefilterde Bronze = Quality + Reject, verschil exact 0; filters apart geteld; recordidentiteiten en multipliciteit gecontroleerd |
| AC-03 | Referentiegelijkheid | Dezelfde bevroren input en contractversie geven dezelfde businesskeys en afgesproken businesswaarden; 0 onverklaarde verschillen |
| AC-04 | Hashing | Hashkey/hashdiff exact gelijk voor dezelfde invoer/conventie; technische timestamps/ID's niet blind vergelijken |
| AC-05 | Herstart | Zelfde delivery opnieuw uitvoeren en falen op relevante commitgrenzen: 0 ongewenste duplicates en 0 ontbrekende historie |
| AC-06 | Chronologie | Oudste niet-uitgezonderde incomplete geregistreerde delivery blokkeert nieuwere; uitzondering uitsluitend expliciet en auditeerbaar |
| AC-07 | Context | Dezelfde persistente batchcontext en UTC-loadtijd in alle relevante lagen en retries; afwijking exact 0 |
| AC-08 | Historie/deletes | Verwachte versies, geldigheidsintervallen en deletes exact volgens referentiescenario; absence-delete alleen bij complete snapshot |
| AC-09 | Publicatie | Bij build/publicatiefalen blijft vorige complete release leesbaar; geen gemengde release via de gekozen consumerroute |
| AC-10 | Operaties | Iedere geinjecteerde fout zichtbaar als FAILED plus afgesproken alert; geen success-shaped fallback |
| AC-11 | Deployment | Lege-workspace-uitrol en herdeploy/upgrade slagen zonder onbedoeld verlies, credentialkopie of orphan-deletion |
| AC-12 | Rechten | Alle toegestane en verboden toegangsscenario's geven de verwachte uitkomst, via SQL/Spark/opslag waar relevant |

Decimalen worden exact vergeleken na het afgesproken type-/schaalcontract.
Voor floats wordt alleen een expliciet onderbouwde kolomspecifieke tolerantie
toegestaan; een generieke tolerantie mag verschillen niet verbergen.
Publieke API's worden niet twee keer live gelezen als gelijkheidsreferentie.

### Nog niet verantwoord vast te stellen

- Definitieve productievolumes en groei: gedocumenteerde stressvolumes zijn
  een kandidaatprofiel, geen bevestigd productievolume.
- RPO/RTO, alertresponstijden en maximale jobduur: geen gemeten of bevestigde
  waarden; geen productie-vrijgave totdat zij zijn vastgesteld en getest.
- Kostenplafond en duurzame productiecapaciteit.
- Juridisch/organisatorisch goedgekeurde retentie en bewaarlocatie van fixtures.
- Consumerroute en bijbehorende publicatie-/refreshconsistentie.

Een architect kan technische nulafwijkingscriteria vaststellen, maar ontbrekende
bedrijfscontinuiteitsdoelen niet als feiten verzinnen. Deze punten blokkeren
productieacceptatie; niet de voorbereiding van het templateontwerp.

## 7. Volgende bewijsstappen en fase-exit

1. Live Databricks-verificatie vervalt na de melding dat de omgeving weg is.
   Onderzoek beschikbare repositoryfixtures, generators en eventueel bewaarde
   exports zonder de verdwenen omgeving opnieuw op te bouwen.
2. Per bron een toegestane immutable referentielevering registreren met pad,
   schema, inhoudsfingerprint, aantallen, delivery/run-ID en metadatarelease.
   Confidential payloads buiten Git en volgens toegangsbeleid bewaren.
3. Historisch bewijs koppelen aan gemeten verwachte Quality/Reject-, historie-
   en Gold-uitkomsten; verschillen in contractversies expliciet verklaren.
4. Open failure-scenario's op een afgebakende testomgeving reproduceren,
   niet op productiegegevens.
5. Productie-SLA's, volume-/kostenprofiel en hersteldoelen vaststellen.

**Fase-exit:** broncontracten plus verifieerbare referentiedata en meetbaar
acceptatiecontract. Op dit moment zijn de lokale baseline en technische
criteria gereed; externe bewijsverwerving en bedrijfsdoelen staan open.
Geen commit/push of Fabric-deployment uitgevoerd.

## 8. Gewijzigde bewijsstrategie: Databricks-omgeving verdwenen

**Aanleiding:** melding van de opdrachtgever op 10 oktober 2026.
**Besluit B-FAB-013, architect onder fase-1-mandaat:** geen live Databricks-
verificatie of herbouw als verplichte acceptatievoorwaarde; toets Fabric aan
expliciete bron-/businesscontracten en reproduceerbare referentiescenario's.
Dit verlaagt de technische correctheidscriteria niet, maar verandert het
soort bewijs dat haalbaar is.

### Bewijscategorieen

1. **Repositorybaseline:** commit, metadata, DDL, generators en geslaagde
   lokale tests beschrijven de beschikbare implementatie.
2. **Historisch documentatiebewijs:** vermelde runs en aantallen blijven
   traceerbaar, maar zijn niet opnieuw uitgevoerd of geverifieerd.
3. **Synthetische golden datasets:** nieuwe, kleine immutable leveringen met
   vooraf gespecificeerde verwachte keys, waarden, DQ-uitkomsten, wijzigingen,
   deletes en historie. Verwachtingen worden niet uitsluitend met dezelfde
   te testen loader berekend; handmatig controleerbare voorbeelden of een
   onafhankelijke referentieberekening zijn noodzakelijk.
4. **Vastgelegde bronsnapshots:** toegestane publieke API-snapshots of
   beschikbare exports kunnen realistische integratietests ondersteunen.
   Provenance, peildatum, contractversie, fingerprint en gebruiksrechten
   moeten worden vastgelegd. Vertrouwelijke exports blijven buiten Git.
5. **Fabric-uitvoeringsbewijs:** echte runs, failure-injection, herdeploy,
   concurrency- en consumerproeven op deze referentieleveringen.

AC-03 betekent nu: dezelfde vastgelegde input levert de vooraf afgesproken
businessuitkomsten, met nul onverklaarde verschillen. Exacte platformpariteit
met Databricks kan alleen worden toegevoegd als passende bewaarde input
**en** output met herkomst alsnog beschikbaar zijn.
AC-04 vereist de vaste hashconventie en onafhankelijk controleerbare
referentiehashes; geen verzonnen Databricks-runtime-uitkomsten.

### Alternatieven en waarom

- Wachten op aanmelding heeft geen nut bij een verdwenen omgeving.
- Databricks opnieuw bouwen brengt kosten en een andere runtime-/datastaat
  mee en herstelt niet automatisch de historische werkelijkheid.
- Alleen historische aantallen vergelijken is te zwak: gelijke counts
  bewijzen geen gelijke inhoud, historie of foutafhandeling.
- Contracttests met onafhankelijke golden verwachtingen leveren wel
  reproduceerbaar correctheidsbewijs voor het Fabric-template.

### Uitvoering, beperkingen en vervolg

Uitgevoerd: acceptatiecontract en bewijsregister aangepast. Geen datasets
gegenereerd, nieuwe API-extracten uitgevoerd of Fabric-tests gestart in deze
correctiestap.

Volgende uitvoering binnen fase 1: beschikbare generators/fixtures onderzoeken,
een referentiescenariomatrix voor alle negen objecten opstellen en kleine
synthetische golden datasets met onafhankelijke verwachtingen implementeren.
Beschikbaarheid van het Fabric SQL-bronsysteem moet apart worden vastgesteld;
het verdwijnen van Databricks bewijst niet dat deze bron verdwenen is.

De resterende blockers zijn referentiedata/verwachtingen en ontbrekende
productie-/hersteldoelen, niet Databricks-authenticatie. Fabric-correctheid
kan aantoonbaar worden; historische gelijkheid zonder oud input-/outputbewijs
blijft expliciet onbewezen.

## 9. Definitieve doelcorrectie: zelfstandige Fabric-correctheid

Op 10 oktober 2026 verduidelijkt de opdrachtgever: een goed functionerend
systeem is het uitgangspunt; aansluiting op de oude omgeving is niet nodig.
Dit vervangt het vergelijkingsgerichte deel van secties 4, 6, 7 en 8.

**Besluit B-FAB-014:** bouwen en accepteren op de eigen functionele en
operationele contracten van het Fabric-template. Geen Databricks-pariteit,
historische referentiedata of reconstructie van oude resultaten vereist.
De bestaande code en metadata zijn herbruikbare ontwerpinput, geen
verplichting om ieder oud gedrag of iedere oude beperking over te nemen.

### Wat vervalt?

- Databricks-referentieruns of exports als voorwaarde voor ontwikkeling.
- Exacte overeenstemming met oude hashwaarden, rijenaantallen, schema's,
  fysieke namen, runtimes of businessuitkomsten.
- Golden datasets maken uitsluitend om het verdwenen systeem na te bootsen.
- Live vergelijking, parallelrun en cutover vanaf het verdwenen Databricks-
  systeem als verplichte go-liveactiviteit.

### Wat blijft verplicht?

- De vijf afgesproken bronsystemen en negen objecten werkend ontsluiten.
- Eigen expliciete bron-, mapping-, DQ-, historisatie- en publicatiecontracten.
- Geautomatiseerde tests met controleerbare verwachte resultaten, passend bij
  het nieuwe contract; geen omvangrijk historisch reconstructieproject.
- Correcte leveringvolgorde, volledigheid, traceerbaarheid, retries en herstel.
- Veilige, herhaalbare deployment en productiehardening.

**Actuele interpretatie van acceptatiecriteria:**

- AC-03: verwerking levert de verwachte businessuitkomsten volgens het
  vastgelegde Fabric-contract, zonder onverklaarde verschillen.
- AC-04: hashing is deterministisch en conform de gekozen, versioneerbare
  conventie; consistentie wordt binnen het nieuwe systeem getest.
- AC-08: historie en deletes werken volgens het nieuwe expliciete contract.
- Overige criteria blijven toepasselijk; verwijzingen naar oude
  referentieresultaten gelden niet als eis.

### Fase 1 en vervolg

De broninventarisatie, lokale regressiebaseline en technische acceptatiebasis
zijn beschikbaar. De externe Databricks-bewijsblokkade vervalt volledig.
Productievolumes, kosten, SLA's en RPO/RTO blijven vast te stellen voorwaarden
voor latere productieacceptatie, maar blokkeren niet het foundationontwerp.

De volgende uitvoering is het eigen templatecontract en de foundation
uitwerken. Gerichte testdata worden toegevoegd bij de implementatie van
de betreffende patronen; een afzonderlijke reconstructie van negen oude
referentiedatasets wordt niet gestart. Er wordt geen productieacceptatie
geclaimd op basis van deze doelcorrectie.

## 10. Formele fase-1-afronding en overdracht

**Datum:** 10 oktober 2026.
**Mandaat:** opdrachtgever geeft expliciet toestemming fase 1 af te ronden
en zonder aanvullende akkoordvragen door te gaan.
**Besluit B-FAB-016:** fase 1 vrijgeven voor fase 2, op basis van de actuele
doelstelling B-FAB-014 en het volledige laagcontract B-FAB-015.

### Exitcontrole

| Voorwaarde voor fase 1 | Resultaat |
|---|---|
| Einddoel en platformbereik | Herbruikbaar, productiegeschikt Fabric-template; geen Databricks-pariteit vereist |
| Bron-/objectscope | SALES, CBS, ECB, NAGER en FABRIC_SALES; negen objecten; SharePoint uitgesloten |
| Laagscope | Raw/Landing t/m Gold Current, inclusief Quality/Reject, reconciliatie en Vault-/referentieroutes |
| Beschikbare implementatiebaseline | Repositorycommit, metadata, tests en bevindingen geregistreerd |
| Lokale regressie | Opnieuw uitgevoerd bij afsluiting: **133 passed in 0.50s** |
| Metadatarelease | Opnieuw geldig; fingerprint onveranderd ten opzichte van sectie 2 |
| Technische acceptatiebasis | AC-01 t/m AC-12 met actuele interpretatie uit sectie 9 |
| Afwijkingen en onzekerheden | A-01 t/m A-08 en ontbrekende productieparameters expliciet geregistreerd |
| Doelomgeving | Bestaande Fabric Trial-workspace en runtime gecontroleerd; geen daadwerkelijke Spark-run |
| Historische bewijsblokkade | Vervallen door gewijzigde opdrachtgeverdoelstelling, niet door alsnog verkregen bewijs |

### Overgedragen werk: eigenaar, fase en vrijgavegrens

De architect/implementatieverantwoordelijke werkt onderstaande punten uit
onder het mandaat. Bedrijfskeuzes zonder feitelijke basis blijven expliciete
open parameters; er worden geen gebruiksvolumes of continuiteitsdoelen verzonnen.

| Werkpunt | Vervolgfase | Voorwaarde / bewijs |
|---|---|---|
| Template-/namespace-/metadata-/runcontextcontract | 2 | Expliciete versieerbare contracten; defaults en platformbindingen gescheiden |
| Consumer- en publicatiecontract | 2, 6 | Ontwerpkeuze en tests van de daadwerkelijke leesroute |
| Frameworknaam, projectstructuur, deployment en authenticatie | 3, 4 | Reproduceerbare scripts/definities en ondersteunde toegang |
| Lakehouses, Environment en toegangsgrenzen | 4 | Werkelijk aangemaakt, geexporteerd waar ondersteund en gecontroleerd |
| Bronconnectiviteit, inclusief Fabric SQL-bron | 5-7 | Brontoegang/extractie daadwerkelijk getest; bestaande metadata is geen connectiviteitsbewijs |
| Baselinefouten en schemaambiguiteit | 2, 5-8 | Nieuwe implementatie expliciet corrigeren en gericht testen; oude baseline niet stilzwijgend herschrijven |
| Testdata en onafhankelijke verwachtingen | 5-8 | Functionele tests per patroon; geen reconstructie van historische Databricks-data |
| Productievolumes, SLA's, budget, RPO/RTO | Voorafgaand aan relevante tests in 8 en uiterlijk 10 | Waarden onderbouwd, vastgesteld en met werkelijke tests bewezen |
| Retentie, privacy en verantwoordelijkheden | 2, 4, 8, 10 | Juridische/organisatorische uitgangspunten en technische afdwinging niet verwarren |
| Duurzame capaciteit en beheeracceptatie | 10 | Productievoorziening; trial en huidige lokale tests zijn onvoldoende |

**Waarom deze vrijgave gerechtvaardigd is:** fase 1 levert inzicht en
acceptatievoorwaarden, niet het werkende eindproduct. De eerder vereiste
Databricks-vergelijking is door de opdrachtgever geschrapt. Implementatie-,
performance-, herstel- en toegangstests horen in de daarvoor bedoelde
vervolgfasen en blijven verplicht voor productievrijgave.

**Uitgevoerd:** lokale tests en releasecheck herhaald, exitcontrole vastgelegd,
documenten bijgewerkt. Geen ETL-runtimecode gewijzigd, geen Fabric-items
aangemaakt, geen jobs gestart en geen brondata gewijzigd.
**Herstel:** deze vrijgave wijzigt alleen de projectstatus. Nieuwe feiten
worden als vervolgbevinding vastgelegd en kunnen relevante releasegates
blokkeren; het historische bewijs blijft bewaard.
**Volgende fase:** fase 2, templatecontract en doelarchitectuur.
