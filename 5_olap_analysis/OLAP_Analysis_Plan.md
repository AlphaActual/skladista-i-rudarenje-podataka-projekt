# OLAP Analiza Automobilskog Tržišta - Plan za Tableau Dashboard

## 📋 Pregled Projekta

### Narativ i Kontekst
**Scenario:** AutoClub Zagreb - renomirani hrvatski autoklub sa 15.000 članova - priprema svoj godišnji izvještaj "Stanje Automobilskog Tržišta 2024" za svoj mjesečni časopis "AutoTrend". Klub redovito analizira tržište da bi svojim članovima pružio uvid u trendove cijena, najpopularnijih proizvođača, i savjete za kupnju automobila.

Analiza će se koristiti za:
- 📰 Glavni članak u časopisu o stanju tržišta
- 💰 Savjete za članove o najboljem omjeru cijene i kvalitete
- 📊 Preporuke za različite kategorije kupaca
- 🔮 Projekcije budućih trendova

---

## 🎯 Ciljevi OLAP Analize

### Primarni Ciljevi:
1. **Cijenovna Analiza** - Identifikacija trendova cijena po segmentima
2. **Analiza Proizvođača** - Pozicioniranje brandova na tržištu
3. **Segmentacija Kupaca** - Različiti profili kupaca i njihove preferencije
4. **Regionalna Analiza** - Razlike u ponudi po regijama
5. **Temporalna Analiza** - Promjene kroz godine i desetljeća

### OLAP Operacije koje ćemo demonstrirati:
- **Slice** - Analiza po jednoj dimenziji (npr. samo BMW vozila)
- **Dice** - Kombinacija više filtara (npr. njemački proizvođači, benzinski motori, 2015-2020)
- **Drill Down** - Od općenite prema detaljnoj analizi (Regija → Zemlja → Grad)
- **Roll Up** - Od detalja prema općenitom (Model → Proizvođač → Regija)
- **Pivot** - Rotacija dimenzija (godine na x osi vs y osi)

---

## 📊 Struktura Dashboard-a

### Dashboard 1: Executive Summary (Glavni Pregled)
**Cilj:** Brz pregled ključnih metrika za vodstvo AutoClub-a

#### KPI Kartice:
- 💰 Prosječna cijena automobila
- 🚗 Ukupan broj automobila u analizi
- 📈 Rast cijena YoY (Year over Year)
- 🏆 Najskuplji prodani automobil

#### Glavni Grafovi:
1. **Trend Cijena kroz Godine** (Line Chart)
   - X-osa: Godine
   - Y-osa: Prosječna cijena
   - Breakdown: Po desetljećima

2. **Top 10 Proizvođača po Tržišnom Udjelu** (Treemap)
   - Veličina: Broj automobila
   - Boja: Prosječna cijena

3. **Distribucija Cijena po Kategorijama** (Box Plot)
   - Kategorije: Klasa motora, Kategorija starosti, Kategorija kilometraže

### Dashboard 2: Detaljne Analize po Proizvođačima
**Cilj:** Dubinska analiza za različite brandove

#### Glavni Elementi:
1. **Scatter Plot: Cijena vs Karakteristike**
   - X-osa: Engine Size
   - Y-osa: Cijena
   - Veličina mjehurića: MPG
   - Boja: Proizvođač
   - Filter: Tip goriva

2. **Heat Map: Proizvođač vs Tip Goriva**
   - Redovi: Proizvođači
   - Stupci: Tipovi goriva
   - Vrijednost: Prosječna cijena
   - Boja: Intenzitet cijene

3. **Bar Chart: Prosječne Cijene po Proizvođačima**
   - Sortiran silazno po cijeni
   - Tooltip: Broj modela, raspon cijena

4. **Multi-Series Line: Trendovi Luksuznih vs Budget Brandova**
   - Podjela proizvođača u kategorije
   - Praćenje kroz godine

### Dashboard 3: Regionalna i Temporalna Analiza
**Cilj:** Geografski i vremenski uvidi

#### Glavni Elementi:
1. **Geografska Karta** (Symbol Map)
   - Lokacija: Zemlje/Regije
   - Veličina: Broj automobila
   - Boja: Prosječna cijena

2. **Kalendar Heat Map**
   - Grid: Godine vs Mjeseci (simulacija)
   - Boja: Aktivnost prodaje
   - Tooltip: Prosječna cijena

3. **Waterfall Chart: Utjecaj Faktora na Cijenu**
   - Bazna cijena
   - +/- utjecaj: Starost, Kilometraža, Tip goriva, etc.

4. **Dual Axis Chart: Broj vs Cijena kroz Godinu**
   - Lijeva Y-osa: Broj automobila
   - Desna Y-osa: Prosječna cijena
   - X-osa: Godine

### Dashboard 4: Segmentacija i Preporuke
**Cilj:** Praktični savjeti za članove kluba

#### Glavni Elementi:
1. **Customer Persona Analysis**
   - Kvadranti: Budget vs Premium, Mladi vs Stari automobili
   - Scatter plot s preporkama

2. **Value for Money Analysis**
   - Bubble chart: Cijena vs MPG vs Pouzdanost (brand score)
   - Highlighting najboljih kupova

3. **Depreciation Analysis**
   - Line chart: Gubitak vrijednosti po godinama
   - Po kategorijama automobila

---

## 🔍 Specific OLAP Scenarios

### Scenario 1: Slice Operation
**Pitanje:** "Kakvo je stanje samo njemačkih luksuznih brandova?"
- **Filter:** Manufacturer Country = "Germany", Brand Type = "Luxury"
- **Vizualizacija:** Detaljni dashboard samo za BMW, Mercedes, Audi
- **Insight:** Pozicioniranje njemačkih luksuznih brandova

### Scenario 2: Dice Operation  
**Pitanje:** "Koji su trendovi benzinskih automobila srednjeg segmenta iz 2015-2020?"
- **Filter:** Fuel Type = "Petrol", Year BETWEEN 2015-2020, Engine Size Class = "Medium"
- **Vizualizacija:** Trend analysis s multiple filters
- **Insight:** Evolucija middle-market segmenta

### Scenario 3: Drill Down
**Pitanje:** "Analiza od Europe → Germany → Premium brands → BMW → Konkretni modeli"
- **Hijerarhija:** Region → Country → Brand Category → Manufacturer → Model
- **Vizualizacija:** Tree struktura s mogućnošću ekspandiranja
- **Insight:** Granularni uvid u hijerarhiju

### Scenario 4: Roll Up
**Pitanje:** "Sažetak od pojedinih modela prema regionalnim trendovima"
- **Agregacija:** Model → Manufacturer → Region → Global
- **Vizualizacija:** Pyramid chart ili hierarchical display
- **Insight:** Big picture iz detaljnih podataka

### Scenario 5: Pivot Analysis
**Pitanje:** "Usporedba godina vs tipova goriva u matrix formatu"
- **Matrix:** Years (rows) vs Fuel Types (columns)
- **Metrics:** Average Price, Count of cars
- **Vizualizacija:** Heat map matrica
- **Insight:** Evolucija fuel technology kroz vrijeme

---

## 📈 Specifični Grafovi i Vizualizacije

### 1. Advanced Analytics Charts

#### A) Correlation Matrix
- **Elementi:** Cijena, Starost, Kilometraža, Veličina motora, MPG, Porez
- **Vizualizacija:** Heat map korelacije
- **Insight:** Koji faktori najviše utječu na cijenu

#### B) Statistical Distribution Charts
- **Box Plots:** Distribucija cijena po kategorijama
- **Histogrami:** Normalnost distribucije različitih metrika
- **Q-Q Plots:** Statistical validation

#### C) Time Series Analysis
- **Seasonal Decomposition:** Trend, sezonalnost, noise
- **Forecasting:** Projekcija budućih cijena (trend line)
- **Anomaly Detection:** Outliers u podacima

### 2. Business Intelligence Charts

#### A) Performance Metrics
- **Gauge Charts:** KPI postignuća (market share, growth rate)
- **Bullet Charts:** Target vs actual performance
- **Sparklines:** Mini trend indicators

#### B) Comparison Charts
- **Parallel Coordinates:** Multi-dimensional comparison
- **Radar Charts:** Brand positioning po više dimenzija
- **Slope Graphs:** Change over time comparison

#### C) Hierarchical Charts
- **Sunburst:** Market share hierarchy
- **Treemap:** Proportional representation
- **Sankey Diagram:** Flow between categories

---

## 📊 Tabele za Izvještaj

### 1. Summary Tables

#### Table 1: Market Overview
| Metric | Value | YoY Change |
|--------|-------|------------|
| Avg Price | €XX,XXX | +X.X% |
| Total Cars Analyzed | X,XXX | +X.X% |
| Market Leaders | Top 5 brands | Changes |
| Price Range | €X,XXX - €XXX,XXX | Stability |

#### Table 2: Manufacturer Ranking
| Rank | Manufacturer | Market Share | Avg Price | Price Position |
|------|-------------|-------------|-----------|----------------|
| 1 | Brand A | XX.X% | €XX,XXX | Premium |
| 2 | Brand B | XX.X% | €XX,XXX | Mid-range |
| ... | ... | ... | ... | ... |

### 2. Analytical Tables

#### Table 3: Correlation Analysis
| Factor | Correlation with Price | Significance |
|--------|----------------------|-------------|
| Age | -0.XX | High |
| Mileage | -0.XX | High |
| Engine Size | +0.XX | Medium |
| MPG | -0.XX | Medium |

#### Table 4: Segment Performance
| Segment | Count | Avg Price | Median Price | Price Range |
|---------|-------|-----------|-------------|-------------|
| Luxury | XXX | €XX,XXX | €XX,XXX | €XX,XXX - €XXX,XXX |
| Premium | XXX | €XX,XXX | €XX,XXX | €XX,XXX - €XX,XXX |
| Mid-range | XXX | €XX,XXX | €XX,XXX | €XX,XXX - €XX,XXX |
| Budget | XXX | €XX,XXX | €XX,XXX | €X,XXX - €XX,XXX |

---

## 🎨 Design i UX Smjernice

### Color Palette:
- **Primary:** AutoClub Blue (#1f4e79)
- **Secondary:** Silver (#c0c0c0) 
- **Accent:** Racing Red (#ff0000)
- **Success:** Green (#4caf50)
- **Warning:** Orange (#ff9800)

### Typography:
- **Headers:** Montserrat Bold
- **Body:** Open Sans Regular
- **Data:** Roboto Mono

### Layout Principles:
- **F-Pattern:** Najvažnije informacije gore-lijevo
- **White Space:** Čist, profesionalan izgled
- **Progressive Disclosure:** Od općenito prema detaljno
- **Responsive:** Funkcionalno na svim uređajima

---

## 🎯 Specifične OLAP Operacije - Praktični Primjeri

### 1. SLICE - "Focus na BMW"
```sql
-- Conceptual query
SELECT * FROM fact_car_sales f
JOIN dim_manufacturer m ON f.manufacturer_tk = m.manufacturer_tk
WHERE m.name = 'BMW' AND m.is_current = TRUE
```
**Tableau Implementation:** Filter na Manufacturer = BMW
**Dashboard Element:** Dedicated BMW analysis section
**Insight:** BMW model performance, pricing strategy, market position

### 2. DICE - "Premium njemački benzinci 2018-2022"
```sql
-- Conceptual query  
SELECT * FROM fact_car_sales f
JOIN dim_manufacturer m ON f.manufacturer_tk = m.manufacturer_tk
JOIN dim_fuel fuel ON f.fuel_tk = fuel.fuel_tk
JOIN dim_date d ON f.date_tk = d.date_tk
WHERE m.country = 'Germany' 
  AND fuel.type = 'Petrol'
  AND d.year BETWEEN 2018 AND 2022
  AND f.price > 50000
```
**Tableau Implementation:** Multiple filters applied simultaneously
**Dashboard Element:** Filtered cross-tab analysis
**Insight:** Premium German petrol car trends in recent years

### 3. DRILL DOWN - "Europa → Njemačka → BMW → 3 Series"
**Level 1:** Region view (Europe-wide analysis)
**Level 2:** Country view (Germany focus)  
**Level 3:** Manufacturer view (BMW details)
**Level 4:** Model view (3 Series specifics)
**Tableau Implementation:** Hierarchical filters with action filters
**Insight:** Granular market analysis capability

### 4. ROLL UP - "Model details → Brand summary"
**Reverse of drill down:** Aggregate individual models to brand level
**Tableau Implementation:** Summary calculations and grouping
**Insight:** Big picture from detailed data

### 5. PIVOT - "Years vs Fuel Types Matrix"
**Rows:** Years (2015, 2016, 2017...)
**Columns:** Fuel Types (Petrol, Diesel, Hybrid, Electric)
**Values:** Average Price, Count of Cars
**Tableau Implementation:** Crosstab with multiple measures
**Insight:** Technology adoption trends over time

---

## 📋 Checklist za Implementaciju

### Dashboard Development:
- [ ] **Data Connection:** Connect Tableau to MySQL data warehouse
- [ ] **Data Source Setup:** Configure relationships between fact and dimension tables
- [ ] **Calculated Fields:** Create business logic calculations
- [ ] **Parameters:** Setup interactive filters and controls
- [ ] **Dashboard Layout:** Design responsive layout
- [ ] **Formatting:** Apply consistent styling and branding
- [ ] **Performance:** Optimize for fast loading
- [ ] **Testing:** Validate all OLAP operations work correctly

### Content Creation:
- [ ] **Executive Summary:** High-level insights for leadership
- [ ] **Detailed Analysis:** Deep dives into specific segments
- [ ] **OLAP Demonstrations:** Clear examples of each operation
- [ ] **Business Recommendations:** Actionable insights
- [ ] **Technical Documentation:** Methods and assumptions
- [ ] **User Guide:** How to interact with dashboards

### Validation:
- [ ] **Data Accuracy:** Cross-check calculations with source data
- [ ] **Business Logic:** Validate insights make business sense
- [ ] **User Testing:** Ensure dashboards are intuitive
- [ ] **Performance Testing:** Acceptable loading times
- [ ] **Cross-browser Testing:** Works in different environments

---

## 🚀 Sljedeći Koraci

1. **Week 1:** Setup Tableau connection to data warehouse
2. **Week 2:** Build core dashboards and implement OLAP operations
3. **Week 3:** Advanced analytics and custom calculations
4. **Week 4:** Design, formatting, and user testing
5. **Week 5:** Final documentation and presentation preparation

---

## 💡 Dodatne Mogućnosti za Razmotriti

### Advanced Analytics:
- **Predictive Modeling:** Price forecasting using Tableau's forecasting
- **Clustering:** Market segmentation using clustering algorithms
- **Statistical Functions:** Advanced calculations for deeper insights

### Integration Options:
- **Real-time Data:** Live connection for current market data
- **External Data:** Integration with economic indicators
- **Export Capabilities:** PDF reports, data exports for further analysis

### Collaboration Features:
- **Tableau Server:** Share dashboards with AutoClub members
- **Commenting:** Collaborative analysis and insights sharing
- **Alerting:** Automated notifications for significant market changes

---

*Ovaj dokument predstavlja kompletan plan za OLAP analizu koji pokriva sve zahtjeve zadatka i pruža praktičan okvir za implementaciju u Tableau-u.*
