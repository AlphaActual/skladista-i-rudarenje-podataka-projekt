# Star Schema - Automobili Data Warehouse

Ova star shema je dizajnirana za analizu automobila na tržištu s fokusom na cijenovno-prodajne analize, trendove po proizvođačima, godinama i drugim ključnim dimenzijama.

---

## Pregled Star Sheme

Star shema se sastoji od:

- **1 Fact tablica** (Fact_Car_Sales) - glavna tablica s mjerama
- **6 Dimension tablica** - kontekstualne informacije  
- **Implementacija SCD (Slowly Changing Dimensions)** - upravljanje promjenama kroz vrijeme

---

## 1. FACT TABLICA: Fact_Car_Sales

**Svrha:** Glavni repozitorij svih prodajnih činjenica vezanih uz automobile na tržištu.

### Atributi

- **`car_sales_tk`** (BIGINT, PK) – Surrogate key, jedinstveni identifikator
- **`date_tk`** (INT, FK) – Strani ključ prema Dim_Date
- **`manufacturer_tk`** (BIGINT, FK) – Strani ključ prema Dim_Manufacturer
- **`vehicle_tk`** (BIGINT, FK) – Strani ključ prema Dim_Vehicle
- **`transmission_tk`** (BIGINT, FK) – Strani ključ prema Dim_Transmission  
- **`fuel_tk`** (BIGINT, FK) – Strani ključ prema Dim_Fuel
- **`location_tk`** (BIGINT, FK) – Strani ključ prema Dim_Location

### Mjere (Measures)

- **`price`** (DECIMAL(10,2)) – Cijena vozila (glavna mjera)
- **`mileage`** (INT) – Kilometraža vozila
- **`tax`** (INT) – Godišnji porez na vozilo
- **`mpg`** (DECIMAL(5,2)) – Milja po galonu (potrošnja)
- **`engine_size`** (DECIMAL(3,1)) – Veličina motora u litrima
- **`age`** (INT) – Starost vozila u godinama

---

## 2. DIMENSION TABLICE

### 2.1 Dim_Date (SCD Type 0 - Static)

**Svrha:** Vremenska dimenzija za analizu trendova po godinama i desetljećima.

**Atributi:**

- **`date_tk`** (INT, PK) – Surrogate key
- **`year`** (INT) – Godina (2014, 2015, 2016...)
- **`decade`** (VARCHAR(10)) – Desetljeće ("2010s", "2020s")

**Primjer:**

```sql
date_tk | year | decade
--------|------|--------
2014    | 2014 | 2010s
2015    | 2015 | 2010s
2020    | 2020 | 2020s
```

### 2.2 Dim_Manufacturer (SCD Type 2 - Historical tracking)

**Svrha:** Informacije o proizvođačima automobila s praćenjem promjena kroz vrijeme.

**Atributi:**

- **`manufacturer_tk`** (BIGINT, PK) – Surrogate key
- **`version`** (INT) – Verzija zapisa
- **`date_from`** (DATETIME) – Datum početka važenja
- **`date_to`** (DATETIME) – Datum kraja važenja (NULL za trenutni)
- **`manufacturer_id`** (INT) – Business key (originalni ID)
- **`name`** (VARCHAR(100)) – Naziv proizvođača
- **`country`** (VARCHAR(50)) – Zemlja porijekla
- **`region`** (VARCHAR(50)) – Regija (Europe, Asia, North America)
- **`is_current`** (BOOLEAN) – Označava trenutni aktivan zapis

**Primjer:**

```sql
manufacturer_tk | version | date_from  | date_to    | manufacturer_id | name    | country | region | is_current
----------------|---------|------------|------------|----------------|---------|---------|---------|------------
1              | 1       | 2020-01-01 | 2024-12-31 | 1              | BMW     | Germany | Europe | 0
2              | 2       | 2025-01-01 | NULL       | 1              | BMW AG  | Germany | Europe | 1
```

### 2.3 Dim_Vehicle (SCD Type 2 - Historical tracking)

**Svrha:** Informacije o vozilima (model i kategorije) s praćenjem promjena.

**Atributi:**

- **`vehicle_tk`** (BIGINT, PK) – Surrogate key
- **`version`** (INT) – Verzija zapisa
- **`date_from`** (DATETIME) – Datum početka važenja
- **`date_to`** (DATETIME) – Datum kraja važenja
- **`vehicle_id`** (INT) – Business key
- **`model_name`** (VARCHAR(100)) – Naziv modela
- **`mileage_category`** (VARCHAR(20)) – Kategorija kilometraže (Very Low, Low, Medium, High, Very High)
- **`engine_size_class`** (VARCHAR(20)) – Klasa veličine motora (Small, Medium, Large)
- **`age_category`** (VARCHAR(20)) – Kategorija starosti (Recent, Mature)
- **`is_current`** (BOOLEAN) – Označava trenutni aktivan zapis

**Primjer:**

```sql
vehicle_tk | version | date_from  | date_to | vehicle_id | model_name | mileage_category | engine_size_class | age_category | is_current
-----------|---------|------------|---------|------------|------------|------------------|-------------------|--------------|------------
1         | 1       | 2020-01-01 | NULL    | 1          | Golf       | Low              | Small             | Recent       | 1
```

### 2.4 Dim_Transmission (SCD Type 1 - Overwrite)

**Svrha:** Tipovi mjenjača - promjene su rijetke i povijest nije kritična.

**Atributi:**

- **`transmission_tk`** (BIGINT, PK) – Surrogate key
- **`transmission_id`** (INT) – Business key
- **`type`** (VARCHAR(20)) – Tip mjenjača (Manual, Automatic, Semi-Auto)
- **`last_updated`** (DATETIME) – Zadnje ažuriranje

**Primjer:**

```sql
transmission_tk | transmission_id | type      | last_updated
----------------|-----------------|-----------|-------------
1              | 1               | Manual    | 2024-01-01
2              | 2               | Automatic | 2024-01-01
3              | 3               | Semi-Auto | 2024-01-01
```

### 2.5 Dim_Fuel (SCD Type 2 - Historical tracking)

**Svrha:** Tipovi goriva s praćenjem evolucije tehnologija kroz vrijeme.

**Atributi:**

- **`fuel_tk`** (BIGINT, PK) – Surrogate key
- **`version`** (INT) – Verzija zapisa
- **`date_from`** (DATETIME) – Datum početka važenja
- **`date_to`** (DATETIME) – Datum kraja važenja
- **`fuel_id`** (INT) – Business key
- **`type`** (VARCHAR(30)) – Tip goriva (Petrol, Diesel, Hybrid, Electric)
- **`is_current`** (BOOLEAN) – Označava trenutni aktivan zapis

**Primjer:**

```sql
fuel_tk | version | date_from  | date_to    | fuel_id | type    | is_current
--------|---------|------------|------------|---------|---------|------------
1      | 1       | 2020-01-01 | 2024-12-31 | 1       | Hybrid  | 0
2      | 2       | 2025-01-01 | NULL       | 1       | Hybrid+ | 1
```

### 2.6 Dim_Location (SCD Type 1 - Overwrite)

**Svrha:** Geografske informacije o tržištu.

**Atributi:**

- **`location_tk`** (BIGINT, PK) – Surrogate key
- **`location_id`** (INT) – Business key
- **`country`** (VARCHAR(50)) – Zemlja
- **`region`** (VARCHAR(50)) – Regija
- **`last_updated`** (DATETIME) – Zadnje ažuriranje

---

## 3. SCD STRATEGIJE PO DIMENZIJAMA

### SCD Type 0 (Static) - Dim_Date

- **Razlog:** Godine se ne mijenjaju nakon unosa
- **Implementacija:** Jednostavna tablica bez verzioniranja

### SCD Type 1 (Overwrite) - Dim_Transmission, Dim_Location

- **Razlog:** Promjene su rijetke i povijest nije kritična
- **Implementacija:** Direktno ažuriranje postojećih zapisa

### SCD Type 2 (Historical tracking) - Dim_Manufacturer, Dim_Vehicle, Dim_Fuel

- **Razlog:** Važno je pratiti promjene kroz vrijeme za analitičke uvide
- **Implementacija:** Nova verzija zapisa za svaku promjenu

---

## 4. KLJUČNE ANALIZE KOJE OMOGUĆAVA SHEMA

### Prodajne analize

- Analiza cijena po proizvođačima i modelima
- Trendovi cijena kroz vrijeme
- Utjecaj kilometraže na cijenu

### Tržišne analize

- Zastupljenost proizvođača po regijama
- Popularnost tipova goriva kroz desetljeća
- Analiza starosti vozila na tržištu

### Operativne analize

- Distribucija poreza po kategorijama vozila
- Analiza potrošnje goriva (MPG) po segmentima
- Korelacija veličine motora i cijene

---

## 5. PREDNOSTI OVOG MODELA

1. **Skalabilnost:** Omogućava dodavanje novih dimenzija bez mijenjanja postojeće strukture
2. **Fleksibilnost:** SCD implementacija omogućava različite pristupe praćenju promjena
3. **Performanse:** Optimiziran je za OLAP upite i analitičke procese
4. **Historijske analize:** Type 2 SCD omogućava analizu trendova kroz vrijeme
5. **Jednostavnost:** Star schema je intuitivan za krajnje korisnike i BI alate

---

## 6. BUDUĆE MOGUĆNOSTI PROŠIRENJA

- **Dodavanje kalendarskih atributa** u Dim_Date (kvartal, mjesec, dan u tjednu)
- **Segmentacija kupaca** kroz novu Dim_Customer dimenziju
- **Lokacijske hijerarhije** (država -> regija -> grad)
- **Dodavanje more fact tablica** za različite poslovne procese (servis, osiguranje)
- **Implementacija aggregate tablica** za poboljšanje performansi

---

*Napomena: Ova star shema je optimizirana za analizu tržišta automobila i može se proširivati prema poslovnim potrebama.*