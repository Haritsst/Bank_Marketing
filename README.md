# Pipeline Data & Machine Learning Bank Marketing (Databricks Free Edition)

Proyek pipeline Big Data end-to-end berbasis arsitektur **Medallion Lakehouse (Bronze → Silver → Gold)** menggunakan **Apache Spark**, **Delta Lake**, **Unity Catalog**, **Databricks Lakeflow Jobs (Workflows)**, **MLflow**, dan **AI/BI Dashboard** pada **Databricks Free Edition (Serverless Compute)**.

---

## 1. Sumber Data & Pertanyaan Bisnis

* **Dataset:** Bank Marketing (`bank-full.csv` — 45.211 baris × 17 kolom)
* **Sumber Resmi:** [UCI Machine Learning Repository (ID: 222)](https://archive.ics.uci.edu/dataset/222/bank+marketing)
* **Format & Periode:** CSV (delimiter `;`), data kampanye pemasaran telepon perbankan di Portugal secara kronologis dari **Mei 2008 – November 2010**
* **Lisensi:** Creative Commons Attribution 4.0 International (CC BY 4.0)

### Pertanyaan Bisnis
1. Bagaimana pengaruh tingkat pendidikan (`education`) dan status pernikahan (`marital`) terhadap tingkat konversi berlangganan deposito (`y`)?
2. Apakah klien dengan saldo tabungan rata-rata tahunan (`balance`) yang lebih tinggi cenderung lebih banyak berlangganan deposito?
3. Bagaimana tren volume kontak dan tingkat konversi kampanye dari waktu ke waktu (Mei 2008 – November 2010) serta hubungan kelompok usia (`age_group`) terhadap keputusan berlangganan?

---

## 2. Arsitektur Pipeline & Desain Layer

```text
HTTPS UCI / GitHub (bank-full.csv)
        │
        ▼  [00_EDA_dan_Akuisisi.ipynb — dijalankan sekali di awal]
Unity Catalog Volume: /Volumes/workspace/bank_marketing/raw_data/incoming/
   ├── bank_batch_2008.csv  (27.729 baris)
   ├── bank_batch_2009.csv  (14.862 baris)
   └── bank_batch_2010.csv  (2.620 baris)
        │
        ▼  [Task 1: 01_Bronze_Layer.ipynb — Incremental + Idempotent]
Delta Table: workspace.bank_marketing.bronze_bank_marketing
        │
        ▼  [Task 2: 02_Silver_Layer.ipynb — Cleansing, Dedup, Feature Eng]
Delta Table: workspace.bank_marketing.silver_bank_marketing
        │
        ├──► [Task 3: 03_Gold_Layer.ipynb — Business Data Marts]
        │       ├── workspace.bank_marketing.gold_monthly_campaign_trend
        │       ├── workspace.bank_marketing.gold_education_marital_summary
        │       └── workspace.bank_marketing.gold_age_balance_summary
        │                 │
        │                 ▼
        │       Databricks AI/BI Dashboard
        │
        └──► [04_ML_Classification_MLflow.ipynb — Klasifikasi + MLflow UC]
                ├── Model Registry: workspace.bank_marketing.bank_marketing_deposit_clf (@prod)
                └── Delta Table: workspace.bank_marketing.gold_ml_predictions
```

### Struktur File Repositori
* [`00_EDA_dan_Akuisisi.ipynb`](00_EDA_dan_Akuisisi.ipynb) — Setup Unity Catalog Schema & Volume, unduh `bank-full.csv`, pecah ke 3 file batch kronologis (`2008`, `2009`, `2010`), dan EDA Spark DataFrame + Spark SQL.
* [`01_Bronze_Layer.ipynb`](01_Bronze_Layer.ipynb) — **Task 1 Workflow**: *incremental ingestion* berbasis file baru dengan skema eksplisit (`StructType`), metadata audit (`batch_id`, `source_file`, `source_path`, `ingested_at`), serta proteksi *idempotent* agar file yang sama tidak terduplikasi.
* [`02_Silver_Layer.ipynb`](02_Silver_Layer.ipynb) — **Task 2 Workflow**: standarisasi tipe kolom, penanganan `NULL`/`"unknown"`, filter validitas domain, deduplikasi deterministik (`Window`), pembuatan fitur turunan (`campaign_year`, `month_num`, `year_month`, `campaign_date`, `age_group`, `balance_segment`, `was_previously_contacted`, `y_flag`), dan laporan kualitas data sebelum/sesudah.
* [`03_Gold_Layer.ipynb`](03_Gold_Layer.ipynb) — **Task 3 Workflow**: membangun 3 tabel Delta Gold dengan grain eksplisit siap konsumsi dashboard.
* [`04_ML_Classification_MLflow.ipynb`](04_ML_Classification_MLflow.ipynb) — Pelatihan model klasifikasi (`LogisticRegression` baseline vs `RandomForest` vs `HistGradientBoosting` tanpa *leakage* `duration`), pelacakan & registrasi ke **MLflow Unity Catalog (`@prod`)**, serta pengujian prediksi sampel & batch ke tabel Delta.
* `data/` — Menyimpan `bank-full.csv` (sebagai sumber fallback/referensi) dan `bank-names.txt` (dokumentasi asli UCI).

---

## 3. Panduan Menjalankan di Databricks Free Edition

### Langkah A: Hubungkan Repositori ke Databricks Workspace
1. Masuk ke **Databricks Free Edition** → menu **Workspace** → **Create** → **Git folder**.
2. Tempel URL repositori: `https://github.com/Haritsst/Bank_Marketing.git`.

### Langkah B: Jalankan Akuisisi & EDA Awal
1. Buka notebook **`00_EDA_dan_Akuisisi.ipynb`**.
2. Pilih compute **Serverless** lalu klik **Run all**.
3. Pastikan di `/Volumes/workspace/bank_marketing/raw_data/incoming/` terbentuk 3 file:
   * `bank_batch_2008.csv` (27.729 baris)
   * `bank_batch_2009.csv` (14.862 baris)
   * `bank_batch_2010.csv` (2.620 baris)

### Langkah C: Buat Databricks Lakeflow Job (Workflow)
1. Buka menu **Jobs & Pipelines** → **Create** → **Job** (beri nama misal `Bank_Marketing_Medallion_ETL`).
2. Tambahkan 3 task bertipe **Notebook** dengan compute **Serverless**:
   * **Task 1 — `Ingest_Bronze`**:
     * Path: `01_Bronze_Layer.ipynb`
     * Parameters: `catalog=workspace`, `schema=bank_marketing`, `volume_name=raw_data`, `bronze_table=bronze_bank_marketing`, `ingest_mode=next_one`
     * Retries: `2`, Timeout: `1800` detik
   * **Task 2 — `Clean_Silver`**:
     * Path: `02_Silver_Layer.ipynb`
     * Depends on: `Ingest_Bronze` (Run if: *All succeeded*)
     * Parameters: `catalog=workspace`, `schema=bank_marketing`, `bronze_table=bronze_bank_marketing`, `silver_table=silver_bank_marketing`
     * Retries: `2`, Timeout: `1800` detik
   * **Task 3 — `Aggregate_Gold`**:
     * Path: `03_Gold_Layer.ipynb`
     * Depends on: `Clean_Silver` (Run if: *All succeeded*)
     * Parameters: `catalog=workspace`, `schema=bank_marketing`, `silver_table=silver_bank_marketing`
     * Retries: `2`, Timeout: `1800` detik
3. (Opsional untuk demo) Tambahkan **Schedule** mingguan/harian, lalu **nonaktifkan (Pause)** jadwal setelah demo selesai.

### Langkah D: Uji Sifat Incremental & Idempotent (Syarat Tugas)
Jalankan Job secara bertahap untuk membuktikan fitur *incremental* tanpa duplikasi:
1. **Run 1** (`ingest_mode=next_one`, `target_file=""`) → `bank_batch_2008.csv` (**27.729 baris**) masuk ke Bronze → Silver → Gold.
2. **Run 2 (Uji Idempotency)** — ubah parameter sementara pada Task 1 menjadi `target_file=bank_batch_2008.csv` → Bronze melaporkan `[IDEMPOTENT SKIP]` dan **0 baris ditambahkan** (total tetap 27.729 baris!).
3. **Run 3** (`target_file=""`, `ingest_mode=next_one`) → `bank_batch_2009.csv` (**14.862 baris**) ditambahkan tanpa menggandakan batch 2008 (total Bronze menjadi **42.591 baris**).
4. **Run 4** (`target_file=""`, `ingest_mode=next_one`) → `bank_batch_2010.csv` (**2.620 baris**) ditambahkan sehingga seluruh dataset (**45.211 baris**) terisi penuh di Bronze, Silver, dan Gold.

### Langkah E: Jalankan Model AI/ML & MLflow
1. Buka **`04_ML_Classification_MLflow.ipynb`** pada compute **Serverless** dan klik **Run all**.
2. Tinjau hasil perbandingan model (`LogisticRegression`, `RandomForest`, `HistGradientBoosting`) pada panel **Experiments**, model ter-register `@prod` di **Catalog → `workspace.bank_marketing.bank_marketing_deposit_clf`**, serta tabel hasil prediksi batch `workspace.bank_marketing.gold_ml_predictions`.

---

## 4. Query SQL untuk Databricks AI/BI Dashboard

Buka menu **Dashboards** → **Create dashboard** dan tambahkan dataset berikut (minimal 1 Custom SQL):

1. **KPI Ringkas (Total Klien, Total Berlangganan & Conversion Rate):**
```sql
SELECT
  SUM(total_klien) AS total_klien,
  SUM(total_subscribed) AS total_subscribed,
  ROUND(SUM(total_subscribed) * 100.0 / SUM(total_klien), 2) AS conversion_rate_pct,
  ROUND(AVG(avg_balance), 2) AS rata_rata_saldo_eur
FROM workspace.bank_marketing.gold_monthly_campaign_trend;
```

2. **Visualisasi Tren Waktu (Conversion Rate & Total Klien per Bulan):**
```sql
SELECT
  campaign_date,
  year_month,
  total_klien,
  total_subscribed,
  conversion_rate_pct,
  avg_balance
FROM workspace.bank_marketing.gold_monthly_campaign_trend
ORDER BY campaign_date;
```

3. **Perbandingan Kategori 1 (Pendidikan × Status Pernikahan):**
```sql
SELECT
  education,
  marital,
  total_klien,
  total_subscribed,
  conversion_rate_pct,
  avg_balance
FROM workspace.bank_marketing.gold_education_marital_summary
ORDER BY conversion_rate_pct DESC;
```

4. **Perbandingan Kategori 2 (Kelompok Usia × Segmen Saldo):**
```sql
SELECT
  age_group,
  balance_segment,
  total_klien,
  total_subscribed,
  conversion_rate_pct,
  avg_balance
FROM workspace.bank_marketing.gold_age_balance_summary
ORDER BY age_group_order, balance_segment_order;
```

5. **Custom SQL Tambahan (Efektivitas Saluran Kontak & Jenis Pekerjaan dari Silver):**
```sql
SELECT
  contact,
  job,
  COUNT(*) AS total_klien,
  SUM(y_flag) AS total_subscribed,
  ROUND(SUM(y_flag) * 100.0 / COUNT(*), 2) AS conversion_rate_pct
FROM workspace.bank_marketing.silver_bank_marketing
GROUP BY contact, job
HAVING COUNT(*) >= 50
ORDER BY conversion_rate_pct DESC;
```
