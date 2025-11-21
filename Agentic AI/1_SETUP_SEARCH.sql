USE ROLE ACCOUNTADMIN;
USE DATABASE BERCA_TEST_DS;
USE SCHEMA GOLD;
USE WAREHOUSE BERCA_WH;

-- 1. Buat Tabel untuk Dokumen (Unstructured Data)
CREATE OR REPLACE TABLE SUPPORT_DOCS (
    DOC_ID VARCHAR,
    TITLE VARCHAR,
    CONTENT VARCHAR,
    LAST_UPDATED TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 2. Isi dengan Dummy Data (Kebijakan Perusahaan)
INSERT INTO SUPPORT_DOCS (DOC_ID, TITLE, CONTENT) VALUES
('POL_001', 'Kebijakan Pengembalian (Return Policy)', 'Pelanggan dapat mengembalikan barang dalam waktu 30 hari setelah diterima. Barang harus dalam kemasan asli. Pengembalian dana diproses dalam 5 hari kerja ke metode pembayaran asli.'),
('POL_002', 'Kebijakan Pengiriman', 'Pengiriman standar memakan waktu 3-5 hari kerja. Pengiriman ekspres 1-2 hari. Kami mengirim ke seluruh wilayah yang terdaftar di tabel Region.'),
('POL_003', 'Keamanan Data', 'Kami melindungi data pelanggan sesuai standar GDPR. Informasi Identifikasi Pribadi (PII) seperti nomor telepon dimasking untuk personel yang tidak berwenang.');

-- 3. Aktifkan Change Tracking (Wajib untuk Cortex Search)
ALTER TABLE SUPPORT_DOCS SET CHANGE_TRACKING = TRUE;

-- 4. Buat Cortex Search Service
-- Service ini mengindeks teks agar bisa dicari oleh AI
CREATE OR REPLACE CORTEX SEARCH SERVICE SUPPORT_KNOWLEDGE_BASE
ON CONTENT
ATTRIBUTES TITLE
WAREHOUSE = BERCA_WH
TARGET_LAG = '1 minute'
AS (
    SELECT CONTENT, TITLE, DOC_ID FROM SUPPORT_DOCS
);

-- Verifikasi service sudah jalan
SHOW CORTEX SEARCH SERVICES;