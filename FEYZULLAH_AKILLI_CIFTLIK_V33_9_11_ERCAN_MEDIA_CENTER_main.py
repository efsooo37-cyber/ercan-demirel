from fastapi import FastAPI, HTTPException, Depends, Header, Query, Body
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
from fastapi import UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from datetime import datetime, timedelta, timezone, date
from pathlib import Path
from fastapi import APIRouter
from typing import Optional, Literal
import requests, sqlite3, hashlib, hmac, secrets, json, os, shutil, tempfile, threading, time, re, html as html_lib
import base64, zlib, types
from concurrent.futures import ThreadPoolExecutor, as_completed
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

APP_DIR = Path(__file__).resolve().parent
# Render/Uvicorn may start with a different working directory or a nested
# repository layout. Add the application directory and locate a nested
# media_center package (if the source archive was extracted one level deeper).
import sys
APP_DIR_STR = str(APP_DIR)
while APP_DIR_STR in sys.path:
    sys.path.remove(APP_DIR_STR)
sys.path.insert(0, APP_DIR_STR)

def _register_media_package_path():
    candidates = [APP_DIR, APP_DIR / "src", APP_DIR / "app", APP_DIR / "backend",
                  APP_DIR.parent, APP_DIR.parent / "src", APP_DIR.parent / "app"]
    for base in candidates:
        package_dir = base / "media_center"
        if (package_dir / "search.py").is_file() and str(base) not in sys.path:
            sys.path.insert(0, str(base))
    # Also handle a shallow nested extraction such as project/project/media_center.
    skip = {".git", ".venv", "venv", "__pycache__", "node_modules"}
    for current, dirs, files in os.walk(APP_DIR):
        rel_depth = len(Path(current).relative_to(APP_DIR).parts)
        dirs[:] = [d for d in dirs if d not in skip and not d.startswith(".")]
        if rel_depth >= 3:
            dirs[:] = []
        if "media_center" in dirs:
            base = Path(current)
            package_dir = base / "media_center"
            if (package_dir / "search.py").is_file() and str(base) not in sys.path:
                sys.path.insert(0, str(base))
    return any((Path(p) / "media_center" / "search.py").is_file() for p in sys.path)

MEDIA_SEARCH_MODULE_PRESENT = _register_media_package_path()
DB_PATH = Path(os.environ.get("DB_PATH", str(APP_DIR / "feyzullah.db")))
DB_PATH.parent.mkdir(parents=True, exist_ok=True)
BACKUP_DIR = Path(os.environ.get("BACKUP_DIR", str(DB_PATH.parent / "backups")))
BACKUP_DIR.mkdir(exist_ok=True)
BINANCE = "https://data-api.binance.vision"
OPEN_METEO_GEO = "https://geocoding-api.open-meteo.com/v1/search"
OPEN_METEO_FORECAST = "https://api.open-meteo.com/v1/forecast"


# --- V27 AI indicators, crop catalogue and AI providers embedded in this file ---
"""Basit teknik göstergeler: SMA20, SMA50 ve RSI14."""


def add_indicators(candles):
    closes = [float(c["close"]) for c in candles]

    def rsi(values, period=14):
        if len(values) <= period:
            return None
        gains, losses = [], []
        for i in range(len(values) - period, len(values)):
            change = values[i] - values[i - 1]
            gains.append(max(change, 0.0))
            losses.append(max(-change, 0.0))
        avg_gain = sum(gains) / period
        avg_loss = sum(losses) / period
        if avg_loss == 0:
            return 100.0
        rs = avg_gain / avg_loss
        return 100.0 - (100.0 / (1.0 + rs))

    result = []
    for i, candle in enumerate(candles):
        subset = closes[: i + 1]
        result.append({
            **candle,
            "sma20": sum(subset[-20:]) / 20 if len(subset) >= 20 else None,
            "sma50": sum(subset[-50:]) / 50 if len(subset) >= 50 else None,
            "rsi14": rsi(subset),
        })
    return result

"""Mevcut ürün kataloğu; tüm kayıtlar korunmuştur."""
CROP_CATALOG = [('Domates', 'Sebze', 90), ('Biber', 'Sebze', 100), ('Salatalık', 'Sebze', 55), ('Kavun', 'Sebze', 100), ('Karpuz', 'Sebze', 95), ('Patlıcan', 'Sebze', 85), ('Kabak', 'Sebze', 55), ('Sakız kabağı', 'Sebze', 50), ('Bamya', 'Sebze', 60), ('Fasulye', 'Baklagil', 60), ('Bezelye', 'Baklagil', 75), ('Börülce', 'Baklagil', 65), ('Nohut', 'Baklagil', 110), ('Mercimek', 'Baklagil', 110), ('Marul', 'Yapraklı', 55), ('Ispanak', 'Yapraklı', 50), ('Roka', 'Yapraklı', 35), ('Maydanoz', 'Yapraklı', 70), ('Dereotu', 'Yapraklı', 50), ('Havuç', 'Kök', 90), ('Turp', 'Kök', 45), ('Pancar', 'Kök', 80), ('Soğan', 'Kök', 150), ('Sarımsak', 'Kök', 210), ('Patates', 'Tarla', 110), ('Buğday', 'Tahıl', 240), ('Arpa', 'Tahıl', 230), ('Mısır', 'Tahıl', 110), ('Yulaf', 'Tahıl', 150), ('Çavdar', 'Tahıl', 240), ('Ayçiçeği', 'Endüstri', 120), ('Pamuk', 'Endüstri', 160), ('Kanola', 'Endüstri', 180), ('Soya', 'Baklagil', 130), ('Şeker pancarı', 'Tarla', 180), ('Yonca', 'Yem bitkisi', 75), ('Fiğ', 'Yem bitkisi', 120), ('Çilek', 'Meyve', 90), ('Ahududu', 'Meyve', 120), ('Böğürtlen', 'Meyve', 150), ('Yaban mersini', 'Meyve', 180), ('Lahana', 'Sebze', 100), ('Kırmızı lahana', 'Sebze', 110), ('Karnabahar', 'Sebze', 100), ('Brokoli', 'Sebze', 90), ('Brüksel lahanası', 'Sebze', 120), ('Pazı', 'Yapraklı', 55), ('Kereviz', 'Kök', 140), ('Pırasa', 'Sebze', 150), ('Kuşkonmaz', 'Sebze', 730), ('Enginar', 'Sebze', 180), ('Taze soğan', 'Sebze', 60), ('Alabaş', 'Kök', 60), ('Şalgam', 'Kök', 65), ('Yer elması', 'Kök', 150), ('Tatlı patates', 'Kök', 120), ('Kırmızı pancar', 'Kök', 80), ('Kıvırcık', 'Yapraklı', 55), ('Endivyen', 'Yapraklı', 65), ('Tere', 'Yapraklı', 30), ('Semizotu', 'Yapraklı', 45), ('Çin lahanası', 'Yapraklı', 65), ('Kale', 'Yapraklı', 80), ('Kuru fasulye', 'Baklagil', 100), ('Barbunya', 'Baklagil', 90), ('Bakla', 'Baklagil', 100), ('Yer fıstığı', 'Baklagil', 130), ('Susam', 'Endüstri', 120), ('Tütün', 'Endüstri', 120), ('Aspir', 'Endüstri', 120), ('Patlamalık mısır', 'Tahıl', 110), ('Darı', 'Tahıl', 100), ('Tritikale', 'Tahıl', 230), ('Ahududu (sonbahar)', 'Meyve', 150), ('Frenk üzümü', 'Meyve', 150), ('Bektaşi üzümü', 'Meyve', 150), ('Kivi', 'Meyve', 1095), ('Muz', 'Meyve', 365), ('Ananas', 'Meyve', 540), ('Üzüm', 'Meyve', 240), ('İncir', 'Meyve', 240), ('Nar', 'Meyve', 240), ('Zeytin', 'Meyve', 180), ('Elma', 'Meyve', 240), ('Armut', 'Meyve', 240), ('Şeftali', 'Meyve', 180), ('Kayısı', 'Meyve', 180), ('Kiraz', 'Meyve', 180), ('Vişne', 'Meyve', 180), ('Erik', 'Meyve', 180), ('Ayva', 'Meyve', 210), ('Ceviz', 'Meyve', 365), ('Badem', 'Meyve', 240), ('Fındık', 'Meyve', 240), ('Antep fıstığı', 'Meyve', 365), ('Silajlık mısır', 'Yem bitkisi', 110), ('Sorgum', 'Yem bitkisi', 100), ('Sudan otu', 'Yem bitkisi', 70), ('Korunga', 'Yem bitkisi', 120), ('İtalyan çimi', 'Yem bitkisi', 90), ('Kırmızı turp', 'Kök', 45), ('Beyaz turp', 'Kök', 50), ('Daikon turp', 'Kök', 60), ('Kırmızı soğan', 'Kök', 150), ('Arpacık soğan', 'Kök', 120), ('Kırmızı biber', 'Sebze', 105), ('Dolmalık biber', 'Sebze', 100), ('Sivri biber', 'Sebze', 95), ('Çarliston biber', 'Sebze', 95), ('Kapya biber', 'Sebze', 110), ('Kokteyl domates', 'Sebze', 85), ('Cherry domates', 'Sebze', 80), ('Sırık domates', 'Sebze', 95), ('Yer domatesi', 'Sebze', 90), ('Kornişon salatalık', 'Sebze', 55), ('Kabak (bal kabağı)', 'Sebze', 110), ('Acı kavun', 'Sebze', 100), ('Kışlık kabak', 'Sebze', 120), ('Kırmızı havuç', 'Kök', 95), ('Mor havuç', 'Kök', 100), ('Beyaz lahana', 'Sebze', 100), ('Savoy lahanası', 'Sebze', 110), ('Kuşkonmaz fasulyesi', 'Baklagil', 60), ('Maş fasulyesi', 'Baklagil', 90), ('Kinoa', 'Tahıl', 120), ('Karabuğday', 'Tahıl', 100), ('Çemen', 'Endüstri', 100), ('Rezene', 'Yapraklı', 90), ('Kişniş', 'Yapraklı', 100), ('Fesleğen', 'Yapraklı', 60), ('Nane', 'Yapraklı', 90), ('Adaçayı', 'Yapraklı', 120), ('Kekik', 'Yapraklı', 150), ('Lavanta', 'Yem bitkisi', 180)]

"""Yerel kural tabanlı AI analiz motoru.
Yerel analiz motoru çevrimdışı çalışır; dış model/API çağrısı yapmaz,
veri uydurmaz ve otomatik işlem emri göndermez.
"""
import asyncio
import math

TIMEOUT = 30


def get_ai_status():
    return {
        "engine": "local_rules_v1",
        "local_engine": "ready",
        "openai": "optional_not_used",
        "available_providers": 1,
        "note": "Yerel analiz motoru API anahtarı olmadan çalışır. Sonuçlar karar desteğidir; doğrulanmış uzman görüşü değildir.",
    }


def _local_analysis(prompt):
    text = str(prompt or "").strip()
    low = text.casefold()
    findings, risks, actions = [], [], []
    domain = "genel"
    if any(k in low for k in ("bist", "hisse", "borsa", "yatırım", "portföy", "thyao", "xu100", "aseLS".casefold(), "btc", "bitcoin", "kripto")):
        domain = "finans_piyasa"
        findings += ["Piyasa yönü tek bir göstergeyle güvenilir biçimde belirlenemez.", "Fiyat, hacim, zaman aralığı ve karşılaştırma endeksi birlikte değerlendirilmelidir."]
        risks += ["Canlı fiyat/veri bu metinden doğrulanamıyor.", "Geçmiş performans gelecekteki getiriyi garanti etmez.", "Likidite, oynaklık ve işlem maliyeti hesaba katılmalıdır."]
        actions += ["En az 20/50 günlük ortalama, RSI(14), hacim ortalaması ve destek/direnç seviyelerini güncel veriyle kontrol edin.", "İşlem öncesi zarar-kes seviyesi ve pozisyon büyüklüğü belirleyin.", "Tek hisseye/varlığa yoğunlaşma riskini ölçün."]
    if any(k in low for k in ("hayvan", "inek", "sığır", "koyun", "keçi", "buzağı", "kuzu", "aşı", "parazit", "rasyon", "yem", "süt", "besi")):
        domain = "hayvancilik" if domain == "genel" else "karma"
        findings += ["Hayvan türü, canlı ağırlık, yaş, verim dönemi ve mevcut sağlık durumu sonucu değiştirir.", "İlaç/doz ve tekrar aralığı ürün etiketi ile veteriner hekim talimatına göre doğrulanmalıdır."]
        risks += ["Eksik hayvan profiliyle kesin doz veya tedavi önerisi verilemez.", "Arınma süreleri ürün ve etken maddeye göre değişir."]
        actions += ["Küpe numarası/grup, tarih, ürün, etken madde ve uygulama kaydını eksiksiz tutun.", "Şüpheli hastalıkta veteriner muayenesini geciktirmeyin."]
    if any(k in low for k in ("ekin", "tarla", "buğday", "arpa", "mısır", "domates", "sulama", "gübre", "ilaçlama", "hasat", "ekim")):
        domain = "tarim" if domain == "genel" else "karma"
        findings += ["Ürün, çeşit, ekim tarihi, bölge, toprak ve hava koşulları birlikte ele alınmalıdır."]
        risks += ["Bölgesel hava ve toprak verisi olmadan kesin takvim oluşturulamaz."]
        actions += ["İl/ilçe bazında güncel meteoroloji ve İl/İlçe Tarım duyurularını kontrol edin.", "Toprak analizi olmadan gübre miktarını kesinleştirmeyin."]
    if not findings:
        findings = ["İstek metni incelendi; karar vermek için ölçülebilir veri ve hedefler netleştirilmelidir."]
        risks = ["Girdi sınırlı olduğu için sonuç genel niteliktedir.", "Kaynağı belirtilmeyen sayısal iddialar doğrulanmış kabul edilmemelidir."]
        actions = ["Soruyu hedef, mevcut durum, tarih ve ölçülebilir verilerle tanımlayın.", "Önemli kararları bağımsız ve güncel kaynaklarla doğrulayın."]
    answer = "YEREL AKILLI ANALİZ\n" + f"Alan: {domain}\n\n" + "Bulgular:\n" + "\n".join("• "+x for x in findings) + "\n\nRiskler / belirsizlikler:\n" + "\n".join("• "+x for x in risks) + "\n\nÖnerilen kontrol adımları:\n" + "\n".join("• "+x for x in actions) + "\n\nSonuç: Bu motor mevcut metin üzerinden kural tabanlı ön değerlendirme yapar. Canlı veri çektiğini, veteriner teşhisi koyduğunu veya kesin yatırım getirisi hesapladığını iddia etmez."
    return {"provider":"Yerel Akıllı Motor", "status":"ok", "analysis":answer, "domain":domain, "findings":findings, "risks":risks, "actions":actions}


async def analyze_with_all(prompt):
    safe_prompt = str(prompt or "")[:8000]
    result = _local_analysis(safe_prompt)
    return {"prompt":safe_prompt, "results":{"local":result}, "engine":"local_rules_v1", "note":"Yerel motor API anahtarı olmadan çalışır; çıktı karar desteğidir."}


async def review_ai_results(prompt, results):
    local = results.get("local") if isinstance(results, dict) else None
    if isinstance(local, dict) and local.get("status") == "ok":
        return {"consensus":local.get("analysis", ""), "reviewer":"Yerel Akıllı Motor", "engine":"local_rules_v1"}
    return {"consensus":"Yerel analiz üretilemedi. Girdi metnini kontrol edip tekrar deneyin.", "reviewer":"Yerel Akıllı Motor"}


app = FastAPI(title="ERCAN DEMİREL | AKILLI ÇİFTLİK", version="33.9.4")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

# V28: all data-changing operations are owner-only. AI requests are analysis-only.
@app.middleware("http")
async def owner_write_guard(request, call_next):
    method = request.method.upper()
    path = request.url.path
    if method in {"POST", "PUT", "PATCH", "DELETE"} and path not in {"/api/register", "/api/login", "/api/logout", "/api/password/recovery"} and not path.startswith("/api/ai/") and path not in {"/ai/status"}:
        auth = request.headers.get("authorization", "")
        if not auth.startswith("Bearer "):
            from starlette.responses import JSONResponse
            return JSONResponse({"detail": "Bu işlem için giriş yapmanız gerekiyor."}, status_code=401)
        token_hash = hashlib.sha256(auth[7:].strip().encode()).hexdigest()
        with db() as con:
            account = con.execute("SELECT u.role,u.status,s.expires_at FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=?", (token_hash,)).fetchone()
        if not account or account["status"] != "active" or datetime.fromisoformat(account["expires_at"]) < datetime.now(timezone.utc):
            from starlette.responses import JSONResponse
            return JSONResponse({"detail": "Oturum geçersiz veya hesap engellenmiş. Yeniden giriş yapın."}, status_code=401)
        if account["role"] != "admin":
            from starlette.responses import JSONResponse
            return JSONResponse({"detail": "Bu işlem yalnızca site sahibine/yöneticiye açıktır. Hesabınız görüntüleme yetkilidir."}, status_code=403)
    return await call_next(request)

# --- Database and safe, additive migrations ---
def db():
    con = sqlite3.connect(DB_PATH, timeout=30)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA foreign_keys=ON")
    con.execute("PRAGMA journal_mode=WAL")
    return con

def columns(con, table):
    return {r[1] for r in con.execute(f"PRAGMA table_info({table})")}

def add_column(con, table, name, definition):
    if name not in columns(con, table): con.execute(f"ALTER TABLE {table} ADD COLUMN {name} {definition}")

def init_db():
    with db() as c:
        c.executescript("""
        CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY AUTOINCREMENT, username TEXT UNIQUE NOT NULL COLLATE NOCASE, salt TEXT NOT NULL, password_hash TEXT NOT NULL, created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS sessions(token_hash TEXT PRIMARY KEY, user_id INTEGER NOT NULL, expires_at TEXT NOT NULL, FOREIGN KEY(user_id) REFERENCES users(id));
        CREATE TABLE IF NOT EXISTS password_recovery_requests(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,username TEXT NOT NULL,phone TEXT NOT NULL,created_at TEXT NOT NULL,status TEXT NOT NULL DEFAULT 'pending',resolved_at TEXT DEFAULT '',FOREIGN KEY(user_id) REFERENCES users(id));
        CREATE TABLE IF NOT EXISTS animals(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, name TEXT NOT NULL, age TEXT NOT NULL DEFAULT '', birth_count TEXT NOT NULL DEFAULT '0', due_date TEXT NOT NULL DEFAULT 'Belirtilmedi');
        CREATE TABLE IF NOT EXISTS feeds(id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL, name TEXT NOT NULL, supplier TEXT NOT NULL, price REAL NOT NULL);
        CREATE TABLE IF NOT EXISTS animal_events(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,animal_id INTEGER NOT NULL,event_type TEXT NOT NULL,event_date TEXT NOT NULL,description TEXT DEFAULT '',amount REAL DEFAULT 0,related_animal_id INTEGER,created_at TEXT NOT NULL,FOREIGN KEY(animal_id) REFERENCES animals(id));
        CREATE TABLE IF NOT EXISTS inventory(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,name TEXT NOT NULL,category TEXT NOT NULL DEFAULT 'Yem',unit TEXT NOT NULL DEFAULT 'kg',quantity REAL NOT NULL DEFAULT 0,min_quantity REAL NOT NULL DEFAULT 0,unit_cost REAL NOT NULL DEFAULT 0,supplier TEXT DEFAULT '',expiry_date TEXT DEFAULT '',created_at TEXT NOT NULL,UNIQUE(user_id,name,category));
        CREATE TABLE IF NOT EXISTS stock_moves(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,item_id INTEGER NOT NULL,move_type TEXT NOT NULL,quantity REAL NOT NULL,unit_cost REAL NOT NULL DEFAULT 0,description TEXT DEFAULT '',created_at TEXT NOT NULL,FOREIGN KEY(item_id) REFERENCES inventory(id));
        CREATE TABLE IF NOT EXISTS transactions(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,kind TEXT NOT NULL,category TEXT NOT NULL,amount REAL NOT NULL,currency TEXT NOT NULL DEFAULT 'TRY',transaction_date TEXT NOT NULL,description TEXT DEFAULT '',related_id INTEGER,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS milk_records(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,animal_id INTEGER,record_date TEXT NOT NULL,liters REAL NOT NULL,unit_price REAL DEFAULT 0,buyer TEXT DEFAULT '',notes TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS health_records(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,animal_id INTEGER NOT NULL,record_type TEXT NOT NULL,record_date TEXT NOT NULL,description TEXT NOT NULL,vet TEXT DEFAULT '',next_date TEXT DEFAULT '',cost REAL DEFAULT 0);
        CREATE TABLE IF NOT EXISTS farm_tasks(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,title TEXT NOT NULL,task_date TEXT NOT NULL,category TEXT DEFAULT '',status TEXT DEFAULT 'Bekliyor',notes TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS equipment(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,name TEXT NOT NULL,kind TEXT DEFAULT '',last_service TEXT DEFAULT '',next_service TEXT DEFAULT '',notes TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS fields(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,name TEXT NOT NULL,crop TEXT DEFAULT '',area REAL DEFAULT 0,unit TEXT DEFAULT 'dekar',plant_date TEXT DEFAULT '',harvest_date TEXT DEFAULT '',notes TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS field_measurements(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,name TEXT NOT NULL,area_m2 REAL NOT NULL,perimeter_m REAL NOT NULL,points_json TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS audit_log(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,action TEXT NOT NULL,entity TEXT NOT NULL,entity_id INTEGER,details TEXT DEFAULT '',created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS crop_calendar(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,crop TEXT NOT NULL,variety TEXT DEFAULT '',method TEXT NOT NULL DEFAULT 'Açık tarla',region TEXT DEFAULT '',sow_date TEXT NOT NULL,transplant_date TEXT DEFAULT '',harvest_start TEXT DEFAULT '',harvest_end TEXT DEFAULT '',notes TEXT DEFAULT '',created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS price_alerts(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,condition TEXT NOT NULL CHECK(condition IN ('above','below')),target REAL NOT NULL,active INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS price_alert_history(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,alert_id INTEGER NOT NULL,symbol TEXT NOT NULL,condition TEXT NOT NULL,target REAL NOT NULL,observed REAL NOT NULL,triggered_at TEXT NOT NULL,FOREIGN KEY(alert_id) REFERENCES price_alerts(id));
        CREATE TABLE IF NOT EXISTS farm_settings(user_id INTEGER PRIMARY KEY,city TEXT DEFAULT 'Konya',district TEXT DEFAULT 'Ereğli',map_url TEXT DEFAULT 'https://maps.app.goo.gl/15ChbNikcC5HL8aF9',updated_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS portfolio_trades(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,symbol TEXT NOT NULL,side TEXT NOT NULL CHECK(side IN ('buy','sell')),quantity REAL NOT NULL,price REAL NOT NULL,fee REAL NOT NULL DEFAULT 0,trade_time TEXT NOT NULL,created_at TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS idx_animals_user ON animals(user_id,id);
        CREATE INDEX IF NOT EXISTS idx_events_user_animal ON animal_events(user_id,animal_id,event_date);
        CREATE INDEX IF NOT EXISTS idx_transactions_user_date ON transactions(user_id,transaction_date);
        """)
        # Additive columns: never drop or rewrite existing user data.
        for name, definition in {
            "tag_no":"TEXT DEFAULT ''", "species":"TEXT DEFAULT 'Büyükbaş'", "sex":"TEXT DEFAULT ''", "breed":"TEXT DEFAULT ''",
            "birth_date":"TEXT DEFAULT ''", "purchase_date":"TEXT DEFAULT ''", "purchase_price":"REAL DEFAULT 0",
            "status":"TEXT DEFAULT 'Aktif'", "death_date":"TEXT DEFAULT ''", "death_reason":"TEXT DEFAULT ''",
            "location":"TEXT DEFAULT ''", "notes":"TEXT DEFAULT ''", "photo":"TEXT DEFAULT ''"
        }.items(): add_column(c,"animals",name,definition)
        c.execute("CREATE INDEX IF NOT EXISTS idx_animals_tag ON animals(user_id,tag_no)")

        c.execute("""CREATE TABLE IF NOT EXISTS support_source_checks(
            source_key TEXT PRIMARY KEY, source_name TEXT NOT NULL, source_url TEXT NOT NULL,
            last_checked TEXT NOT NULL, http_status INTEGER, page_title TEXT DEFAULT '',
            content_hash TEXT DEFAULT '', content_excerpt TEXT DEFAULT '', changed_at TEXT DEFAULT '',
            status TEXT NOT NULL DEFAULT 'unknown', error TEXT DEFAULT ''
        )""")
        # V33.6: additive animal health planner tables; existing records are untouched.
        c.execute("""CREATE TABLE IF NOT EXISTS animal_health_plans(
            id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,animal_id INTEGER,
            species TEXT NOT NULL DEFAULT '',scope TEXT NOT NULL DEFAULT 'animal',group_name TEXT DEFAULT '',
            category TEXT NOT NULL,title TEXT NOT NULL,product_name TEXT DEFAULT '',active_ingredient TEXT DEFAULT '',
            planned_date TEXT NOT NULL,repeat_months INTEGER,repeat_days INTEGER,source_url TEXT DEFAULT '',source_note TEXT DEFAULT '',
            notes TEXT DEFAULT '',status TEXT NOT NULL DEFAULT 'Bekliyor',created_at TEXT NOT NULL,updated_at TEXT NOT NULL
        )""")
        c.execute("""CREATE TABLE IF NOT EXISTS animal_health_applications(
            id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,plan_id INTEGER NOT NULL,
            animal_id INTEGER,animals_json TEXT DEFAULT '[]',applied_date TEXT NOT NULL,category TEXT NOT NULL,
            title TEXT NOT NULL,product_name TEXT DEFAULT '',active_ingredient TEXT DEFAULT '',dose_note TEXT DEFAULT '',
            applicator TEXT DEFAULT '',withdrawal_meat_days INTEGER DEFAULT 0,withdrawal_milk_days INTEGER DEFAULT 0,
            next_date TEXT DEFAULT '',notes TEXT DEFAULT '',created_at TEXT NOT NULL
        )""")
        # V33.7: product catalog stores only explicitly sourced product data; no guessed doses are seeded.
        c.execute("""CREATE TABLE IF NOT EXISTS animal_health_product_catalog(
            id INTEGER PRIMARY KEY AUTOINCREMENT, product_name TEXT NOT NULL, category TEXT NOT NULL,
            target_species TEXT NOT NULL DEFAULT 'Belirtilmedi', disease_protected TEXT DEFAULT '',
            active_ingredient TEXT DEFAULT '', dosage_rule TEXT DEFAULT '', application_route TEXT DEFAULT '',
            min_age_months INTEGER, repeat_days INTEGER, withdrawal_meat_days INTEGER, withdrawal_milk_days INTEGER,
            source_url TEXT DEFAULT '', source_note TEXT DEFAULT '', verification_status TEXT NOT NULL DEFAULT 'Doğrulanmadı',
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(product_name,category))
        """)
        c.execute("CREATE INDEX IF NOT EXISTS idx_health_product_category ON animal_health_product_catalog(category,product_name)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_health_plans_user_date ON animal_health_plans(user_id,planned_date,status)")
        c.execute("CREATE INDEX IF NOT EXISTS idx_health_apps_user_date ON animal_health_applications(user_id,applied_date)")
        # V33.9: additive day-based interval; legacy repeat_months records remain valid.
        add_column(c, "animal_health_plans", "repeat_days", "INTEGER")
        add_column(c, "animal_health_plans", "last_application_date", "TEXT DEFAULT ''")
        add_column(c, "animal_health_plans", "ear_tags", "TEXT DEFAULT ''")
        add_column(c, "animal_health_applications", "ear_tags", "TEXT DEFAULT ''")
        add_column(c, "support_source_checks", "content_hash", "TEXT DEFAULT ''")
        add_column(c, "support_source_checks", "content_excerpt", "TEXT DEFAULT ''")
        add_column(c, "support_source_checks", "changed_at", "TEXT DEFAULT ''")
        # V28: additive user role/status fields; existing users and records are retained.
        add_column(c, "users", "role", "TEXT NOT NULL DEFAULT 'member'")
        add_column(c, "users", "status", "TEXT NOT NULL DEFAULT 'active'")
        # V29 additive profile fields; preserve all existing accounts and records.
        add_column(c, "users", "full_name", "TEXT NOT NULL DEFAULT ''")
        add_column(c, "users", "birthplace", "TEXT NOT NULL DEFAULT ''")
        add_column(c, "users", "phone", "TEXT NOT NULL DEFAULT ''")
        # Exactly one administrator: retain the oldest current admin; otherwise promote oldest account.
        admins = c.execute("SELECT id FROM users WHERE role='admin' ORDER BY id").fetchall()
        if admins:
            c.execute("UPDATE users SET role='member' WHERE role='admin' AND id<>?", (admins[0]['id'],))
        else:
            first = c.execute("SELECT id FROM users ORDER BY id LIMIT 1").fetchone()
            if first: c.execute("UPDATE users SET role='admin' WHERE id=?", (first['id'],))
        c.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_users_phone_unique ON users(phone) WHERE phone<>''")

init_db()

# V31 ERCAN MEDIA CENTER: isolated media storage; no existing farm files are moved.
V31_MEDIA_ROOT = APP_DIR / "media_center" / "library"
V31_MEDIA_CATEGORIES = {"films": {"label": "Filmler", "extensions": {".mp4", ".webm", ".m4v", ".mov"}}, "series": {"label": "Diziler", "extensions": {".mp4", ".webm", ".m4v", ".mov"}}, "music": {"label": "Müzik", "extensions": {".mp3", ".m4a", ".ogg", ".wav", ".flac"}}, "live_tv": {"label": "Canlı TV kayıtları", "extensions": {".mp4", ".webm", ".m3u8", ".mp3", ".m4a"}}}
for _category in V31_MEDIA_CATEGORIES:
    (V31_MEDIA_ROOT / _category).mkdir(parents=True, exist_ok=True)
with db() as _v31media_db:
    _v31media_db.execute("CREATE TABLE IF NOT EXISTS media_favorites(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,media_key TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(user_id,media_key))")
    add_column(_v31media_db, "media_favorites", "item_json", "TEXT NOT NULL DEFAULT '{}'" )
    _v31media_db.execute("CREATE TABLE IF NOT EXISTS media_history(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,media_key TEXT NOT NULL,item_json TEXT NOT NULL DEFAULT '{}',position_seconds REAL NOT NULL DEFAULT 0,duration_seconds REAL NOT NULL DEFAULT 0,last_played TEXT NOT NULL,UNIQUE(user_id,media_key))")
    _v31media_db.execute("CREATE INDEX IF NOT EXISTS idx_media_history_user_date ON media_history(user_id,last_played DESC)")
    _v31media_db.execute("CREATE TABLE IF NOT EXISTS media_playlists(id INTEGER PRIMARY KEY AUTOINCREMENT,user_id INTEGER NOT NULL,name TEXT NOT NULL,created_at TEXT NOT NULL,UNIQUE(user_id,name))")
    _v31media_db.execute("CREATE TABLE IF NOT EXISTS media_playlist_items(id INTEGER PRIMARY KEY AUTOINCREMENT,playlist_id INTEGER NOT NULL,media_key TEXT NOT NULL,item_json TEXT NOT NULL DEFAULT '{}',added_at TEXT NOT NULL,UNIQUE(playlist_id,media_key),FOREIGN KEY(playlist_id) REFERENCES media_playlists(id) ON DELETE CASCADE)")


# V31 additive table: shared administrator-managed home-screen items.
with db() as _v31db:
    _v31db.execute("CREATE TABLE IF NOT EXISTS home_widgets(id INTEGER PRIMARY KEY AUTOINCREMENT,title TEXT NOT NULL DEFAULT '',content TEXT NOT NULL,kind TEXT NOT NULL CHECK(kind IN ('text','phone','address','link')),text_case TEXT NOT NULL DEFAULT 'normal',font_size INTEGER NOT NULL DEFAULT 18,color TEXT NOT NULL DEFAULT '#f1f5f9',sort_order INTEGER NOT NULL DEFAULT 0,created_at TEXT NOT NULL)")
    _home_widget_cols={r[1] for r in _v31db.execute("PRAGMA table_info(home_widgets)").fetchall()}
    if "font_weight" not in _home_widget_cols: _v31db.execute("ALTER TABLE home_widgets ADD COLUMN font_weight TEXT NOT NULL DEFAULT 'bold'")
    if "font_style" not in _home_widget_cols: _v31db.execute("ALTER TABLE home_widgets ADD COLUMN font_style TEXT NOT NULL DEFAULT 'italic'")
with db() as _home_prefs_db:
    _home_prefs_db.execute("CREATE TABLE IF NOT EXISTS home_preferences(id INTEGER PRIMARY KEY CHECK(id=1),theme TEXT NOT NULL DEFAULT 'dark',clock_style TEXT NOT NULL DEFAULT 'digital',app_order TEXT NOT NULL DEFAULT '[]',folder_map TEXT NOT NULL DEFAULT '{}',updated_at TEXT NOT NULL DEFAULT '')")
    _home_prefs_db.execute("INSERT OR IGNORE INTO home_preferences(id,theme,clock_style,app_order,folder_map,updated_at) VALUES(1,'dark','digital','[]','{}','')")

# --- Government support / agricultural finance monitoring (additive V26) ---
SUPPORT_SOURCES = [
    {"key":"haygem","name":"Tarım ve Orman Bakanlığı HAYGEM duyuruları","url":"https://www.tarimorman.gov.tr/haygem/Sayfalar/Detay.aspx?Liste=Duyuru","category":"Hayvancılık ve ödeme takvimleri"},
    {"key":"milk","name":"Çiğ süt desteği ve birim fiyatları","url":"https://www.tarimorman.gov.tr/Sayfalar/Detay.aspx?Liste=Destekler&OgeId=2216","category":"Çiğ süt"},
    {"key":"plant_animal","name":"Bitkisel ve hayvansal üretim destekleri","url":"https://www.tarimorman.gov.tr/Haber/7258/Bitkisel-Ve-Hayvansal-Uretimde-Destek-Tutarlari-Artirildi","category":"Bitkisel ve hayvansal üretim"},
    {"key":"ziraat","name":"Ziraat Bankası hayvansal üretim kredileri","url":"https://www.ziraatbank.com.tr/tr/ticari/tarim/hayvancilik-ve-su-urunleri-kredileri/hayvansal-uretim-kredileri","category":"Tarımsal krediler"},
    {"key":"kosgeb","name":"KOSGEB Girişimci Destek Programı","url":"https://www.kosgeb.gov.tr/site/tr/genel/destekdetay/1231/","category":"KOSGEB"},
]
SUPPORT_ITEMS = [
    {"id":"plant-2026-base","category":"Bitkisel üretim","name":"2026 temel destek katsayı değeri","amount":"367 TL/dekar katsayı değeri","period":"2026 üretim yılı","payment":"Ödeme takvimi kaynakta ayrıca ilan edilmelidir","application":"Ürün, planlama bölgesi ve kayıt şartlarına bağlı; ürün bazlı toplam tutar değişir","deadline":"","source_key":"plant_animal","note":"367 TL/dekar katsayı değeri; her ürüne doğrudan ödenecek sabit tutar değildir."},
    {"id":"plant-2027-base","category":"Bitkisel üretim","name":"2027 temel destek katsayı değeri","amount":"442 TL/dekar katsayı değeri","period":"2027 üretim yılı","payment":"Ödeme tarihi henüz açıklanmadı","application":"Ürün ve üretim planlaması şartlarına bağlı","deadline":"","source_key":"plant_animal","note":"Katsayı değeridir; ürün bazlı destek hesabı farklılaşır."},
    {"id":"wheat-barley-corn-2026","category":"Bitkisel üretim","name":"Buğday / arpa / mısır temel + planlı üretim desteği","amount":"954 TL/dekar","period":"2026 üretim yılı","payment":"Ödeme takvimi ayrıca kontrol edilmeli","application":"Ürün ve planlı üretim koşullarına bağlı","deadline":"","source_key":"plant_animal","note":"Resmî Bakanlık açıklamasında belirtilen güncel toplam tutar."},
    {"id":"calf-2026","category":"Hayvancılık","name":"Buzağı temel desteği","amount":"2.000 TL/hayvan","period":"2026 yılı","payment":"Kişisel ödeme günü açıklanmış değil","application":"Hayvancılık destekleme tebliği, TÜRKVET ve diğer şartlara bağlı","deadline":"2026-12-01","source_key":"haygem","note":"İlave katsayı ve şartlara göre tutar değişebilir."},
    {"id":"buffalo-calf-2026","category":"Hayvancılık","name":"Malak temel desteği","amount":"4.000 TL/hayvan","period":"2026 yılı","payment":"Kişisel ödeme günü açıklanmış değil","application":"Hayvancılık destekleme tebliği ve kayıt şartlarına bağlı","deadline":"2026-12-01","source_key":"haygem","note":"İlave destekler şartlara göre uygulanabilir."},
    {"id":"lamb-kid-2026","category":"Hayvancılık","name":"Kuzu / oğlak temel desteği","amount":"430 TL/hayvan","period":"2026 yılı","payment":"Kişisel ödeme günü açıklanmış değil","application":"Küçükbaş destekleme şartları ve birlik başvurusu koşullarına bağlı","deadline":"2026-12-01","source_key":"haygem","note":"İlave destek ve uygunluk koşulları bulunabilir."},
    {"id":"shepherd-2026","category":"Hayvancılık","name":"Çoban desteği","amount":"116.100 TL","period":"2026 yılı","payment":"Ödeme takvimi ayrıca ilan edilmelidir","application":"Sürü büyüklüğü, sigortalı çoban ve diğer mevzuat şartlarına bağlı","deadline":"","source_key":"plant_animal","note":"Toplam destek; başvuru ve uygunluk koşulları geçerlidir."},
    {"id":"milk-2026","category":"Çiğ süt","name":"Çiğ süt desteği","amount":"Birim fiyat Bakanlıkça dönem/kriter bazında belirlenir; sayfada sabit TL/L yayımlanmamış","period":"2025–2026 destek dönemi","payment":"Dönemsel çalışma takvimini HAYGEM duyurularından takip edin","application":"BSKS aylık kaydı, sözleşmeli satış belgesi ve tarımsal amaçlı örgüt üyeliği gibi şartlar var","deadline":"","source_key":"milk","note":"Süt türü, pazarlama şekli ve katsayılar ödeme tutarını etkiler; kişisel hakediş buradan hesaplanamaz."},
    {"id":"ziraat-credit","category":"Tarımsal krediler","name":"Ziraat Bankası hayvansal üretim kredileri","amount":"Limit/faiz/vade başvuru ve güncel kredi koşullarına göre belirlenir","period":"Güncel banka ürün sayfası","payment":"Kredi ödemesi değil; banka değerlendirmesi ve kullandırım süreci","application":"Banka şubesi ve kredi değerlendirmesi; uygunluk teminat/işletme şartlarına bağlı","deadline":"","source_key":"ziraat","note":"Büyükbaş, küçükbaş, kanatlı, arıcılık ve su ürünleri alanları için kredi seçenekleri bulunur."},
    {"id":"kosgeb-entrepreneur","category":"KOSGEB","name":"KOSGEB Girişimci Destek Programı","amount":"Program ve işletme uygunluğuna göre değişir; sabit ödeme tutarı değildir","period":"Güncel program koşulları","payment":"Hibe/geri ödemeli destek ve çağrı takvimine göre değişir","application":"NACE faaliyet kodu, işletme yaşı ve çağrı koşulları kontrol edilmeli","deadline":"","source_key":"kosgeb","note":"Tarım işletmelerinin uygunluğu faaliyet kodu ve program kapsamına göre ayrıca doğrulanmalıdır."},
]

def _support_page_title(html):
    m=re.search(r'<title[^>]*>(.*?)</title>', html or '', re.I|re.S)
    if not m: return ''
    return re.sub(r'\s+',' ',re.sub(r'<[^>]+>',' ',m.group(1))).strip()[:300]

def refresh_support_sources(force=False):
    """Fetch only official pages; never infer payment dates or personal entitlements from snippets."""
    results=[]
    for src in SUPPORT_SOURCES:
        now=datetime.now(timezone.utc).isoformat()
        try:
            resp=requests.get(src['url'],timeout=14,headers={'User-Agent':'FeyzullahAkilliCiftlik/26 (+official-public-page-monitor)'})
            title=_support_page_title(resp.text) if resp.ok else ''
            status='ok' if resp.ok else 'http_error'
            err='' if resp.ok else f'HTTP {resp.status_code}'
            plain=re.sub(r'<(script|style)[^>]*>.*?</\1>', ' ', resp.text or '', flags=re.I|re.S) if resp.ok else ''
            plain=html_lib.unescape(re.sub(r'<[^>]+>', ' ', plain))
            plain=re.sub(r'\s+', ' ', plain).strip()
            excerpt=plain[:1800]
            digest=hashlib.sha256(plain.encode('utf-8')).hexdigest() if plain else ''
            with db() as c:
                old=c.execute('SELECT content_hash,changed_at FROM support_source_checks WHERE source_key=?',(src['key'],)).fetchone()
                changed=bool(digest and old and old['content_hash'] and old['content_hash'] != digest)
                changed_at=now if changed else (old['changed_at'] if old else '')
                c.execute("""INSERT INTO support_source_checks(source_key,source_name,source_url,last_checked,http_status,page_title,content_hash,content_excerpt,changed_at,status,error)
                VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET source_name=excluded.source_name,source_url=excluded.source_url,last_checked=excluded.last_checked,http_status=excluded.http_status,page_title=excluded.page_title,content_hash=excluded.content_hash,content_excerpt=excluded.content_excerpt,changed_at=excluded.changed_at,status=excluded.status,error=excluded.error""",
                (src['key'],src['name'],src['url'],now,resp.status_code,title,digest,excerpt,changed_at,status,err))
            results.append({"key":src['key'],"status":status,"http_status":resp.status_code,"last_checked":now,"title":title,"content_changed":changed,"changed_at":changed_at})
        except Exception as exc:
            with db() as c:
                c.execute("""INSERT INTO support_source_checks(source_key,source_name,source_url,last_checked,status,error)
                VALUES(?,?,?,?,?,?) ON CONFLICT(source_key) DO UPDATE SET last_checked=excluded.last_checked,status=excluded.status,error=excluded.error""",
                (src['key'],src['name'],src['url'],now,'error',str(exc)[:500]))
            results.append({"key":src['key'],"status":"error","last_checked":now,"error":str(exc)[:250]})
    return results

def support_monitor_loop():
    while True:
        try: refresh_support_sources()
        except Exception: pass
        time.sleep(6*60*60)


# --- Auth ---
def password_digest(password, salt): return hashlib.pbkdf2_hmac("sha256", password.encode(), salt, 310_000).hex()
def current_user(authorization: str | None = Header(default=None)):
    if not authorization or not authorization.startswith("Bearer "): raise HTTPException(401,"Oturum açmanız gerekiyor.")
    token_hash=hashlib.sha256(authorization[7:].strip().encode()).hexdigest()
    with db() as c: row=c.execute("SELECT s.user_id,s.expires_at,u.status FROM sessions s JOIN users u ON u.id=s.user_id WHERE s.token_hash=?",(token_hash,)).fetchone()
    if not row or datetime.fromisoformat(row["expires_at"]) < datetime.now(timezone.utc): raise HTTPException(401,"Oturum süresi dolmuş. Yeniden giriş yapın.")
    if row["status"] != "active": raise HTTPException(403,"Hesabınız yönetici tarafından engellenmiş veya kapatılmış.")
    return int(row["user_id"])

def require_admin(uid: int = Depends(current_user)):
    with db() as c: row=c.execute("SELECT role,status FROM users WHERE id=?",(uid,)).fetchone()
    if not row or row["status"] != "active": raise HTTPException(403,"Hesap aktif değil.")
    if row["role"] != "admin": raise HTTPException(403,"Bu bölüm yalnızca site sahibine açıktır.")
    return uid
def audit(uid, action, entity, entity_id=None, details=""):
    with db() as c: c.execute("INSERT INTO audit_log(user_id,action,entity,entity_id,details,created_at) VALUES(?,?,?,?,?,?)",(uid,action,entity,entity_id,details,datetime.now(timezone.utc).isoformat()))
def owned_animal(c, uid, aid):
    row=c.execute("SELECT * FROM animals WHERE id=? AND user_id=?",(aid,uid)).fetchone()
    if not row: raise HTTPException(404,"Hayvan bulunamadı.")
    return row

class AuthBody(BaseModel):
    username:str=Field(min_length=3,max_length=40)
    password:str=Field(min_length=8,max_length=128)
    full_name:str=Field(min_length=2,max_length=100)
    birthplace:str=Field(min_length=2,max_length=100)
    phone:str=Field(min_length=10,max_length=20)
class LoginBody(BaseModel): username:str=Field(min_length=1,max_length=80); password:str=Field(min_length=1,max_length=128)
class ProfileUpdateBody(BaseModel):
    full_name:str=Field(min_length=2,max_length=100)
    birthplace:str=Field(min_length=2,max_length=100)
    phone:str=Field(min_length=10,max_length=20)
class AnimalBody(BaseModel):
    name:str=Field(min_length=1,max_length=100); age:str=""; birthCount:str="0"; dueDate:str="Belirtilmedi"
    tag_no:str=""; species:str="Büyükbaş"; sex:str=""; breed:str=""; birth_date:str=""; purchase_date:str=""; purchase_price:float=0
    location:str=""; notes:str=""
class EventBody(BaseModel): event_type:Literal["death","birth","health","vaccine","sale","transfer","weight","other"]; event_date:str; description:str=""; amount:float=0; related_animal_id:Optional[int]=None
class HealthBody(BaseModel): record_type:str; record_date:str; description:str; vet:str=""; next_date:str=""; cost:float=0
class AnimalHealthPlanBody(BaseModel):
    species:str=Field(min_length=1,max_length=30); scope:Literal["animal","group"]="animal"
    animal_id:Optional[int]=None; group_name:str=""; category:Literal["Aşılar ve tekrar aşıları","İç parazit","Dış parazit","Hap ve ağızdan ilaç","Enjeksiyon","Dökme uygulama (pour-on)","Vitamin ve mineral","Gebelik kontrolü","Doğum öncesi bakım","Doğum sonrası bakım","Yavru bakımı","Üreme takibi","Karantina ve biyogüvenlik","Sürü sağlık taraması","Tırnak ve ayak bakımı","Veteriner muayenesi","Diğer veteriner uygulamaları"]
    title:str=Field(min_length=2,max_length=160); product_name:str=""; active_ingredient:str=""
    planned_date:str; repeat_months:Optional[int]=Field(default=None,ge=1,le=60); repeat_days:Optional[int]=Field(default=None,ge=1,le=3650)
    source_url:str=""; source_note:str=""; notes:str=""
class AnimalHealthCompleteBody(BaseModel):
    applied_date:str; animal_ids:list[int]=Field(default_factory=list,max_length=500)
    product_name:str=""; active_ingredient:str=""; dose_note:str=""; applicator:str=""
    withdrawal_meat_days:int=Field(default=0,ge=0,le=365); withdrawal_milk_days:int=Field(default=0,ge=0,le=365)
    notes:str=""
class FeedBody(BaseModel): name:str=Field(min_length=1,max_length=100); supplier:str="Belirtilmedi"; price:float=Field(gt=0)
class StockBody(BaseModel): name:str; category:str="Yem"; unit:str="kg"; quantity:float=0; min_quantity:float=0; unit_cost:float=0; supplier:str=""; expiry_date:str=""
class StockMoveBody(BaseModel): item_id:int; move_type:Literal["in","out","adjustment"]; quantity:float=Field(gt=0); unit_cost:float=0; description:str=""
class TransactionBody(BaseModel): kind:Literal["income","expense"]; category:str; amount:float=Field(gt=0); currency:str="TRY"; transaction_date:str; description:str=""; related_id:Optional[int]=None
class MilkBody(BaseModel): animal_id:Optional[int]=None; record_date:str; liters:float=Field(gt=0); unit_price:float=0; buyer:str=""; notes:str=""
class TaskBody(BaseModel): title:str; task_date:str; category:str=""; status:str="Bekliyor"; notes:str=""
class EquipmentBody(BaseModel): name:str; kind:str=""; last_service:str=""; next_service:str=""; notes:str=""
class FieldBody(BaseModel): name:str; crop:str=""; area:float=0; unit:str="dekar"; plant_date:str=""; harvest_date:str=""; notes:str=""
class FieldMeasurementBody(BaseModel): name:str=Field(min_length=1,max_length=100); area_m2:float=Field(gt=0); perimeter_m:float=Field(gt=0); points:list[dict]=Field(min_length=3,max_length=2000)
class AIPrompt(BaseModel): prompt:str=Field(min_length=1,max_length=8000)
class AIInvestmentRequest(BaseModel): symbol:str=Field(default="BTCUSDT",min_length=3,max_length=30); question:str=Field(min_length=5,max_length=4000)
class CropCalendarBody(BaseModel):
    crop:str=Field(min_length=1,max_length=100); variety:str=""; method:Literal["Açık tarla","Sera","Fide"]="Açık tarla"; region:str=""; sow_date:str; transplant_date:str=""; harvest_start:str=""; harvest_end:str=""; notes:str=""
class PriceAlertBody(BaseModel): symbol:str=Field(min_length=3,max_length=30); condition:Literal["above","below"]; target:float=Field(gt=0)
class FarmSettingsBody(BaseModel): city:str="Konya"; district:str="Ereğli"; map_url:str="https://maps.app.goo.gl/15ChbNikcC5HL8aF9"
class PortfolioTradeBody(BaseModel): symbol:str=Field(min_length=3,max_length=30); side:Literal["buy","sell"]; quantity:float=Field(gt=0); price:float=Field(gt=0); fee:float=Field(ge=0,default=0)
class HomeWidgetBody(BaseModel):
    title:str=Field(default="",max_length=100)
    content:str=Field(min_length=1,max_length=1000)
    kind:Literal["text","phone","address","link"]="text"
    text_case:Literal["upper","lower","title","normal"]="normal"
    font_size:int=Field(default=18,ge=12,le=48)
    color:str=Field(default="#f1f5f9",pattern=r"^#[0-9A-Fa-f]{6}$")
    font_weight:Literal["normal","bold"]="bold"
    font_style:Literal["normal","italic"]="italic"
class HomePreferencesBody(BaseModel):
    theme:Literal["dark","green","light","blue","sunset"]="dark"
    clock_style:Literal["digital","neon","classic","minimal"]="digital"
    app_order:list[str]=Field(default_factory=list,max_length=40)
    folder_map:dict[str,str]=Field(default_factory=dict)
class HomeWidgetOrderBody(BaseModel):
    widget_ids:list[int]=Field(min_length=0,max_length=100)

@app.post("/api/register")
def register(data:AuthBody):
    salt=secrets.token_bytes(16); digest=password_digest(data.password,salt)
    phone="".join(ch for ch in data.phone if ch.isdigit())
    if len("".join(ch for ch in phone if ch.isdigit())) < 10: raise HTTPException(422,"Telefon numarası en az 10 rakam içermelidir.")
    try:
        with db() as c:
            c.execute("BEGIN IMMEDIATE")
            duplicate=c.execute("SELECT 1 FROM users WHERE lower(trim(username))=lower(?) LIMIT 1",(data.username.strip(),)).fetchone()
            if duplicate: raise HTTPException(409,"Bu kullanıcı adı zaten kayıtlı. Mevcut hesabınızla giriş yapın.")
            phone_tail=phone[-10:]
            existing_phones=c.execute("SELECT phone FROM users WHERE COALESCE(phone,'')<>''").fetchall()
            if any("".join(ch for ch in (r["phone"] or "") if ch.isdigit())[-10:]==phone_tail for r in existing_phones):
                raise HTTPException(409,"Bu telefon numarasıyla hesap zaten kayıtlı. Yeni kayıt açmak yerine mevcut hesabınızla giriş yapın.")
            has_admin=c.execute("SELECT 1 FROM users WHERE role='admin' LIMIT 1").fetchone()
            role="member" if has_admin else "admin"
            c.execute("INSERT INTO users(username,salt,password_hash,created_at,role,status,full_name,birthplace,phone) VALUES(?,?,?,?,?,'active',?,?,?)",(data.username.strip(),salt.hex(),digest,datetime.now(timezone.utc).isoformat(),role,data.full_name.strip(),data.birthplace.strip(),phone))
    except sqlite3.IntegrityError as e:
        if "phone" in str(e).lower(): raise HTTPException(409,"Bu telefon numarası başka bir hesapta kayıtlı.")
        raise HTTPException(409,"Bu kullanıcı adı zaten kayıtlı.")
    return {"message": "Kayıt başarılı. İlk hesap tek yönetici olarak açıldı." if role=="admin" else "Kayıt başarılı. Yönetici yetkisi olmadan üye hesabınız açıldı.", "role":role}
@app.post("/api/login")
def login(data:LoginBody):
    # Keep old accounts usable: username comparison ignores letter case and surrounding
    # whitespace; phone comparison strips spaces, punctuation and country-code symbols.
    identifier=data.username.strip()
    if not identifier or not data.password: raise HTTPException(401,"Kullanıcı adı ve şifre girin.")
    digits="".join(ch for ch in identifier if ch.isdigit())
    with db() as c:
        u=c.execute("SELECT * FROM users WHERE lower(trim(username))=lower(?) ORDER BY id LIMIT 1",(identifier,)).fetchone()
        if u is None and digits:
            candidates=c.execute("SELECT * FROM users WHERE COALESCE(phone,'')<>'' ORDER BY id").fetchall()
            matches=[]
            for candidate in candidates:
                stored_digits="".join(ch for ch in (candidate["phone"] or "") if ch.isdigit())
                # Accept either full international or national form when the stored
                # number contains a country prefix; never match a short fragment.
                if stored_digits==digits or (len(digits)>=10 and len(stored_digits)>=10 and stored_digits.endswith(digits)) or (len(stored_digits)>=10 and len(digits)>=10 and digits.endswith(stored_digits)):
                    matches.append(candidate)
            if len(matches)==1: u=matches[0]
    if not u or not hmac.compare_digest(password_digest(data.password,bytes.fromhex(u["salt"])),u["password_hash"]): raise HTTPException(401,"Kullanıcı adı/telefon veya şifre hatalı. Kullanıcı adını ve şifreyi kayıt sırasında kullandığınız şekilde girin.")
    if u["status"] != "active": raise HTTPException(403,"Bu hesap site sahibi tarafından engellenmiş veya kapatılmış.")
    token=secrets.token_urlsafe(32); expiry=datetime.now(timezone.utc)+timedelta(days=14)
    with db() as c: c.execute("INSERT INTO sessions VALUES(?,?,?)",(hashlib.sha256(token.encode()).hexdigest(),u["id"],expiry.isoformat()))
    return {"token":token,"username":u["username"],"role":u["role"],"expires_at":expiry.isoformat()}
@app.post("/api/logout")
def logout(authorization:str|None=Header(default=None)):
    if authorization and authorization.startswith("Bearer "):
        with db() as c: c.execute("DELETE FROM sessions WHERE token_hash=?",(hashlib.sha256(authorization[7:].strip().encode()).hexdigest(),))
    return {"message":"Oturum kapatıldı."}
@app.get("/api/me")
def me(uid:int=Depends(current_user)):
    with db() as c: r=c.execute("SELECT id,username,role,status,created_at,full_name,birthplace,phone FROM users WHERE id=?",(uid,)).fetchone()
    return dict(r) if r else {}

@app.put("/api/me/profile")
def update_my_profile(data:ProfileUpdateBody,uid:int=Depends(current_user)):
    phone="".join(ch for ch in data.phone if ch.isdigit())
    if len("".join(ch for ch in phone if ch.isdigit())) < 10: raise HTTPException(422,"Telefon numarası en az 10 rakam içermelidir.")
    try:
        with db() as c:
            existing=c.execute("SELECT id,phone FROM users WHERE id<>? AND COALESCE(phone,'')<>''",(uid,)).fetchall()
            tail=phone[-10:]
            if any("".join(ch for ch in (r["phone"] or "") if ch.isdigit())[-10:]==tail for r in existing):
                raise HTTPException(409,"Bu telefon numarası başka bir hesapta kayıtlı.")
            c.execute("UPDATE users SET full_name=?,birthplace=?,phone=? WHERE id=?",(data.full_name.strip(),data.birthplace.strip(),phone,uid))
    except sqlite3.IntegrityError: raise HTTPException(409,"Bu telefon numarası başka bir hesapta kayıtlı.")
    return {"message":"Profil bilgileriniz kaydedildi. Yönetici telefon numaranızla da giriş yapabilirsiniz."}

@app.get("/api/admin/users")
def admin_users(uid:int=Depends(require_admin)):
    with db() as c:
        rows=c.execute("SELECT id,username,full_name,birthplace,phone,role,status,created_at FROM users ORDER BY id").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/password/recovery")
def request_password_recovery(data:dict):
    identifier=str(data.get("identifier","")).strip()
    if not identifier: raise HTTPException(400,"Kullanıcı adı veya kayıtlı telefon numarası girin.")
    digits="".join(ch for ch in identifier if ch.isdigit())
    with db() as c:
        user=c.execute("SELECT id,username,phone FROM users WHERE lower(trim(username))=lower(?) LIMIT 1",(identifier,)).fetchone()
        if user is None and len(digits)>=10:
            candidates=c.execute("SELECT id,username,phone FROM users WHERE COALESCE(phone,'')<>'' ORDER BY id").fetchall()
            matches=[u for u in candidates if "".join(ch for ch in (u["phone"] or "") if ch.isdigit())[-10:]==digits[-10:]]
            if len(matches)==1: user=matches[0]
            elif len(matches)>1: raise HTTPException(409,"Telefon numarası birden fazla hesapla eşleşiyor. Lütfen kullanıcı adınızı kullanın.")
        if not user: raise HTTPException(404,"Bu bilgilerle eşleşen hesap bulunamadı.")
        cur=c.execute("INSERT INTO password_recovery_requests(user_id,username,phone,created_at,status) VALUES(?,?,?,?, 'pending')",(user["id"],user["username"],user["phone"],datetime.now(timezone.utc).isoformat()))
    return {"message":"Şifre kurtarma talebiniz yöneticiye iletildi. Yönetici sizinle iletişime geçecektir.","request_id":cur.lastrowid}

@app.get("/api/admin/recovery")
def admin_recovery_requests(uid:int=Depends(require_admin)):
    with db() as c:
        rows=c.execute("SELECT id,user_id,username,phone,created_at,status FROM password_recovery_requests ORDER BY id DESC LIMIT 200").fetchall()
    return [dict(r) for r in rows]

@app.post("/api/admin/recovery/{request_id}/reset")
def admin_reset_recovery(request_id:int,uid:int=Depends(require_admin)):
    temporary=secrets.token_urlsafe(9)
    salt=secrets.token_bytes(16); digest=password_digest(temporary,salt)
    with db() as c:
        req=c.execute("SELECT user_id,status FROM password_recovery_requests WHERE id=?",(request_id,)).fetchone()
        if not req: raise HTTPException(404,"Kurtarma talebi bulunamadı.")
        if req["status"]!="pending": raise HTTPException(409,"Bu talep daha önce işleme alınmış.")
        c.execute("UPDATE users SET salt=?,password_hash=? WHERE id=?",(salt.hex(),digest,req["user_id"]))
        c.execute("DELETE FROM sessions WHERE user_id=?",(req["user_id"],))
        c.execute("UPDATE password_recovery_requests SET status='resolved',resolved_at=? WHERE id=?",(datetime.now(timezone.utc).isoformat(),request_id))
    return {"temporary_password":temporary,"message":"Geçici şifre oluşturuldu. Kullanıcıya güvenli bir kanaldan iletin; ilk girişte yeni şifre belirlemesini sağlayın."}

@app.post("/api/admin/users/{user_id}/block")
def admin_block_user(user_id:int,uid:int=Depends(require_admin)):
    if user_id==uid: raise HTTPException(400,"Kendi yönetici hesabınızı engelleyemezsiniz.")
    with db() as c:
        target=c.execute("SELECT id,role FROM users WHERE id=?",(user_id,)).fetchone()
        if not target: raise HTTPException(404,"Kullanıcı bulunamadı.")
        if target["role"]=="admin": raise HTTPException(403,"Yönetici hesabı buradan engellenemez.")
        c.execute("UPDATE users SET status='blocked' WHERE id=?",(user_id,))
        c.execute("DELETE FROM sessions WHERE user_id=?",(user_id,))
    return {"message":"Kullanıcı engellendi; açık oturumları kapatıldı."}

@app.post("/api/admin/users/{user_id}/unblock")
def admin_unblock_user(user_id:int,uid:int=Depends(require_admin)):
    with db() as c:
        cur=c.execute("UPDATE users SET status='active' WHERE id=? AND role!='admin' AND status='blocked'",(user_id,))
        if cur.rowcount==0: raise HTTPException(404,"Engelli kullanıcı bulunamadı.")
    return {"message":"Kullanıcı yeniden etkinleştirildi."}

@app.delete("/api/admin/users/{user_id}")
def admin_delete_user(user_id:int,uid:int=Depends(require_admin)):
    if user_id==uid: raise HTTPException(400,"Kendi yönetici hesabınızı silemezsiniz.")
    with db() as c:
        target=c.execute("SELECT id,role FROM users WHERE id=?",(user_id,)).fetchone()
        if not target: raise HTTPException(404,"Kullanıcı bulunamadı.")
        if target["role"]=="admin": raise HTTPException(403,"Yönetici hesabı silinemez.")
        c.execute("UPDATE users SET status='deleted', username=? WHERE id=?",("silinmis_kullanici_"+str(user_id),user_id))
        c.execute("DELETE FROM sessions WHERE user_id=?",(user_id,))
    return {"message":"Üyelik kapatıldı. Güvenlik ve kayıt bütünlüğü için eski kayıtlar veritabanında saklandı; kullanıcı artık giriş yapamaz."}

# --- Animals: existing endpoints remain; removal is now archival, not destructive ---
@app.get("/api/animals")
def get_animals(include_archived:bool=False,uid:int=Depends(current_user)):
    sql="SELECT * FROM animals WHERE user_id=?"+("" if include_archived else " AND status='Aktif'")+" ORDER BY id DESC"
    with db() as c: rows=c.execute(sql,(uid,)).fetchall()
    return [dict(r) for r in rows]
@app.post("/api/animals")
def save_animal(data:AnimalBody,uid:int=Depends(current_user)):
    with db() as c:
        if data.tag_no and c.execute("SELECT 1 FROM animals WHERE user_id=? AND tag_no=?",(uid,data.tag_no)).fetchone(): raise HTTPException(409,"Bu küpe numarası zaten kayıtlı.")
        cur=c.execute("""INSERT INTO animals(user_id,name,age,birth_count,due_date,tag_no,species,sex,breed,birth_date,purchase_date,purchase_price,status,location,notes)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?, 'Aktif',?,?)""",(uid,data.name.strip(),data.age,data.birthCount,data.dueDate,data.tag_no.strip(),data.species,data.sex,data.breed,data.birth_date,data.purchase_date,data.purchase_price,data.location,data.notes))
        aid=cur.lastrowid
    audit(uid,"ekle","animal",aid,data.name)
    return {"id":aid,"message":"Hayvan kaydedildi."}
@app.patch("/api/animals/{aid}")
def update_animal(aid:int,data:AnimalBody,uid:int=Depends(current_user)):
    with db() as c:
        owned_animal(c,uid,aid)
        if data.tag_no and c.execute("SELECT 1 FROM animals WHERE user_id=? AND tag_no=? AND id<>?",(uid,data.tag_no,aid)).fetchone(): raise HTTPException(409,"Bu küpe numarası başka hayvanda kayıtlı.")
        c.execute("""UPDATE animals SET name=?,age=?,birth_count=?,due_date=?,tag_no=?,species=?,sex=?,breed=?,birth_date=?,purchase_date=?,purchase_price=?,location=?,notes=? WHERE id=? AND user_id=?""",(data.name,data.age,data.birthCount,data.dueDate,data.tag_no,data.species,data.sex,data.breed,data.birth_date,data.purchase_date,data.purchase_price,data.location,data.notes,aid,uid))
    audit(uid,"güncelle","animal",aid,data.name); return {"message":"Hayvan bilgileri güncellendi."}
@app.delete("/api/animals/{aid}")
def archive_animal(aid:int,uid:int=Depends(current_user)):
    with db() as c: owned_animal(c,uid,aid); c.execute("UPDATE animals SET status='Arşiv' WHERE id=? AND user_id=?",(aid,uid))
    audit(uid,"arşivle","animal",aid,"Silinmedi; arşive taşındı."); return {"message":"Hayvan arşive taşındı. Kayıt silinmedi."}
@app.post("/api/animals/{aid}/events")
def add_animal_event(aid:int,data:EventBody,uid:int=Depends(current_user)):
    with db() as c:
        a=owned_animal(c,uid,aid)
        if data.related_animal_id: owned_animal(c,uid,data.related_animal_id)
        cur=c.execute("INSERT INTO animal_events(user_id,animal_id,event_type,event_date,description,amount,related_animal_id,created_at) VALUES(?,?,?,?,?,?,?,?)",(uid,aid,data.event_type,data.event_date,data.description,data.amount,data.related_animal_id,datetime.now(timezone.utc).isoformat()))
        if data.event_type=="death": c.execute("UPDATE animals SET status='Ölü',death_date=?,death_reason=? WHERE id=?",(data.event_date,data.description,aid))
        elif data.event_type=="sale": c.execute("UPDATE animals SET status='Satıldı' WHERE id=?",(aid,))
        elif data.event_type=="transfer": c.execute("UPDATE animals SET status='Nakledildi' WHERE id=?",(aid,))
        elif data.event_type=="birth":
            c.execute("UPDATE animals SET birth_count=CAST(COALESCE(NULLIF(birth_count,''),'0') AS INTEGER)+1 WHERE id=?",(aid,))
        eid=cur.lastrowid
        if data.event_type in ("sale","birth") and data.amount>0:
            kind="income" if data.event_type=="sale" else "expense"
            c.execute("INSERT INTO transactions(user_id,kind,category,amount,currency,transaction_date,description,related_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",(uid,kind,"Hayvan satışı" if kind=="income" else "Doğum gideri",data.amount,"TRY",data.event_date,data.description,aid,datetime.now(timezone.utc).isoformat()))
    audit(uid,data.event_type,"animal_event",eid,f"Hayvan #{aid}: {data.description}")
    return {"id":eid,"message":"Hayvan olayı kaydedildi; geçmiş kaydı korundu."}
@app.get("/api/animals/{aid}/events")
def animal_events(aid:int,uid:int=Depends(current_user)):
    with db() as c: owned_animal(c,uid,aid); rows=c.execute("SELECT * FROM animal_events WHERE user_id=? AND animal_id=? ORDER BY event_date DESC,id DESC",(uid,aid)).fetchall()
    return [dict(r) for r in rows]
@app.post("/api/animals/{aid}/health")
def add_health(aid:int,data:HealthBody,uid:int=Depends(current_user)):
    with db() as c:
        owned_animal(c,uid,aid); cur=c.execute("INSERT INTO health_records(user_id,animal_id,record_type,record_date,description,vet,next_date,cost) VALUES(?,?,?,?,?,?,?,?)",(uid,aid,data.record_type,data.record_date,data.description,data.vet,data.next_date,data.cost))
        if data.cost>0: c.execute("INSERT INTO transactions(user_id,kind,category,amount,currency,transaction_date,description,related_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",(uid,"expense","Veteriner / sağlık",data.cost,"TRY",data.record_date,data.description,aid,datetime.now(timezone.utc).isoformat()))
    audit(uid,"sağlık kaydı","health",cur.lastrowid,data.record_type); return {"message":"Sağlık kaydı eklendi."}
@app.get("/api/animals/{aid}/health")
def get_health(aid:int,uid:int=Depends(current_user)):
    with db() as c: owned_animal(c,uid,aid); rows=c.execute("SELECT * FROM health_records WHERE user_id=? AND animal_id=? ORDER BY record_date DESC",(uid,aid)).fetchall()
    return [dict(r) for r in rows]

# --- Existing feed price list ---
# --- Animal health calendar (additive; does not alter legacy health records) ---
def _add_months_iso(value:str, months:int)->str:
    d=date.fromisoformat(value)
    m=d.month-1+months; year=d.year+m//12; month=m%12+1
    import calendar
    day=min(d.day,calendar.monthrange(year,month)[1])
    return date(year,month,day).isoformat()

@app.get("/api/animal-health/catalog")
def get_animal_health_catalog(category: str = "all", uid: int = Depends(current_user)):
    with db() as c:
        if category == "all":
            rows = c.execute("SELECT * FROM animal_health_product_catalog ORDER BY category,product_name").fetchall()
        else:
            rows = c.execute("SELECT * FROM animal_health_product_catalog WHERE category=? ORDER BY product_name", (category,)).fetchall()
    return [dict(r) for r in rows]

class HealthCatalogBody(BaseModel):
    product_name: str = Field(min_length=2, max_length=160)
    category: Literal["Aşılar ve tekrar aşıları","İç parazit","Dış parazit","Hap ve ağızdan ilaç","Enjeksiyon","Dökme uygulama (pour-on)","Vitamin ve mineral","Gebelik kontrolü","Doğum öncesi bakım","Doğum sonrası bakım","Yavru bakımı","Üreme takibi","Karantina ve biyogüvenlik","Sürü sağlık taraması","Tırnak ve ayak bakımı","Veteriner muayenesi","Diğer veteriner uygulamaları"]
    target_species: str = "Belirtilmedi"
    disease_protected: str = ""
    active_ingredient: str = ""
    dosage_rule: str = ""
    application_route: str = ""
    min_age_months: Optional[int] = Field(default=None, ge=0, le=240)
    repeat_days: Optional[int] = Field(default=None, ge=1, le=3650)
    withdrawal_meat_days: Optional[int] = Field(default=None, ge=0, le=365)
    withdrawal_milk_days: Optional[int] = Field(default=None, ge=0, le=365)
    source_url: str = ""
    source_note: str = ""
    verification_status: Literal["Doğrulanmadı","Resmî kaynakla doğrulandı"] = "Doğrulanmadı"

@app.get("/api/animal-health/dose-calculate")
def calculate_animal_health_dose(product_id:int, species:str, weight_kg:float=Query(...,gt=0,le=2500), uid:int=Depends(current_user)):
    """Calculate only a sourced, explicitly verified catalog rule; never infer a veterinary dose."""
    with db() as c:
        row=c.execute("SELECT * FROM animal_health_product_catalog WHERE id=?",(product_id,)).fetchone()
    if not row: raise HTTPException(404,"Katalog ürünü bulunamadı.")
    item=dict(row)
    if item.get("verification_status")!="Resmî kaynakla doğrulandı" or not (item.get("source_url") or item.get("source_note")):
        raise HTTPException(422,"Bu ürünün doz kuralı resmî kaynakla doğrulanmamış. Otomatik doz hesaplanmadı.")
    target=(item.get("target_species") or "").strip().casefold()
    if target not in {"", "belirtilmedi", "tümü", "tum hayvanlar", species.strip().casefold()}:
        raise HTTPException(422,"Ürün kataloğundaki hayvan türü seçilen türle uyuşmuyor.")
    rule=(item.get("dosage_rule") or "").strip().lower().replace(",", ".")
    m=re.fullmatch(r"(\d+(?:\.\d+)?)\s*(ml|mg|g|tablet|hap)\s*/\s*(?:(\d+(?:\.\d+)?)\s*)?kg",rule)
    if not m: raise HTTPException(422,"Doz kuralı otomatik hesaplanabilecek biçimde değil. Örnek: 1 ml/50 kg. Dozu elle yorumlamayın; etiketi kontrol edin.")
    amount=float(m.group(1)); unit=m.group(2); base=float(m.group(3) or 1)
    if base<=0: raise HTTPException(422,"Doz kuralındaki ağırlık değeri geçersiz.")
    total=amount*weight_kg/base
    return {"product_id":product_id,"product_name":item["product_name"],"species":species,"weight_kg":weight_kg,"dosage_rule":item["dosage_rule"],"calculated_amount":round(total,4),"unit":unit,"source_url":item.get("source_url") or "","source_note":item.get("source_note") or "","verification_status":item["verification_status"],"warning":"Bu matematiksel hesap, ürün etiketindeki dozun doğruluğunu veya hayvana uygunluğunu tek başına garanti etmez. Uygulama öncesi veteriner hekim ve güncel prospektüs kontrolü zorunludur."}

@app.post("/api/animal-health/catalog")
def save_animal_health_catalog_item(data: HealthCatalogBody, uid: int = Depends(require_admin)):
    # Dosyada veya kullanıcı girdisinde doz bulunması tek başına doğrulama sayılmaz.
    if data.verification_status == "Resmî kaynakla doğrulandı" and not (data.source_url.strip() or data.source_note.strip()):
        raise HTTPException(422, "Doz doğrulaması için resmî prospektüs/kaynak bağlantısı veya açık kaynak notu zorunludur.")
    now = datetime.now(timezone.utc).isoformat()
    with db() as c:
        c.execute("""INSERT INTO animal_health_product_catalog(product_name,category,target_species,disease_protected,active_ingredient,dosage_rule,application_route,min_age_months,repeat_days,withdrawal_meat_days,withdrawal_milk_days,source_url,source_note,verification_status,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(product_name,category) DO UPDATE SET target_species=excluded.target_species,disease_protected=excluded.disease_protected,active_ingredient=excluded.active_ingredient,dosage_rule=excluded.dosage_rule,application_route=excluded.application_route,min_age_months=excluded.min_age_months,repeat_days=excluded.repeat_days,withdrawal_meat_days=excluded.withdrawal_meat_days,withdrawal_milk_days=excluded.withdrawal_milk_days,source_url=excluded.source_url,source_note=excluded.source_note,verification_status=excluded.verification_status,updated_at=excluded.updated_at""",
        (data.product_name.strip(),data.category,data.target_species.strip(),data.disease_protected.strip(),data.active_ingredient.strip(),data.dosage_rule.strip(),data.application_route.strip(),data.min_age_months,data.repeat_days,data.withdrawal_meat_days,data.withdrawal_milk_days,data.source_url.strip(),data.source_note.strip(),data.verification_status,now,now))
    audit(uid,"katalog kaydet/güncelle","animal_health_product",None,data.product_name)
    return {"message":"Ürün kataloğu kaydedildi. Doz yalnızca resmî kaynakla doğrulandı olarak işaretlenmişse doğrulanmış kabul edilir."}

@app.get("/api/animal-health/plans")
def list_animal_health_plans(uid:int=Depends(current_user), status:str="all"):
    with db() as c:
        sql="SELECT p.*,a.name AS animal_name,a.tag_no FROM animal_health_plans p LEFT JOIN animals a ON a.id=p.animal_id WHERE p.user_id=?"
        args=[uid]
        if status!="all": sql+=" AND p.status=?"; args.append(status)
        rows=c.execute(sql+" ORDER BY p.planned_date ASC,p.id DESC",args).fetchall()
    today=date.today().isoformat(); out=[]
    for r in rows:
        x=dict(r); x["days_left"]=(date.fromisoformat(x["planned_date"])-date.today()).days
        x["countdown_status"]="Gecikti" if x["days_left"]<0 and x["status"]!="Tamamlandı" else ("Bugün" if x["days_left"]==0 and x["status"]!="Tamamlandı" else ("Yaklaşıyor" if x["days_left"]<=7 and x["status"]!="Tamamlandı" else x["status"]))
        out.append(x)
    return out

@app.post("/api/animal-health/plans")
def create_animal_health_plan(data:AnimalHealthPlanBody,uid:int=Depends(current_user)):
    if data.scope=="animal" and not data.animal_id: raise HTTPException(422,"Tekil takip için hayvan seçin.")
    if data.scope=="group" and not data.group_name.strip(): raise HTTPException(422,"Sürü/grup adı girin.")
    try: date.fromisoformat(data.planned_date)
    except ValueError: raise HTTPException(422,"Plan tarihi YYYY-AA-GG biçiminde olmalı.")
    if data.repeat_days and data.repeat_months:
        raise HTTPException(422,"Tekrar aralığında gün veya ay seçeneklerinden yalnızca birini girin.")
    now=datetime.now(timezone.utc).isoformat()
    ear_tags=""
    with db() as c:
        if data.animal_id:
            a=owned_animal(c,uid,data.animal_id)
            if a["species"]!=data.species: raise HTTPException(422,"Seçilen hayvanın türü ile plan türü uyuşmuyor.")
            ear_tags=(a["tag_no"] or "").strip()
        cur=c.execute("""INSERT INTO animal_health_plans(user_id,animal_id,species,scope,group_name,category,title,product_name,active_ingredient,planned_date,repeat_months,repeat_days,source_url,source_note,notes,status,ear_tags,created_at,updated_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,'Bekliyor',?,?,?)""",(uid,data.animal_id,data.species,data.scope,data.group_name.strip(),data.category,data.title.strip(),data.product_name.strip(),data.active_ingredient.strip(),data.planned_date,data.repeat_months,data.repeat_days,data.source_url.strip(),data.source_note.strip(),data.notes.strip(),ear_tags,now,now))
        pid=cur.lastrowid
    audit(uid,"oluştur","animal_health_plan",pid,data.title)
    return {"id":pid,"message":"Sağlık takvimi kaydedildi."}

@app.post("/api/animal-health/plans/{plan_id}/complete")
def complete_animal_health_plan(plan_id:int,data:AnimalHealthCompleteBody,uid:int=Depends(current_user)):
    try: date.fromisoformat(data.applied_date)
    except ValueError: raise HTTPException(422,"Uygulama tarihi YYYY-AA-GG biçiminde olmalı.")
    now=datetime.now(timezone.utc).isoformat()
    with db() as c:
        p=c.execute("SELECT * FROM animal_health_plans WHERE id=? AND user_id=?",(plan_id,uid)).fetchone()
        if not p: raise HTTPException(404,"Takvim kaydı bulunamadı.")
        ids=data.animal_ids
        if p["scope"]=="animal":
            ids=[p["animal_id"]]
        elif ids:
            for aid in ids: owned_animal(c,uid,aid)
        elif p["scope"]=="group":
            raise HTTPException(422,"Toplu uygulamada uygulanan hayvanların küpe kayıtlarını seçin.")
        tag_values=[]
        for aid in ids:
            a=owned_animal(c,uid,aid)
            if a["species"]!=p["species"]: raise HTTPException(422,"Seçilen hayvanların türleri planla uyuşmuyor.")
            tag=(a["tag_no"] or "").strip()
            if tag: tag_values.append(tag)
        ear_tags=", ".join(dict.fromkeys(tag_values))
        next_date=(date.fromisoformat(data.applied_date)+timedelta(days=int(p["repeat_days"]))).isoformat() if p["repeat_days"] else (_add_months_iso(data.applied_date,p["repeat_months"]) if p["repeat_months"] else "")
        c.execute("""INSERT INTO animal_health_applications(user_id,plan_id,animal_id,animals_json,applied_date,category,title,product_name,active_ingredient,dose_note,applicator,withdrawal_meat_days,withdrawal_milk_days,next_date,notes,ear_tags,created_at)
        VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",(uid,plan_id,p["animal_id"],json.dumps(ids,ensure_ascii=False),data.applied_date,p["category"],p["title"],data.product_name or p["product_name"],data.active_ingredient or p["active_ingredient"],data.dose_note,data.applicator,data.withdrawal_meat_days,data.withdrawal_milk_days,next_date,data.notes,ear_tags,now))
        if next_date:
            c.execute("UPDATE animal_health_plans SET planned_date=?,status='Bekliyor',product_name=?,active_ingredient=?,last_application_date=?,ear_tags=?,updated_at=? WHERE id=? AND user_id=?",(next_date,data.product_name or p["product_name"],data.active_ingredient or p["active_ingredient"],data.applied_date,ear_tags or p["ear_tags"],now,plan_id,uid))
        else:
            c.execute("UPDATE animal_health_plans SET status='Tamamlandı',last_application_date=?,ear_tags=?,updated_at=? WHERE id=? AND user_id=?",(data.applied_date,ear_tags or p["ear_tags"],now,plan_id,uid))
    audit(uid,"tamamla","animal_health_plan",plan_id,p["title"])
    return {"message":"Uygulama geçmişe kaydedildi."+(" Sonraki tarih: "+next_date if next_date else " Tek seferlik plan tamamlandı."),"next_date":next_date}

@app.get("/api/animal-health/history")
def list_animal_health_history(uid:int=Depends(current_user)):
    with db() as c: rows=c.execute("SELECT h.*,a.name AS animal_name,a.tag_no,p.group_name,p.scope FROM animal_health_applications h LEFT JOIN animals a ON a.id=h.animal_id LEFT JOIN animal_health_plans p ON p.id=h.plan_id WHERE h.user_id=? ORDER BY h.applied_date DESC,h.id DESC",(uid,)).fetchall()
    return [dict(x) for x in rows]

@app.get("/api/feeds")
def get_feeds(uid:int=Depends(current_user)):
    with db() as c: rows=c.execute("SELECT id,name,supplier,price FROM feeds WHERE user_id=? ORDER BY price ASC",(uid,)).fetchall()
    return [dict(r) for r in rows]
@app.post("/api/feeds")
def save_feed(data:FeedBody,uid:int=Depends(current_user)):
    with db() as c: cur=c.execute("INSERT INTO feeds(user_id,name,supplier,price) VALUES(?,?,?,?)",(uid,data.name.strip(),data.supplier.strip() or "Belirtilmedi",data.price))
    audit(uid,"ekle","feed_price",cur.lastrowid,data.name); return {"id":cur.lastrowid,"message":"Yem fiyatı kaydedildi."}
@app.delete("/api/feeds/{fid}")
def delete_feed(fid:int,uid:int=Depends(current_user)):
    with db() as c: cur=c.execute("DELETE FROM feeds WHERE id=? AND user_id=?",(fid,uid))
    if not cur.rowcount: raise HTTPException(404,"Yem kaydı bulunamadı.")
    return {"message":"Yem fiyatı kaydı silindi."}

# --- Stock / warehouse ---
@app.get("/api/stock")
def list_stock(uid:int=Depends(current_user)):
    with db() as c: rows=c.execute("SELECT * FROM inventory WHERE user_id=? ORDER BY name",(uid,)).fetchall()
    return [dict(r) for r in rows]
@app.post("/api/stock")
def create_stock(data:StockBody,uid:int=Depends(current_user)):
    if data.quantity<0 or data.min_quantity<0 or data.unit_cost<0: raise HTTPException(400,"Stok ve fiyat negatif olamaz.")
    with db() as c:
        try: cur=c.execute("INSERT INTO inventory(user_id,name,category,unit,quantity,min_quantity,unit_cost,supplier,expiry_date,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(uid,data.name,data.category,data.unit,data.quantity,data.min_quantity,data.unit_cost,data.supplier,data.expiry_date,datetime.now(timezone.utc).isoformat()))
        except sqlite3.IntegrityError: raise HTTPException(409,"Bu stok kalemi zaten var.")
        iid=cur.lastrowid
        if data.quantity>0: c.execute("INSERT INTO stock_moves(user_id,item_id,move_type,quantity,unit_cost,description,created_at) VALUES(?,?,?,?,?,?,?)",(uid,iid,"in",data.quantity,data.unit_cost,"İlk stok girişi",datetime.now(timezone.utc).isoformat()))
    audit(uid,"stok oluştur","inventory",iid,data.name); return {"id":iid,"message":"Stok kalemi oluşturuldu."}
@app.post("/api/stock/move")
def stock_move(data:StockMoveBody,uid:int=Depends(current_user)):
    with db() as c:
        item=c.execute("SELECT * FROM inventory WHERE id=? AND user_id=?",(data.item_id,uid)).fetchone()
        if not item: raise HTTPException(404,"Stok kalemi bulunamadı.")
        newq=float(item["quantity"])+(data.quantity if data.move_type=="in" else -data.quantity if data.move_type=="out" else data.quantity-float(item["quantity"]))
        if newq<0: raise HTTPException(400,"Yetersiz stok. Eksi stok oluşturulmadı.")
        c.execute("UPDATE inventory SET quantity=?,unit_cost=CASE WHEN ?>0 THEN ? ELSE unit_cost END WHERE id=?",(newq,data.unit_cost,data.unit_cost,data.item_id))
        c.execute("INSERT INTO stock_moves(user_id,item_id,move_type,quantity,unit_cost,description,created_at) VALUES(?,?,?,?,?,?,?)",(uid,data.item_id,data.move_type,data.quantity,data.unit_cost,data.description,datetime.now(timezone.utc).isoformat()))
        if data.move_type=="in" and data.unit_cost>0:
            c.execute("INSERT INTO transactions(user_id,kind,category,amount,currency,transaction_date,description,related_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",(uid,"expense","Stok / satın alma",data.quantity*data.unit_cost,"TRY",date.today().isoformat(),data.description or item["name"],data.item_id,datetime.now(timezone.utc).isoformat()))
    audit(uid,"stok hareketi","inventory",data.item_id,data.move_type); return {"quantity":newq,"message":"Stok güncellendi."}
@app.get("/api/stock/alerts")
def stock_alerts(uid:int=Depends(current_user)):
    with db() as c: rows=c.execute("SELECT * FROM inventory WHERE user_id=? AND quantity<=min_quantity ORDER BY quantity",(uid,)).fetchall()
    return [dict(r) for r in rows]

# --- Accounting, milk, tasks, fields and equipment ---
@app.get("/api/transactions")
def get_transactions(uid:int=Depends(current_user),limit:int=Query(300,ge=1,le=2000)):
    with db() as c: rows=c.execute("SELECT * FROM transactions WHERE user_id=? ORDER BY transaction_date DESC,id DESC LIMIT ?",(uid,limit)).fetchall()
    return [dict(r) for r in rows]
@app.post("/api/transactions")
def add_transaction(data:TransactionBody,uid:int=Depends(current_user)):
    with db() as c: cur=c.execute("INSERT INTO transactions(user_id,kind,category,amount,currency,transaction_date,description,related_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",(uid,data.kind,data.category,data.amount,data.currency,data.transaction_date,data.description,data.related_id,datetime.now(timezone.utc).isoformat()))
    audit(uid,"muhasebe kaydı","transaction",cur.lastrowid,data.category); return {"id":cur.lastrowid,"message":"Muhasebe kaydı eklendi."}
@app.get("/api/accounting/summary")
def accounting_summary(uid:int=Depends(current_user)):
    with db() as c:
        r=c.execute("SELECT COALESCE(SUM(CASE WHEN kind='income' AND currency='TRY' THEN amount ELSE 0 END),0) income,COALESCE(SUM(CASE WHEN kind='expense' AND currency='TRY' THEN amount ELSE 0 END),0) expense FROM transactions WHERE user_id=?",(uid,)).fetchone()
        n=c.execute("SELECT COUNT(*) n FROM animals WHERE user_id=? AND status='Aktif'",(uid,)).fetchone()["n"]
        milk=c.execute("SELECT COALESCE(SUM(liters),0) liters FROM milk_records WHERE user_id=?",(uid,)).fetchone()["liters"]
    return {"income":r["income"],"expense":r["expense"],"profit":r["income"]-r["expense"],"active_animals":n,"milk_liters":milk}
@app.get("/api/milk")
def get_milk(uid:int=Depends(current_user)):
    with db() as c: rows=c.execute("SELECT m.*,a.name animal_name FROM milk_records m LEFT JOIN animals a ON a.id=m.animal_id WHERE m.user_id=? ORDER BY record_date DESC,id DESC",(uid,)).fetchall()
    return [dict(r) for r in rows]
@app.post("/api/milk")
def add_milk(data:MilkBody,uid:int=Depends(current_user)):
    with db() as c:
        if data.animal_id: owned_animal(c,uid,data.animal_id)
        cur=c.execute("INSERT INTO milk_records(user_id,animal_id,record_date,liters,unit_price,buyer,notes) VALUES(?,?,?,?,?,?,?)",(uid,data.animal_id,data.record_date,data.liters,data.unit_price,data.buyer,data.notes))
        if data.unit_price>0: c.execute("INSERT INTO transactions(user_id,kind,category,amount,currency,transaction_date,description,related_id,created_at) VALUES(?,?,?,?,?,?,?,?,?)",(uid,"income","Süt satışı",data.liters*data.unit_price,"TRY",data.record_date,data.buyer or "Süt satışı",cur.lastrowid,datetime.now(timezone.utc).isoformat()))
    return {"id":cur.lastrowid,"message":"Süt kaydı eklendi."}
@app.get("/api/tasks")
def get_tasks(uid:int=Depends(current_user)):
    with db() as c: rows=c.execute("SELECT * FROM farm_tasks WHERE user_id=? ORDER BY task_date,status",(uid,)).fetchall()
    return [dict(r) for r in rows]
@app.post("/api/tasks")
def add_task(data:TaskBody,uid:int=Depends(current_user)):
    with db() as c: cur=c.execute("INSERT INTO farm_tasks(user_id,title,task_date,category,status,notes) VALUES(?,?,?,?,?,?)",(uid,data.title,data.task_date,data.category,data.status,data.notes))
    return {"id":cur.lastrowid,"message":"Görev eklendi."}
@app.get("/api/equipment")
def get_equipment(uid:int=Depends(current_user)):
    with db() as c: return [dict(x) for x in c.execute("SELECT * FROM equipment WHERE user_id=? ORDER BY name",(uid,)).fetchall()]
@app.post("/api/equipment")
def add_equipment(data:EquipmentBody,uid:int=Depends(current_user)):
    with db() as c: cur=c.execute("INSERT INTO equipment(user_id,name,kind,last_service,next_service,notes) VALUES(?,?,?,?,?,?)",(uid,data.name,data.kind,data.last_service,data.next_service,data.notes))
    return {"id":cur.lastrowid,"message":"Ekipman kaydedildi."}
@app.get("/api/fields")
def get_fields(uid:int=Depends(current_user)):
    with db() as c: return [dict(x) for x in c.execute("SELECT * FROM fields WHERE user_id=? ORDER BY name",(uid,)).fetchall()]
@app.post("/api/fields")
def add_field(data:FieldBody,uid:int=Depends(current_user)):
    with db() as c: cur=c.execute("INSERT INTO fields(user_id,name,crop,area,unit,plant_date,harvest_date,notes) VALUES(?,?,?,?,?,?,?,?)",(uid,data.name,data.crop,data.area,data.unit,data.plant_date,data.harvest_date,data.notes))
    return {"id":cur.lastrowid,"message":"Tarla/sera kaydedildi."}
@app.get("/api/field-measurements")
def get_field_measurements(uid:int=Depends(current_user)):
    with db() as c: rows=c.execute("SELECT id,name,area_m2,perimeter_m,created_at FROM field_measurements WHERE user_id=? ORDER BY id DESC",(uid,)).fetchall()
    return [dict(x,area_dekar=x["area_m2"]/1000) for x in rows]
@app.post("/api/field-measurements")
def save_field_measurement(data:FieldMeasurementBody,uid:int=Depends(current_user)):
    if len(data.points)<3: raise HTTPException(400,"Alan için en az 3 GPS noktası gerekir.")
    for pt in data.points:
        if not isinstance(pt.get("lat"),(int,float)) or not isinstance(pt.get("lng"),(int,float)) or not -90<=pt["lat"]<=90 or not -180<=pt["lng"]<=180: raise HTTPException(400,"GPS koordinatları geçersiz.")
    with db() as c: cur=c.execute("INSERT INTO field_measurements(user_id,name,area_m2,perimeter_m,points_json,created_at) VALUES(?,?,?,?,?,?)",(uid,data.name,data.area_m2,data.perimeter_m,json.dumps(data.points,ensure_ascii=False),datetime.now(timezone.utc).isoformat()))
    return {"id":cur.lastrowid,"message":"Parsel ölçümü kaydedildi."}
@app.delete("/api/field-measurements/{measurement_id}")
def delete_field_measurement(measurement_id:int,uid:int=Depends(current_user)):
    with db() as c: cur=c.execute("DELETE FROM field_measurements WHERE id=? AND user_id=?",(measurement_id,uid))
    if not cur.rowcount: raise HTTPException(404,"Ölçüm bulunamadı.")
    return {"message":"Ölçüm silindi."}
@app.get("/api/dashboard")
def dashboard(uid:int=Depends(current_user)):
    with db() as c:
        animals=c.execute("SELECT status,COUNT(*) n FROM animals WHERE user_id=? GROUP BY status",(uid,)).fetchall()
        stocks=c.execute("SELECT COUNT(*) n FROM inventory WHERE user_id=?",(uid,)).fetchone()["n"]
        alerts=c.execute("SELECT COUNT(*) n FROM inventory WHERE user_id=? AND quantity<=min_quantity",(uid,)).fetchone()["n"]
        money=c.execute("SELECT COALESCE(SUM(CASE WHEN kind='income' AND currency='TRY' THEN amount ELSE 0 END),0) income,COALESCE(SUM(CASE WHEN kind='expense' AND currency='TRY' THEN amount ELSE 0 END),0) expense FROM transactions WHERE user_id=?",(uid,)).fetchone()
        milk=c.execute("SELECT COALESCE(SUM(liters),0) liters FROM milk_records WHERE user_id=? AND record_date>=date('now','-30 day')",(uid,)).fetchone()["liters"]
    return {"animals":{r["status"]:r["n"] for r in animals},"stock_items":stocks,"stock_alerts":alerts,"income":money["income"],"expense":money["expense"],"profit":money["income"]-money["expense"],"milk_last_30_days":milk}

# V31 home screen quick items: visible to authenticated users, writable by admin only.
@app.get("/api/home-widgets")
def get_home_widgets(uid:int=Depends(current_user)):
    with db() as c: rows=c.execute("SELECT id,title,content,kind,text_case,font_size,color,font_weight,font_style,sort_order FROM home_widgets ORDER BY sort_order,id").fetchall()
    return {"items":[dict(r) for r in rows]}

@app.post("/api/home-widgets")
def add_home_widget(body:HomeWidgetBody,uid:int=Depends(require_admin)):
    content=body.content.strip()
    if body.kind=="link" and not content.lower().startswith(("https://","http://")): raise HTTPException(400,"Bağlantı http:// veya https:// ile başlamalıdır.")
    if body.kind=="phone" and not re.fullmatch(r"[+0-9() .-]{7,25}",content): raise HTTPException(400,"Telefon numarasını kontrol edin.")
    with db() as c:
        order=c.execute("SELECT COALESCE(MAX(sort_order),0)+1 n FROM home_widgets").fetchone()["n"]
        cur=c.execute("INSERT INTO home_widgets(title,content,kind,text_case,font_size,color,font_weight,font_style,sort_order,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",(body.title.strip(),content,body.kind,body.text_case,body.font_size,body.color,body.font_weight,body.font_style,order,datetime.now(timezone.utc).isoformat()))
        wid=cur.lastrowid
    audit(uid,"ana ekrana öğe ekle","home_widget",wid,body.kind)
    return {"id":wid,"message":"Ana ekran öğesi eklendi."}

@app.delete("/api/home-widgets/{widget_id}")
def delete_home_widget(widget_id:int,uid:int=Depends(require_admin)):
    with db() as c: cur=c.execute("DELETE FROM home_widgets WHERE id=?",(widget_id,))
    if not cur.rowcount: raise HTTPException(404,"Öğe bulunamadı.")
    audit(uid,"ana ekran öğesi sil","home_widget",widget_id)
    return {"message":"Öğe silindi."}
@app.put("/api/home-widgets/{widget_id}")
def update_home_widget(widget_id:int,body:HomeWidgetBody,uid:int=Depends(require_admin)):
    content=body.content.strip()
    if body.kind=="link" and not content.lower().startswith(("https://","http://")): raise HTTPException(400,"Bağlantı http:// veya https:// ile başlamalıdır.")
    if body.kind=="phone" and not re.fullmatch(r"[+0-9() .-]{7,25}",content): raise HTTPException(400,"Telefon numarasını kontrol edin.")
    with db() as c:
        cur=c.execute("UPDATE home_widgets SET title=?,content=?,kind=?,text_case=?,font_size=?,color=?,font_weight=?,font_style=? WHERE id=?",(body.title.strip(),content,body.kind,body.text_case,body.font_size,body.color,body.font_weight,body.font_style,widget_id))
    if not cur.rowcount: raise HTTPException(404,"Öğe bulunamadı.")
    audit(uid,"ana ekran öğesi düzenle","home_widget",widget_id,body.kind)
    return {"message":"Ana ekran öğesi güncellendi."}

@app.post("/api/home-widgets/reorder")
def reorder_home_widgets(body:HomeWidgetOrderBody,uid:int=Depends(require_admin)):
    with db() as c:
        existing={r["id"] for r in c.execute("SELECT id FROM home_widgets").fetchall()}
        order=[x for x in body.widget_ids if x in existing]
        for index,wid in enumerate(order): c.execute("UPDATE home_widgets SET sort_order=? WHERE id=?",(index+1,wid))
    return {"message":"Ana ekran öğe sırası kaydedildi."}

@app.get("/api/home-preferences")
def get_home_preferences(uid:int=Depends(current_user)):
    with db() as c: row=c.execute("SELECT theme,clock_style,app_order,folder_map FROM home_preferences WHERE id=1").fetchone()
    return {"theme":row["theme"],"clock_style":row["clock_style"],"app_order":json.loads(row["app_order"] or "[]"),"folder_map":json.loads(row["folder_map"] or "{}")}

@app.put("/api/home-preferences")
def save_home_preferences(body:HomePreferencesBody,uid:int=Depends(require_admin)):
    allowed={"animals","feed","farming","milk","accounting","tasks","equipment","finance","banking","bist","hunters","ai","supports","media","admin"}
    order=[]
    for app_id in body.app_order:
        if app_id in allowed and app_id not in order: order.append(app_id)
    folders={k:v.strip()[:30] for k,v in body.folder_map.items() if k in allowed and isinstance(v,str) and v.strip()}
    with db() as c:
        c.execute("UPDATE home_preferences SET theme=?,clock_style=?,app_order=?,folder_map=?,updated_at=? WHERE id=1",(body.theme,body.clock_style,json.dumps(order,ensure_ascii=False),json.dumps(folders,ensure_ascii=False),datetime.now(timezone.utc).isoformat()))
    audit(uid,"ana ekran görünümünü düzenle","home_preferences",1,body.theme+" / "+body.clock_style)
    return {"message":"Ana ekran tasarımı kaydedildi."}

# --- Backup: authenticated, local database snapshot ---
@app.post("/api/backup")
def create_backup(uid:int=Depends(current_user)):
    stamp=datetime.now().strftime("%Y%m%d_%H%M%S"); dest=BACKUP_DIR/f"feyzullah_user{uid}_{stamp}.db"
    with db() as source, sqlite3.connect(dest) as target: source.backup(target)
    audit(uid,"yedek oluştur","backup",None,dest.name)
    return {"message":"Veritabanı yedeği oluşturuldu.","filename":dest.name}
@app.get("/api/backups")
def list_backups(uid:int=Depends(current_user)):
    with db() as c: users=c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
    if users != 1: raise HTTPException(403,"Güvenlik için yedek indirme/geri yükleme tek kullanıcılı kurulumda açılır.")
    return [{"filename":x.name,"size":x.stat().st_size,"created_at":datetime.fromtimestamp(x.stat().st_mtime,timezone.utc).isoformat()} for x in sorted(BACKUP_DIR.glob(f"feyzullah_user{uid}_*.db"),key=lambda p:p.stat().st_mtime,reverse=True)]
@app.get("/api/backups/{filename}")
def download_backup(filename:str,uid:int=Depends(current_user)):
    with db() as c: users=c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
    if users != 1: raise HTTPException(403,"Güvenlik için yedek indirme tek kullanıcılı kurulumda açılır.")
    if Path(filename).name != filename or not filename.startswith(f"feyzullah_user{uid}_") or not filename.endswith(".db"):
        raise HTTPException(404,"Yedek bulunamadı.")
    path=BACKUP_DIR/filename
    if not path.is_file(): raise HTTPException(404,"Yedek bulunamadı.")
    return FileResponse(path,media_type="application/octet-stream",filename=filename)
@app.post("/api/backup/restore")
async def restore_backup(file:UploadFile=File(...),uid:int=Depends(current_user)):
    if Path(file.filename or "").suffix.lower() not in (".db",".sqlite",".sqlite3"):
        raise HTTPException(400,"Yalnızca SQLite .db/.sqlite yedeği yükleyin.")
    c=db()
    try:
        user_count=c.execute("SELECT COUNT(*) n FROM users").fetchone()["n"]
        current=c.execute("SELECT username FROM users WHERE id=?",(uid,)).fetchone()
    finally: c.close()
    if user_count != 1 or not current:
        raise HTTPException(403,"Güvenli geri yükleme yalnızca tek kullanıcılı kurulumda etkin. Çok kullanıcılı veritabanı değiştirilmedi.")
    raw=await file.read(100*1024*1024+1)
    if len(raw)>100*1024*1024: raise HTTPException(413,"Yedek dosyası 100 MB sınırını aşıyor.")
    fd,tmp_name=tempfile.mkstemp(prefix="v21_restore_",suffix=".db",dir=str(DB_PATH.parent));os.close(fd)
    try:
        Path(tmp_name).write_bytes(raw)
        test=sqlite3.connect(tmp_name)
        try:
            integrity=test.execute("PRAGMA integrity_check").fetchone()[0]
            tables={r[0] for r in test.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            if integrity!="ok" or not {"users","sessions","animals","inventory"}.issubset(tables):
                raise HTTPException(400,"Dosya geçerli Akıllı Çiftlik SQLite yedeği değil.")
            restored=test.execute("SELECT username FROM users WHERE id=?",(uid,)).fetchone()
            restored_count=test.execute("SELECT COUNT(*) FROM users").fetchone()[0]
            if not restored or restored[0].lower()!=current[0].lower() or restored_count!=1:
                raise HTTPException(400,"Yedek bu kullanıcıya ait değil veya birden fazla kullanıcı içeriyor. Geri yükleme iptal edildi.")
        finally: test.close()
        stamp=datetime.now().strftime("%Y%m%d_%H%M%S")
        safety=BACKUP_DIR/f"feyzullah_user{uid}_before_restore_{stamp}.db"
        source=db();target=sqlite3.connect(safety)
        try: source.backup(target)
        finally: source.close();target.close()
        for suffix in ("-wal","-shm"):
            side=Path(str(DB_PATH)+suffix)
            if side.exists(): side.unlink()
        os.replace(tmp_name,DB_PATH)
        init_db()
        audit(uid,"yedekten geri yükle","backup",None,safety.name)
        return {"message":"Yedek güvenlik kontrolünden geçti ve geri yüklendi. Oturum kapanırsa yeniden giriş yapın.","safety_backup":safety.name}
    finally:
        if os.path.exists(tmp_name): os.unlink(tmp_name)
@app.get("/api/audit")
def get_audit(uid:int=Depends(current_user),limit:int=Query(200,ge=1,le=1000)):
    with db() as c: return [dict(r) for r in c.execute("SELECT * FROM audit_log WHERE user_id=? ORDER BY id DESC LIMIT ?",(uid,limit)).fetchall()]

# --- V21 crop calendar, persistent alerts and farm settings ---
@app.get("/api/crops/catalog")
def crop_catalog():
    return [{"name":n,"category":c,"indicative_days":d} for n,c,d in CROP_CATALOG]

# Broad Turkey planting windows. These are planning estimates, not agronomic guarantees.
def crop_planting_months(name, category):
    n=name.casefold()
    fixed={
        "buğday":(10,11,12),"arpa":(10,11,12),"çavdar":(10,11,12),"tritikale":(10,11,12),"yulaf":(10,11,12,3),"kanola":(9,10),
        "mısır":(4,5,6),"silajlık mısır":(4,5,6),"patlamalık mısır":(4,5,6),"pamuk":(4,5),"ayçiçeği":(3,4,5),"soya":(4,5),"şeker pancarı":(3,4),
        "domates":(3,4,5),"biber":(3,4,5),"patlıcan":(3,4,5),"salatalık":(3,4,5,6),"kavun":(4,5),"karpuz":(4,5),"bamya":(4,5,6),"fasulye":(4,5,6),"kuru fasulye":(4,5),"barbunya":(4,5),"börülce":(4,5,6),"kabak":(3,4,5,6),
        "patates":(3,4,5),"nohut":(2,3,4),"mercimek":(10,11,2,3),"bezelye":(2,3,10,11),"bakla":(10,11,2,3),"ıspanak":(9,10,11,2,3),"marul":(2,3,9,10,11),"roka":(2,3,9,10,11),"turp":(2,3,9,10,11),"havuç":(2,3,9,10,11),"soğan":(10,11,2,3),"sarımsak":(10,11,2,3),
    }
    for key, months in fixed.items():
        if n==key or n.startswith(key+" ") or key in n: return months
    if category=="Yapraklı" or category=="Kök": return (2,3,4,9,10,11)
    if category=="Baklagil": return (2,3,4,5)
    if category=="Tahıl": return (3,4,10,11)
    if category=="Meyve": return (11,12,1,2,3)
    if category=="Yem bitkisi": return (3,4,9,10)
    if category=="Endüstri": return (3,4,5)
    return (3,4,5,9,10)

def build_auto_crop_plan(region="Konya / Ereğli", today=None, location=""):
    """Bölgeye göre yaklaşık ekim planı; yerel çeşit/toprak verisi yerine geçmez."""
    today=today or date.today(); result=[]
    climate_days={"Marmara":0,"Ege":-7,"Akdeniz":-10,"İç Anadolu":10,"Karadeniz":7,"Doğu Anadolu":21,"Güneydoğu Anadolu":-5}
    zone=(region or "").strip()
    climate_shift=climate_days.get(zone,0)
    display_region=(zone + (" / "+location.strip() if location else "")).strip(" /")
    for name,category,days in CROP_CATALOG:
        months=crop_planting_months(name,category); candidate=None
        # Bölgesel farktan sonra tarih, ürünün ekim ayı aralığında kalır.
        for offset in range(1, 550):
            shifted=today+timedelta(days=offset+climate_shift)
            if shifted>today and shifted.month in months:
                candidate=shifted
                break
        if candidate is None: candidate=today+timedelta(days=30)
        harvest=candidate+timedelta(days=days)
        result.append({"name":name,"category":category,"indicative_days":days,"method":"Açık tarla","region":display_region or "Türkiye","location":location,"sow_date":candidate.isoformat(),"harvest_date":harvest.isoformat(),"days_to_sow":(candidate-today).days,"days_to_harvest":(harvest-today).days})
    return result

@app.get("/api/crops/auto-plan")
def get_auto_crop_plan(region:str=Query("İç Anadolu",max_length=100),location:str=Query("Konya / Ereğli",max_length=100)):
    if not region or not location: raise HTTPException(400,"Otomatik plan için bölge ve il/konum seçin.")
    return build_auto_crop_plan(region,location=location)

@app.post("/api/crops/auto-plan/save")
def save_auto_crop_plan(region:str=Query("İç Anadolu",max_length=100),location:str=Query("Konya / Ereğli",max_length=100),uid:int=Depends(current_user)):
    if not region or not location: raise HTTPException(400,"Otomatik plan için bölge ve il/konum seçin.")
    plan=build_auto_crop_plan(region,location=location); added=0; skipped=0
    with db() as c:
        for item in plan:
            exists=c.execute("SELECT 1 FROM crop_calendar WHERE user_id=? AND crop=? AND sow_date=? AND method='Açık tarla' LIMIT 1",(uid,item["name"],item["sow_date"])).fetchone()
            if exists: skipped+=1; continue
            c.execute("INSERT INTO crop_calendar(user_id,crop,variety,method,region,sow_date,transplant_date,harvest_start,harvest_end,notes,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(uid,item["name"],"Otomatik plan",item["method"],item["region"],item["sow_date"],"",item["harvest_date"],"","Katalogdaki yaklaşık yetişme süresine ve genel Türkiye ekim penceresine göre otomatik önerildi; yerel iklim/çeşit bilgisine göre kontrol edin.",datetime.now(timezone.utc).isoformat())); added+=1
    audit(uid,"otomatik ürün ekim planı oluştur","crop_calendar",None,f"Eklenen: {added}; mevcut: {skipped}")
    return {"message":f"{added} ürün takvime eklendi; {skipped} mevcut kayıt atlandı.","added":added,"skipped":skipped}
@app.get("/api/crops/calendar")
def get_crop_calendar(uid:int=Depends(current_user)):
    with db() as c: rows=c.execute("SELECT * FROM crop_calendar WHERE user_id=? ORDER BY sow_date,id",(uid,)).fetchall()
    return [dict(x) for x in rows]
@app.post("/api/crops/calendar")
def add_crop_calendar(data:CropCalendarBody,uid:int=Depends(current_user)):
    try: date.fromisoformat(data.sow_date)
    except ValueError: raise HTTPException(400,"Ekim tarihi YYYY-AA-GG biçiminde olmalı.")
    for v in (data.transplant_date,data.harvest_start,data.harvest_end):
        if v:
            try: date.fromisoformat(v)
            except ValueError: raise HTTPException(400,"Tarihler YYYY-AA-GG biçiminde olmalı.")
    with db() as c:
        cur=c.execute("INSERT INTO crop_calendar(user_id,crop,variety,method,region,sow_date,transplant_date,harvest_start,harvest_end,notes,created_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",(uid,data.crop,data.variety,data.method,data.region,data.sow_date,data.transplant_date,data.harvest_start,data.harvest_end,data.notes,datetime.now(timezone.utc).isoformat()))
    audit(uid,"ekim takvimi oluştur","crop_calendar",cur.lastrowid,data.crop)
    return {"id":cur.lastrowid,"message":"Ekim ve hasat takvimi kaydedildi."}
@app.delete("/api/crops/calendar/{cid}")
def delete_crop_calendar(cid:int,uid:int=Depends(current_user)):
    with db() as c: cur=c.execute("DELETE FROM crop_calendar WHERE id=? AND user_id=?",(cid,uid))
    if not cur.rowcount: raise HTTPException(404,"Takvim kaydı bulunamadı.")
    audit(uid,"takvim kaydı sil","crop_calendar",cid,"Kullanıcı isteği")
    return {"message":"Takvim kaydı silindi."}
@app.get("/api/alerts")
def get_price_alerts(uid:int=Depends(current_user)):
    with db() as c: return [dict(x) for x in c.execute("SELECT * FROM price_alerts WHERE user_id=? ORDER BY active DESC,id DESC",(uid,)).fetchall()]
@app.post("/api/alerts")
def create_price_alert(data:PriceAlertBody,uid:int=Depends(current_user)):
    symbol=data.symbol.upper().strip()
    if not symbol.endswith("USDT"): raise HTTPException(400,"Şimdilik yalnızca USDT pariteleri destekleniyor.")
    with db() as c: cur=c.execute("INSERT INTO price_alerts(user_id,symbol,condition,target,created_at) VALUES(?,?,?,?,?)",(uid,symbol,data.condition,data.target,datetime.now(timezone.utc).isoformat()))
    audit(uid,"fiyat alarmı oluştur","price_alert",cur.lastrowid,symbol)
    return {"id":cur.lastrowid,"message":"Fiyat alarmı kaydedildi."}
@app.post("/api/alerts/{aid}/trigger")
def trigger_price_alert(aid:int,data:dict,uid:int=Depends(current_user)):
    observed=float(data.get("observed",0))
    with db() as c:
        a=c.execute("SELECT * FROM price_alerts WHERE id=? AND user_id=? AND active=1",(aid,uid)).fetchone()
        if not a: raise HTTPException(404,"Aktif alarm bulunamadı.")
        hit=observed>=a["target"] if a["condition"]=="above" else observed<=a["target"]
        if not hit: return {"triggered":False}
        c.execute("INSERT INTO price_alert_history(user_id,alert_id,symbol,condition,target,observed,triggered_at) VALUES(?,?,?,?,?,?,?)",(uid,aid,a["symbol"],a["condition"],a["target"],observed,datetime.now(timezone.utc).isoformat()))
        c.execute("UPDATE price_alerts SET active=0 WHERE id=?",(aid,))
    return {"triggered":True,"message":f"{a['symbol']} fiyat alarmı tetiklendi: {observed}"}
@app.delete("/api/alerts/{aid}")
def delete_price_alert(aid:int,uid:int=Depends(current_user)):
    with db() as c: cur=c.execute("DELETE FROM price_alerts WHERE id=? AND user_id=?",(aid,uid))
    if not cur.rowcount: raise HTTPException(404,"Alarm bulunamadı.")
    return {"message":"Alarm silindi."}
@app.get("/api/alerts/history")
def price_alert_history(uid:int=Depends(current_user)):
    with db() as c: return [dict(x) for x in c.execute("SELECT * FROM price_alert_history WHERE user_id=? ORDER BY id DESC LIMIT 100",(uid,)).fetchall()]
@app.get("/api/settings/farm")
def get_farm_settings(uid:int=Depends(current_user)):
    with db() as c:
        row=c.execute("SELECT * FROM farm_settings WHERE user_id=?",(uid,)).fetchone()
        return dict(row) if row else {"city":"Konya","district":"Ereğli","map_url":"https://maps.app.goo.gl/15ChbNikcC5HL8aF9"}
@app.put("/api/settings/farm")
def save_farm_settings(data:FarmSettingsBody,uid:int=Depends(current_user)):
    if not data.map_url.startswith("https://maps.app.goo.gl/") and not data.map_url.startswith("https://www.google.com/maps"):
        raise HTTPException(400,"Google Haritalar bağlantısı geçerli değil.")
    with db() as c: c.execute("INSERT INTO farm_settings(user_id,city,district,map_url,updated_at) VALUES(?,?,?,?,?) ON CONFLICT(user_id) DO UPDATE SET city=excluded.city,district=excluded.district,map_url=excluded.map_url,updated_at=excluded.updated_at",(uid,data.city,data.district,data.map_url,datetime.now(timezone.utc).isoformat()))
    return {"message":"Çiftlik ayarları kaydedildi."}

# --- V21 feed consumption cost report and paper portfolio ---
@app.get("/api/reports/consumption")
def consumption_report(days:int=Query(30,ge=1,le=365),uid:int=Depends(current_user)):
    with db() as c:
        rows=c.execute("""SELECT i.id item_id,i.name,i.unit,COALESCE(SUM(CASE WHEN m.move_type='out' THEN m.quantity ELSE 0 END),0) quantity,
        COALESCE(SUM(CASE WHEN m.move_type='out' THEN m.quantity*m.unit_cost ELSE 0 END),0) cost
        FROM inventory i LEFT JOIN stock_moves m ON m.item_id=i.id AND m.user_id=i.user_id AND m.created_at>=?
        WHERE i.user_id=? GROUP BY i.id,i.name,i.unit ORDER BY cost DESC""",((datetime.now(timezone.utc)-timedelta(days=days)).isoformat(),uid)).fetchall()
    return {"days":days,"items":[dict(x) for x in rows],"total_cost":sum(float(x["cost"] or 0) for x in rows)}
@app.post("/api/portfolio/trades")
def add_portfolio_trade(data:PortfolioTradeBody,uid:int=Depends(current_user)):
    symbol=data.symbol.upper().strip()
    if not symbol.endswith("USDT"): raise HTTPException(400,"Sanal portföyde USDT paritesi girin.")
    now=datetime.now(timezone.utc).isoformat()
    with db() as c:
        bought=c.execute("SELECT COALESCE(SUM(CASE WHEN side='buy' THEN quantity ELSE -quantity END),0) q FROM portfolio_trades WHERE user_id=? AND symbol=?",(uid,symbol)).fetchone()["q"]
        if data.side=="sell" and data.quantity>float(bought)+1e-12: raise HTTPException(400,"Sanal portföyde yeterli coin yok.")
        cur=c.execute("INSERT INTO portfolio_trades(user_id,symbol,side,quantity,price,fee,trade_time,created_at) VALUES(?,?,?,?,?,?,?,?)",(uid,symbol,data.side,data.quantity,data.price,data.fee,now,now))
    audit(uid,"sanal portföy işlemi","portfolio_trade",cur.lastrowid,f"{data.side} {data.quantity} {symbol}")
    return {"id":cur.lastrowid,"message":"Sanal portföy işlemi kaydedildi. Binance'te gerçek emir gönderilmedi."}
@app.get("/api/portfolio")
def get_portfolio(uid:int=Depends(current_user)):
    with db() as c:
        rows=c.execute("""SELECT symbol,SUM(CASE WHEN side='buy' THEN quantity ELSE -quantity END) quantity,
        SUM(CASE WHEN side='buy' THEN quantity*price+fee ELSE 0 END) buy_cost,
        SUM(CASE WHEN side='buy' THEN quantity ELSE 0 END) bought_qty,
        SUM(CASE WHEN side='sell' THEN quantity*price-fee ELSE 0 END) sell_value
        FROM portfolio_trades WHERE user_id=? GROUP BY symbol HAVING quantity>0.0000000001 ORDER BY symbol""",(uid,)).fetchall()
    try: prices={x["symbol"]:float(x["price"]) for x in binance_get("/api/v3/ticker/price")}
    except Exception: prices={}
    out=[]
    for r in rows:
        q=float(r["quantity"]); avg=float(r["buy_cost"] or 0)/max(float(r["bought_qty"] or 0),1e-18); last=prices.get(r["symbol"])
        out.append({"symbol":r["symbol"],"quantity":q,"average_buy_cost":avg,"last_price":last,"market_value":q*last if last is not None else None,"unrealized_pnl":(last-avg)*q if last is not None else None})
    return {"holdings":out,"note":"Sanal portföydür; gerçek Binance hesabına emir göndermez."}
@app.get("/api/portfolio/history")
def portfolio_history(uid:int=Depends(current_user)):
    with db() as c: return [dict(x) for x in c.execute("SELECT * FROM portfolio_trades WHERE user_id=? ORDER BY id DESC LIMIT 200",(uid,)).fetchall()]

# --- Markets and weather ---
def binance_get(path,params=None):
    try:
        r=requests.get(BINANCE+path,params=params,timeout=10); r.raise_for_status(); return r.json()
    except requests.RequestException: raise HTTPException(503,"Binance piyasa verisine şu anda ulaşılamıyor.")

_ALERT_STOP=threading.Event()
_ALERT_THREAD=None

def telegram_send(message):
    token=os.getenv("TELEGRAM_BOT_TOKEN",""); chat_id=os.getenv("TELEGRAM_CHAT_ID","")
    if not token or not chat_id: return False
    try:
        r=requests.post(f"https://api.telegram.org/bot{token}/sendMessage",json={"chat_id":chat_id,"text":message},timeout=12)
        r.raise_for_status(); return True
    except requests.RequestException: return False

def scan_server_price_alerts():
    # Sunucu tarafı tarama yalnızca Telegram yapılandırılmışsa açılır; aksi halde
    # mevcut tarayıcı/WebSocket alarm akışı aynen çalışmaya devam eder.
    if not (os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID")): return
    with db() as c:
        alerts=[dict(x) for x in c.execute("SELECT * FROM price_alerts WHERE active=1").fetchall()]
    if not alerts: return
    try:
        prices={x.get("symbol"):float(x.get("price",0)) for x in binance_get("/api/v3/ticker/price")}
    except Exception: return
    notifications=[]
    with db() as c:
        for a in alerts:
            observed=prices.get(a["symbol"])
            if observed is None: continue
            hit=observed>=a["target"] if a["condition"]=="above" else observed<=a["target"]
            if not hit: continue
            cur=c.execute("UPDATE price_alerts SET active=0 WHERE id=? AND active=1",(a["id"],))
            if cur.rowcount:
                stamp=datetime.now(timezone.utc).isoformat()
                c.execute("INSERT INTO price_alert_history(user_id,alert_id,symbol,condition,target,observed,triggered_at) VALUES(?,?,?,?,?,?,?)",(a["user_id"],a["id"],a["symbol"],a["condition"],a["target"],observed,stamp))
                notifications.append(f"🔔 FEYZULLAH V25 fiyat alarmı\n{a['symbol']}\nKoşul: {a['condition']}\nHedef: {a['target']}\nGözlenen: {observed}")
    for message in notifications: telegram_send(message)

def _alert_worker_loop():
    while not _ALERT_STOP.wait(60):
        try: scan_server_price_alerts()
        except Exception: pass

@app.on_event("startup")
def start_server_alert_worker():
    global _ALERT_THREAD
    if os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID") and (_ALERT_THREAD is None or not _ALERT_THREAD.is_alive()):
        _ALERT_STOP.clear(); _ALERT_THREAD=threading.Thread(target=_alert_worker_loop,name="price-alert-worker",daemon=True); _ALERT_THREAD.start()

@app.on_event("shutdown")
def stop_server_alert_worker():
    _ALERT_STOP.set()
@app.get("/market/metals")
def market_metals():
    out={}
    for code,label in (("XAU","gold"),("XAG","silver")):
        try:
            r=requests.get(f"https://api.gold-api.com/price/{code}",timeout=7); r.raise_for_status(); p=r.json().get("price"); out[label]={"price":float(p),"currency":"USD/ons"} if p is not None else None
        except (requests.RequestException,ValueError,TypeError): out[label]=None
    try:
        fx=binance_get("/api/v3/ticker/price",{"symbol":"USDTTRY"})
        out["usdtry"]={"price":float(fx["price"]),"currency":"TRY"}
    except Exception: out["usdtry"]=None
    if out.get("gold") and out.get("usdtry"):
        out["gram_gold_try"]={"price":out["gold"]["price"]*out["usdtry"]["price"]/31.1034768,"currency":"TRY/gram","indicative":True}
    return out
@app.get("/market/movers")
def market_movers():
    data=binance_get("/api/v3/ticker/24hr"); valid=[]
    for x in data:
        if x.get("symbol","").endswith("USDT"):
            try: valid.append({"symbol":x["symbol"],"lastPrice":x["lastPrice"],"priceChangePercent":float(x["priceChangePercent"]),"quoteVolume":float(x.get("quoteVolume",0))})
            except (KeyError,ValueError,TypeError): pass
    valid.sort(key=lambda x:x["priceChangePercent"],reverse=True)
    return {"top_gainers":valid[:20],"top_losers":sorted(valid,key=lambda x:x["priceChangePercent"])[:20],"updated_at":datetime.now(timezone.utc).isoformat()}
@app.get("/market/symbols")
def market_symbols():
    info=binance_get("/api/v3/exchangeInfo"); return [{"symbol":s["symbol"],"baseAsset":s["baseAsset"],"quoteAsset":s["quoteAsset"],"status":s["status"],"isSpotTradingAllowed":s.get("isSpotTradingAllowed",False),"binance_url":f"https://www.binance.com/en/trade/{s['baseAsset']}_{s['quoteAsset']}?type=spot"} for s in info.get("symbols",[]) if s.get("quoteAsset")=="USDT" and s.get("status")=="TRADING" and s.get("isSpotTradingAllowed",False)]
@app.get("/api/weather")
def weather(city:str,district:str=""):
    place=", ".join(x for x in [district.strip(),city.strip(),"Türkiye"] if x)
    try:
        g=requests.get(OPEN_METEO_GEO,params={"name":place,"count":1,"language":"tr","format":"json"},timeout=12); g.raise_for_status(); results=g.json().get("results") or []
        if not results and district:
            g=requests.get(OPEN_METEO_GEO,params={"name":f"{city}, Türkiye","count":5,"language":"tr","format":"json"},timeout=12); g.raise_for_status(); results=g.json().get("results") or []
        if not results: raise HTTPException(404,"Konum bulunamadı.")
        loc=results[0]; f=requests.get(OPEN_METEO_FORECAST,params={"latitude":loc["latitude"],"longitude":loc["longitude"],"daily":"weather_code,temperature_2m_max,temperature_2m_min,precipitation_probability_max","current":"temperature_2m,relative_humidity_2m,weather_code,wind_speed_10m","timezone":"auto","forecast_days":7},timeout=15); f.raise_for_status(); p=f.json()
        return {"location":{"name":loc.get("name"),"admin1":loc.get("admin1"),"latitude":loc["latitude"],"longitude":loc["longitude"]},"current":p.get("current",{}),"daily":p.get("daily",{})}
    except requests.RequestException: raise HTTPException(503,"Hava durumu servisine ulaşılamıyor.")
@app.get("/api/supports")
def get_supports():
    with db() as c:
        checks={r['source_key']:dict(r) for r in c.execute('SELECT * FROM support_source_checks').fetchall()}
    sources=[]
    for src in SUPPORT_SOURCES:
        check=checks.get(src['key'],{})
        sources.append({**src,"last_checked":check.get('last_checked'),"status":check.get('status','Henüz kontrol edilmedi'),"http_status":check.get('http_status'),"page_title":check.get('page_title',''),"error":check.get('error',''),"content_changed_at":check.get('changed_at',''),"content_excerpt":check.get('content_excerpt','')})
    items=[]
    for item in SUPPORT_ITEMS:
        src=next((x for x in sources if x['key']==item['source_key']),None)
        items.append({**item,"source_url":src['url'] if src else '',"source_last_checked":src.get('last_checked') if src else None,"source_status":src.get('status') if src else 'unknown',"verification_status":"source_changed_review_required" if src and src.get('changed_at') else "source_access_checked_amount_date_not_semantically_verified","verification_note":"Kaynak sayfası erişimi ve değişikliği izlenir; destek tutarı ve kişisel ödeme tarihi otomatik olarak kesinleştirilmez."})
    return {"items":items,"sources":sources,"generated_at":datetime.now(timezone.utc).isoformat(),"refresh_interval_hours":6}

@app.post("/api/supports/refresh")
def refresh_supports(uid:int=Depends(require_admin)):
    results=refresh_support_sources(force=True)
    return {"results":results,"checked_at":datetime.now(timezone.utc).isoformat()}

@app.on_event("startup")
def start_support_monitor():
    global _support_monitor_thread
    if not globals().get('_support_monitor_thread') or not _support_monitor_thread.is_alive():
        _support_monitor_thread=threading.Thread(target=support_monitor_loop,daemon=True,name='support-source-monitor')
        _support_monitor_thread.start()

@app.get("/health")
def health():
    return {"status":"healthy","platform":"FEYZULLAH DEMİREL TARIM VE HAYVANCILIK AKILLI DEPOSU","version":"33.9.4"}

@app.get("/farm/status")
def farm_status():
    # Sahada gerçek sensör cihazı bağlanmadığı için sıcaklık/nem gibi değerler uydurulmaz.
    return {"status":"active","platform":"Akıllı Çiftlik Sistemleri Aktif","gps_measurement":"browser_geolocation","sensor_telemetry":"not_connected","note":"Gerçek sensör verileri için fiziksel cihaz/API bağlantısı gerekir."}

@app.get("/markets")
@app.get("/api/markets")
def markets():
    return {"markets":["Altın","Gümüş","Borsa","Döviz","Kripto"],"crypto_source":"Binance public market data","external_source":"Twelve Data (API anahtarı isteğe bağlı)","status":"active"}

@app.get("/market/price/{symbol}")
@app.get("/api/crypto/price/{symbol}")
def crypto_price(symbol:str):
    return binance_get("/api/v3/ticker/price",{"symbol":symbol.upper()})

@app.get("/market/tickers")
@app.get("/api/crypto/tickers")
def crypto_tickers(quote:str="USDT"):
    suffix=quote.upper().strip()
    if not suffix or len(suffix)>12: raise HTTPException(400,"Geçersiz piyasa para birimi.")
    return [x for x in binance_get("/api/v3/ticker/24hr") if x.get("symbol","").endswith(suffix)]

@app.get("/api/crypto/klines/{symbol}")
def crypto_klines(symbol:str,interval:str="1h",limit:int=Query(200,ge=20,le=1000),indicators:bool=False):
    allowed={"1m","3m","5m","15m","30m","1h","2h","4h","6h","8h","12h","1d","3d","1w","1M"}
    if interval not in allowed: raise HTTPException(400,"Geçersiz grafik aralığı.")
    rows=binance_get("/api/v3/klines",{"symbol":symbol.upper(),"interval":interval,"limit":limit})
    candles=[{"time":int(x[0]/1000),"open":float(x[1]),"high":float(x[2]),"low":float(x[3]),"close":float(x[4]),"volume":float(x[5])} for x in rows]
    return add_indicators(candles) if indicators else candles

@app.get("/api/crypto/movers")
def crypto_movers():
    data=[x for x in binance_get("/api/v3/ticker/24hr") if x.get("symbol","").endswith("USDT")]
    data.sort(key=lambda x:float(x.get("priceChangePercent",0)),reverse=True)
    return {"gainers":data[:25],"losers":list(reversed(data[-25:]))}

@app.get("/api/crypto/small-volume")
def crypto_small_volume(limit:int=Query(50,ge=1,le=200)):
    data=[x for x in binance_get("/api/v3/ticker/24hr") if x.get("symbol","").endswith("USDT") and float(x.get("quoteVolume",0))>0]
    data.sort(key=lambda x:float(x.get("quoteVolume",0)))
    return {"warning":"Düşük hacim, düşük piyasa değeri veya yatırım fırsatı anlamına gelmez.","items":data[:limit]}

@app.get("/api/crypto/new-symbols")
def crypto_new_symbols():
    info=binance_get("/api/v3/exchangeInfo")
    current={x["symbol"] for x in info.get("symbols",[]) if x.get("status")=="TRADING"}
    with db() as c:
        c.execute("CREATE TABLE IF NOT EXISTS known_symbols(symbol TEXT PRIMARY KEY,first_seen TEXT NOT NULL)")
        known={x[0] for x in c.execute("SELECT symbol FROM known_symbols").fetchall()}
        new_items=sorted(current-known) if known else []
        stamp=datetime.now(timezone.utc).isoformat()
        c.executemany("INSERT OR IGNORE INTO known_symbols(symbol,first_seen) VALUES(?,?)",[(x,stamp) for x in current])
    return {"new_since_previous_scan":new_items,"count":len(new_items),"note":"İlk tarama başlangıç listesini kaydeder; resmi listeleme duyurusu değildir."}

@app.get("/api/external/quote")
def external_quote(symbol:str):
    key=os.getenv("TWELVE_DATA_API_KEY","")
    if not key: raise HTTPException(501,"Twelve Data API anahtarı yapılandırılmamış.")
    try:
        r=requests.get("https://api.twelvedata.com/quote",params={"symbol":symbol,"apikey":key},timeout=15); r.raise_for_status(); return r.json()
    except requests.RequestException: raise HTTPException(503,"Harici piyasa verisine ulaşılamıyor.")

@app.get("/api/external/price")
def external_price(symbol:str):
    key=os.getenv("TWELVE_DATA_API_KEY","")
    if not key: raise HTTPException(501,"Twelve Data API anahtarı yapılandırılmamış.")
    try:
        r=requests.get("https://api.twelvedata.com/price",params={"symbol":symbol,"apikey":key},timeout=15); r.raise_for_status(); return r.json()
    except requests.RequestException: raise HTTPException(503,"Harici piyasa verisine ulaşılamıyor.")

# --- BIST / Turkish equities (Twelve Data; optional API key) ---
BIST_DEFAULT_SYMBOLS = ["XU100", "THYAO", "ASELS", "BIMAS", "TUPRS", "GARAN", "EREGL"]

def _bist_symbol(value: str) -> str:
    symbol = (value or "").strip().upper()
    if not re.fullmatch(r"[A-Z0-9._-]{1,20}", symbol):
        raise HTTPException(400, "Geçersiz BIST sembolü.")
    return symbol

def _twelve_data_get(endpoint: str, params: dict):
    key = os.getenv("TWELVE_DATA_API_KEY", "").strip()
    if not key:
        raise HTTPException(501, "BIST verileri için Render ortam değişkenlerine TWELVE_DATA_API_KEY ekleyin.")
    query = {**params, "apikey": key}
    try:
        response = requests.get(f"https://api.twelvedata.com/{endpoint}", params=query, timeout=18)
        response.raise_for_status()
        data = response.json()
    except requests.RequestException:
        raise HTTPException(503, "Twelve Data piyasa servisine ulaşılamıyor.")
    except ValueError:
        raise HTTPException(502, "Piyasa sağlayıcısından geçersiz yanıt alındı.")
    if isinstance(data, dict) and (data.get("status") == "error" or data.get("code")):
        message = str(data.get("message") or data.get("code") or "Veri sağlayıcı sembolü kabul etmedi")[:240]
        return {"error": message, "provider": "Twelve Data", "symbol": params.get("symbol")}
    return data

@app.get("/api/bist/quotes")
def bist_quotes(symbols: str = "XU100,THYAO,ASELS,BIMAS,TUPRS,GARAN,EREGL"):
    requested = [_bist_symbol(x) for x in symbols.split(",") if x.strip()][:12]
    if not requested:
        requested = BIST_DEFAULT_SYMBOLS[:]
    results = []
    for symbol in requested:
        data = _twelve_data_get("quote", {"symbol": symbol, "exchange": "BIST"})
        if "error" in data:
            results.append(data)
            continue
        results.append({
            "symbol": data.get("symbol", symbol), "name": data.get("name", symbol),
            "exchange": data.get("exchange", "BIST"), "currency": data.get("currency", "TRY"),
            "price": data.get("close") or data.get("price"),
            "change": data.get("change"), "percent_change": data.get("percent_change"),
            "volume": data.get("volume"), "datetime": data.get("datetime"),
            "source": "Twelve Data"
        })
    return {"items": results, "provider": "Twelve Data", "exchange": "BIST",
            "requested_at": datetime.now(timezone.utc).isoformat(),
            "note": "Veriler sağlayıcı kapsamı, gecikme ve API kotasına bağlıdır; yatırım tavsiyesi değildir."}

@app.get("/api/bist/hunter")
def bist_hunter_scanner(min_volume: float = Query(1000000, ge=0), min_change: float = Query(0, ge=0, le=50), symbols: str = ""):
    """Hacim eşiği ve fiyat momentumu tarayıcısı. Emir akışı verisi yoksa baskı uydurmaz."""
    if not os.getenv("TWELVE_DATA_API_KEY", "").strip():
        raise HTTPException(501, "BIST avcı taraması için Render ortam değişkenlerine TWELVE_DATA_API_KEY ekleyin.")
    configured = list(dict.fromkeys(x.strip().upper() for x in os.getenv("BIST100_SYMBOLS", "").split(",") if x.strip()))[:100]
    requested = [x.strip().upper() for x in symbols.split(",") if x.strip()] if symbols.strip() else (configured or BIST_DEFAULT_SYMBOLS[:])
    requested = list(dict.fromkeys(_bist_symbol(x) for x in requested))[:100]
    if not requested:
        raise HTTPException(422, "Tarama için en az bir BIST sembolü gerekli.")
    results = []
    def fetch(symbol):
        data = _twelve_data_get("quote", {"symbol": symbol, "exchange": "BIST"})
        if "error" in data:
            return {"symbol": symbol, "error": data["error"], "source": "Twelve Data"}
        def number(value):
            try: return float(value) if value not in (None, "", "-", "None") else None
            except (TypeError, ValueError): return None
        price = number(data.get("close") or data.get("price"))
        volume = number(data.get("volume"))
        change = number(data.get("percent_change"))
        return {"symbol": data.get("symbol",symbol), "name": data.get("name",symbol), "price": price,
            "change_percent": change, "volume": volume, "volume_over_threshold": volume is not None and volume >= min_volume,
            "momentum": "Pozitif fiyat momentumu" if change is not None and change > 1.5 else ("Negatif fiyat momentumu" if change is not None and change < -1.5 else "Nötr / sınırlı hareket"),
            "market_pressure": "Veri yok – işlem yönü/emir defteri gerekir",
            "provider_timestamp": data.get("datetime"), "requested_at": datetime.now(timezone.utc).isoformat(), "source": "Twelve Data"}
    errors=[]
    # Bounded concurrency; API provider quota still applies per symbol.
    with ThreadPoolExecutor(max_workers=min(5,len(requested))) as pool:
        futures = {pool.submit(fetch,s):s for s in requested}
        for future in as_completed(futures):
            try: results.append(future.result())
            except HTTPException as e: errors.append({"symbol":futures[future],"error":e.detail})
            except Exception as e: errors.append({"symbol":futures[future],"error":str(e)[:180]})
    results.sort(key=lambda x: abs(x.get("change_percent") or 0), reverse=True)
    if min_change > 0:
        results = [x for x in results if x.get("change_percent") is not None and abs(x["change_percent"]) >= min_change]
    return {"module":"BIST Avcı Tarayıcı", "requested_count":len(requested), "scanned_count":len(results),
        "items":results, "errors":errors, "min_volume_threshold":min_volume, "min_absolute_change_percent":min_change, "universe":("BIST100_SYMBOLS ortam değişkeni" if len(configured) >= 100 else f"Kısmi sunucu listesi ({len(configured)} sembol)" if configured else "Kullanıcı sembol listesi" if symbols.strip() else "Sınırlı varsayılan izleme listesi"),
        "full_bist100_configured":len(configured) >= 100, "configured_symbol_count":len(configured), "provider":"Twelve Data",
        "requested_at":datetime.now(timezone.utc).isoformat(),
        "note":"Hacim eşiği hacim patlaması kanıtı değildir; gerçek emir baskısı için alış/satış işlem yönü veya derinlik verisi gerekir. Kapsam, gecikme ve kota veri sağlayıcısına bağlıdır."}

@app.get("/api/bist/series/{symbol}")
def bist_series(symbol: str, interval: str = "1day", outputsize: int = Query(60, ge=10, le=200)):
    symbol = _bist_symbol(symbol)
    if interval not in {"1min", "5min", "15min", "30min", "1h", "1day", "1week", "1month"}:
        raise HTTPException(400, "Geçersiz grafik aralığı.")
    data = _twelve_data_get("time_series", {"symbol": symbol, "exchange": "BIST", "interval": interval, "outputsize": outputsize, "format": "JSON"})
    if "error" in data:
        raise HTTPException(502, data["error"])
    return data

@app.get("/api/integrations")
def integrations():
    ai=get_ai_status()
    return {"binance_public_market_data":"ready","twelve_data":"configured" if os.getenv("TWELVE_DATA_API_KEY") else "needs_api_key","bist_equities":"configured" if os.getenv("TWELVE_DATA_API_KEY") else "needs_api_key","telegram":"configured" if os.getenv("TELEGRAM_BOT_TOKEN") and os.getenv("TELEGRAM_CHAT_ID") else "optional_setup","openai":ai.get("openai","not_configured"),"automatic_trading":"disabled"}

@app.get("/ai/status")
@app.get("/api/ai/status")
def ai_status():
    return {"platform":"AI Yatırım Laboratuvarı","providers":get_ai_status()}

@app.post("/api/ai/analyze")
@app.post("/api/ai/consensus")
async def api_analyze(data:AIPrompt,uid:int=Depends(current_user)):
    status=get_ai_status(); analysis=await analyze_with_all(data.prompt); consensus=await review_ai_results(data.prompt,analysis.get("results",{})); audit(uid,"AI analizi","ai",None,data.prompt[:160]); return {"status":status,"analysis":analysis,"consensus":consensus}

@app.post("/api/ai/investment-analysis")
async def api_investment_analyze(data:AIInvestmentRequest,uid:int=Depends(current_user)):
    symbol=data.symbol.upper().strip()
    try:
        market=binance_get("/api/v3/ticker/24hr",{"symbol":symbol})
        rows=binance_get("/api/v3/klines",{"symbol":symbol,"interval":"1h","limit":100})
        candles=[{"time":int(x[0]/1000),"open":float(x[1]),"high":float(x[2]),"low":float(x[3]),"close":float(x[4]),"volume":float(x[5])} for x in rows]
        technical=add_indicators(candles)
        latest=technical[-1] if technical else {}
        context={"symbol":symbol,"last_price":market.get("lastPrice"),"price_change_percent_24h":market.get("priceChangePercent"),"quote_volume_24h":market.get("quoteVolume"),"latest_1h_indicators":{k:latest.get(k) for k in ("close","sma20","sma50","rsi14")},"user_question":data.question}
        prompt="Aşağıdaki güncel piyasa verisini ve teknik göstergeleri temkinli biçimde değerlendir. Kesin fiyat tahmini, al/sat emri veya kazanç vaadi verme; eksik verileri ve riskleri belirt.\n"+json.dumps(context,ensure_ascii=False)
        analysis=await analyze_with_all(prompt); consensus=await review_ai_results(prompt,analysis.get("results",{})); audit(uid,"AI yatırım analizi","ai",None,symbol+": "+data.question[:120])
        return {"market":context,"analysis":analysis,"consensus":consensus,"status":get_ai_status()}
    except HTTPException: raise
    except (KeyError,ValueError,TypeError): raise HTTPException(502,"Piyasa verisi beklenen biçimde alınamadı.")

# --- Original-style responsive UI, expanded with V20 management pages ---
HTML_CONTENT = '<!DOCTYPE html><html lang="tr"><head><meta charset="UTF-8"><meta name="viewport" content="width=device-width, initial-scale=1"><meta name="theme-color" content="#101827"><title>FEYZULLAH DEMİREL | AKILLI ÇİFTLİK V33.9.7</title>\n<style>\n*{box-sizing:border-box}body{margin:0;background:#0b1420;color:#f1f5f9;font-family:Arial,sans-serif}header{background:linear-gradient(135deg,#14532d,#166534);padding:20px 12px;text-align:center}header h1{margin:0;font-size:24px;letter-spacing:1px}header h2{font-size:13px;margin:9px 0;color:#cbd5e1}#clock{color:#ffe08a;margin:8px;font-size:15px;font-weight:bold;font-family:monospace}.header-actions{margin-top:10px;display:flex;justify-content:center;gap:10px;flex-wrap:wrap}.location-btn,.auth-btn{background:#15803d;color:white;padding:8px 16px;border-radius:20px;font-size:12px;border:0;font-weight:bold;cursor:pointer}.auth-btn{background:#1d4ed8}nav{display:flex;overflow-x:auto;background:#17283a;padding:10px;gap:7px;position:sticky;top:0;z-index:5}nav button,.action{padding:10px 14px;border:0;border-radius:8px;background:#16834d;color:white;font-weight:bold;cursor:pointer;white-space:nowrap}main{padding:12px;max-width:1100px;margin:auto}.page{display:none}.page.active{display:block}.card{background:#17283a;border:1px solid #30465b;padding:16px;border-radius:12px;margin:12px 0}.grid-2{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:10px}.grid-3{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:10px}.stat{background:#20364a;padding:15px;text-align:center;border-radius:10px}.stat b{display:block;color:#4ade80;font-size:22px;margin-top:7px}input,select,textarea{width:100%;padding:10px;margin:6px 0 10px;background:#0b1724;color:white;border:1px solid #40566b;border-radius:8px;font-size:14px}label{font-size:13px;color:#cbd5e1;display:block;margin-top:5px}.item{background:#20364a;padding:12px;border-radius:8px;margin-bottom:12px;border-left:4px solid #16834d}.muted{font-size:12px;color:#94a3b8;line-height:1.5}.green{color:#4ade80;font-weight:bold}.red{color:#f87171;font-weight:bold}table{width:100%;border-collapse:collapse;margin-top:10px;font-size:14px}th,td{padding:10px;text-align:left;border-bottom:1px solid #30465b}th{color:#94a3b8}.modal{display:none;position:fixed;z-index:100;left:0;top:0;width:100%;height:100%;background:rgba(0,0,0,.7);align-items:center;justify-content:center}.modal-content{background:#17283a;padding:20px;border-radius:12px;width:90%;max-width:440px;border:1px solid #30465b}.danger{background:#7f1d1d!important}.weather-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(110px,1fr));gap:8px;margin-top:12px}.weather-day{background:#20364a;padding:10px;border-radius:8px;text-align:center;border:1px solid #30465b}.ration-box{background:#102235;border:2px solid #16834d;padding:18px;border-radius:12px;margin-top:15px}.chart{display:flex;align-items:end;gap:3px;height:130px;background:#0b1724;padding:10px;border-radius:8px;overflow:hidden}.bar{flex:1;min-width:3px;background:#16a34a;border-radius:3px 3px 0 0}.tag{display:inline-block;background:#29465f;padding:3px 7px;border-radius:6px;font-size:11px;margin:2px}.actions{display:flex;gap:6px;flex-wrap:wrap}.actions button{font-size:12px;padding:7px 9px}@media(max-width:600px){.grid-2,.grid-3{grid-template-columns:1fr}header h1{font-size:21px}th,td{padding:7px;font-size:12px}}\n\n.support-item{border:1px solid #334155;border-radius:10px;padding:12px;margin:8px 0;background:#111c2e}.support-item h4{margin:0 0 7px}.support-meta{font-size:12px;color:#aab8ca;margin:5px 0}.support-source{font-size:12px;color:#7dd3fc}.support-pill{display:inline-block;padding:3px 7px;border-radius:20px;background:#26364d;font-size:11px;margin:3px 4px 3px 0}.support-deadline{color:#fbbf24;font-weight:700}.support-item a,.support-source a{color:#7dd3fc}\n/* V33.4 home typography and planner cleanup */\n#clock{display:flex;flex-direction:column;align-items:flex-end;gap:2px;margin:0 0 0 auto;padding:8px 12px;min-width:150px;text-align:right;border-radius:10px;background:rgba(0,0,0,.18)}#clock .clock-time{font-size:24px;line-height:1.1;font-weight:900;letter-spacing:1px}#clock .clock-date{font-size:12px;opacity:.9}#clock.clock-neon .clock-time{text-shadow:0 0 8px currentColor,0 0 18px currentColor}#clock.clock-classic{font-family:Georgia,serif;background:#f4e7c5;color:#3d2d1b;border:3px double #8a6b35}#clock.clock-minimal{background:transparent;border:0;padding:4px 8px}header{display:flex;flex-wrap:wrap;align-items:center;gap:8px}header h1,header h2,header>div:not(#clock){flex-basis:100%;text-align:center}header #clock{position:absolute;right:12px;top:12px}header{position:relative;padding-top:54px}\nbody[data-home-theme="green"]{background:#071b13;color:#eafff2}body[data-home-theme="green"] header{background:linear-gradient(135deg,#064e3b,#15803d)}body[data-home-theme="green"] .card{background:#103b2b;border-color:#24634a}body[data-home-theme="green"] nav{background:#0b2a20}\nbody[data-home-theme="light"]{background:#eef4f7;color:#17212b}body[data-home-theme="light"] header{background:linear-gradient(135deg,#d7f0df,#a9d8c0);color:#18372a}body[data-home-theme="light"] header h2{color:#274d3a}body[data-home-theme="light"] .card{background:#fff;color:#17212b;border-color:#cbd5e1}body[data-home-theme="light"] .item,body[data-home-theme="light"] .stat{background:#edf3f7;color:#17212b}body[data-home-theme="light"] nav{background:#dbe7ee}body[data-home-theme="light"] input,body[data-home-theme="light"] select,body[data-home-theme="light"] textarea{background:#fff;color:#17212b;border-color:#aab8c5}\nbody[data-home-theme="blue"]{background:#07172d}body[data-home-theme="blue"] header{background:linear-gradient(135deg,#1e3a8a,#0369a1)}body[data-home-theme="blue"] .card{background:#102b4c;border-color:#24527e}body[data-home-theme="blue"] nav{background:#0b2442}\nbody[data-home-theme="sunset"]{background:#21121d}body[data-home-theme="sunset"] header{background:linear-gradient(135deg,#7c2d12,#9d174d)}body[data-home-theme="sunset"] .card{background:#3a1e2c;border-color:#744052}body[data-home-theme="sunset"] nav{background:#321827}\n.home-app-groups{display:grid;gap:14px}.home-folder{background:rgba(20,40,60,.55);border:1px solid #456078;border-radius:16px;padding:14px}.home-folder h4{margin:0 0 12px;font-size:16px}.home-app-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(118px,1fr));gap:12px}.home-app{min-height:112px;border:1px solid #547087;border-radius:15px;background:linear-gradient(145deg,#29445d,#172b40);display:flex;flex-direction:column;align-items:center;justify-content:center;gap:7px;padding:12px;text-align:center;position:relative;touch-action:pan-y}.home-app .app-icon{font-size:34px;line-height:1.2}.home-app .app-label{font-weight:700;font-size:13px}.home-app .app-move{display:flex;gap:5px}.home-app .app-move button{padding:4px 8px;font-size:12px;margin:0}.home-app.dragging{opacity:.35;border-style:dashed}.home-app.drag-over{outline:3px solid #4ade80}body[data-home-theme="light"] .home-folder{background:#f8fbfd;border-color:#b9c8d4}body[data-home-theme="light"] .home-app{background:linear-gradient(145deg,#e1edf4,#fff);color:#17212b;border-color:#b7c8d5}\n.widget-editor{display:none;border:1px dashed #66839a;border-radius:12px;padding:12px;margin:10px 0}.widget-editor.open{display:block}.ration-feeds{display:grid;gap:8px}.ration-feed{display:grid;grid-template-columns:minmax(120px,1.5fr) repeat(4,minmax(70px,1fr));gap:6px;align-items:center;background:#20364a;padding:8px;border-radius:8px}.ration-feed label{margin:0}.ration-feed input{margin:0;padding:7px}.ration-feed .feed-name{font-size:13px;font-weight:700}\n/* V1: Uygulamalarım/Klasörlerim kart görünümünü site geneline yay ve mobil taşmaları önle. */\n.card,.item,.stat,.support-item,.weather-day{border-radius:15px;border:1px solid #456078;box-shadow:0 7px 20px rgba(0,0,0,.16)}\n.card{background:linear-gradient(145deg,#203b53,#17283a)}\n.item,.stat{background:linear-gradient(145deg,#29445d,#172b40)}\nbutton.action,.location-btn,.auth-btn{display:inline-flex;align-items:center;justify-content:center;gap:7px;min-height:40px;line-height:1.25;white-space:normal;text-align:center;vertical-align:middle}\n.modal{padding:14px;overflow-y:auto;align-items:flex-start;justify-content:center}\n.modal-content{max-height:calc(100dvh - 28px);overflow-y:auto;margin:auto;overscroll-behavior:contain}\n.auth-modal-content{width:min(100%,480px);padding:18px;}\n.auth-modal-head{display:flex;align-items:center;justify-content:space-between;gap:10px}\n.auth-modal-head h3{margin:0}\n.auth-close{width:38px;height:38px;flex:0 0 38px;border:0;border-radius:10px;background:#334155;color:#fff;font-size:20px}\n.auth-tabs,.auth-button-stack{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:9px;margin:12px 0}\n.auth-tab{min-width:0;min-height:44px}\n.auth-tab.active{background:linear-gradient(145deg,#16834d,#0f5e3a);outline:2px solid #4ade80}\n.auth-panel{padding:12px;border:1px solid #456078;border-radius:14px;background:rgba(8,20,32,.38)}\n.auth-panel .action{width:100%;margin:4px 0}\n.auth-button-stack{grid-template-columns:1fr}\n.auth-primary{background:linear-gradient(145deg,#16834d,#0f5e3a)!important}\n.auth-secondary{background:linear-gradient(145deg,#34516c,#20364a)!important}\n.auth-profile{margin-top:12px;padding-top:10px;border-top:1px solid #456078}\n.auth-profile details summary{cursor:pointer;color:#86efac;padding:8px 0}\n.auth-logout{width:100%;margin-top:10px}\n#bist .actions{align-items:center}\n#bist .actions select{max-width:160px;min-width:120px}\n#bistQuotes .item{min-width:0;overflow-wrap:anywhere}\n@media(max-width:600px){.auth-tabs{grid-template-columns:1fr 1fr}.auth-modal-content{padding:14px}.auth-panel{padding:10px}.card{padding:13px}}\n@media(max-width:650px){header #clock{position:static;order:5;margin:8px auto 0;align-items:center;text-align:center}.ration-feed{grid-template-columns:1fr 1fr}.ration-feed .feed-name{grid-column:1/-1}.home-app-grid{grid-template-columns:repeat(2,minmax(0,1fr))}}\n\n/* V33.9.7 — supplied mobile/desktop reference theme. Additive presentation layer only. */\n:root{color-scheme:dark}\nhtml,body{min-height:100%;background:#0b1420!important;color:#e8eef6!important}\nbody{font-family:Arial,Helvetica,sans-serif!important}\nheader{display:flex!important;flex-direction:column!important;align-items:center!important;justify-content:center!important;gap:10px!important;min-height:190px!important;padding:26px 14px 22px!important;background:linear-gradient(180deg,#0e1a29 0%,#0b1420 100%)!important;border-bottom:1px solid #24384d!important;color:#f8fafc!important}\nheader h1,header>div[style*="font-weight:bold"]{display:none!important}\nheader h2{display:block!important;flex:0 0 auto!important;width:100%!important;max-width:760px!important;margin:0!important;color:#f8fafc!important;font-size:clamp(19px,3vw,30px)!important;font-weight:900!important;line-height:1.25!important;letter-spacing:.2px!important;text-align:center!important}\nheader #clock{display:none!important}\n.header-actions{display:flex!important;justify-content:center!important;align-items:center!important;gap:12px!important;flex-wrap:wrap!important;margin:10px 0 0!important}\n.location-btn,.auth-btn{min-height:46px!important;padding:10px 18px!important;border:0!important;border-radius:11px!important;font-size:15px!important;font-weight:800!important;box-shadow:none!important}\n.location-btn{background:#168044!important;color:#fff!important}.auth-btn{background:#216fe0!important;color:#fff!important}\nnav{position:sticky!important;top:0!important;z-index:50!important;display:flex!important;flex-wrap:nowrap!important;gap:10px!important;overflow-x:auto!important;overflow-y:hidden!important;padding:14px 18px!important;background:#111e2d!important;border-bottom:1px solid #263a50!important;scrollbar-width:thin!important;scrollbar-color:#36516c #111e2d!important}\nnav button,nav .action{flex:0 0 auto!important;min-height:46px!important;padding:10px 17px!important;border:1px solid #293f57!important;border-radius:14px!important;background:#172638!important;color:#d7e1ed!important;font-size:15px!important;font-weight:800!important;white-space:nowrap!important}\nnav button:first-child,nav button.active,nav button[aria-current="page"]{background:#216fe0!important;border-color:#3988ff!important;color:#fff!important}\nmain{width:100%!important;max-width:1500px!important;margin:0 auto!important;padding:18px 20px 28px!important}\n#home.page.active{display:flex!important;flex-direction:column!important;gap:14px!important}#home.page:not(.active){display:none!important}\n#home>#v333AppLauncher{order:-2!important;margin:0!important;padding:18px!important;background:#0e1b2a!important;border:1px solid #263b50!important;border-radius:19px!important;box-shadow:none!important}\n#home>#v333AppLauncher>div:first-child h3{margin:0!important;color:#e6edf6!important;font-size:clamp(18px,2vw,24px)!important;font-weight:900!important}\n#home>#v333AppLauncher>div:first-child .muted{font-size:13px!important;color:#9aaabd!important}\n.home-app-groups{display:grid!important;gap:16px!important}\n.home-folder{padding:16px!important;background:#101e2e!important;border:1px solid #263b50!important;border-radius:17px!important;box-shadow:none!important}\n.home-folder h4{margin:0 0 13px!important;color:#f4c542!important;font-size:18px!important;font-weight:900!important}\n.home-app-grid{display:grid!important;grid-template-columns:repeat(6,minmax(0,1fr))!important;gap:12px!important}\n.home-app{min-width:0!important;min-height:160px!important;padding:13px 9px!important;gap:9px!important;border:1px solid #263b50!important;border-radius:17px!important;background:linear-gradient(145deg,#17283a,#122033)!important;color:#edf3f9!important;box-shadow:0 5px 12px rgba(0,0,0,.16)!important}\n.home-app .app-icon{font-size:42px!important;line-height:1.2!important}\n.home-app .app-label{font-size:16px!important;line-height:1.25!important;font-weight:800!important;overflow-wrap:anywhere!important}\n.home-app>.action{min-width:58px!important;min-height:36px!important;margin-top:2px!important;padding:7px 15px!important;border:0!important;border-radius:9px!important;background:#117b95!important;color:#fff!important;font-size:15px!important;font-weight:800!important}\n.home-app .app-move button{min-height:30px!important;padding:4px 8px!important;border-radius:7px!important;background:#243b52!important;color:#e8eef6!important}\n.home-app.drag-over{outline:3px solid #3b82f6!important}\n#home>.card:not(#v333AppLauncher){background:#111f30!important;border:1px solid #263b50!important;border-radius:17px!important;color:#e8eef6!important}\n.card,.item,.stat,.support-item,.weather-day{border-color:#263b50!important;box-shadow:0 5px 14px rgba(0,0,0,.14)!important}\n.card{background:#111f30!important;color:#e8eef6!important}\n.item,.stat,.weather-day{background:#17283a!important;color:#e8eef6!important}\nbutton.action{background:#117b95!important;color:#fff!important;border-radius:9px!important;font-weight:800!important}\ninput,select,textarea{background:#0c1724!important;color:#edf3f9!important;border:1px solid #344b63!important}\nlabel,.muted{color:#a9b7c8!important}\n@media(min-width:1100px){main{padding:22px 28px 34px!important}#home>#v333AppLauncher{padding:22px!important}.home-app{min-height:150px!important}}\n@media(min-width:651px) and (max-width:1099px){.home-app-grid{grid-template-columns:repeat(4,minmax(0,1fr))!important}.home-app{min-height:145px!important}}\n@media(max-width:650px){header{min-height:185px!important;padding:25px 12px 20px!important}header h2{font-size:21px!important;max-width:440px!important}.header-actions{gap:9px!important}.location-btn,.auth-btn{min-height:44px!important;padding:9px 13px!important;font-size:14px!important}nav{padding:10px!important;gap:8px!important}nav button,nav .action{min-height:43px!important;padding:9px 13px!important;font-size:14px!important}main{padding:12px 10px 22px!important}#home>#v333AppLauncher{padding:13px!important}.home-folder{padding:12px!important}.home-folder h4{font-size:16px!important}.home-app-grid{grid-template-columns:repeat(3,minmax(0,1fr))!important;gap:9px!important}.home-app{min-height:132px!important;padding:10px 5px!important;gap:7px!important;border-radius:14px!important}.home-app .app-icon{font-size:34px!important}.home-app .app-label{font-size:13px!important}.home-app>.action{min-width:54px!important;min-height:33px!important;padding:6px 13px!important;font-size:14px!important}.home-app .app-move{flex-wrap:wrap;justify-content:center}.home-app .app-move button{font-size:11px!important}}\n\n</style></head><body><header><h1>ERCAN DEMİREL</h1><div style="font-weight:bold;color:#bbf7d0;letter-spacing:1px">@ERCAN3442</div><h2>FEYZULLAH DEMİREL TARIM VE HAYVANCILIK</h2><div id="clock">Yükleniyor...</div><div class="header-actions"><a class="location-btn" id="farmMapLink" href="https://maps.app.goo.gl/15ChbNikcC5HL8aF9" target="_blank" rel="noopener" style="text-decoration:none">📍 Çiftliğimin Konumu</a><button class="location-btn" onclick="openLocationModal()">🌤️ Konum & Hava Durumu</button><button class="auth-btn" id="authBtn" onclick="openAuthModal()">🔐 Üye Girişi / Kayıt</button></div></header>\n<nav><button class="nav-apps" onclick="switchPage(\'home\')">📁 Uygulamalar</button><button onclick="switchPage(\'home\')">🏠 Ana Sayfa</button><button onclick="switchPage(\'animals\')">🐄 Hayvanlar</button><button id="animalHealthNav" onclick="openAnimalHealthPlanner()">🩺 Aşı & Hayvan Sağlığı</button><button onclick="switchPage(\'feed\')">🌾 Yem & Stok</button><button onclick="switchPage(\'farming\')">🌱 Tarla & Sera</button><button onclick="switchPage(\'milk\')">🥛 Süt</button><button onclick="switchPage(\'accounting\')">📒 Muhasebe</button><button onclick="switchPage(\'tasks\')">📅 Görevler</button><button onclick="switchPage(\'equipment\')">🚜 Ekipman</button><button onclick="switchPage(\'finance\')">💰 Finans</button><button onclick="switchPage(\'banking\')">🏦 Banka & Kredi</button><button onclick="switchPage(\'bist\')">📈 BIST & Hisseler</button><button onclick="switchPage(\'hunters\')">🦅 Binance Avcıları</button><button onclick="switchPage(\'ai\')">🤖 Yapay Zekâ</button><button onclick="switchPage(\'supports\')">🏛️ Devlet Destekleri</button><button id="adminNav" style="display:none" onclick="switchPage(\'admin\')">🛡️ Üye Yönetimi</button><a class="action" href="/media" style="text-decoration:none">🎬 ERCAN MEDIA CENTER</a></nav><main>\n<section id="home" class="page active"><div class="card"><h3>🌱 FEYZULLAH AKILLI ÇİFTLİK YÖNETİM MERKEZİ</h3><p class="muted">Hayvan, üretim, stok, muhasebe, tarım ve canlı piyasa tek merkezde.</p><div id="dashboard" class="grid-3"><div class="stat">Aktif hayvan<b>—</b></div><div class="stat">Stok uyarısı<b>—</b></div><div class="stat">Net gelir<b>—</b></div></div></div><div class="card" id="v333AppLauncher"><div style="display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap"><h3>📱 Uygulamalarım ve Klasörlerim</h3><span class="muted">Sürükleyerek veya oklarla taşı</span></div><div id="homeAppGroups" class="home-app-groups"></div><div id="homeDesignControls" style="display:none;margin-top:14px"><div class="grid-2"><div><label>Ana ekran teması</label><select id="homeThemeSelect" onchange="homePreferences.theme=this.value;applyHomePreferences()"><option value="dark">Koyu</option><option value="green">Çiftlik yeşili</option><option value="light">Açık</option><option value="blue">Mavi</option><option value="sunset">Gün batımı</option></select></div><div><label>Saat teması</label><select id="clockStyleSelect" onchange="homePreferences.clock_style=this.value;applyHomePreferences()"><option value="digital">Dijital</option><option value="neon">Neon</option><option value="classic">Klasik</option><option value="minimal">Minimal</option></select></div></div><div id="homeFolderAssignments"></div><button class="action" onclick="saveHomePreferences()">💾 Ana ekran tasarımını kaydet</button></div></div><div class="card"><h3>🛡️ Veri Güvenliği / Yedekleme</h3><button class="action" onclick="createBackup()">Şimdi Yedek Al</button><div id="backupList"></div><label>SQLite yedeğinden güvenli geri yükle</label><input id="restoreFile" type="file" accept=".db,.sqlite,.sqlite3"><button class="action danger" onclick="restoreBackup()">Yedeği Geri Yükle</button><p class="muted">Geri yükleme öncesi mevcut veritabanının güvenlik kopyası alınır. Güvenlik nedeniyle bu özellik yalnızca tek kullanıcılı kurulumda çalışır.</p></div><div class="card"><h3>🌤️ 7 Günlük Hava Durumu</h3><div id="weatherStatus" class="muted">Konum bekleniyor</div><div id="weatherForecastContainer" class="weather-grid"></div></div><div class="card" id="rationCalculator"><h3>🐂 Akıllı Rasyon Ön Hesaplama</h3><p class="muted">Kaba ve kesif yemleri seçin, günlük miktarlarını girin. Değerler örnektir; mümkünse kendi yem analizinizdeki kuru madde, ham protein ve metabolik enerji değerlerini kullanın.</p><div class="grid-2"><div><label>Hayvan türü</label><select id="rationSpecies" onchange="updateRationBreeds();renderRationFeeds()"><option value="cattle">Büyükbaş</option><option value="sheep">Koyun</option><option value="goat">Keçi</option></select></div><div><label>Irk / cins</label><select id="rationBreed"></select></div><div><label>Üretim amacı</label><select id="rationPurpose" onchange="renderRationFeeds()"><option value="dairy">Süt</option><option value="beef">Besi / canlı ağırlık artışı</option><option value="growth">Genç hayvan büyütme</option></select></div><div><label>Canlı ağırlık (kg)</label><input id="rationWeight" type="number" min="10" max="1500" value="600"></div><div><label>Hedef canlı ağırlık (kg)</label><input id="rationTargetWeight" type="number" min="10" max="1800" value="700"></div><div><label>Günlük hedef süt (litre; süt hayvanında)</label><input id="rationMilkTarget" type="number" min="0" max="80" step="0.5" value="20"></div><div><label>Günlük hedef canlı ağırlık artışı (kg)</label><input id="rationGainTarget" type="number" min="0" max="2" step="0.05" value="0.8"></div><div><label>Laktasyon / büyüme dönemi notu</label><input id="rationStage" placeholder="Örn. 2. ay laktasyon / 8 aylık dana"></div></div><h4>🌾 Yem maddelerini seç ve miktarını gir</h4><div class="ration-feed" style="font-weight:bold;background:#14283b"><span>Yem maddesi</span><span>kg/gün</span><span>KM %</span><span>HP %</span><span>ME MJ</span></div><div id="rationFeedRows" class="ration-feeds"></div><div class="actions"><button class="action" onclick="addCustomRationFeed()">＋ Özel yem ekle</button><button class="action" onclick="calculateSmartRation()">🧮 Rasyonu hesapla</button><button class="action" onclick="resetRationFeeds()">Temizle</button></div><div id="rationResult" class="ration-box">Yemleri seçip miktarlarını girdikten sonra hesapla düğmesine basın. Sonuçlar yaklaşık ön değerlendirmedir.</div><p class="muted">Uyarı: Bu araç veteriner/zooteknist tarafından hazırlanmış dengeli rasyonun yerine geçmez. Hayvanın gebelik, laktasyon, yaş, kondisyon, yem analizleri, mineral-vitamin ve su ihtiyacı ayrıca değerlendirilmelidir. Süt ve canlı ağırlık artışı tahmindir; garanti değildir.</p></div></section>\n<section id="animals" class="page"><div class="card"><h3>🐄 Yeni Hayvan Kaydı</h3><div class="grid-2"><div><label>Küpe numarası</label><input id="aTag" placeholder="TR... / Küpe no"></div><div><label>Hayvan adı</label><input id="aName" placeholder="Hayvan adı"></div><div><label>Tür</label><select id="aSpecies"><option>Büyükbaş</option><option>Buzağı</option><option>Küçükbaş</option><option>Keçi</option><option>Diğer</option></select></div><div><label>Cinsiyet</label><select id="aSex"><option value="">Belirtilmedi</option><option>Dişi</option><option>Erkek</option></select></div><div><label>Irk</label><input id="aBreed"></div><div><label>Doğum tarihi</label><input id="aBirthDate" type="date"></div><div><label>Yaş (eski alan)</label><input id="aAge"></div><div><label>Kaçıncı doğum?</label><input id="aBirthCount" type="number" value="0"></div><div><label>Beklenen doğum</label><input id="aDue" type="date"></div><div><label>Alış tarihi</label><input id="aPurchaseDate" type="date"></div><div><label>Alış bedeli (TL)</label><input id="aPurchasePrice" type="number" min="0" step="0.01" value="0"></div><div><label>Ahır / bölme</label><input id="aLocation"></div></div><label>Notlar</label><textarea id="aNotes"></textarea><button class="action" onclick="addAnimal()">➕ Hayvanı Kaydet</button></div><div class="card"><h3>📋 Sürü Listesi</h3><div id="animalList">Giriş yapın.</div></div><div class="card"><h3>🗂️ Hayvan Geçmişi / İşlem Kaydı</h3><label>Hayvan ID</label><input id="eventAnimalId" type="number" placeholder="Listede görünen ID"><div class="grid-2"><div><label>İşlem</label><select id="eventType"><option value="death">Ölüm</option><option value="birth">Doğum</option><option value="sale">Satış</option><option value="transfer">Nakil</option><option value="weight">Kilo ölçümü</option><option value="other">Diğer</option></select></div><div><label>Tarih</label><input id="eventDate" type="date"></div></div><label>Açıklama / ölüm nedeni / yavru bilgisi</label><textarea id="eventDescription"></textarea><label>İşlem tutarı (TL; satış/doğum gideri için)</label><input id="eventAmount" type="number" min="0" step="0.01" value="0"><button class="action" onclick="addAnimalEvent()">İşlemi Kaydet</button><p class="muted">Ölüm, satış ve nakil kaydı hayvanı aktif listeden çıkarır; kayıt silinmez, geçmişte kalır. Resmî küpe/ölüm/nakil bildirimleri ilgili yetkili sisteme ayrıca yapılmalıdır.</p></div></div></section>\n<section id="feed" class="page"><div class="card"><h3>🌾 Yem Fiyatları</h3><label>Yem adı</label><input id="feedName"><label>Tedarikçi</label><input id="feedSupplier"><label>Fiyat (TL)</label><input id="feedPrice" type="number" min="0" step="0.01"><button class="action" onclick="addFeed()">Fiyat Kaydet</button><div id="feedList"></div></div><div class="card"><h3>📦 Depo / Stok Kalemi</h3><div class="grid-2"><div><label>Ürün adı</label><input id="stockName"></div><div><label>Kategori</label><select id="stockCategory"><option>Yem</option><option>İlaç</option><option>Aşı</option><option>Tohum</option><option>Gübre</option><option>Yakıt</option><option>Diğer</option></select></div><div><label>Birim</label><input id="stockUnit" value="kg"></div><div><label>Başlangıç miktarı</label><input id="stockQty" type="number" value="0"></div><div><label>Minimum stok uyarısı</label><input id="stockMin" type="number" value="0"></div><div><label>Birim maliyet (TL)</label><input id="stockCost" type="number" value="0" step="0.01"></div></div><label>Tedarikçi</label><input id="stockSupplier"><button class="action" onclick="addStock()">Stok Oluştur</button><div id="stockList"></div><button class="action" onclick="loadConsumptionReport()">📊 Son 30 Gün Tüketim / Maliyet Raporu</button><div id="consumptionReport"></div><h4>Stok hareketi</h4><div class="grid-2"><input id="moveItem" type="number" placeholder="Stok ID"><select id="moveType"><option value="in">Giriş / Satın alma</option><option value="out">Çıkış / Tüketim</option></select><input id="moveQty" type="number" placeholder="Miktar"><input id="moveCost" type="number" placeholder="Birim maliyet"></div><input id="moveDesc" placeholder="Açıklama"><button class="action" onclick="stockMove()">Stok Hareketini Kaydet</button></div></section>\n<section id="farming" class="page"><div class="card"><h3>🌱 Tarla / Sera Kaydı</h3><div class="grid-2"><input id="fieldName" placeholder="Tarla / sera adı"><input id="fieldCrop" placeholder="Ürün / ekin"><input id="fieldArea" type="number" placeholder="Alan"><input id="fieldUnit" value="dekar" placeholder="Birim"><label>Ekim tarihi<input id="fieldPlant" type="date"></label><label>Hasat tarihi<input id="fieldHarvest" type="date"></label></div><textarea id="fieldNotes" placeholder="Sulama, gübreleme, ilaçlama notları"></textarea><button class="action" onclick="addField()">Tarla Kaydet</button><div id="fieldList"></div></div><div class="card"><h3>📍 GPS ile Parsel / Tarla Alanı Ölçümü</h3><p class="muted">Telefonun konum iznini aç. Ölçüme başla ve tarlanın sınırını yürüyerek dolaş. Başladığın noktaya dönünce ölçümü bitir. 1 dönüm = 1.000 m². En iyi sonuç için açık alanda, gökyüzü görüşü açıkken yürü.</p><label>Parsel adı</label><input id="measureName" placeholder="Örn. Köy yolu üstü tarla 1"><div class="grid-2"><div class="stat">Kaydedilen GPS noktası<b id="measurePoints">0</b></div><div class="stat">Anlık GPS doğruluğu<b id="measureAccuracy">—</b></div><div class="stat">Ortalama doğruluk<b id="measureAvgAccuracy">—</b></div></div><div class="grid-2"><button class="action" id="measureStartBtn" onclick="startFieldMeasure()">▶ Ölçüme Başla</button><button class="action danger" onclick="stopFieldMeasure()">■ Ölçümü Durdur</button></div><div id="measureStatus" class="item">Henüz ölçüm başlamadı.</div><div class="grid-2"><div class="stat">Alan (m²)<b id="measureArea">—</b></div><div class="stat">Alan (dönüm)<b id="measureDekar">—</b></div></div><div class="grid-2"><div class="stat">Çevre<b id="measurePerimeter">—</b></div><div class="stat">Yaklaşık dönüm / hektar<b id="measureHectare">—</b></div></div><div class="actions"><button class="action" onclick="undoMeasurePoint()">↶ Son Noktayı Geri Al</button><button class="action" onclick="finishFieldMeasure()">✓ Alanı Hesapla</button><button class="action" onclick="clearFieldMeasure()">Temizle</button><button class="action" id="measureSaveBtn" onclick="saveFieldMeasure()" disabled>💾 Ölçümü Kaydet</button></div><p class="muted">Not: Telefon GPS\'i birkaç metre hatta daha fazla sapabilir. Bu ölçüm yaklaşık alan verir; tapu/kadastro ve resmi sınır ölçümü yerine geçmez. Tarla sınırını güvenli şekilde dolaş.</p><div id="savedMeasurements"></div></div><div class="card"><h3>🌾 Kapsamlı Ürün Ekim / Hasat Takvimi</h3><p class="muted">Sebze, meyve, baklagil, tahıl, endüstri ve yem bitkileri kataloğu. Önerilen yetişme günleri yaklaşık değerdir; kesin tarihler bölge, çeşit, iklim ve yetiştirme koşullarına göre kullanıcı tarafından girilir.</p><div class="grid-2"><div><label>Ürün</label><input id="cropName" list="cropOptions" placeholder="Domates, biber, buğday..." onchange="estimateHarvestFromSow()"><datalist id="cropOptions"></datalist></div><div><label>Çeşit</label><input id="cropVariety" placeholder="Cherry, kapya, Kırkağaç..."></div><div><label>Yetiştirme</label><select id="cropMethod"><option>Açık tarla</option><option>Sera</option><option>Fide</option></select></div><div><label>Bölge / ilçe</label><input id="cropRegion" placeholder="Örn. Konya / Ereğli"></div><div><label>Ekim / tohum tarihi</label><input id="cropSow" type="date" onchange="estimateHarvestFromSow()"></div><div><label>Şaşırtma / dikim tarihi</label><input id="cropTransplant" type="date"></div><div><label>İlk hasat tarihi</label><input id="cropHarvestStart" type="date"></div><div><label>Son hasat tarihi</label><input id="cropHarvestEnd" type="date"></div></div><textarea id="cropNotes" placeholder="Sulama, gübreleme, çeşit, parsel ve diğer notlar"></textarea><button class="action" onclick="addCropCalendar()">Takvime Ekle</button><div id="cropCalendarList"></div></div><div class="card"><h3>🌱 V25 Akıllı Ekim & Hasat Planlayıcı</h3><p class="muted">Yalnızca bölge ve il seçin. Katalogdaki bütün ürünlerin önerilen ekim/hasat tarihleri ve canlı geri sayımları otomatik listelenir. İsterseniz tamamını kalıcı takvime kaydedebilirsiniz.</p><div class="grid-2"><div><label>🌍 Bölge</label><select id="cropAutoRegion" onchange="loadAutoCropPlanner()"><option value="">Bölge seçin</option><option>Marmara</option><option>Ege</option><option>Akdeniz</option><option>İç Anadolu</option><option>Karadeniz</option><option>Doğu Anadolu</option><option>Güneydoğu Anadolu</option></select></div><div><label>📍 İl / konum</label><select id="cropAutoLocation" onchange="loadAutoCropPlanner()"><option value="">İl seçin</option><option>Adana</option><option>Adıyaman</option><option>Afyonkarahisar</option><option>Ağrı</option><option>Amasya</option><option>Ankara</option><option>Antalya</option><option>Artvin</option><option>Aydın</option><option>Balıkesir</option><option>Bilecik</option><option>Bingöl</option><option>Bitlis</option><option>Bolu</option><option>Burdur</option><option>Bursa</option><option>Çanakkale</option><option>Çankırı</option><option>Çorum</option><option>Denizli</option><option>Diyarbakır</option><option>Edirne</option><option>Elazığ</option><option>Erzincan</option><option>Erzurum</option><option>Eskişehir</option><option>Gaziantep</option><option>Giresun</option><option>Gümüşhane</option><option>Hakkari</option><option>Hatay</option><option>Isparta</option><option>Mersin</option><option>İstanbul</option><option>İzmir</option><option>Kars</option><option>Kastamonu</option><option>Kayseri</option><option>Kırklareli</option><option>Kırşehir</option><option>Kocaeli</option><option>Konya</option><option>Kütahya</option><option>Malatya</option><option>Manisa</option><option>Kahramanmaraş</option><option>Mardin</option><option>Muğla</option><option>Muş</option><option>Nevşehir</option><option>Niğde</option><option>Ordu</option><option>Rize</option><option>Sakarya</option><option>Samsun</option><option>Siirt</option><option>Sinop</option><option>Sivas</option><option>Tekirdağ</option><option>Tokat</option><option>Trabzon</option><option>Tunceli</option><option>Şanlıurfa</option><option>Uşak</option><option>Van</option><option>Yozgat</option><option>Zonguldak</option><option>Aksaray</option><option>Bayburt</option><option>Karaman</option><option>Kırıkkale</option><option>Batman</option><option>Şırnak</option><option>Bartın</option><option>Ardahan</option><option>Iğdır</option><option>Yalova</option><option>Karabük</option><option>Kilis</option><option>Osmaniye</option><option>Düzce</option></select></div></div><div id="autoCropResult" class="item">Ekim sayacını başlatmak için bölge ve il seçin.</div><div class="actions"><button class="action secondary" onclick="saveAllAutoCropPlans()" id="autoCropSaveAllBtn" disabled>134 ürünün tamamını takvime kaydet</button></div><div id="autoCropPlannerList" class="grid-2"></div><small class="muted">Tarihler genel ekim dönemleri ve yaklaşık yetişme sürelerine dayalı planlama önerisidir; çeşit, rakım, don riski ve yerel hava koşullarıyla kontrol edilmelidir.</small></div></section></section>\n<section id="milk" class="page"><div class="card"><h3>🥛 Süt Üretimi ve Satışı</h3><div class="grid-2"><input id="milkAnimal" type="number" placeholder="Hayvan ID (isteğe bağlı)"><label>Tarih<input id="milkDate" type="date"></label><input id="milkLiters" type="number" step="0.1" placeholder="Litre"><input id="milkPrice" type="number" step="0.01" placeholder="Litre satış fiyatı TL"><input id="milkBuyer" placeholder="Alıcı / mandıra"></div><textarea id="milkNotes" placeholder="Not"></textarea><button class="action" onclick="addMilk()">Süt Kaydet</button><div id="milkList"></div></div></section>\n<section id="accounting" class="page"><div class="card"><h3>📒 Gelir / Gider Kaydı</h3><div class="grid-2"><select id="txKind"><option value="income">Gelir</option><option value="expense">Gider</option></select><input id="txCategory" placeholder="Kategori (süt, yem, hayvan satışı...)"><input id="txAmount" type="number" step="0.01" placeholder="Tutar"><select id="txCurrency"><option>TRY</option><option>USD</option><option>EUR</option></select><label>Tarih<input id="txDate" type="date"></label></div><textarea id="txDescription" placeholder="Açıklama"></textarea><button class="action" onclick="addTransaction()">Muhasebe Kaydet</button><div id="accountSummary" class="grid-3"></div><div id="txList"></div></div></section>\n<section id="tasks" class="page"><div class="card"><h3>📅 Görev / Aşı / Bakım Takvimi</h3><input id="taskTitle" placeholder="Görev adı"><div class="grid-2"><label>Tarih<input id="taskDate" type="date"></label><input id="taskCategory" placeholder="Kategori: aşı, bakım, ekim..."></div><textarea id="taskNotes" placeholder="Açıklama"></textarea><button class="action" onclick="addTask()">Görev Ekle</button><div id="taskList"></div></div></section>\n<section id="equipment" class="page"><div class="card"><h3>🚜 Makine / Ekipman Bakım Takibi</h3><input id="eqName" placeholder="Traktör / makine adı"><input id="eqKind" placeholder="Tür / model"><div class="grid-2"><label>Son bakım<input id="eqLast" type="date"></label><label>Sonraki bakım<input id="eqNext" type="date"></label></div><textarea id="eqNotes"></textarea><button class="action" onclick="addEquipment()">Ekipman Kaydet</button><div id="equipmentList"></div></div></section>\n<section id="finance" class="page"><div class="card"><h3>💰 Altın / Gümüş / Kripto</h3><div class="grid-2"><div class="stat">Altın ons<b id="goldPrice">—</b></div><div class="stat">Gümüş ons<b id="silverPrice">—</b></div><div class="stat">Teorik gram altın<b id="gramGoldPrice">—</b></div><div class="stat">Teorik çeyrek altın<b id="quarterGoldPrice">—</b></div><div class="stat">BTC / USDT<b id="btcPrice">—</b><span id="btcChange"></span></div><div class="stat">ETH / USDT<b id="ethPrice">—</b><span id="ethChange"></span></div></div><p class="muted">Gram altın teorik hesaplaması için ons altın ve USD/TRY gerekir. Kuyumcu alış-satış fiyatları ayrıca farklı olabilir.</p><div id="liveGainers"></div><div id="liveLosers"></div><hr><h4>📊 Sanal Kripto Portföyü</h4><p class="muted">Bu modül deneme amaçlı portföy kaydıdır; gerçek alım-satım emri göndermez.</p><div class="grid-2"><input id="portfolioSymbol" placeholder="BTCUSDT"><select id="portfolioSide"><option value="buy">Sanal alış</option><option value="sell">Sanal satış</option></select><input id="portfolioQty" type="number" step="any" placeholder="Miktar"><input id="portfolioPrice" type="number" step="any" placeholder="İşlem fiyatı USDT"><input id="portfolioFee" type="number" step="any" value="0" placeholder="Komisyon"></div><button class="action" onclick="addPortfolioTrade()">Portföy İşlemini Kaydet</button><div id="portfolioList"></div></div></section>\n<section id="banking" class="page">\n<div class="card"><h3>🏦 BANKA FAİZLERİ & KREDİ MERKEZİ</h3><p class="muted">Banka teklifleri müşteri profiline, kampanyaya ve tarihe göre değişir. Bu sürümde doğrulanmamış faiz oranı gösterilmez. Bankanın resmî teklifindeki oranı girerek hesaplama ve karşılaştırma yapabilirsiniz. Resmî oran bağlantıları bankanın sitesine yönlendirir.</p><div class="item"><b>Güncellik uyarısı:</b> Otomatik ve doğrulanmış canlı banka faiz akışı henüz bağlı değildir. Oran alanlarını bankanın güncel teklifinden doldurun; son kontrol tarihini kendiniz kaydedin.</div></div>\n<div class="card"><h3>🧮 Kredi Hesaplama</h3><div class="grid-2"><div><label>Kredi türü</label><select id="loanType"><option>İhtiyaç kredisi</option><option>Konut kredisi</option><option>Taşıt kredisi</option><option>Tarımsal kredi</option><option>İşletme kredisi</option></select></div><div><label>Kredi tutarı (TL)</label><input id="loanAmount" type="number" min="1" step="0.01" value="100000"></div><div><label>Aylık faiz (%)</label><input id="loanRate" type="number" min="0" step="0.01" value="3.5"></div><div><label>Vade (ay)</label><input id="loanMonths" type="number" min="1" max="360" value="36"></div><div><label>Ek masraf / sigorta (TL)</label><input id="loanFees" type="number" min="0" step="0.01" value="0"></div></div><button class="action" onclick="calculateLoan()">Krediyi Hesapla</button><div id="loanResult" class="item">Aylık taksit ve toplam geri ödeme burada gösterilir.</div><p class="muted">Sonuç yaklaşık eşit taksitli ödeme hesabıdır. Bankanın tahsis ücreti, sigorta, vergi ve ödeme planı farklı olabilir.</p></div>\n<div class="card"><h3>💰 Vadeli Mevduat / Katılım Getirisi</h3><div class="grid-2"><div><label>Yatırılacak tutar (TL)</label><input id="depositAmount" type="number" min="1" step="0.01" value="100000"></div><div><label>Yıllık brüt faiz / kâr payı (%)</label><input id="depositRate" type="number" min="0" step="0.01" value="35"></div><div><label>Vade (gün)</label><input id="depositDays" type="number" min="1" max="3660" value="32"></div><div><label>Stopaj (%)</label><input id="depositTax" type="number" min="0" max="100" step="0.01" placeholder="Bankanın/ürünün geçerli oranı"></div></div><button class="action" onclick="calculateDeposit()">Mevduatı Hesapla</button><div id="depositResult" class="item">Brüt ve stopaj girilirse net getiri burada hesaplanır.</div><p class="muted">Stopaj oranı ürün ve vade başlangıç tarihine göre değişebilir. Doğru oranı resmî kaynaktan girin. Katılım hesabında sonuç kâr payı varsayımıdır, garanti getiri değildir.</p></div>\n<div class="card"><h3>📊 Banka Tekliflerini Karşılaştır</h3><div class="grid-2"><div><label>Banka</label><select id="bankName"><option>Akbank</option><option>Albaraka Türk</option><option>Alternatif Bank</option><option>QNB</option><option>DenizBank</option><option> kuveyt Türk</option><option>OdeaBank</option><option>Türkiye Finans</option><option>TEB</option><option>ING</option><option>Enpara</option><option>Garanti BBVA</option><option>Halkbank</option><option>İş Bankası</option><option>Şekerbank</option><option>VakıfBank</option><option>Yapı Kredi</option><option>Ziraat Bankası</option><option>Ziraat Katılım</option><option>Vakıf Katılım</option><option>Emlak Katılım</option><option>Fibabanka</option><option>HSBC</option><option>ICBC Turkey</option></select></div><div><label>Teklif edilen aylık kredi faizi (%)</label><input id="compareRate" type="number" min="0" step="0.01" placeholder="Resmî teklif oranı"></div><div><label>Teklif tarihi</label><input id="compareDate" type="date"></div><div><label>Kredi türü / not</label><input id="compareNote" placeholder="Örn. 100.000 TL ihtiyaç kredisi"></div></div><button class="action" onclick="addBankOffer()">Teklifi Listeye Ekle</button><div id="bankOfferList" class="item">Henüz teklif eklenmedi. Karşılaştırma için bankadan aldığınız oranları girin.</div><button class="action danger" onclick="clearBankOffers()">Karşılaştırma listesini temizle</button></div>\n<div class="card"><h3>🔗 Bankaların Resmî İnternet Siteleri</h3><div id="bankOfficialLinks" class="grid-2"></div><p class="muted">Bu bağlantılar bilgi ve teklif kontrolü içindir. Banka oranları otomatik çekilmiş veya teyit edilmiş olarak sunulmaz.</p></div>\n</section>\n<section id="bist" class="page"><div class="card"><h3>📈 BIST 100 ve Borsa İstanbul Hisseleri</h3><p class="muted">Canlı/son fiyatlar Twelve Data üzerinden alınır. Veri gecikmesi, sembol kapsamı ve kullanım kotası sağlayıcıya bağlıdır. Gerçek fiyat yoksa tahmini değer gösterilmez.</p><div class="grid-2"><div><label>Hisse / endeks sembolü</label><input id="bistSymbol" value="THYAO" placeholder="Örn. THYAO, ASELS, XU100"></div><button class="action" onclick="loadOneBistQuote()">Sembolü Getir</button></div><div class="actions"><button class="action" onclick="loadBistQuotes()">📊 BIST listesini yenile</button><button class="action" onclick="loadBistSeries()">📉 Seçili hissenin grafiğini getir</button><select id="bistInterval"><option value="1day">Günlük</option><option value="1week">Haftalık</option><option value="1month">Aylık</option><option value="1h">Saatlik</option></select></div><div class="card"><h4>🦅 BIST Avcı Tarayıcı</h4><label>Tarama sembolleri (virgülle ayırın; boşsa sunucu listesi)</label><input id="bistHunterSymbols" placeholder="THYAO,ASELS,BIMAS,TUPRS,GARAN"><label>Minimum işlem hacmi eşiği</label><input id="bistHunterMinVolume" type="number" min="0" value="1000000"><label>Minimum mutlak günlük değişim (%)</label><input id="bistHunterMinChange" type="number" min="0" max="50" step="0.1" value="0"><div class="muted">0 = tüm hisseler. Örn. 1,5 = günlük değişimi en az ±%1,5 olanlar.</div><button class="action" onclick="runBistHunter()">🦅 Avcı taramasını başlat</button><div id="bistHunterStatus" class="muted">Tarama henüz yapılmadı.</div><div id="bistHunterResults"></div><p class="muted">Tam BIST 100 taraması için Render ortam değişkeni BIST100_SYMBOLS alanına güncel semboller virgülle yazılmalıdır. Emir defteri/gerçek alış-satış baskısı bu sağlayıcı uç noktasıyla doğrulanamaz.</p></div><div id="bistStatus" class="muted">Veri sağlayıcısı ayarı bekleniyor.</div><div id="bistQuotes" class="grid-2"></div><h4>📉 Fiyat geçmişi</h4><div id="bistSeries" class="item">Grafik için hisse seçip getir düğmesine basın.</div><p class="muted">Not: Bu ekran yatırım tavsiyesi vermez. TWELVE_DATA_API_KEY anahtarı Render ortam değişkenlerine eklenmelidir. Anahtar kod içine yazılmamalıdır.</p></div></section>\n<section id="hunters" class="page"><div class="card"><h3>🦅 Binance Coin Avcısı</h3><p class="muted">Tüm aktif USDT spot pariteleri listelenir. Fiyatlar WebSocket akışıyla güncellenir; yeni pariteler exchangeInfo yenilemesiyle keşfedilir. Listeleme tespiti gecikmeli olabilir.</p><input id="coinSearch" placeholder="Coin ara (örn. PEPEUSDT)" oninput="renderCoins()"><div id="marketStatus" class="muted">Bağlantı hazırlanıyor</div><div class="grid-2"><select id="coinSort" onchange="renderCoins()"><option value="symbol">Sembol A-Z</option><option value="change_desc">En çok yükselen</option><option value="change_asc">En çok düşen</option><option value="price_desc">Fiyat yüksekten</option><option value="price_asc">Fiyat düşükten</option></select><select id="coinPageSize" onchange="coinPage=1;renderCoins()"><option>50</option><option selected>100</option><option>250</option></select></div><div id="coinPager" class="actions"></div><div id="coinList"></div><hr><h4>🔔 Kalıcı Fiyat Alarmları</h4><div class="grid-2"><input id="alertSymbol" placeholder="Örn. BTCUSDT"><select id="alertCondition"><option value="above">Şu fiyatın üzerine çıkınca</option><option value="below">Şu fiyatın altına inince</option></select><input id="alertTarget" type="number" step="any" placeholder="Hedef fiyat"></div><button class="action" onclick="addPriceAlert()">Alarm Kaydet</button><div id="priceAlertList"></div><h4>📜 Alarm geçmişi</h4><div id="alertHistory"></div></div></section>\n<section id="ai" class="page"><div class="card"><h3>🤖 Yapay Zekâ Konsensüs</h3><textarea id="aiPrompt" placeholder="Çiftlik, tarım, finans veya yatırım sorunuzu yazın..."></textarea><button class="action" onclick="runAI()">Tüm Yapay Zekâlara Sor</button><pre id="aiResult" style="white-space:pre-wrap"></pre></div><div class="card"><h3>📈 AI Yatırım Laboratuvarı</h3><p class="muted">Binance verisi üzerinden 1 saatlik mumlar, SMA20, SMA50 ve RSI14 göstergeleriyle temkinli AI değerlendirmesi.</p><div class="grid-2"><input id="aiMarketSymbol" value="BTCUSDT" placeholder="Örn. BTCUSDT"><button class="action" onclick="loadTechnicalAnalysis()">Teknik Veriyi Getir</button></div><pre id="aiMarketResult" style="white-space:pre-wrap"></pre><textarea id="aiMarketQuestion" placeholder="Bu sembolün risklerini ve teknik görünümünü değerlendir.">Son 24 saat hareketini, teknik göstergeleri ve temel riskleri değerlendir.</textarea><button class="action" onclick="runInvestmentAI()">Sembolü Çoklu Yapay Zekâya Analiz Ettir</button><pre id="aiInvestmentResult" style="white-space:pre-wrap"></pre><p class="muted">Bu modül araştırma amaçlıdır; gerçek alım-satım emri göndermez ve yatırım tavsiyesi değildir.</p></div></section>\n\n<section id="supports" class="page">\n <div class="card"><h3>🏛️ DEVLET DESTEKLERİ & TARIMSAL KREDİLER</h3><p class="muted">Resmî kurum duyuruları izlenir. Tutarı veya ödeme tarihi açıklanmayan desteklerde tahmin gösterilmez. Kaynak kontrolü sunucu tarafından yaklaşık 6 saatte bir yapılır.</p><div class="actions"><button class="action" onclick="loadSupports()">↻ Verileri yenile</button><button class="action" onclick="refreshSupportSources(this)">☁️ Resmî kaynakları şimdi kontrol et</button></div><div id="supportStatus" class="muted">Destek bilgileri yükleniyor...</div></div>\n <div class="grid-2"><div class="card"><h3>🌱 Bitkisel üretim</h3><div id="supportPlant"></div></div><div class="card"><h3>🐄 Hayvancılık ve çiğ süt</h3><div id="supportAnimal"></div></div><div class="card"><h3>🏦 Tarımsal krediler</h3><div id="supportCredit"></div></div><div class="card"><h3>🚀 KOSGEB</h3><div id="supportKosgeb"></div></div></div>\n <div class="card"><h3>🔎 Resmî kaynakların son kontrol durumu</h3><div id="supportSources" class="grid-2"></div><p class="muted">Son kontrol zamanı, sayfanın sunucudan başarıyla alındığını gösterir; her kurum sayfasının içeriği otomatik ve hatasız biçimde yapılandırılmış veri olarak okunabildiği anlamına gelmez. Hakediş ve kişisel ödeme tarihi için ilgili kurum/e-Devlet kaydı esas alınır.</p></div>\n</section>\n\n<section id="admin" class="page"><div class="card"><h3>🛡️ Üye Yönetimi</h3><p class="muted">Yalnızca site sahibi yönetebilir. Üyeler yalnızca kendi kayıtlarını görüntüler; kayıt ekleme, değiştirme ve silme yetkisi yöneticiye aittir.</p><button class="action" onclick="loadAdminUsers()">Üyeleri Yenile</button><div id="adminUserList">Yönetici hesabıyla giriş yapın.</div><h3>🔑 Şifre Kurtarma Talepleri</h3><button class="action" onclick="loadRecoveryRequests()">Talepleri Yenile</button><div id="recoveryRequestList">Bekleyen talepler burada görünür.</div></div></section></main><div id="authModal" class="modal"><div class="modal-content auth-modal-content"><div class="auth-modal-head"><h3>🔐 Üye İşlemleri</h3><button type="button" class="auth-close" onclick="closeAuthModal()" aria-label="Kapat">✕</button></div><p class="muted">Tek yönetici vardır. İlk hesap yönetici olur; sonraki hesaplar üye olarak açılır.</p><div class="auth-tabs"><button type="button" class="action auth-tab active" id="authLoginTab" onclick="setAuthMode(\'login\')">🔑 Üye Girişi</button><button type="button" class="action auth-tab" id="authRegisterTab" onclick="setAuthMode(\'register\')">＋ Kayıt Ol</button></div><section id="authLoginPanel" class="auth-panel"><label>Kullanıcı adı veya kayıtlı telefon</label><input id="loginUsername" autocomplete="username" placeholder="Kullanıcı adı veya telefon"><label>Şifre</label><input id="loginPassword" type="password" autocomplete="current-password" placeholder="Şifreniz"><div class="auth-button-stack"><button type="button" class="action auth-primary" onclick="login()">🔓 Giriş Yap</button><button type="button" class="action auth-secondary" onclick="requestRecovery()">🔑 Şifremi Unuttum</button></div></section><section id="authRegisterPanel" class="auth-panel" style="display:none"><label>Kullanıcı adı *</label><input id="regUsername" maxlength="40" autocomplete="username" placeholder="3–40 karakter"><label>Şifre *</label><input id="regPassword" type="password" maxlength="128" autocomplete="new-password" placeholder="En az 8 karakter"><label>Gerçek ad ve soyad *</label><input id="regFullName" maxlength="100" placeholder="Adınız Soyadınız"><label>Doğum yeri *</label><input id="regBirthplace" maxlength="100" placeholder="İl / İlçe"><label>Telefon numarası *</label><input id="regPhone" type="tel" maxlength="20" autocomplete="tel" placeholder="05XXXXXXXXX"><p class="muted">Tüm kayıt bilgileri zorunludur. Daha önce hesabınız varsa yeniden kayıt olmak yerine Üye Girişi bölümünü kullanın.</p><button type="button" class="action auth-primary" onclick="register()">＋ Hesabımı Oluştur</button></section><section class="auth-profile"><details><summary>Profil bilgilerimi güncelle (giriş yaptıktan sonra)</summary><input id="myFullName" placeholder="Gerçek ad ve soyad"><input id="myBirthplace" placeholder="Doğum yeri"><input id="myPhone" type="tel" placeholder="Telefon numaranız"><button type="button" class="action" onclick="updateMyProfile()">Profilimi Kaydet</button></details><button type="button" class="action danger auth-logout" onclick="logout()">Çıkış Yap</button></section></div></div><div id="locationModal" class="modal"><div class="modal-content"><h3>📍 Konum</h3><input id="city" value="Konya" placeholder="İl"><input id="district" value="Ereğli" placeholder="İlçe"><button class="action" onclick="fetchWeather()">Hava Durumunu Getir</button><button class="action" onclick="closeLocationModal()">Kapat</button></div></div>\n<script>\nlet isAdmin=false;let token=localStorage.getItem(\'feyzullah_token\')||\'\',socket=null,tickers=new Map(),symbols=[],financeStarted=false,coinPage=1,coinPageSize=100,knownSymbols=new Set(),newlyListed=[],cropCountdownTimer=null,alertCheckTimer=null,alertLastState=new Set(),cropCatalogCache=[],autoCropPlanCache=[];const $=id=>document.getElementById(id);const esc=v=>String(v??\'\').replace(/[&<>"\']/g,c=>({\'&\':\'&amp;\',\'<\':\'&lt;\',\'>\':\'&gt;\',\'"\':\'&quot;\',"\'":\'&#39;\'}[c]));const fmt=n=>Number(n||0).toLocaleString(\'tr-TR\',{maximumFractionDigits:8});\nfunction headers(){return token?{\'Authorization\':\'Bearer \'+token,\'Content-Type\':\'application/json\'}:{\'Content-Type\':\'application/json\'}}function apiErrorMessage(detail){if(typeof detail===\'string\')return detail;if(Array.isArray(detail))return detail.map(x=>{const loc=Array.isArray(x?.loc)?x.loc.filter(v=>v!==\'body\').join(\'.\'):\'\',type=String(x?.type||\'\'),msg=String(x?.msg||\'Geçersiz bilgi\');if(loc===\'username\')return \'Kullanıcı adı en az 3, en fazla 40 karakter olmalıdır.\';if(loc===\'password\')return \'Şifre en az 8, en fazla 128 karakter olmalıdır.\';if(loc===\'full_name\')return \'Gerçek ad ve soyad bilgisi geçersiz.\';if(loc===\'birthplace\')return \'Doğum yeri bilgisi geçersiz.\';if(loc===\'phone\')return \'Telefon numarası bilgisi geçersiz.\';return msg}).join(\'\\n\');if(detail&&typeof detail===\'object\')return detail.message||detail.msg||\'İşlem başarısız.\';return \'İşlem başarısız. Lütfen tekrar deneyin.\'}async function api(url,opt={}){opt.headers={...(opt.headers||{}),...headers()};let r=await fetch(url,opt),d={};try{d=await r.json()}catch{}if(!r.ok)throw Error(apiErrorMessage(d.detail??d));return d}function post(url,obj){return api(url,{method:\'POST\',body:JSON.stringify(obj)})}\nconst ADMIN_ACTIONS=new Set([\'addAnimal\',\'addAnimalEvent\',\'archiveAnimal\',\'addFeed\',\'addStock\',\'stockMove\',\'addMilk\',\'addTransaction\',\'addTask\',\'addEquipment\',\'addField\',\'saveFieldMeasure\',\'deleteFieldMeasurement\',\'addCropCalendar\',\'deleteCropCalendar\',\'saveAllAutoCropPlans\',\'addPriceAlert\',\'deletePriceAlert\',\'addPortfolioTrade\',\'createBackup\',\'restoreBackup\',\'refreshSupportSources\',\'deleteCropCalendar\',\'deletePriceAlert\']);\nfunction applyPermissions(){const quick=document.getElementById(\'v31AddWidget\');if(quick)quick.style.display=isAdmin?\'\':\'none\';const design=document.getElementById(\'homeDesignControls\');if(design)design.style.display=isAdmin?\'block\':\'none\';const nav=document.getElementById(\'adminNav\');if(nav)nav.style.display=isAdmin?\'\':\'none\';document.querySelectorAll(\'button[onclick]\').forEach(b=>{const m=(b.getAttribute(\'onclick\')||\'\').match(/^\\s*([A-Za-z_$][\\w$]*)\\s*\\(/);if(m&&ADMIN_ACTIONS.has(m[1]))b.style.display=isAdmin?\'\':\'none\';});}\nasync function syncAccount(){if(!token){isAdmin=false;applyPermissions();return}const me=await api(\'/api/me\');isAdmin=me.role===\'admin\';applyPermissions();if(isAdmin){await loadAdminUsers();await loadRecoveryRequests()}}\nasync function loadRecoveryRequests(){if(!isAdmin)return;api(\'/api/admin/recovery\').then(rows=>{$(\'recoveryRequestList\').innerHTML=rows.map(r=>`<div class="item"><b>${esc(r.username)}</b> • ${esc(r.phone||\'Telefon yok\')} • ${esc(r.status)}<div class="muted">Talep: ${esc(r.created_at)}</div>${r.status===\'pending\'?`<button class="action" onclick="resetRecovery(${r.id})">Geçici şifre oluştur</button>`:\'\'}</div>`).join(\'\')||\'Kurtarma talebi yok.\'}).catch(e=>$(\'recoveryRequestList\').textContent=e.message)}async function resetRecovery(id){if(!confirm(\'Geçici şifre oluşturulsun mu?\'))return;try{const d=await post(\'/api/admin/recovery/\'+id+\'/reset\',{});alert(\'Geçici şifre: \'+d.temporary_password+\'\\nBu şifreyi kullanıcıya güvenli bir kanaldan iletin.\');await loadRecoveryRequests()}catch(e){showError(e)}}async function loadAdminUsers(){if(!isAdmin)return;try{const users=await api(\'/api/admin/users\');$(\'adminUserList\').innerHTML=users.map(u=>`<div class="item"><b>${esc(u.full_name||\'Ad-soyad girilmemiş\')}</b> • ${esc(u.username)} • ${u.role===\'admin\'?\'Yönetici\':\'Üye\'} • <span class="${u.status===\'active\'?\'green\':\'red\'}">${esc(u.status)}</span><div class="muted">Doğum yeri: ${esc(u.birthplace||\'—\')}<br>Telefon: ${esc(u.phone||\'—\')}<br>Kayıt: ${esc(u.created_at||\'—\')}</div><div class="actions">${u.role===\'admin\'?\'\':(u.status===\'active\'?`<button class="action danger" onclick="adminBlock(${u.id})">Engelle</button>`:u.status===\'blocked\'?`<button class="action" onclick="adminUnblock(${u.id})">Engeli kaldır</button>`:\'Kapalı\')}${u.role===\'admin\'?\'\':`<button class="action danger" onclick="adminDelete(${u.id})">Üyeliği kapat</button>`}</div></div>`).join(\'\')||\'Üye yok.\'}catch(e){$(\'adminUserList\').textContent=e.message}}\nasync function adminBlock(id){if(!confirm(\'Bu üyeyi engellemek istiyor musunuz?\'))return;try{await post(\'/api/admin/users/\'+id+\'/block\',{});await loadAdminUsers()}catch(e){showError(e)}}\nasync function adminUnblock(id){try{await post(\'/api/admin/users/\'+id+\'/unblock\',{});await loadAdminUsers()}catch(e){showError(e)}}\nasync function adminDelete(id){if(!confirm(\'Bu üyeliği kapatmak istiyor musunuz? Eski kayıtlar korunur.\'))return;try{await api(\'/api/admin/users/\'+id,{method:\'DELETE\'});await loadAdminUsers()}catch(e){showError(e)}}\nfunction openAuthModal(){$(\'authModal\').style.display=\'flex\';setAuthMode(\'login\')}function closeAuthModal(){$(\'authModal\').style.display=\'none\'}function openLocationModal(){$(\'locationModal\').style.display=\'flex\'}function closeLocationModal(){$(\'locationModal\').style.display=\'none\'}const BANK_OFFICIAL_LINKS=[[\'Akbank\',\'https://www.akbank.com/\'],[\'Albaraka Türk\',\'https://www.albaraka.com.tr/\'],[\'Alternatif Bank\',\'https://www.alternatifbank.com.tr/\'],[\'QNB\',\'https://www.qnb.com.tr/\'],[\'DenizBank\',\'https://www.denizbank.com/\'],[\'Kuveyt Türk\',\'https://www.kuveytturk.com.tr/\'],[\'OdeaBank\',\'https://www.odeabank.com.tr/\'],[\'Türkiye Finans\',\'https://www.turkiyefinans.com.tr/\'],[\'TEB\',\'https://www.teb.com.tr/\'],[\'ING\',\'https://www.ing.com.tr/\'],[\'Enpara\',\'https://www.enpara.com/\'],[\'Garanti BBVA\',\'https://www.garantibbva.com.tr/\'],[\'Halkbank\',\'https://www.halkbank.com.tr/\'],[\'İş Bankası\',\'https://www.isbank.com.tr/\'],[\'Şekerbank\',\'https://www.sekerbank.com.tr/\'],[\'VakıfBank\',\'https://www.vakifbank.com.tr/\'],[\'Yapı Kredi\',\'https://www.yapikredi.com.tr/\'],[\'Ziraat Bankası\',\'https://www.ziraatbank.com.tr/\'],[\'Ziraat Katılım\',\'https://www.ziraatkatilim.com.tr/\'],[\'Vakıf Katılım\',\'https://www.vakifkatilim.com.tr/\'],[\'Emlak Katılım\',\'https://www.emlakkatilim.com.tr/\'],[\'Fibabanka\',\'https://www.fibabanka.com.tr/\'],[\'HSBC\',\'https://www.hsbc.com.tr/\'],[\'ICBC Turkey\',\'https://www.icbc.com.tr/\']];\nlet bankOffers=[];\nfunction initBanking(){const box=$(\'bankOfficialLinks\');if(box&&!box.dataset.ready){box.innerHTML=BANK_OFFICIAL_LINKS.map(([n,u])=>`<div class="item"><a href="${u}" target="_blank" rel="noopener noreferrer">${esc(n)} resmî sitesi ↗</a></div>`).join(\'\');box.dataset.ready=\'1\';}const d=$(\'compareDate\');if(d&&!d.value)d.value=new Date().toISOString().slice(0,10);}\nfunction calculateLoan(){const p=Number($(\'loanAmount\').value),annual=Number($(\'loanRate\').value),n=Math.floor(Number($(\'loanMonths\').value)),fees=Math.max(0,Number($(\'loanFees\').value)||0);if(!(p>0)||!(n>0)||n>360||annual<0)return alert(\'Kredi tutarı, faiz ve vade bilgilerini kontrol edin.\');const r=annual/100;const payment=r===0?p/n:p*r/(1-Math.pow(1+r,-n));const total=payment*n+fees;const interest=payment*n-p; $(\'loanResult\').innerHTML=`<b>${esc($(\'loanType\').value)}</b><div class="grid-2"><div class="stat">Aylık taksit<b>${fmt(payment)} TL</b></div><div class="stat">Toplam faiz<b>${fmt(interest)} TL</b></div><div class="stat">Toplam geri ödeme + masraf<b>${fmt(total)} TL</b></div><div class="stat">Vade<b>${n} ay</b></div></div>`;}\nfunction calculateDeposit(){const p=Number($(\'depositAmount\').value),rate=Number($(\'depositRate\').value),days=Math.floor(Number($(\'depositDays\').value)),taxRaw=$(\'depositTax\').value; if(!(p>0)||rate<0||!(days>0)||days>3660)return alert(\'Tutar, oran ve vade bilgilerini kontrol edin.\');const gross=p*(rate/100)*days/365;const tax=taxRaw===\'\'?null:Math.max(0,Math.min(100,Number(taxRaw)));const withholding=tax===null?null:gross*tax/100;const net=withholding===null?null:gross-withholding; $(\'depositResult\').innerHTML=`<div class="grid-2"><div class="stat">Brüt getiri<b>${fmt(gross)} TL</b></div><div class="stat">Stopaj kesintisi<b>${withholding===null?\'Stopaj girilmedi\':fmt(withholding)+\' TL\'}</b></div><div class="stat">Net getiri<b>${net===null?\'Hesaplanmadı\':fmt(net)+\' TL\'}</b></div><div class="stat">Vade sonu toplam<b>${net===null?\'Stopaj bekleniyor\':fmt(p+net)+\' TL\'}</b></div></div><small>Basit faiz yaklaşımıyla hesaplandı; bileşik faiz, gün sayım yöntemi ve ürün koşulları farklı olabilir.</small>`;}\nfunction addBankOffer(){const bank=$(\'bankName\').value,rate=Number($(\'compareRate\').value),date=$(\'compareDate\').value,note=$(\'compareNote\').value.trim();if(!Number.isFinite(rate)||rate<0||!date)return alert(\'Geçerli teklif faizi ve teklif tarihi girin.\');bankOffers.push({bank,rate,date,note});renderBankOffers();$(\'compareRate\').value=\'\';$(\'compareNote\').value=\'\';}\nfunction renderBankOffers(){const el=$(\'bankOfferList\');if(!el)return;if(!bankOffers.length){el.textContent=\'Henüz teklif eklenmedi.\';return;}const sorted=[...bankOffers].sort((a,b)=>a.rate-b.rate);el.innerHTML=sorted.map((x,i)=>`<div class="item"><b>${i+1}. ${esc(x.bank)}</b> • Aylık faiz: <b>%${fmt(x.rate)}</b><div class="muted">Teklif tarihi: ${esc(x.date)} • ${esc(x.note||\'Not yok\')}</div><button class="action danger" onclick="removeBankOffer(${bankOffers.indexOf(x)})">Kaldır</button></div>`).join(\'\')+\'<p class="muted">Sıralama yalnızca girilen aylık faiz oranına göredir; masraf, sigorta ve kampanya şartları dahil değildir.</p>\';}\nfunction removeBankOffer(i){bankOffers.splice(i,1);renderBankOffers();}function clearBankOffers(){if(confirm(\'Karşılaştırma listesindeki tüm teklifler silinsin mi?\')){bankOffers=[];renderBankOffers();}}\nfunction switchPage(id){document.querySelectorAll(\'.page\').forEach(x=>x.classList.remove(\'active\'));$(id).classList.add(\'active\');if(id===\'hunters\')startMarkets();if(token)loadPageData(id).catch(showError);else if(id===\'supports\')loadSupports();else if(id===\'bist\')loadBistQuotes()}function openAnimalHealthPlanner(){switchPage(\'animals\');setTimeout(()=>{const panel=$(\'animalHealthPlanner\');if(panel){panel.scrollIntoView({behavior:\'smooth\',block:\'start\'});if(token){ahLoadAnimals();ahLoadPlans()}}},120)}function showError(e){alert(typeof e?.message===\'string\'?e.message:\'Beklenmeyen bir hata oluştu. Lütfen tekrar deneyin.\')}\nfunction setAuthMode(mode){const loginMode=mode!==\'register\';$(\'authLoginPanel\').style.display=loginMode?\'block\':\'none\';$(\'authRegisterPanel\').style.display=loginMode?\'none\':\'block\';$(\'authLoginTab\').classList.toggle(\'active\',loginMode);$(\'authRegisterTab\').classList.toggle(\'active\',!loginMode)}async function register(){const username=$(\'regUsername\').value.trim(),password=$(\'regPassword\').value,full_name=$(\'regFullName\').value.trim(),birthplace=$(\'regBirthplace\').value.trim(),phone=$(\'regPhone\').value.trim();if(username.length<3||username.length>40)return alert(\'Kullanıcı adı 3–40 karakter olmalıdır.\');if(password.length<8||password.length>128)return alert(\'Şifre 8–128 karakter olmalıdır.\');if(!full_name||!birthplace||!phone)return alert(\'Gerçek ad-soyad, doğum yeri ve telefon numarası zorunludur.\');try{const d=await post(\'/api/register\',{username,password,full_name,birthplace,phone});alert(d.message+\' Şimdi Üye Girişi sekmesinden hesabınıza giriş yapabilirsiniz.\');$(\'loginUsername\').value=username;$(\'loginPassword\').value=password;setAuthMode(\'login\')}catch(e){showError(e)}}\nasync function requestRecovery(){const identifier=$(\'loginUsername\').value.trim();if(!identifier)return alert(\'Kullanıcı adınızı veya kayıtlı telefon numaranızı yazın.\');try{alert((await post(\'/api/password/recovery\',{identifier})).message)}catch(e){showError(e)}}async function updateMyProfile(){if(!token)return alert(\'Önce hesabınıza giriş yapın.\');const full_name=$(\'myFullName\').value.trim(),birthplace=$(\'myBirthplace\').value.trim(),phone=$(\'myPhone\').value.trim();if(!full_name||!birthplace||!phone)return alert(\'Ad-soyad, doğum yeri ve telefon zorunludur.\');try{alert((await api(\'/api/me/profile\',{method:\'PUT\',body:JSON.stringify({full_name,birthplace,phone})})).message)}catch(e){showError(e)}}\nasync function login(){try{let d=await post(\'/api/login\',{username:$(\'loginUsername\').value.trim(),password:$(\'loginPassword\').value});token=d.token;localStorage.setItem(\'feyzullah_token\',token);localStorage.setItem(\'feyzullah_username\',d.username);$(\'authBtn\').textContent=\'👤 \'+d.username;closeAuthModal();isAdmin=d.role===\'admin\';applyPermissions();alert(\'Giriş başarılı. \'+(isAdmin?\'Yönetici yetkisi.\':\'Görüntüleme yetkisi.\'));try{await syncAccount();await loadAll();await loadBackups();await loadHomePreferences();await v31LoadWidgets()}catch(postLoginError){console.warn(\'Giriş tamamlandı; bazı ekran verileri yüklenemedi:\',postLoginError.message)}}catch(e){showError(e)}}async function logout(){try{await api(\'/api/logout\',{method:\'POST\'})}catch{}token=\'\';isAdmin=false;applyPermissions();localStorage.removeItem(\'feyzullah_token\');localStorage.removeItem(\'feyzullah_username\');$(\'authBtn\').textContent=\'🔐 Üye Girişi / Kayıt\';document.querySelectorAll(\'[id$="List"]\').forEach(x=>x.textContent=\'Giriş yapın.\');closeAuthModal()}\nasync function addAnimal(){try{let d=await post(\'/api/animals\',{name:$(\'aName\').value.trim(),age:$(\'aAge\').value,birthCount:$(\'aBirthCount\').value,dueDate:$(\'aDue\').value||\'Belirtilmedi\',tag_no:$(\'aTag\').value,species:$(\'aSpecies\').value,sex:$(\'aSex\').value,breed:$(\'aBreed\').value,birth_date:$(\'aBirthDate\').value,purchase_date:$(\'aPurchaseDate\').value,purchase_price:+$(\'aPurchasePrice\').value,location:$(\'aLocation\').value,notes:$(\'aNotes\').value});alert(d.message);await loadAnimals()}catch(e){showError(e)}}async function loadAnimals(){let a=await api(\'/api/animals\');$(\'animalList\').innerHTML=a.length?a.map(x=>`<div class="item"><b>#${x.id} ${esc(x.name)}</b> <span class="tag">${esc(x.species)}</span><span class="tag">${esc(x.status)}</span><br>Küpe: ${esc(x.tag_no||\'—\')} • ${esc(x.sex||\'\')} • ${esc(x.breed||\'\')}<br>Yaş: ${esc(x.age)} • Doğum: ${esc(x.birth_count)} • Beklenen: ${esc(x.due_date)}<div class="actions"><button class="action" onclick="showAnimalHistory(${x.id})">Geçmiş</button><button class="action danger" onclick="archiveAnimal(${x.id})">Arşivle</button></div></div>`).join(\'\'):\'Aktif hayvan yok.\';$(\'eventAnimalId\').placeholder=\'Hayvan ID\';if($(\'ahAnimal\')){await ahLoadAnimals();await ahLoadPlans()}}async function archiveAnimal(id){if(!confirm(\'Hayvan arşive taşınacak; kayıt silinmeyecek. Devam?\'))return;try{await api(\'/api/animals/\'+id,{method:\'DELETE\'});await loadAnimals()}catch(e){showError(e)}}async function addAnimalEvent(){try{let id=+$(\'eventAnimalId\').value;if(!id)throw Error(\'Hayvan ID girin.\');let d=await post(`/api/animals/${id}/events`,{event_type:$(\'eventType\').value,event_date:$(\'eventDate\').value||new Date().toISOString().slice(0,10),description:$(\'eventDescription\').value,amount:+$(\'eventAmount\').value});alert(d.message);await loadAnimals()}catch(e){showError(e)}}async function showAnimalHistory(id){try{let [ev,h]=await Promise.all([api(`/api/animals/${id}/events`),api(`/api/animals/${id}/health`)]);alert(\'HAYVAN #\'+id+\'\\n\\nİŞLEMLER\\n\'+ev.map(x=>x.event_date+\' \'+x.event_type+\': \'+x.description).join(\'\\n\')+\'\\n\\nSAĞLIK\\n\'+h.map(x=>x.record_date+\' \'+x.record_type+\': \'+x.description).join(\'\\n\'))}catch(e){showError(e)}}\nasync function addFeed(){try{await post(\'/api/feeds\',{name:$(\'feedName\').value,supplier:$(\'feedSupplier\').value,price:+$(\'feedPrice\').value});await loadFeeds()}catch(e){showError(e)}}async function loadFeeds(){let a=await api(\'/api/feeds\');$(\'feedList\').innerHTML=a.map(x=>`<div class="item">${esc(x.name)} • ${esc(x.supplier)} • <b>${fmt(x.price)} TL</b></div>`).join(\'\')||\'Kayıt yok.\'}async function addStock(){try{await post(\'/api/stock\',{name:$(\'stockName\').value,category:$(\'stockCategory\').value,unit:$(\'stockUnit\').value,quantity:+$(\'stockQty\').value,min_quantity:+$(\'stockMin\').value,unit_cost:+$(\'stockCost\').value,supplier:$(\'stockSupplier\').value});await loadStock()}catch(e){showError(e)}}async function loadStock(){let a=await api(\'/api/stock\');$(\'stockList\').innerHTML=a.map(x=>`<div class="item">#${x.id} <b>${esc(x.name)}</b> • ${fmt(x.quantity)} ${esc(x.unit)} • Min ${fmt(x.min_quantity)} ${x.quantity<=x.min_quantity?\'<span class="red">STOK AZ</span>\':\'\'}</div>`).join(\'\')||\'Stok kaydı yok.\'}async function stockMove(){try{await post(\'/api/stock/move\',{item_id:+$(\'moveItem\').value,move_type:$(\'moveType\').value,quantity:+$(\'moveQty\').value,unit_cost:+$(\'moveCost\').value,description:$(\'moveDesc\').value});await loadStock();alert(\'Stok hareketi kaydedildi.\')}catch(e){showError(e)}}\nasync function addTransaction(){try{await post(\'/api/transactions\',{kind:$(\'txKind\').value,category:$(\'txCategory\').value,amount:+$(\'txAmount\').value,currency:$(\'txCurrency\').value,transaction_date:$(\'txDate\').value||new Date().toISOString().slice(0,10),description:$(\'txDescription\').value});await loadAccounting()}catch(e){showError(e)}}async function loadAccounting(){let [s,t]=await Promise.all([api(\'/api/accounting/summary\'),api(\'/api/transactions\')]);$(\'accountSummary\').innerHTML=`<div class="stat">Gelir<b>${fmt(s.income)} TL</b></div><div class="stat">Gider<b>${fmt(s.expense)} TL</b></div><div class="stat">Net<b>${fmt(s.profit)} TL</b></div>`;$(\'txList\').innerHTML=t.map(x=>`<div class="item">${esc(x.transaction_date)} • ${esc(x.category)} • <b class="${x.kind===\'income\'?\'green\':\'red\'}">${x.kind===\'income\'?\'+\':\'-\'}${fmt(x.amount)} ${esc(x.currency)}</b><br>${esc(x.description)}</div>`).join(\'\')}\nasync function addMilk(){try{await post(\'/api/milk\',{animal_id:+$(\'milkAnimal\').value||null,record_date:$(\'milkDate\').value||new Date().toISOString().slice(0,10),liters:+$(\'milkLiters\').value,unit_price:+$(\'milkPrice\').value,buyer:$(\'milkBuyer\').value,notes:$(\'milkNotes\').value});await loadMilk()}catch(e){showError(e)}}async function loadMilk(){let a=await api(\'/api/milk\');$(\'milkList\').innerHTML=a.map(x=>`<div class="item">${x.record_date} • ${fmt(x.liters)} L • ${esc(x.animal_name||\'Genel\')} • ${esc(x.buyer)}</div>`).join(\'\')||\'Süt kaydı yok.\'}\nasync function addTask(){try{await post(\'/api/tasks\',{title:$(\'taskTitle\').value,task_date:$(\'taskDate\').value||new Date().toISOString().slice(0,10),category:$(\'taskCategory\').value,notes:$(\'taskNotes\').value});await loadTasks()}catch(e){showError(e)}}async function loadTasks(){let a=await api(\'/api/tasks\');$(\'taskList\').innerHTML=a.map(x=>`<div class="item">${x.task_date} • <b>${esc(x.title)}</b> • ${esc(x.category)} • ${esc(x.status)}</div>`).join(\'\')||\'Görev yok.\'}async function addEquipment(){try{await post(\'/api/equipment\',{name:$(\'eqName\').value,kind:$(\'eqKind\').value,last_service:$(\'eqLast\').value,next_service:$(\'eqNext\').value,notes:$(\'eqNotes\').value});await loadEquipment()}catch(e){showError(e)}}async function loadEquipment(){let a=await api(\'/api/equipment\');$(\'equipmentList\').innerHTML=a.map(x=>`<div class="item"><b>${esc(x.name)}</b> • ${esc(x.kind)} • Sonraki bakım: ${esc(x.next_service)}</div>`).join(\'\')||\'Ekipman yok.\'}async function addField(){try{await post(\'/api/fields\',{name:$(\'fieldName\').value,crop:$(\'fieldCrop\').value,area:+$(\'fieldArea\').value,unit:$(\'fieldUnit\').value,plant_date:$(\'fieldPlant\').value,harvest_date:$(\'fieldHarvest\').value,notes:$(\'fieldNotes\').value});await loadFields()}catch(e){showError(e)}}async function loadFields(){let a=await api(\'/api/fields\');$(\'fieldList\').innerHTML=a.map(x=>`<div class="item"><b>${esc(x.name)}</b> • ${esc(x.crop)} • ${fmt(x.area)} ${esc(x.unit)} • Ekim ${esc(x.plant_date)} • Hasat ${esc(x.harvest_date)}</div>`).join(\'\')||\'Tarla/sera kaydı yok.\'}\n// GPS parcel measurement: local live tracking, server-side persistence after explicit save.\nlet measureWatchId=null,measurePoints=[],measureLastFix=null,measureResult=null,measureSaved=false;\nfunction measureDistance(a,b){const R=6371000,toRad=x=>x*Math.PI/180,lat1=toRad(a.lat),lat2=toRad(b.lat),dLat=lat2-lat1,dLng=toRad(b.lng-a.lng);const h=Math.sin(dLat/2)**2+Math.cos(lat1)*Math.cos(lat2)*Math.sin(dLng/2)**2;return 2*R*Math.asin(Math.sqrt(h))}\nfunction calculateMeasure(){if(measurePoints.length<3){$(\'measureArea\').textContent=\'En az 3 nokta\';$(\'measureDekar\').textContent=\'—\';$(\'measurePerimeter\').textContent=\'—\';$(\'measureHectare\').textContent=\'—\';measureResult=null;$(\'measureSaveBtn\').disabled=true;return}const R=6378137,lat0=measurePoints.reduce((a,p)=>a+p.lat,0)/measurePoints.length*Math.PI/180,lng0=measurePoints.reduce((a,p)=>a+p.lng,0)/measurePoints.length*Math.PI/180;const xy=measurePoints.map(p=>({x:R*(p.lng*Math.PI/180-lng0)*Math.cos(lat0),y:R*(p.lat*Math.PI/180-lat0)}));let twice=0,per=0;for(let i=0;i<xy.length;i++){let a=xy[i],b=xy[(i+1)%xy.length];twice+=a.x*b.y-b.x*a.y;per+=measureDistance(measurePoints[i],measurePoints[(i+1)%measurePoints.length])}let area=Math.abs(twice)/2;if(!Number.isFinite(area)||!Number.isFinite(per)||area<1||per<3){measureResult=null;$(\'measureArea\').textContent=\'Geçersiz sınır\';$(\'measureDekar\').textContent=\'—\';$(\'measurePerimeter\').textContent=\'—\';$(\'measureHectare\').textContent=\'—\';$(\'measureSaveBtn\').disabled=true;$(\'measureStatus\').textContent=\'Noktalar geçerli bir parsel alanı oluşturmuyor. Sınırı yeniden dolaş.\';return}measureResult={area_m2:area,perimeter_m:per};$(\'measureArea\').textContent=fmt(area);$(\'measureDekar\').textContent=fmt(area/1000);$(\'measurePerimeter\').textContent=fmt(per)+\' m\';$(\'measureHectare\').textContent=fmt(area/10000)+\' ha\';$(\'measureSaveBtn\').disabled=false}\nfunction updateMeasureCount(){$(\'measurePoints\').textContent=measurePoints.length;let avg=measurePoints.length?measurePoints.reduce((n,p)=>n+(Number(p.accuracy)||0),0)/measurePoints.length:0;let el=$(\'measureAvgAccuracy\');if(el)el.textContent=measurePoints.length?\'±\'+Math.round(avg)+\' m\':\'—\'}\nfunction startFieldMeasure(){if(!navigator.geolocation){$(\'measureStatus\').textContent=\'Bu tarayıcı GPS konumunu desteklemiyor.\';return}if(measureWatchId!==null)return;measureResult=null;measureSaved=false;$(\'measureSaveBtn\').disabled=true;$(\'measureStatus\').textContent=\'GPS konumu bekleniyor. Konum izni sorulursa İzin ver.\';$(\'measureStartBtn\').textContent=\'● Ölçüm sürüyor\';measureWatchId=navigator.geolocation.watchPosition(pos=>{let c=pos.coords,accuracy=c.accuracy||999;$(\'measureAccuracy\').textContent=\'±\'+Math.round(accuracy)+\' m\';if(accuracy>30){$(\'measureStatus\').textContent=\'GPS doğruluğu düşük (±\'+Math.round(accuracy)+\' m). Daha açık bir yere geç; bu nokta kaydedilmedi.\';return}let pt={lat:c.latitude,lng:c.longitude,accuracy:accuracy,at:new Date(pos.timestamp).toISOString()};if(measureLastFix&&measureDistance(measureLastFix,pt)<2.5)return;if(measureLastFix&&measureDistance(measureLastFix,pt)>100){$(\'measureStatus\').textContent=\'Konumda ani sıçrama algılandı; bu nokta atlandı.\';return}measurePoints.push(pt);measureLastFix=pt;updateMeasureCount();$(\'measureStatus\').textContent=\'Ölçüm devam ediyor • Son nokta doğruluğu ±\'+Math.round(accuracy)+\' m\';if(measurePoints.length>=3)calculateMeasure()},err=>{$(\'measureStatus\').textContent=err.code===1?\'Konum izni reddedildi. Telefon ayarlarından tarayıcıya konum izni ver.\':err.code===2?\'GPS konumu alınamadı. Konum/GPS ayarını aç.\':\'GPS zaman aşımına uğradı. Açık alanda tekrar dene.\';stopFieldMeasure()},{enableHighAccuracy:true,maximumAge:1000,timeout:20000})}\nfunction stopFieldMeasure(){if(measureWatchId!==null){navigator.geolocation.clearWatch(measureWatchId);measureWatchId=null}$(\'measureStartBtn\').textContent=\'▶ Ölçüme Devam Et\';if(measurePoints.length>=3){calculateMeasure();$(\'measureStatus\').textContent=\'Ölçüm durduruldu. Alanı kontrol edip kaydedebilirsin.\'}else if(measurePoints.length)$(\'measureStatus\').textContent=\'Ölçüm durduruldu. Alan için en az 3 nokta gerekir.\'}\nfunction finishFieldMeasure(){stopFieldMeasure();calculateMeasure()}\nfunction undoMeasurePoint(){measurePoints.pop();measureLastFix=measurePoints[measurePoints.length-1]||null;updateMeasureCount();calculateMeasure();$(\'measureStatus\').textContent=\'Son GPS noktası geri alındı.\'}\nfunction clearFieldMeasure(){stopFieldMeasure();measurePoints=[];measureLastFix=null;measureResult=null;measureSaved=false;updateMeasureCount();$(\'measureAccuracy\').textContent=\'—\';if($(\'measureAvgAccuracy\'))$(\'measureAvgAccuracy\').textContent=\'—\';$(\'measureArea\').textContent=\'—\';$(\'measureDekar\').textContent=\'—\';$(\'measurePerimeter\').textContent=\'—\';$(\'measureHectare\').textContent=\'—\';$(\'measureSaveBtn\').disabled=true;$(\'measureStatus\').textContent=\'Ölçüm temizlendi.\'}\nasync function saveFieldMeasure(){if(!measureResult||measurePoints.length<3){showError(Error(\'Önce en az 3 GPS noktasıyla alanı hesapla.\'));return}if(measureSaved){$(\'measureStatus\').textContent=\'Bu ölçüm zaten kaydedildi. Yeni ölçüm için Temizle düğmesini kullan.\';return}let name=$(\'measureName\').value.trim()||(\'Parsel \'+new Date().toLocaleDateString(\'tr-TR\'));try{await post(\'/api/field-measurements\',{name,area_m2:measureResult.area_m2,perimeter_m:measureResult.perimeter_m,points:measurePoints});measureSaved=true;$(\'measureSaveBtn\').disabled=true;$(\'measureStatus\').textContent=\'Parsel ölçümü hesabına kaydedildi.\';await loadSavedMeasurements()}catch(e){showError(e)}}\nasync function loadSavedMeasurements(){if(!token)return;try{let a=await api(\'/api/field-measurements\');$(\'savedMeasurements\').innerHTML=\'<h4>📋 Kaydedilen Parsel Ölçümleri</h4>\'+a.map(x=>`<div class="item"><b>${esc(x.name)}</b> • ${fmt(x.area_dekar)} dönüm (${fmt(x.area_m2)} m²) • Çevre ${fmt(x.perimeter_m)} m <button class="action danger" onclick="deleteFieldMeasurement(${x.id})">Sil</button></div>`).join(\'\')||\'<div class="muted">Henüz kayıtlı ölçüm yok.</div>\'}catch(e){$(\'savedMeasurements\').textContent=e.message}}\nasync function deleteFieldMeasurement(id){if(!confirm(\'Bu parsel ölçümü silinsin mi?\'))return;try{await api(\'/api/field-measurements/\'+id,{method:\'DELETE\'});await loadSavedMeasurements()}catch(e){showError(e)}}\nasync function createBackup(){try{let d=await post(\'/api/backup\',{});alert(d.message);await loadBackups()}catch(e){showError(e)}}async function loadBackups(){if(!token)return;try{let a=await api(\'/api/backups\');$(\'backupList\').innerHTML=a.map(x=>`<div class="item">${esc(x.filename)} • ${(x.size/1024).toFixed(1)} KB • <a style="color:#4ade80" href="/api/backups/${encodeURIComponent(x.filename)}" onclick="return downloadBackup(event,this)">İndir</a></div>`).join(\'\')||\'Henüz yedek yok.\'}catch(e){$(\'backupList\').textContent=e.message}}async function downloadBackup(ev,el){ev.preventDefault();try{let r=await fetch(el.href,{headers:token?{\'Authorization\':\'Bearer \'+token}:{}});if(!r.ok)throw Error(\'Yedek indirilemedi\');let b=await r.blob(),a=document.createElement(\'a\'),u=URL.createObjectURL(b);a.href=u;a.download=decodeURIComponent(el.href.split(\'/\').pop());a.click();URL.revokeObjectURL(u)}catch(e){showError(e)}return false}async function restoreBackup(){let f=$(\'restoreFile\').files[0];if(!f){showError(Error(\'Önce yedek dosyasını seçin.\'));return}if(!confirm(\'Mevcut veritabanı güvenlik kopyası alınarak seçilen yedek geri yüklenecek. Devam edilsin mi?\'))return;try{let fd=new FormData();fd.append(\'file\',f);let r=await fetch(\'/api/backup/restore\',{method:\'POST\',headers:token?{\'Authorization\':\'Bearer \'+token}:{},body:fd}),d=await r.json();if(!r.ok)throw Error(d.detail||\'Geri yükleme başarısız\');alert(d.message);location.reload()}catch(e){showError(e)}}\nasync function loadConsumptionReport(){try{let d=await api(\'/api/reports/consumption?days=30\');$(\'consumptionReport\').innerHTML=`<div class="item"><b>Son ${d.days} gün toplam tüketim maliyeti: ${fmt(d.total_cost)} TL</b></div>`+d.items.map(x=>`<div class="item">${esc(x.name)} • ${fmt(x.quantity)} ${esc(x.unit)} • <b>${fmt(x.cost)} TL</b></div>`).join(\'\')}catch(e){showError(e)}}\nasync function addPortfolioTrade(){try{let d=await post(\'/api/portfolio/trades\',{symbol:$(\'portfolioSymbol\').value.trim().toUpperCase(),side:$(\'portfolioSide\').value,quantity:+$(\'portfolioQty\').value,price:+$(\'portfolioPrice\').value,fee:+$(\'portfolioFee\').value||0});alert(d.message);await loadPortfolio()}catch(e){showError(e)}}async function loadPortfolio(){if(!token)return;try{let d=await api(\'/api/portfolio\');$(\'portfolioList\').innerHTML=d.holdings.map(x=>`<div class="item"><b>${esc(x.symbol)}</b> • ${fmt(x.quantity)} adet • Ortalama maliyet ${fmt(x.average_buy_cost)} • Son fiyat ${x.last_price===null?\'Veri yok\':fmt(x.last_price)} USDT • Değer ${x.market_value===null?\'—\':fmt(x.market_value)+\' USDT\'} • K/Z ${x.unrealized_pnl===null?\'—\':fmt(x.unrealized_pnl)+\' USDT\'}</div>`).join(\'\')||\'Sanal portföy boş.\'}catch(e){$(\'portfolioList\').textContent=e.message}}\nasync function loadCropCalendar(){if(!token)return;try{let [items,catalog,fields]=await Promise.all([api(\'/api/crops/calendar\'),api(\'/api/crops/catalog\'),api(\'/api/fields\')]);cropCatalogCache=catalog;$(\'cropOptions\').innerHTML=catalog.map(x=>`<option value="${esc(x.name)}">${esc(x.category)} • yaklaşık ${x.indicative_days} gün</option>`).join(\'\');let render=(x)=>{let now=Date.now(),sow=x.sow_date?new Date(x.sow_date+\'T00:00:00\').getTime():null,hs=x.harvest_start?new Date(x.harvest_start+\'T00:00:00\').getTime():null,he=x.harvest_end?new Date(x.harvest_end+\'T00:00:00\').getTime():null;return `<div class="item crop-timer" data-sow="${sow||\'\'}" data-harvest="${hs||\'\'}" data-end="${he||\'\'}"><b>${esc(x.crop)}</b> ${x.variety?\'• \'+esc(x.variety):\'\'}<span class="tag">${esc(x.method)}</span><span class="tag">${esc(x.region||\'Bölge belirtilmedi\')}</span><div class="muted">Ekim: ${esc(x.sow_date)} ${x.transplant_date?\'• Dikim: \'+esc(x.transplant_date):\'\'} • Hasat: ${esc(x.harvest_start||\'Belirtilmedi\')} – ${esc(x.harvest_end||\'Belirtilmedi\')}</div><div class="green timer-sow">Ekim zamanı: —</div><div class="green timer-harvest">Hasada kalan: —</div><button class="action danger" onclick="deleteCropCalendar(${x.id})">Takvimden kaldır</button></div>`};let oldFields=fields.map(x=>({id:\'field\'+x.id,crop:x.crop||x.name,variety:\'Mevcut tarla/sera kaydı\',method:\'Tarla / sera\',region:\'Kayıtlı alan\',sow_date:x.plant_date||\'\',harvest_start:x.harvest_date||\'\',harvest_end:\'\',transplant_date:\'\',notes:x.notes||\'\'}));let all=[...items,...oldFields];$(\'cropCalendarList\').innerHTML=all.map(x=>x.id&&String(x.id).startsWith(\'field\')?`<div class="item crop-timer" data-sow="${x.sow_date?new Date(x.sow_date+\'T00:00:00\').getTime():\'\'}" data-harvest="${x.harvest_start?new Date(x.harvest_start+\'T00:00:00\').getTime():\'\'}" data-end=""><b>${esc(x.crop)}</b><span class="tag">${x.variety}</span><div class="muted">Ekim: ${esc(x.sow_date||\'—\')} • Hasat: ${esc(x.harvest_start||\'—\')}</div><div class="green timer-sow">Ekim geri sayımı: —</div><div class="green timer-harvest">Hasat geri sayımı: —</div></div>`:render(x)).join(\'\')||\'Henüz ekim/hasat takvimi yok.\';updateCropCountdowns();if(!cropCountdownTimer)cropCountdownTimer=setInterval(updateCropCountdowns,1000);if($(\'autoCropPlannerList\')&&$(\'cropAutoRegion\')?.value&&$(\'cropAutoLocation\')?.value)await loadAutoCropPlanner()}catch(e){$(\'cropCalendarList\').textContent=e.message}}\nasync function loadAutoCropPlanner(){let region=$(\'cropAutoRegion\')?.value.trim()||\'\',location=$(\'cropAutoLocation\')?.value.trim()||\'\',list=$(\'autoCropPlannerList\'),result=$(\'autoCropResult\'),save=$(\'autoCropSaveAllBtn\');if(!region||!location){autoCropPlanCache=[];if(list)list.innerHTML=\'\';if(result)result.textContent=\'Ekim sayacını başlatmak için bölge ve il seçin.\';if(save)save.disabled=true;return}try{if(result)result.textContent=\'Bütün ürünlerin tarihleri hesaplanıyor...\';autoCropPlanCache=await api(\'/api/crops/auto-plan?region=\'+encodeURIComponent(region)+\'&location=\'+encodeURIComponent(location));if(list)list.innerHTML=autoCropPlanCache.map((x,i)=>{let sow=Date.parse(x.sow_date+\'T00:00:00\'),harvest=Date.parse(x.harvest_date+\'T00:00:00\');return `<div class="item auto-crop-timer" data-sow="${sow}" data-harvest="${harvest}"><b>${i+1}. ${esc(x.name)}</b> <span class="tag">${esc(x.category)}</span><div class="muted">${esc(x.region)} • Ekim: <b>${esc(x.sow_date)}</b> • Tahmini hasat: <b>${esc(x.harvest_date)}</b></div><div class="green timer-sow">Ekime kalan: —</div><div class="green timer-harvest">Hasada kalan: —</div></div>`}).join(\'\');if(result)result.textContent=autoCropPlanCache.length+\' ürün listelendi; bütün sayaçlar otomatik başladı.\';if(save)save.disabled=!autoCropPlanCache.length;updateCropCountdowns()}catch(e){if(result)result.textContent=e.message}}\nfunction showSelectedAutoCrop(){const value=$(\'autoCropProduct\')?.value;if(value===\'\')return;const card=document.querySelectorAll(\'.auto-crop-timer\')[Number(value)];if(card)card.scrollIntoView({behavior:\'smooth\',block:\'center\'})}\nasync function saveSelectedAutoCrop(){let v=$(\'autoCropProduct\')?.value;if(v===\'\')return;let i=Number(v),x=autoCropPlanCache[i];if(!x){showError(Error(\'Önce ürün seçin.\'));return}try{await post(\'/api/crops/calendar\',{crop:x.name,variety:\'Otomatik plan\',method:x.method,region:x.region,sow_date:x.sow_date,transplant_date:\'\',harvest_start:x.harvest_date,harvest_end:\'\',notes:\'Bölge ve il seçimine göre otomatik yaklaşık ekim önerisi; yerel koşullara göre kontrol edin.\'});alert(x.name+\' takvime kaydedildi.\');await loadCropCalendar()}catch(e){showError(e)}}\nasync function saveAllAutoCropPlans(){try{let region=$(\'cropAutoRegion\').value.trim(),location=$(\'cropAutoLocation\').value.trim();if(!region||!location)throw Error(\'Önce bölge ve il/konum seçin.\');let d=await api(\'/api/crops/auto-plan/save?region=\'+encodeURIComponent(region)+\'&location=\'+encodeURIComponent(location),{method:\'POST\'});alert(d.message);await loadCropCalendar()}catch(e){showError(e)}}\nfunction estimateHarvestFromSow(){let name=$(\'cropName\').value.trim().toLocaleLowerCase(\'tr-TR\'),item=cropCatalogCache.find(x=>x.name.toLocaleLowerCase(\'tr-TR\')===name),sow=$(\'cropSow\').value;if(!item||!sow||$(\'cropHarvestStart\').value)return;let d=new Date(sow+\'T12:00:00\');d.setDate(d.getDate()+item.indicative_days);$(\'cropHarvestStart\').value=d.toISOString().slice(0,10)}\nfunction countdownText(ms){if(ms===null||!Number.isFinite(ms))return \'Tarih girilmedi\';if(ms<=0)return \'Tarih geldi / geçti\';let sec=Math.floor(ms/1000),d=Math.floor(sec/86400);sec%=86400;let h=Math.floor(sec/3600);sec%=3600;let m=Math.floor(sec/60);sec%=60;return `${d} gün ${h} saat ${m} dk ${sec} sn`}\nfunction updateCropCountdowns(){document.querySelectorAll(\'.crop-timer,.auto-crop-timer\').forEach(el=>{let now=Date.now(),s=el.dataset.sow?+el.dataset.sow:null,h=el.dataset.harvest?+el.dataset.harvest:null,e=el.dataset.end?+el.dataset.end:null;let sow=el.querySelector(\'.timer-sow\'),har=el.querySelector(\'.timer-harvest\');if(sow)sow.textContent=\'Ekim geri sayımı: \'+countdownText(s===null?null:s-now);if(har)har.textContent=\'İlk hasada kalan: \'+countdownText(h===null?null:h-now)+(e===null?\'\':\' • Son hasat: \'+countdownText(e-now))})}\nasync function addCropCalendar(){try{let d={crop:$(\'cropName\').value.trim(),variety:$(\'cropVariety\').value,method:$(\'cropMethod\').value,region:$(\'cropRegion\').value,sow_date:$(\'cropSow\').value,transplant_date:$(\'cropTransplant\').value,harvest_start:$(\'cropHarvestStart\').value,harvest_end:$(\'cropHarvestEnd\').value,notes:$(\'cropNotes\').value};if(!d.crop||!d.sow_date)throw Error(\'Ürün ve ekim tarihi zorunlu.\');await post(\'/api/crops/calendar\',d);$(\'cropName\').value=\'\';$(\'cropVariety\').value=\'\';$(\'cropNotes\').value=\'\';await loadCropCalendar()}catch(e){showError(e)}}async function deleteCropCalendar(id){if(!confirm(\'Bu takvim kaydı kaldırılsın mı?\'))return;try{await api(\'/api/crops/calendar/\'+id,{method:\'DELETE\'});await loadCropCalendar()}catch(e){showError(e)}}\nasync function addPriceAlert(){try{await post(\'/api/alerts\',{symbol:$(\'alertSymbol\').value.trim().toUpperCase(),condition:$(\'alertCondition\').value,target:+$(\'alertTarget\').value});$(\'alertSymbol\').value=\'\';$(\'alertTarget\').value=\'\';await loadPriceAlerts()}catch(e){showError(e)}}async function loadPriceAlerts(){if(!token)return;try{let [a,h]=await Promise.all([api(\'/api/alerts\'),api(\'/api/alerts/history\')]);$(\'priceAlertList\').innerHTML=a.map(x=>`<div class="item">${esc(x.symbol)} • ${x.condition===\'above\'?\'Üstü\':\'Altı\'} ${fmt(x.target)} • ${x.active?\'Aktif\':\'Pasif\'} <button class="action danger" onclick="deletePriceAlert(${x.id})">Sil</button></div>`).join(\'\')||\'Alarm yok.\';$(\'alertHistory\').innerHTML=h.map(x=>`<div class="item">${esc(x.triggered_at)} • ${esc(x.symbol)} • ${fmt(x.observed)} (hedef ${fmt(x.target)})</div>`).join(\'\')||\'Henüz tetiklenen alarm yok.\'}catch(e){console.warn(e)}}async function deletePriceAlert(id){try{await api(\'/api/alerts/\'+id,{method:\'DELETE\'});await loadPriceAlerts()}catch(e){showError(e)}}\nasync function checkPriceAlerts(){if(!token||!symbols.length)return;try{let alerts=await api(\'/api/alerts\');for(let a of alerts){let t=tickers.get(a.symbol);if(!t)continue;let price=+t.c,hit=a.condition===\'above\'?price>=a.target:price<=a.target;if(hit&&!alertLastState.has(a.id)){alertLastState.add(a.id);try{let result=await post(\'/api/alerts/\'+a.id+\'/trigger\',{observed:price});if(result.triggered){alert(result.message);await loadPriceAlerts()}}catch{}}if(!hit)alertLastState.delete(a.id)}}catch{}}\nasync function loadDashboard(){let d=await api(\'/api/dashboard\');$(\'dashboard\').innerHTML=`<div class="stat">Aktif hayvan<b>${d.animals.Aktif||0}</b><small>Ölü: ${d.animals[\'Ölü\']||0} • Satıldı: ${d.animals.Satıldı||0}</small></div><div class="stat">Stok uyarısı<b>${d.stock_alerts}</b></div><div class="stat">Net gelir<b>${fmt(d.profit)} TL</b><small>Gelir ${fmt(d.income)} / Gider ${fmt(d.expense)}</small></div><div class="stat">Stok kalemi<b>${d.stock_items}</b></div><div class="stat">Son 30 gün süt<b>${fmt(d.milk_last_30_days)} L</b></div><div class="stat">Aktif sürü<b>${d.animals.Aktif||0}</b></div>`}\n\nasync function loadSupports(){try{let d=await api(\'/api/supports\');let groups={plant:[],animal:[],credit:[],kosgeb:[]};d.items.forEach(x=>{if(x.category===\'Bitkisel üretim\')groups.plant.push(x);else if(x.category===\'Hayvancılık\'||x.category===\'Çiğ süt\')groups.animal.push(x);else if(x.category===\'Tarımsal krediler\')groups.credit.push(x);else groups.kosgeb.push(x)});const card=x=>{let deadline=x.deadline?new Date(x.deadline+\'T23:59:59\'):null;let left=deadline?Math.ceil((deadline-Date.now())/86400000):null;return `<div class="support-item"><h4>${esc(x.name)}</h4><div><b>${esc(x.amount)}</b> <span class="support-pill">${esc(x.period)}</span></div><div class="support-meta">Başvuru: ${esc(x.application)}</div><div class="support-meta">Ödeme: ${esc(x.payment)}</div><div class="support-meta">Tutar/ödeme doğrulaması: ${esc(x.verification_note||\'Resmî kaynak üzerinden kontrol edilmeli.\')}</div>${deadline?`<div class="support-deadline">Son başvuru: ${esc(x.deadline)} • ${left>=0?left+\' gün kaldı\':\'Süre geçti\'}</div>`:\'<div class="support-meta">Son başvuru tarihi: Resmî kaynakta belirtilmemiş / dönemsel kontrol edilmeli</div>\'}<div class="support-meta">${esc(x.note||\'\')}</div><div class="support-source"><a href="${esc(x.source_url)}" target="_blank" rel="noopener">Resmî kaynağı aç ↗</a> • Son kontrol: ${x.source_last_checked?new Date(x.source_last_checked).toLocaleString(\'tr-TR\'):\'Henüz yapılmadı\'}</div></div>`};$(\'supportPlant\').innerHTML=groups.plant.map(card).join(\'\');$(\'supportAnimal\').innerHTML=groups.animal.map(card).join(\'\');$(\'supportCredit\').innerHTML=groups.credit.map(card).join(\'\');$(\'supportKosgeb\').innerHTML=groups.kosgeb.map(card).join(\'\');$(\'supportSources\').innerHTML=d.sources.map(x=>`<div class="item"><b>${esc(x.name)}</b><div class="support-meta">${esc(x.category)} • ${x.status===\'ok\'?\'Erişilebilir\':x.status===\'error\'?\'Bağlantı hatası\':x.status===\'http_error\'?\'HTTP hatası\':\'Henüz kontrol edilmedi\'} ${x.content_changed_at?\'• İçerik değişikliği algılandı\':\'\'}</div><div class="support-meta">Son kontrol: ${x.last_checked?new Date(x.last_checked).toLocaleString(\'tr-TR\'):\'—\'}</div>${x.error?`<div class="support-meta">${esc(x.error)}</div>`:\'\'}${x.content_changed_at?`<div class="support-meta">Son içerik değişikliği algılandı: ${new Date(x.content_changed_at).toLocaleString(\'tr-TR\')} • Tutar ve ödeme tarihi için inceleme gerekli.</div>`:\'\'}${x.content_excerpt?`<details><summary>Resmî sayfa metin özeti (otomatik teyit değildir)</summary><div class="support-meta">${esc(String(x.content_excerpt).slice(0,700))}</div></details>`:\'\'}<a href="${esc(x.url)}" target="_blank" rel="noopener">Kaynağı aç ↗</a></div>`).join(\'\');$(\'supportStatus\').textContent=`${d.items.length} destek/kredi kaydı • Son veri oluşturma: ${new Date(d.generated_at).toLocaleString(\'tr-TR\')} • Sunucu kaynak kontrol aralığı: ${d.refresh_interval_hours} saat`;}catch(e){$(\'supportStatus\').textContent=\'Destek bilgileri alınamadı: \'+e.message}}\nasync function refreshSupportSources(b){if(!token)return;if(b){b.disabled=true;b.textContent=\'Kontrol ediliyor...\'}try{let r=await post(\'/api/supports/refresh\',{});await loadSupports();alert(\'Resmî kaynak kontrolü tamamlandı. Başarılı: \'+r.results.filter(x=>x.status===\'ok\').length+\' / \'+r.results.length+\' • İçerik değişikliği algılanan: \'+r.results.filter(x=>x.content_changed).length)}catch(e){showError(e)}finally{if(b){b.disabled=false;b.textContent=\'☁️ Resmî kaynakları şimdi kontrol et\'}}}\n\nasync function loadPageData(id){if(id===\'animals\')await loadAnimals();if(id===\'feed\'){await loadFeeds();await loadStock()}if(id===\'milk\')await loadMilk();if(id===\'accounting\')await loadAccounting();if(id===\'tasks\')await loadTasks();if(id===\'equipment\')await loadEquipment();if(id===\'farming\'){await loadFields();await loadCropCalendar();await loadSavedMeasurements()}if(id===\'hunters\'){await loadPriceAlerts()}if(id===\'finance\')await loadPortfolio();if(id===\'home\'){await loadDashboard();await loadCropCalendar();await loadBackups()}if(id===\'supports\')await loadSupports();if(id===\'bist\')await loadBistQuotes()}async function loadAll(){await Promise.all([loadAnimals(),loadFeeds(),loadStock(),loadDashboard()])}\nfunction calcRation(){let w=Math.max(1,+$(\'rationWeight\').value||600),t=$(\'rationType\').value,pct=t===\'Koyun\'?.03:t===\'Keçi\'?.035:.022;let dm=w*pct;$(\'rationResult\').innerHTML=`Yaklaşık kuru madde hedefi: <b>${dm.toFixed(1)} kg/gün</b>. Bu değer tam rasyon değildir; kaba yem, kesif yem, enerji, protein, mineral ve su ihtiyacı analiz edilmelidir.`}\nasync function fetchWeather(){let city=$(\'city\').value||localStorage.getItem(\'feyzullah_city\')||\'Konya\',district=$(\'district\').value||\'Ereğli\';localStorage.setItem(\'feyzullah_city\',city);localStorage.setItem(\'feyzullah_district\',district);try{let d=await api(\'/api/weather?\'+new URLSearchParams({city,district}));$(\'weatherStatus\').textContent=`${d.location.name}, ${d.location.admin1||\'\'} • ${d.current.temperature_2m}°C • Nem %${d.current.relative_humidity_2m}`;let days=[\'Pazar\',\'Pazartesi\',\'Salı\',\'Çarşamba\',\'Perşembe\',\'Cuma\',\'Cumartesi\'];$(\'weatherForecastContainer\').innerHTML=d.daily.time.map((x,i)=>`<div class="weather-day"><b>${days[new Date(x+\'T12:00:00\').getDay()]}</b>${x}<p>${d.daily.temperature_2m_max[i]}° / ${d.daily.temperature_2m_min[i]}°</p><small>Yağış %${d.daily.precipitation_probability_max?.[i]??\'—\'}</small></div>`).join(\'\');closeLocationModal()}catch(e){$(\'weatherStatus\').textContent=e.message}}\nfunction startMarkets(){if(socket&&socket.readyState<=1)return;loadSymbols();loadMovers();if(!window.symbolRefreshTimer)window.symbolRefreshTimer=setInterval(loadSymbols,300000);if(!alertCheckTimer)alertCheckTimer=setInterval(checkPriceAlerts,3000);if(!window.coinPaintTimer)window.coinPaintTimer=setInterval(renderCoins,1000);socket=new WebSocket(\'wss://stream.binance.com:9443/ws/!miniTicker@arr\');socket.onopen=()=>$(\'marketStatus\').textContent=\'● Binance canlı akış aktif\';socket.onmessage=e=>{try{let a=JSON.parse(e.data);if(!Array.isArray(a))return;a.forEach(x=>{let old=tickers.get(x.s)||{},prev=Number(old.c),cur=Number(x.c);tickers.set(x.s,{...old,...x,secondChange:prev>0&&cur>0?(cur/prev-1)*100:null,lastTick:Date.now()})});renderCoins();[\'BTCUSDT\',\'ETHUSDT\'].forEach(s=>{let x=tickers.get(s);if(x){$(s===\'BTCUSDT\'?\'btcPrice\':\'ethPrice\').textContent=fmt(x.c)+\' USDT\'}})}catch{}};socket.onclose=()=>{$(\'marketStatus\').textContent=\'Bağlantı kesildi; yeniden bağlanılıyor\';setTimeout(startMarkets,2500)};socket.onerror=()=>$(\'marketStatus\').textContent=\'WebSocket bağlantı hatası\'}async function loadSymbols(){try{let fresh=await api(\'/market/symbols\');let oldSet=new Set(symbols.map(x=>x.symbol));newlyListed=fresh.filter(x=>oldSet.size&&!oldSet.has(x.symbol));symbols=fresh;knownSymbols=new Set(symbols.map(x=>x.symbol));$(\'marketStatus\').textContent=`${symbols.length} aktif USDT spot paritesi keşfedildi • WebSocket canlı fiyat • ${newlyListed.length} yeni keşif` ;renderCoins()}catch(e){$(\'marketStatus\').textContent=\'Parite listesi alınamadı: \'+e.message}}function renderCoins(){let q=($(\'coinSearch\')?.value||\'\').toUpperCase();let list=symbols.filter(s=>s.symbol.includes(q));let sort=$(\'coinSort\')?.value||\'symbol\';list.sort((a,b)=>{let x=tickers.get(a.symbol)||{},y=tickers.get(b.symbol)||{},cx=x.secondChange??-Infinity,cy=y.secondChange??-Infinity;if(sort===\'change_desc\')return cy-cx;if(sort===\'change_asc\')return cx-cy;if(sort===\'price_desc\')return (+y.c||0)-(+x.c||0);if(sort===\'price_asc\')return (+x.c||0)-(+y.c||0);return a.symbol.localeCompare(b.symbol)});coinPageSize=+$(\'coinPageSize\')?.value||100;let pages=Math.max(1,Math.ceil(list.length/coinPageSize));coinPage=Math.min(coinPage,pages);let shown=list.slice((coinPage-1)*coinPageSize,coinPage*coinPageSize);$(\'coinPager\').innerHTML=`<span class="muted">${list.length} coin • Sayfa ${coinPage}/${pages}</span><button class="action" onclick="coinPage=Math.max(1,coinPage-1);renderCoins()">‹ Önceki</button><button class="action" onclick="coinPage=Math.min(${pages},coinPage+1);renderCoins()">Sonraki ›</button>`;$(\'coinList\').innerHTML=(newlyListed.length&&coinPage===1&&!q?\'<div class="item"><b>🆕 Yeni keşfedilen pariteler:</b> \'+newlyListed.slice(0,30).map(x=>esc(x.symbol)).join(\', \')+\'</div>\':\'\')+shown.map(s=>{let x=tickers.get(s.symbol),price=x?fmt(x.c):\'Akış bekleniyor\',change=x?.secondChange??null;return `<div class="item" style="display:flex;justify-content:space-between;gap:8px;align-items:center;flex-wrap:wrap"><div><b>${esc(s.symbol)}</b><br><small>${esc(s.baseAsset)}/${esc(s.quoteAsset)}</small></div><div><b>${price}</b><br><small class="${change===null?\'\':change>=0?\'green\':\'red\'}">${change===null?\'İlk canlı veri bekleniyor\':change.toFixed(4)+\'% (son WebSocket güncellemesi)\'}</small></div><a target="_blank" rel="noopener" href="${s.binance_url}" style="color:#4ade80">Binance grafiği ↗</a></div>`}).join(\'\')||\'Eşleşen coin yok.\'}\nasync function loadMetals(){try{let d=await api(\'/market/metals\');$(\'goldPrice\').textContent=d.gold?fmt(d.gold.price)+\' USD/ons\':\'Veri yok\';$(\'silverPrice\').textContent=d.silver?fmt(d.silver.price)+\' USD/ons\':\'Veri yok\';$(\'gramGoldPrice\').textContent=d.gram_gold_try?fmt(d.gram_gold_try.price)+\' TL/gr\':\'Kur/veri yok\';$(\'quarterGoldPrice\').textContent=d.gram_gold_try?fmt(d.gram_gold_try.price*(22/24)*1.754)+\' TL (teorik)\':\'Kur/veri yok\'}catch{}}async function loadMovers(){try{let d=await api(\'/market/movers\');$(\'liveGainers\').innerHTML=\'<h4>🔥 Yükselenler</h4>\'+d.top_gainers.slice(0,10).map(x=>`<div class="item">${x.symbol} • ${fmt(x.lastPrice)} • <span class="green">+${x.priceChangePercent.toFixed(2)}%</span></div>`).join(\'\');$(\'liveLosers\').innerHTML=\'<h4>🔻 Düşenler</h4>\'+d.top_losers.slice(0,10).map(x=>`<div class="item">${x.symbol} • ${fmt(x.lastPrice)} • <span class="red">${x.priceChangePercent.toFixed(2)}%</span></div>`).join(\'\')}catch{}}async function loadTechnicalAnalysis(){let symbol=$(\'aiMarketSymbol\').value.trim().toUpperCase();if(!symbol)return;try{let data=await api(\'/api/crypto/klines/\'+encodeURIComponent(symbol)+\'?interval=1h&limit=100&indicators=true\');let x=data[data.length-1];$(\'aiMarketResult\').textContent=JSON.stringify({symbol,latest:x,records:data.length},null,2)}catch(e){showError(e)}}async function runInvestmentAI(){try{$(\'aiInvestmentResult\').textContent=JSON.stringify(await post(\'/api/ai/investment-analysis\',{symbol:$(\'aiMarketSymbol\').value.trim(),question:$(\'aiMarketQuestion\').value}),null,2)}catch(e){showError(e)}}async function runAI(){try{$(\'aiResult\').textContent=JSON.stringify(await post(\'/api/ai/analyze\',{prompt:$(\'aiPrompt\').value}),null,2)}catch(e){showError(e)}}\nasync function loadBistQuotes(){const box=$(\'bistQuotes\'),status=$(\'bistStatus\');if(!box||!status)return;status.textContent=\'BIST verileri alınıyor...\';box.innerHTML="<div class=\'item\'>Twelve Data üzerinden gerçek fiyatlar sorgulanıyor…</div>";try{const d=await api(\'/api/bist/quotes\');box.innerHTML=d.items.map(x=>x.error?`<div class="item"><b>${esc(x.symbol)}</b><div class="red">${esc(x.error)}</div></div>`:`<div class="item"><b>${esc(x.symbol)} • ${esc(x.name||\'\')}</b><div>Fiyat: <b>${x.price==null?\'—\':fmt(x.price)+\' \'+esc(x.currency||\'TRY\')}</b></div><div>Değişim: ${x.percent_change==null?\'—\':fmt(x.percent_change)+\'%\'}</div><small>Son veri: ${esc(x.datetime||\'—\')} • Kaynak: ${esc(x.source||\'Twelve Data\')}</small></div>`).join(\'\');status.textContent=\'Son sorgu: \'+new Date(d.requested_at).toLocaleString(\'tr-TR\')+\' • \'+d.note}catch(e){status.textContent=e.message;if(e.message.includes(\'TWELVE_DATA_API_KEY\'))box.innerHTML=\'<div class="item"><b>API anahtarı eksik</b><br>Render → Environment → TWELVE_DATA_API_KEY ekleyin ve servisi yeniden dağıtın. Anahtarı sohbet veya kaynak koda yazmayın.</div>\';else box.innerHTML=\'<div class="item red">Piyasa verisi alınamadı: \'+esc(e.message)+\'</div>\'}}async function runBistHunter(){const box=$("bistHunterResults"),status=$("bistHunterStatus");if(!box||!status)return;status.textContent="Avcı taraması çalışıyor…";box.innerHTML="";try{const q=new URLSearchParams({min_volume:$("bistHunterMinVolume").value||"1000000",min_change:$("bistHunterMinChange")?.value||"0"});const symbols=$("bistHunterSymbols").value.trim();if(symbols)q.set("symbols",symbols);const d=await api("/api/bist/hunter?"+q.toString());box.innerHTML=(d.items||[]).map(x=>x.error?`<div class="item red"><b>${esc(x.symbol)}</b> • ${esc(x.error)}</div>`:`<div class="item"><b>${esc(x.symbol)} • ${esc(x.name||"")}</b><div>Fiyat: ${x.price==null?"—":fmt(x.price)} • Değişim: ${x.change_percent==null?"—":fmt(x.change_percent)+"%"}</div><div>Hacim: ${x.volume==null?"—":fmt(x.volume)} • Eşik üstü: <b>${x.volume_over_threshold?"Evet":"Hayır / veri yok"}</b></div><div>${esc(x.momentum||"")}</div><small>Veri zamanı: ${esc(x.provider_timestamp||"sağlayıcı zamanı yok")} • ${esc(x.source||"")}</small></div>`).join("")||"Tarama sonucu yok.";status.textContent=`${d.scanned_count}/${d.requested_count} sembol tarandı • Evren: ${d.universe} • ${d.note}`}catch(e){status.textContent=e.message;box.innerHTML=`<div class="item red">Tarama yapılamadı: ${esc(e.message)}</div>`}}async function loadOneBistQuote(){const s=$(\'bistSymbol\').value.trim().toUpperCase();if(!s)return;try{const d=await api(\'/api/bist/quotes?\'+new URLSearchParams({symbols:s}));$(\'bistQuotes\').innerHTML=d.items.map(x=>x.error?`<div class="item"><b>${esc(x.symbol)}</b><div class="red">${esc(x.error)}</div></div>`:`<div class="item"><b>${esc(x.symbol)} • ${esc(x.name||\'\')}</b><div>Fiyat: <b>${x.price==null?\'—\':fmt(x.price)+\' \'+esc(x.currency||\'TRY\')}</b></div><div>Değişim: ${x.percent_change==null?\'—\':fmt(x.percent_change)+\'%\'}</div><small>${esc(x.datetime||\'—\')} • Twelve Data</small></div>`).join(\'\');$(\'bistStatus\').textContent=\'Sembol sorgulandı.\'}catch(e){$(\'bistStatus\').textContent=e.message}}async function loadBistSeries(){const s=$(\'bistSymbol\').value.trim().toUpperCase();if(!s)return;try{const d=await api(\'/api/bist/series/\'+encodeURIComponent(s)+\'?\'+new URLSearchParams({interval:$(\'bistInterval\').value,outputsize:\'60\'}));const rows=(d.values||[]).slice().reverse();if(!rows.length){$(\'bistSeries\').textContent=\'Grafik verisi bulunamadı.\';return}const vals=rows.map(x=>Number(x.close)).filter(Number.isFinite);if(!vals.length){$(\'bistSeries\').textContent=\'Grafik için geçerli kapanış verisi yok.\';return}const lo=Math.min(...vals),hi=Math.max(...vals),span=hi-lo||1,pts=rows.map((x,i)=>`${(i/Math.max(1,rows.length-1))*760+10},${210-((Number(x.close)-lo)/span)*180}`).join(\' \');$(\'bistSeries\').innerHTML=`<div><b>${esc(d.meta?.symbol||s)}</b> • ${esc(d.meta?.interval||\'\')} • ${rows.length} kayıt • Min ${fmt(lo)} / Maks ${fmt(hi)}</div><svg viewBox="0 0 780 240" role="img" aria-label="${esc(s)} fiyat grafiği" style="width:100%;height:auto;max-height:260px"><polyline fill="none" stroke="#16a34a" stroke-width="3" points="${pts}"/></svg><div class="muted">${esc(rows[0].datetime||\'\')} — ${esc(rows[rows.length-1].datetime||\'\')} • Son kapanış: <b>${fmt(vals[vals.length-1])}</b></div>`}catch(e){$(\'bistSeries\').textContent=e.message}}\nfunction clock(){let n=new Date(),c=$(\'clock\');if(c)c.innerHTML=`<span class=\"clock-date\">${n.toLocaleDateString(\'tr-TR\',{weekday:\'long\',day:\'2-digit\',month:\'long\',year:\'numeric\'})}</span><span class=\"clock-time\">${n.toLocaleTimeString(\'tr-TR\',{hour:\'2-digit\',minute:\'2-digit\',second:\'2-digit\'})}</span>`;requestAnimationFrame(clock)}window.addEventListener(\'load\',async()=>{requestAnimationFrame(clock);applyPermissions();updateRationBreeds();renderRationFeeds();renderHomeApps();if($(\'ahDate\'))$(\'ahDate\').value=new Date().toISOString().slice(0,10);$(\'city\').value=localStorage.getItem(\'feyzullah_city\')||\'Konya\';$(\'district\').value=localStorage.getItem(\'feyzullah_district\')||\'Ereğli\';if(token){$(\'authBtn\').textContent=\'👤 \'+(localStorage.getItem(\'feyzullah_username\')||\'Hesabım\');try{await syncAccount()}catch(e){token=\'\';isAdmin=false;localStorage.removeItem(\'feyzullah_token\');applyPermissions()}if(token){await loadAll().catch(e=>console.warn(\'Hesap açıldı; bazı kayıtlar yüklenemedi:\',e.message));await loadBackups();await loadHomePreferences();await v31LoadWidgets()}}if(localStorage.getItem(\'feyzullah_city\'))fetchWeather();if(token){api(\'/api/settings/farm\').then(x=>{if(x.map_url)$(\'farmMapLink\').href=x.map_url;$(\'city\').value=x.city||$(\'city\').value;$(\'district\').value=x.district||$(\'district\').value}).catch(()=>{});loadCropCalendar().catch(()=>{})}loadMetals();loadSupports();if($(\'cropAutoRegion\')&&$(\'cropAutoLocation\')){ $(\'cropAutoRegion\').value=\'İç Anadolu\'; $(\'cropAutoLocation\').value=\'Konya\'; loadAutoCropPlanner(); }setInterval(loadMetals,60000);new MutationObserver(()=>applyPermissions()).observe(document.querySelector(\'main\'),{childList:true,subtree:true})});\nlet ahAnimals=[];\nfunction ahScopeChanged(){let g=$("ahScope").value==="group";$("ahAnimalWrap").style.display=g?"none":"block";$("ahGroupWrap").style.display=g?"block":"none"}\nasync function ahLoadAnimals(){try{ahAnimals=await api("/api/animals");let sp=$("ahSpecies").value;$("ahAnimal").innerHTML=ahAnimals.filter(a=>a.species===sp).map(a=>`<option value="${a.id}">#${a.id} • ${esc(a.name)} • Küpe ${esc(a.tag_no||"—")}</option>`).join("")||"<option>Bu türde kayıtlı hayvan yok</option>"}catch(e){}}\nfunction ahApplyGuide(){let title=$("ahTitle").value.toLocaleLowerCase("tr-TR"),sp=$("ahSpecies").value,repeat=$("ahRepeat"),note=$("ahSourceNote"),url=$("ahSource");let guide=null;if((sp==="Küçükbaş"||sp==="Keçi")&&(title.includes("şap")||title.includes("enterotoksemi")||title.includes("çelerme")||title.includes("agalaksi"))){guide={months:6,note:"Hakkari İl Tarım ve Orman Müdürlüğünün yerel yıl boyu küçükbaş programında 6 aylık tekrar bilgisi yer alır; il/ilçe kampanya ve ürün talimatı ile doğrulayın.",url:"https://hakkari.tarimorman.gov.tr/Sayfalar/Detay.aspx?SayfaId=28"}}if(guide){if(!repeat.value)repeat.value=guide.months;if(!note.value)note.value=guide.note;if(!url.value)url.value=guide.url}else if(title.includes("kuzu iç parazit")){if(!note.value)note.value="Samsun İl Tarım bilgilendirmesi: ilk uygulama yaklaşık 1,5–2 aylık yaşta; tekrar aralığı parazit yoğunluğuna göre 3–4 ay. Kuzuya özgü rehberdir, tüm yaşlara genellenmez.";if(!url.value)url.value="https://samsun.tarimorman.gov.tr/Haber/1635/Kuzu-Bakim-Besleme-Ve-Saglik-Kontrolleri-Hakkinda-Bilgilendirme"}}\nasync function ahCreatePlan(){try{let d=await post("/api/animal-health/plans",{species:$("ahSpecies").value,scope:$("ahScope").value,animal_id:$("ahScope").value==="animal"?Number($("ahAnimal").value)||null:null,group_name:$("ahGroup").value,category:$("ahCategory").value,title:$("ahTitle").value.trim(),product_name:$("ahProduct").value,active_ingredient:$("ahIngredient").value,planned_date:$("ahDate").value,repeat_months:Number($("ahRepeat").value)||null,repeat_days:Number($("ahRepeatDays").value)||null,source_note:$("ahSourceNote").value,source_url:$("ahSource").value,notes:$("ahNotes").value});alert(d.message);$("ahTitle").value="";await ahLoadPlans()}catch(e){showError(e)}}\nasync function ahComplete(id){let applied=prompt("Gerçek uygulama tarihi (YYYY-AA-GG):",new Date().toISOString().slice(0,10));if(!applied)return;let p=ahPlans.find(x=>x.id===id),ids=[];if(p.scope==="group"){let eligible=ahAnimals.filter(a=>a.species===p.species&&a.status==="Aktif");let raw=prompt("Uygulanan hayvanların küpe numaralarını veya ID numaralarını virgülle yazın. Kayıt: "+eligible.map(a=>(a.tag_no||a.id)).join(", "));if(!raw)return;ids=raw.split(",").map(v=>{let t=v.trim();let a=eligible.find(x=>String(x.id)===t||String(x.tag_no||"")===t);return a?.id}).filter(Boolean);if(!ids.length)return alert("Girilen küpe/ID numaralarıyla eşleşen hayvan bulunamadı.")}let product=prompt("Kullanılan ürün adı (boş bırakılabilir):",p.product_name||"")??p.product_name;let ingredient=prompt("Etken madde (boş bırakılabilir):",p.active_ingredient||"")??p.active_ingredient;let dose=prompt("Doz / ürün etiketi notu (zorunlu doz önerisi değildir):","")||"";let meat=Number(prompt("Et için arınma süresi (gün; ürün etiketinden):","0"))||0;let milk=Number(prompt("Süt için arınma süresi (gün; ürün etiketinden):","0"))||0;try{let d=await post(`/api/animal-health/plans/${id}/complete`,{applied_date:applied,animal_ids:ids,product_name:product,active_ingredient:ingredient,dose_note:dose,withdrawal_meat_days:meat,withdrawal_milk_days:milk});alert(d.message);await ahLoadPlans()}catch(e){showError(e)}}\nasync function ahLoadCatalog(){let box=$("ahCatalogList");if(!box)return;try{let rows=await api("/api/animal-health/catalog");box.innerHTML=rows.length?rows.map(x=>`<div class="item"><b>${esc(x.product_name)}</b> • ${esc(x.category)}<div>${esc(x.target_species||"Tür belirtilmedi")} • ${esc(x.active_ingredient||"Etken madde belirtilmedi")}</div><div>Doz: ${esc(x.dosage_rule||"Kayıtlı değil")} • Uygulama: ${esc(x.application_route||"Belirtilmedi")}</div><small>${esc(x.verification_status)} • ${esc(x.source_url||x.source_note||"Kaynak girilmemiş")}</small></div>`).join(""):"Henüz doğrulanmış ürün kataloğu kaydı yok. Bu alan bilerek varsayılan dozlarla doldurulmadı."}catch(e){box.textContent=e.message}}\nlet ahPlans=[];\nfunction ahTickCountdowns(){document.querySelectorAll(".ah-countdown").forEach(el=>{const out=el.querySelector(".ah-countdown-value");if(!out)return;if(el.dataset.ahStatus==="Tamamlandı"){out.textContent="✓ Tamamlandı";return}const parts=el.dataset.ahDate.split("-").map(Number);const due=new Date(parts[0],parts[1]-1,parts[2]+1,0,0,0,0);const diff=due.getTime()-Date.now();if(!Number.isFinite(diff)){out.textContent="Tarih okunamadı";return}const late=diff<=0,remain=Math.abs(diff),d=Math.floor(remain/86400000),h=Math.floor(remain%86400000/3600000),m=Math.floor(remain%3600000/60000),sec=Math.floor(remain%60000/1000);out.textContent=late?`⚠ ${d} gün ${String(h).padStart(2,"0")}:${String(m).padStart(2,"0")}:${String(sec).padStart(2,"0")} gecikti`:`${d} gün ${String(h).padStart(2,"0")}:${String(m).padStart(2,"0")}:${String(sec).padStart(2,"0")}`})}\nif(!window.ahCountdownTimer)window.ahCountdownTimer=setInterval(ahTickCountdowns,1000);\nasync function ahLoadPlans(){ahLoadCatalog();try{ahPlans=await api("/api/animal-health/plans");let today=new Date();today.setHours(0,0,0,0);let open=ahPlans.filter(x=>x.status!=="Tamamlandı"),late=open.filter(x=>x.days_left<0),soon=open.filter(x=>x.days_left>=0&&x.days_left<=7);$("ahLate").textContent=late.length;$("ahSoon").textContent=soon.length;$("ahTotal").textContent=open.length;$("ahPlanList").innerHTML=ahPlans.length?ahPlans.map(p=>{let state=p.status==="Tamamlandı"?"Tamamlandı":p.days_left<0?`Gecikti: ${Math.abs(p.days_left)} gün`:p.days_left===0?"Bugün yapılacak":`${p.days_left} gün kaldı`;let color=p.status==="Tamamlandı"?"#4ade80":p.days_left<0?"#f87171":p.days_left<=7?"#fbbf24":"#7dd3fc";return `<div class="item"><b>${esc(p.title)}</b> <span class="tag">${esc(p.category)}</span><br>${esc(p.species)} • ${p.scope==="animal"?`#${p.animal_id} ${esc(p.animal_name||"")} / ${esc(p.tag_no||"")}`:`Grup: ${esc(p.group_name)} • Küpeler: ${esc(p.ear_tags||"Uygulama sırasında seçilecek")}`}<br><b style="color:${color}">${state}</b> • Tarih: ${esc(p.planned_date)} • Tekrar: ${p.repeat_days? p.repeat_days+" gün":(p.repeat_months? p.repeat_months+" ay":"Tek sefer")}<div class="ah-countdown" data-ah-date="${esc(p.planned_date)}" data-ah-status="${esc(p.status)}" style="margin:9px 0;padding:10px;border-radius:9px;background:#102235;border:1px solid #36536b"><small>⏱️ GERİ SAYIM</small><div class="ah-countdown-value" style="font-size:18px;font-weight:900;letter-spacing:.5px">Hesaplanıyor…</div></div><div class="muted">${esc(p.product_name||"")} ${p.active_ingredient?"• "+esc(p.active_ingredient):""} ${p.source_note?"• "+esc(p.source_note):""}</div>${p.status!=="Tamamlandı"?`<button class="action" onclick="ahComplete(${p.id})">✓ Uygulandı / kaydet</button>`:""}</div>`}).join(""):"Henüz plan yok.";ahTickCountdowns()}catch(e){$("ahPlanList").textContent=e.message}try{let h=await api("/api/animal-health/history");$("ahHistory").innerHTML=h.length?h.map(x=>`<div class="item"><b>${esc(x.title)}</b> • ${esc(x.applied_date)}<br>${esc(x.animal_name||x.group_name||"Sürü uygulaması")} • ${esc(x.product_name||"")}<br>Küpe: ${esc(x.ear_tags||x.tag_no||"—")}<div class="muted">Et arınma: ${x.withdrawal_meat_days||0} gün • Süt arınma: ${x.withdrawal_milk_days||0} gün • Sonraki: ${esc(x.next_date||"Yok")}</div></div>`).join(""):"Henüz uygulama kaydı yok."}catch(e){}}\n</script></body></html>'

# V31 UI is layered onto the original V30 markup; existing sections remain untouched.
HTML_CONTENT = HTML_CONTENT.replace('<section id="home" class="page active">', '<section id="home" class="page active"><div class="card" id="v31HomeQuickPanel"><div style="display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap"><button class="action" id="v31AddWidget" style="display:none" onclick="v31AddWidget()">＋ Öğe Ekle</button></div><div id="v31WidgetList" class="grid-2"></div><div id="v31WidgetEditor" class="widget-editor"><h4>✏️ Ana ekran bilgisi ekle / düzenle</h4><input id="widgetEditId" type="hidden"><div class="grid-2"><div><label>Başlık</label><input id="widgetTitleInput" maxlength="100" placeholder="Örn. Çiftlik iletişim"></div><div><label>Bilgi türü</label><select id="widgetKindInput"><option value="text">Serbest yazı</option><option value="phone">Telefon numarası</option><option value="address">Adres / konum</option><option value="link">Web bağlantısı</option></select></div></div><label>Yazı / numara / adres / bağlantı</label><textarea id="widgetContentInput" maxlength="1000" rows="3" placeholder="Ana ekranda gösterilecek içerik"></textarea><div class="grid-2"><div><label>Yazı boyutu</label><input id="widgetFontSizeInput" type="number" min="12" max="48" value="22"></div><div><label>Yazı rengi</label><input id="widgetColorInput" type="color" value="#f1f5f9"></div><div><label>Kalınlık</label><select id="widgetWeightInput"><option value="bold">Kalın</option><option value="normal">Normal</option></select></div><div><label>Yazı stili</label><select id="widgetStyleInput"><option value="italic">İtalik</option><option value="normal">Düz</option></select></div><div><label>Yazı biçimi</label><select id="widgetCaseInput"><option value="normal">Normal</option><option value="upper">BÜYÜK HARF</option><option value="lower">küçük harf</option><option value="title">Baş Harfler Büyük</option></select></div></div><div class="actions"><button class="action" onclick="saveHomeWidget()">Kaydet</button><button class="action" onclick="document.getElementById(&quot;v31WidgetEditor&quot;).classList.remove(&quot;open&quot;)">Vazgeç</button></div></div></div>', 1)
HTML_CONTENT = HTML_CONTENT.replace("</script>", 'const HOME_APP_CATALOG=[{id:"animals",label:"Hayvanlar",icon:"🐄",folder:"Çiftlik"},{id:"health",label:"Aşı Listem",icon:"💉",folder:"Çiftlik"},{id:"feed",label:"Yem & Stok",icon:"🌾",folder:"Çiftlik"},{id:"farming",label:"Tarla & Sera",icon:"🌱",folder:"Çiftlik"},{id:"milk",label:"Süt",icon:"🥛",folder:"Çiftlik"},{id:"tasks",label:"Görevler",icon:"📅",folder:"Çiftlik"},{id:"equipment",label:"Ekipman",icon:"🚜",folder:"Çiftlik"},{id:"accounting",label:"Muhasebe",icon:"📒",folder:"Yönetim ve Finans"},{id:"finance",label:"Finans",icon:"💰",folder:"Yönetim ve Finans"},{id:"banking",label:"Banka & Kredi",icon:"🏦",folder:"Yönetim ve Finans"},{id:"bist",label:"BIST & Hisseler",icon:"📈",folder:"Yönetim ve Finans"},{id:"hunters",label:"Binance Avcıları",icon:"🦅",folder:"Yönetim ve Finans"},{id:"ai",label:"Yapay Zekâ",icon:"🤖",folder:"Yönetim ve Finans"},{id:"supports",label:"Devlet Destekleri",icon:"🏛️",folder:"Yönetim ve Finans"},{id:"media",label:"ERCAN MEDIA CENTER",icon:"🎬",folder:"Medya"},{id:"admin",label:"Üye Yönetimi",icon:"🛡️",folder:"Yönetim ve Finans",admin:true}];\nlet homePreferences={theme:"dark",clock_style:"digital",app_order:[],folder_map:{}};let homeWidgetsCache=[];\nfunction v31Case(t,c){if(c==="upper")return t.toLocaleUpperCase("tr-TR");if(c==="lower")return t.toLocaleLowerCase("tr-TR");if(c==="title")return t.toLocaleLowerCase("tr-TR").replace(/(^|\\s)\\S/g,m=>m.toLocaleUpperCase("tr-TR"));return t}\nfunction visibleHomeApps(){let order=homePreferences.app_order||[],arr=HOME_APP_CATALOG.filter(x=>!(x.admin&&!isAdmin));return arr.sort((a,b)=>{let ai=order.indexOf(a.id),bi=order.indexOf(b.id);return (ai<0?999:ai)-(bi<0?999:bi)})}\nfunction renderHomeApps(){const box=document.getElementById("homeAppGroups");if(!box)return;const groups={};visibleHomeApps().forEach(app=>{let folder=homePreferences.folder_map?.[app.id]||app.folder;(groups[folder]??=[]).push(app)});box.innerHTML=Object.entries(groups).map(([folder,apps])=>`<div class="home-folder"><h4>📁 ${esc(folder)}</h4><div class="home-app-grid">${apps.map(app=>`<div class="home-app" draggable="true" data-app-id="${app.id}" ondragstart="homeAppDragStart(event)" ondragover="event.preventDefault();this.classList.add(\'drag-over\')" ondragleave="this.classList.remove(\'drag-over\')" ondrop="homeAppDrop(event,\'${app.id}\')"><span class="app-icon">${app.icon}</span><span class="app-label">${esc(app.label)}</span><button class="action" onclick="openHomeApp(\'${app.id}\')">Aç</button>${isAdmin?`<div class="app-move"><button onclick="moveHomeApp(\'${app.id}\',-1)" title="Önceki">←</button><button onclick="moveHomeApp(\'${app.id}\',1)" title="Sonraki">→</button><button onclick="changeHomeAppFolder(\'${app.id}\')" title="Klasör değiştir">📁</button></div>`:""}</div>`).join("")}</div></div>`).join("")}\nfunction openHomeApp(id){if(id==="media"){location.href="/media";return}if(id==="health"){openAnimalHealthPlanner();return}switchPage(id)}\nfunction currentAppOrder(){return visibleHomeApps().map(x=>x.id)}\nfunction moveHomeApp(id,delta){let a=currentAppOrder(),i=a.indexOf(id),j=Math.max(0,Math.min(a.length-1,i+delta));if(i<0||i===j)return;[a[i],a[j]]=[a[j],a[i]];homePreferences.app_order=a;renderHomeApps()}\nlet draggingHomeApp="";function homeAppDragStart(e){draggingHomeApp=e.currentTarget.dataset.appId;e.dataTransfer.setData("text/plain",draggingHomeApp);e.dataTransfer.effectAllowed="move"}function homeAppDrop(e,target){e.preventDefault();e.currentTarget.classList.remove("drag-over");let from=e.dataTransfer.getData("text/plain")||draggingHomeApp,to=target,a=currentAppOrder(),i=a.indexOf(from),j=a.indexOf(to);if(i<0||j<0||i===j)return;a.splice(i,1);a.splice(j,0,from);homePreferences.app_order=a;renderHomeApps()}\nfunction changeHomeAppFolder(id){let app=HOME_APP_CATALOG.find(x=>x.id===id),current=homePreferences.folder_map?.[id]||app.folder,folder=prompt("Klasör adı (ör. Çiftlik, Yönetim, Medya veya kendi klasörünüz):",current);if(folder&&folder.trim()){homePreferences.folder_map[id]=folder.trim().slice(0,30);renderHomeApps()}}\nfunction applyHomePreferences(){document.body.dataset.homeTheme=homePreferences.theme||"dark";let c=document.getElementById("clock");if(c)c.className="clock-"+(homePreferences.clock_style||"digital");let t=document.getElementById("homeThemeSelect"),cs=document.getElementById("clockStyleSelect");if(t)t.value=homePreferences.theme||"dark";if(cs)cs.value=homePreferences.clock_style||"digital";renderHomeApps()}\nasync function loadHomePreferences(){if(!token)return;try{homePreferences=await api("/api/home-preferences");applyHomePreferences();let ctl=document.getElementById("homeDesignControls");if(ctl)ctl.style.display=isAdmin?"block":"none";renderFolderAssignments()}catch(e){console.warn("Ana ekran ayarları:",e.message)}}\nfunction renderFolderAssignments(){let box=document.getElementById("homeFolderAssignments");if(!box||!isAdmin)return;box.innerHTML="<label>Uygulamaların klasörünü seç</label>"+visibleHomeApps().map(a=>`<div class="grid-2"><span>${a.icon} ${esc(a.label)}</span><input aria-label="${esc(a.label)} klasörü" value="${esc(homePreferences.folder_map?.[a.id]||a.folder)}" onchange="homePreferences.folder_map[\'${a.id}\']=this.value.trim()||\'Diğer\';renderHomeApps()"></div>`).join("")}\nasync function saveHomePreferences(){if(!isAdmin)return;try{await api("/api/home-preferences",{method:"PUT",body:JSON.stringify({theme:homePreferences.theme,clock_style:homePreferences.clock_style,app_order:currentAppOrder(),folder_map:homePreferences.folder_map||{}})});alert("Ana ekran düzeni kaydedildi.");applyHomePreferences()}catch(e){alert(e.message)}}\nasync function v31LoadWidgets(){const box=document.getElementById("v31WidgetList");if(!box||!token)return;try{const d=await api("/api/home-widgets");homeWidgetsCache=d.items||[];box.innerHTML=homeWidgetsCache.map(w=>{const t=v31Case(w.content,w.text_case),sty=`font-size:${Math.max(12,Math.min(48,Number(w.font_size)||22))}px;color:${esc(w.color||"#f1f5f9")};font-weight:${w.font_weight==="normal"?"400":"800"};font-style:${w.font_style==="normal"?"normal":"italic"};white-space:pre-wrap;text-decoration:none`;let a="";if(w.kind==="phone")a=`<a class="action home-widget-content" style="${sty}" href="tel:${encodeURIComponent(w.content)}">📞 ${esc(t)}</a>`;else if(w.kind==="address")a=`<a class="action home-widget-content" style="${sty}" target="_blank" rel="noopener" href="https://www.google.com/maps/search/?api=1&query=${encodeURIComponent(w.content)}">📍 ${esc(t)}</a>`;else if(w.kind==="link")a=`<a class="action home-widget-content" style="${sty}" target="_blank" rel="noopener" href="${esc(w.content)}">🔗 ${esc(w.title||t)}</a>`;else a=`<div class="home-widget-content" style="${sty}">${esc(t)}</div>`;return `<div class="item"><div style="display:flex;justify-content:space-between;gap:6px"><b>${esc(w.title||"Ana ekran bilgisi")}</b>${isAdmin?`<span class="app-move"><button onclick="moveHomeWidget(${w.id},-1)">↑</button><button onclick="moveHomeWidget(${w.id},1)">↓</button></span>`:""}</div><div style="margin-top:6px">${a}</div>${isAdmin?`<div class="actions"><button class="action" onclick="editHomeWidget(${w.id})">Düzenle</button><button class="action danger" onclick="v31DeleteWidget(${w.id})">Sil</button></div>`:""}</div>`}).join("")||"<p class=muted>Henüz öğe eklenmedi.</p>"}catch(e){box.textContent=e.message}}\nfunction openWidgetEditor(id){if(!isAdmin)return;const f=document.getElementById("v31WidgetEditor");if(!f)return;f.classList.add("open");document.getElementById("widgetEditId").value=id||"";document.getElementById("widgetTitleInput").value="";document.getElementById("widgetContentInput").value="";document.getElementById("widgetKindInput").value="text";document.getElementById("widgetFontSizeInput").value=22;document.getElementById("widgetColorInput").value="#f1f5f9";document.getElementById("widgetWeightInput").value="bold";document.getElementById("widgetStyleInput").value="italic";document.getElementById("widgetCaseInput").value="normal";if(id){let w=homeWidgetsCache.find(x=>x.id===id);if(w){document.getElementById("widgetTitleInput").value=w.title;document.getElementById("widgetContentInput").value=w.content;document.getElementById("widgetKindInput").value=w.kind;document.getElementById("widgetFontSizeInput").value=w.font_size;document.getElementById("widgetColorInput").value=w.color;document.getElementById("widgetWeightInput").value=w.font_weight||"bold";document.getElementById("widgetStyleInput").value=w.font_style||"italic";document.getElementById("widgetCaseInput").value=w.text_case}}f.scrollIntoView({behavior:"smooth",block:"center"})}\nfunction editHomeWidget(id){openWidgetEditor(id)}\nasync function saveHomeWidget(){if(!isAdmin)return;let id=document.getElementById("widgetEditId").value,title=document.getElementById("widgetTitleInput").value.trim(),content=document.getElementById("widgetContentInput").value.trim(),kind=document.getElementById("widgetKindInput").value,text_case=document.getElementById("widgetCaseInput").value,font_size=Number(document.getElementById("widgetFontSizeInput").value)||22,color=document.getElementById("widgetColorInput").value,font_weight=document.getElementById("widgetWeightInput").value,font_style=document.getElementById("widgetStyleInput").value;if(!content)return alert("İçerik alanını doldurun.");let body={title,content,kind,text_case,font_size,color,font_weight,font_style};try{await api(id?"/api/home-widgets/"+id:"/api/home-widgets",{method:id?"PUT":"POST",body:JSON.stringify(body)});document.getElementById("v31WidgetEditor").classList.remove("open");await v31LoadWidgets()}catch(e){alert(e.message)}}\nasync function moveHomeWidget(id,delta){if(!isAdmin)return;let a=homeWidgetsCache.map(x=>x.id),i=a.indexOf(id),j=Math.max(0,Math.min(a.length-1,i+delta));if(i<0||i===j)return;[a[i],a[j]]=[a[j],a[i]];try{await post("/api/home-widgets/reorder",{widget_ids:a});await v31LoadWidgets()}catch(e){alert(e.message)}}\nasync function v31AddWidget(){openWidgetEditor("")}\nasync function v31DeleteWidget(id){if(!isAdmin||!confirm("Öğe silinsin mi?"))return;try{await api("/api/home-widgets/"+id,{method:"DELETE"});await v31LoadWidgets()}catch(e){alert(e.message)}}\nconst RATION_FEEDS=[{id:"cornsilage",name:"Mısır silajı (kaba yem)",dm:35,cp:8.5,me:10.5},{id:"alfalfa",name:"Yonca kuru otu",dm:88,cp:18,me:9.5},{id:"grasshay",name:"Çayır otu",dm:88,cp:10,me:8.5},{id:"wheatstraw",name:"Buğday samanı",dm:90,cp:4,me:6.5},{id:"barley",name:"Arpa kırması",dm:88,cp:11,me:12.5},{id:"corn",name:"Mısır kırması",dm:88,cp:9,me:13.5},{id:"bran",name:"Buğday kepeği",dm:89,cp:16,me:11.5},{id:"soy",name:"Soya küspesi",dm:89,cp:46,me:12.5},{id:"cotton",name:"Pamuk tohumu küspesi",dm:90,cp:38,me:10.5},{id:"beet",name:"Pancar posası (kuru)",dm:90,cp:9,me:11.5},{id:"beetwet",name:"Pancar posası (yaş)",dm:22,cp:9,me:11.0},{id:"oat",name:"Yulaf",dm:89,cp:12,me:11.5}];\nconst RATION_BREEDS={cattle:["Holştayn","Simental","Montofon","Yerli Kara","Angus","Limuzin","Şarole"],sheep:["Akkaraman","Merinos","İvesi","Kıvırcık","Karayaka"],goat:["Saanen","Kilis","Ankara","Kıl keçisi","Halep"]};\nfunction updateRationBreeds(){let s=document.getElementById("rationSpecies").value,b=document.getElementById("rationBreed");b.innerHTML=RATION_BREEDS[s].map(x=>`<option>${x}</option>`).join("");let w=document.getElementById("rationWeight"),t=document.getElementById("rationTargetWeight"),g=document.getElementById("rationGainTarget"),m=document.getElementById("rationMilkTarget");if(s==="cattle"){w.value=600;t.value=700;g.value=.8;m.value=20}else if(s==="sheep"){w.value=55;t.value=70;g.value=.15;m.value=1.5}else{w.value=50;t.value=65;g.value=.12;m.value=2}}\nfunction renderRationFeeds(){let box=document.getElementById("rationFeedRows");if(!box)return;box.innerHTML=RATION_FEEDS.map(f=>`<div class="ration-feed"><span class="feed-name">${f.name}</span><input aria-label="${f.name} kg/gün" id="rf-${f.id}-kg" type="number" min="0" step="0.1" value="0"><input aria-label="${f.name} kuru madde yüzde" id="rf-${f.id}-dm" type="number" min="1" max="100" step="0.5" value="${f.dm}"><input aria-label="${f.name} ham protein yüzde" id="rf-${f.id}-cp" type="number" min="0" max="100" step="0.1" value="${f.cp}"><input aria-label="${f.name} enerji MJ" id="rf-${f.id}-me" type="number" min="0" max="30" step="0.1" value="${f.me}"></div>`).join("")}\nfunction addCustomRationFeed(){let name=prompt("Özel yem adı:");if(!name)return;let id="custom"+Date.now(),f={id,name:name.slice(0,50),dm:90,cp:10,me:10};RATION_FEEDS.push(f);renderRationFeeds()}\nfunction resetRationFeeds(){RATION_FEEDS.forEach(f=>{let x=document.getElementById("rf-"+f.id+"-kg");if(x)x.value=0});document.getElementById("rationResult").innerHTML="Yem miktarları sıfırlandı."}\nfunction calculateSmartRation(){let species=document.getElementById("rationSpecies").value,purpose=document.getElementById("rationPurpose").value,w=Number(document.getElementById("rationWeight").value),target=Number(document.getElementById("rationTargetWeight").value),milkTarget=Number(document.getElementById("rationMilkTarget").value)||0,gainTarget=Number(document.getElementById("rationGainTarget").value)||0;if(!(w>0)||w>1800||!(target>0))return alert("Canlı ağırlık ve hedef ağırlığı kontrol edin.");let dm=0,cp=0,me=0,lines=[];for(const f of RATION_FEEDS){let kg=Number(document.getElementById("rf-"+f.id+"-kg")?.value)||0;if(kg<=0)continue;let dmp=Number(document.getElementById("rf-"+f.id+"-dm").value),cpp=Number(document.getElementById("rf-"+f.id+"-cp").value),mep=Number(document.getElementById("rf-"+f.id+"-me").value);if(dmp<=0||dmp>100||cpp<0||cpp>100||mep<0||mep>30)return alert(f.name+" besin değerlerini kontrol edin.");let d=kg*dmp/100;dm+=d;cp+=d*cpp/100;me+=d*mep;lines.push({name:f.name,kg,dm:d})}if(dm<=0)return alert("En az bir yem maddesine günlük miktar girin.");let ratio=species==="cattle"? (purpose==="dairy"?0.022:0.02):(species==="sheep"?0.03:0.032),dmTarget=w*ratio,cpPct=cp/dm*100,mePerKg=me/dm,bw075=Math.pow(w,.75),maint=(species==="cattle"?.53:.55)*bw075,litreEnergy=Math.max(0,(me-maint)/5.2),cpMaint=.0035*bw075,litreProtein=Math.max(0,(cp-cpMaint)/.09),milkCapacity=Math.min(litreEnergy,litreProtein),growthEnergy=Math.max(0,me-maint)/(species==="cattle"?25:15),growthProtein=Math.max(0,cp-cpMaint)/(species==="cattle"?.55:.25),adg=Math.max(0,Math.min(growthEnergy,growthProtein)),days=adg>0&&target>w?Math.ceil((target-w)/adg):null,energyReq=maint+(purpose==="dairy"?milkTarget*5.2:gainTarget*(species==="cattle"?25:15)),energyBalance=me-energyReq,dmStatus=dm<dmTarget*.9?"Kuru madde düşük":dm>dmTarget*1.15?"Kuru madde yüksek":"Kuru madde hedef aralığında",scaleFactor=dmTarget/dm,scaledCP=cp*scaleFactor,scaledME=me*scaleFactor,scaledMix=lines.map(x=>({name:x.name,kg:x.kg*scaleFactor}));let milkText=purpose==="dairy"?`<div class="stat">Tahmini süt kapasitesi<b>${milkCapacity.toFixed(1)} L/gün</b></div>`:"";let growthText=purpose!=="dairy"?`<div class="stat">Tahmini canlı ağırlık artışı<b>${(adg*1000).toFixed(0)} g/gün</b></div><div class="stat">Hedef kiloya süre<b>${days?Math.ceil(days/30)+" ay ("+days+" gün)":"Hesaplanamadı"}</b></div>`:"";document.getElementById("rationResult").innerHTML=`<h4>📊 Rasyon analiz sonucu</h4><p><b>${esc(document.getElementById("rationBreed").value)}</b> • ${w} kg • ${esc(purpose==="dairy"?"Süt":purpose==="beef"?"Besi":"Büyütme")}</p><div class="grid-2"><div class="stat">Toplam yaş yem<b>${lines.reduce((a,x)=>a+x.kg,0).toFixed(2)} kg/gün</b></div><div class="stat">Kuru madde<b>${dm.toFixed(2)} kg/gün</b></div><div class="stat">Ham protein<b>${cp.toFixed(2)} kg • %${cpPct.toFixed(1)} KM</b></div><div class="stat">Metabolik enerji<b>${me.toFixed(1)} MJ/gün</b></div><div class="stat">Enerji yoğunluğu<b>${mePerKg.toFixed(2)} MJ/kg KM</b></div><div class="stat">Kuru madde hedefi<b>${dmTarget.toFixed(2)} kg • ${dmStatus}</b></div><div class="stat">Hedef üretime göre enerji farkı<b>${energyBalance.toFixed(1)} MJ/gün</b></div>${milkText}${growthText}</div><p class="muted">Enerji bakım gereksinimi için yaklaşık ${maint.toFixed(1)} MJ/gün kullanıldı. Süt kapasitesi enerji ve ham protein kaba sınırından düşük olanına göre tahmin edildi. Canlı ağırlık artışı enerji/protein fazlası senaryosudur.</p><b>Seçilen yemler:</b><ul>${lines.map(x=>`<li>${esc(x.name)}: ${x.kg.toFixed(2)} kg yaş yem (${x.dm.toFixed(2)} kg KM)</li>`).join("")}</ul><b>Hedef kuru maddeye göre aynı karışım oranında ölçekli miktarlar:</b><ul>${scaledMix.map(x=>`<li>${esc(x.name)}: ${x.kg.toFixed(2)} kg/gün</li>`).join("")}</ul><p class="muted">Bu ölçekli karışım yaklaşık ${dmTarget.toFixed(2)} kg KM, ${scaledCP.toFixed(2)} kg ham protein ve ${scaledME.toFixed(1)} MJ enerji sağlar. Bu yalnızca mevcut karışım oranının kuru madde hedefine göre matematiksel ölçeklemesidir; dengeli rasyon onayı değildir.</p><p class="muted">Bu ön hesaplamada ırk, dönem ve üretim hedefi profil bilgisi olarak kullanılır; kesin ihtiyaçlar için canlı yem analizi, hayvanın kondisyonu, gebelik/laktasyon evresi ve mineral-vitamin dengesi gerekir.</p>`}\n</script>', 1)
HTML_CONTENT = HTML_CONTENT.replace("</body>", "<script>window.addEventListener(\'load\',()=>{const b=document.getElementById(\'v31AddWidget\');if(b)b.style.display=isAdmin?\'\':\'none\';if(token)v31LoadWidgets()});</script></body>", 1)


# V33.9: visible catalog editor and sourced liveweight dose calculator.
_V339_CATALOG_UI = r"""<div id="ahCatalogEditor" class="item" style="display:none;border-left-color:#38bdf8"><h4>➕ Ürün kataloğuna ürün ekle</h4><div class="grid-2"><div><label>Ürün adı</label><input id="ahCatName" maxlength="160"></div><div><label>Kategori</label><select id="ahCatCategory"><option>Aşılar ve tekrar aşıları</option><option>İç parazit</option><option>Dış parazit</option><option>Hap ve ağızdan ilaç</option><option>Enjeksiyon</option><option>Dökme uygulama (pour-on)</option><option>Vitamin ve mineral</option><option>Gebelik kontrolü</option><option>Doğum öncesi bakım</option><option>Doğum sonrası bakım</option><option>Yavru bakımı</option><option>Üreme takibi</option><option>Karantina ve biyogüvenlik</option><option>Sürü sağlık taraması</option><option>Tırnak ve ayak bakımı</option><option>Veteriner muayenesi</option><option>Diğer veteriner uygulamaları</option></select></div><div><label>Hayvan türü</label><select id="ahCatSpecies"><option>Belirtilmedi</option><option>Büyükbaş</option><option>Buzağı</option><option>Küçükbaş</option><option>Keçi</option><option>Tümü</option></select></div><div><label>Etken madde</label><input id="ahCatIngredient"></div><div><label>Doz kuralı (prospektüsten aynen)</label><input id="ahCatDose" placeholder="Örn. 1 ml/50 kg"></div><div><label>Uygulama yolu</label><input id="ahCatRoute" placeholder="SC / IM / oral / pour-on"></div><div><label>Tekrar aralığı (gün)</label><input id="ahCatRepeat" type="number" min="1" max="3650"></div><div><label>Et arınma (gün)</label><input id="ahCatMeat" type="number" min="0" max="365"></div><div><label>Süt arınma (gün)</label><input id="ahCatMilk" type="number" min="0" max="365"></div><div><label>Doğrulama</label><select id="ahCatVerified"><option>Doğrulanmadı</option><option>Resmî kaynakla doğrulandı</option></select></div></div><label>Resmî kaynak bağlantısı</label><input id="ahCatSource" placeholder="https://..."><label>Kaynak / prospektüs notu</label><textarea id="ahCatSourceNote"></textarea><button class="action" onclick="ahSaveCatalogItem()">Kataloğa kaydet</button><p class="muted">Doz bilgisini güncel prospektüsten aynen girin. Doğrulama için kaynak bağlantısı veya notu gerekir.</p></div><div class="item"><h4>⚖️ Canlı ağırlığa göre doz hesaplama</h4><div class="grid-2"><div><label>Ürün</label><select id="ahDoseProduct"><option value="">Katalog ürünü seçin</option></select></div><div><label>Hayvan türü</label><select id="ahDoseSpecies"><option>Büyükbaş</option><option>Buzağı</option><option>Küçükbaş</option><option>Keçi</option></select></div><div><label>Canlı ağırlık (kg)</label><input id="ahDoseWeight" type="number" min="1" max="2500" step="0.1" placeholder="Örn. 450"></div></div><button class="action" onclick="ahCalculateDose()">Dozu hesapla</button><div id="ahDoseResult" class="muted"></div></div>"""
HTML_CONTENT = HTML_CONTENT.replace('<div id="ahCatalogList">Katalog yükleniyor.</div>', _V339_CATALOG_UI + '<div id="ahCatalogList">Katalog yükleniyor.</div>', 1)
_V339_JS = r"""
const _ahLoadCatalogV339=ahLoadCatalog;
ahLoadCatalog=async function(){await _ahLoadCatalogV339();try{let rows=await api("/api/animal-health/catalog"),picker=$("ahDoseProduct"),editor=$("ahCatalogEditor");if(picker)picker.innerHTML='<option value="">Katalog ürünü seçin</option>'+rows.map(x=>`<option value="${x.id}">${esc(x.product_name)} • ${esc(x.category)} • ${esc(x.verification_status)}</option>`).join("");if(editor)editor.style.display=isAdmin?"block":"none"}catch(e){}};
async function ahSaveCatalogItem(){if(!isAdmin)return alert("Yönetici yetkisi gerekir.");let body={product_name:$("ahCatName").value.trim(),category:$("ahCatCategory").value,target_species:$("ahCatSpecies").value,active_ingredient:$("ahCatIngredient").value,dosage_rule:$("ahCatDose").value,application_route:$("ahCatRoute").value,repeat_days:Number($("ahCatRepeat").value)||null,withdrawal_meat_days:Number($("ahCatMeat").value)||null,withdrawal_milk_days:Number($("ahCatMilk").value)||null,source_url:$("ahCatSource").value,source_note:$("ahCatSourceNote").value,verification_status:$("ahCatVerified").value};try{let d=await post("/api/animal-health/catalog",body);alert(d.message);await ahLoadCatalog()}catch(e){showError(e)}}
async function ahCalculateDose(){let id=Number($("ahDoseProduct").value),species=$("ahDoseSpecies").value,weight=Number($("ahDoseWeight").value);if(!id||!(weight>0))return alert("Ürün ve geçerli canlı ağırlık seçin.");let box=$("ahDoseResult");box.textContent="Hesaplanıyor…";try{let q=new URLSearchParams({product_id:String(id),species,weight_kg:String(weight)}),d=await api("/api/animal-health/dose-calculate?"+q.toString());box.innerHTML=`<b>${esc(d.product_name)}</b>: <strong>${fmt(d.calculated_amount)} ${esc(d.unit)}</strong><br>Hesap: ${esc(d.dosage_rule)} • ${d.weight_kg} kg<br><span class="red">${esc(d.warning)}</span><br><small>${esc(d.source_url||d.source_note||"")}</small>`}catch(e){box.textContent=e.message}}
"""
HTML_CONTENT = HTML_CONTENT.replace("</script>", _V339_JS + "</script>", 1)

# --- V31 ERCAN MEDIA CENTER (isolated module) ---
V33_MEDIA_HTML = r'''<!doctype html><html lang="tr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><meta name="theme-color" content="#101827"><title>FEYZULLAH DEMİREL TARIM VE HAYVANCILIK • İYİ SEYİRLER DİLER</title><script src="https://cdn.jsdelivr.net/npm/hls.js@latest"></script><script src="https://www.gstatic.com/cv/js/sender/v1/cast_sender.js?loadCastFramework=1"></script><style>
*{box-sizing:border-box}body{margin:0;background:#0b1420;color:#eef2ff;font:15px Arial}header,main{max-width:1100px;margin:auto;padding:14px}header{background:#17283a;border-bottom:2px solid #16834d}h1{margin:5px 0;color:#86efac}.panel{background:#17283a;border:1px solid #334155;border-radius:12px;padding:14px;margin:12px 0}button,select,input{padding:12px;border-radius:8px;border:1px solid #475569;background:#0b1724;color:white;margin:3px;max-width:100%}button{background:#15803d;cursor:pointer;font-weight:bold}.searchline{display:flex;gap:6px;flex-wrap:wrap}.searchline input{flex:1;min-width:180px;font-size:17px}.muted{color:#94a3b8;font-size:12px;line-height:1.5}.results{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr));gap:10px}.result{background:#102235;border:1px solid #334155;border-radius:10px;padding:10px;overflow:hidden}.result img{width:100%;height:135px;object-fit:cover;border-radius:7px;background:#0b1420}.result h3{font-size:15px;margin:8px 0}.result p{font-size:12px;color:#b8c5d5;max-height:65px;overflow:hidden}.tag{display:inline-block;padding:3px 7px;border-radius:20px;background:#29465f;font-size:11px;margin:2px}.provider{padding:6px 8px;border-radius:7px;background:#20364a;margin:3px;font-size:12px;display:inline-block}.status{padding:9px;background:#102235;border-radius:8px;margin:8px 0}.player{width:100%;height:auto;max-height:78vh;aspect-ratio:16/9;background:#000;border:0;border-radius:10px}.screen-stage{position:relative;background:#000;border:1px solid #334155;border-radius:12px;overflow:hidden;min-height:220px;display:flex;align-items:center;justify-content:center}.screen-stage:fullscreen{width:100vw;height:100vh;border:0;border-radius:0;background:#000}.screen-stage:fullscreen .player,.screen-stage:fullscreen video{width:100%;height:100%;max-height:100vh;object-fit:contain;border-radius:0}video,audio{width:100%;max-height:78vh;background:#000;border-radius:10px}.player-tools{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-top:9px}.player-tools button{margin:0}.playbtn{width:100%;margin-top:8px}.source-link{display:inline-block;margin-top:8px;color:#7dd3fc;font-size:13px;text-decoration:none}.source-link:hover{text-decoration:underline}.quality{max-width:180px}@media(max-width:600px){header,main{padding:9px}.result img{height:115px}}
</style></head><body><header><a href="/" style="color:#7dd3fc">← Akıllı Çiftliğe dön</a><h1>FEYZULLAH DEMİREL TARIM VE HAYVANCILIK</h1><div class="muted" style="font-size:18px;font-weight:900;letter-spacing:.6px;color:#86efac">İYİ SEYİRLER DİLER</div><div class="muted">Tek arama motoru • Canlı TV • Film • Dizi • Video • Müzik • Tek oynatıcı</div></header><main>
<section class="panel" id="ercanVpnPanel"><h2>🔐 ERCAN VPN</h2><div class="player-tools"><select id="ercanVpnCountry"><option value="TR">Türkiye</option><option value="DE">Almanya</option><option value="NL">Hollanda</option><option value="US">ABD</option><option value="GB">Birleşik Krallık</option><option value="FR">Fransa</option><option value="JP">Japonya</option></select><button type="button" onclick="ercanVpnConnect()">Bağlan</button><button type="button" onclick="ercanVpnDisconnect()">Bağlantıyı kes</button><button type="button" onclick="ercanVpnRefresh()">Durumu yenile</button></div><div id="ercanVpnStatus" class="status muted">VPN companion uygulaması bekleniyor. Bağlı durumu yalnızca Android VPN servisi doğrular.</div></section>
<section class="panel" id="playerPanel"><h2>▶️ TEK MEDYA OYNATICI</h2><div class="status" style="font-weight:800;text-align:center;letter-spacing:.3px">FEYZULLAH DEMİREL TARIM VE HAYVANCILIK İYİ SEYİRLER DİLER</div>
<div id="playerTitle" class="status">Henüz içerik seçilmedi.</div><div id="screenStage" class="screen-stage"><div id="idleScreen" style="color:#94a3b8;text-align:center;padding:45px 12px">▶<br>Oynatmak için arama sonucunu seçin</div><div id="embedWrap" style="display:none;width:100%"><iframe id="embedPlayer" class="player" allow="accelerometer;autoplay;clipboard-write;encrypted-media;gyroscope;picture-in-picture;web-share" allowfullscreen referrerpolicy="strict-origin-when-cross-origin"></iframe></div><video id="video" controls playsinline preload="none" style="display:none"></video><audio id="audio" controls preload="none" style="display:none"></audio></div><div class="player-tools"><button type="button" onclick="togglePlayerFullscreen()">⛶ TAM EKRAN</button><button type="button" id="airplayBtn" onclick="mediaAirPlay()">AirPlay</button><button type="button" id="castBtn" onclick="mediaCast()">Chromecast</button><select id="quality" class="quality" onchange="setQuality()"><option value="auto">Otomatik veri</option><option value="low">Veri tasarrufu</option><option value="lowest">En az ön yükleme</option></select><span id="playerStatus" class="muted">Arama sonucuna dokununca uygun içerik burada açılır.</span></div><div class="player-tools"><button type="button" onclick="mediaPrevious()">⏮ Önceki</button><button type="button" onclick="mediaNext()">Sonraki ⏭</button><button type="button" id="mediaFavoriteBtn" onclick="mediaToggleFavorite()">☆ Favoriye ekle</button><button type="button" onclick="mediaAddPlaylist()">＋ Listeye ekle</button><label style="display:flex;align-items:center;gap:6px">Hız <select id="mediaSpeed" onchange="mediaSetSpeed()"><option value="0.5">0.5×</option><option value="0.75">0.75×</option><option value="1" selected>1×</option><option value="1.25">1.25×</option><option value="1.5">1.5×</option><option value="1.75">1.75×</option><option value="2">2×</option></select></label><label style="display:flex;align-items:center;gap:6px">Ses <input id="mediaVolume" type="range" min="0" max="1" step="0.05" value="1" oninput="mediaSetVolume(this.value)" style="width:110px"></label></div><div class="player-tools"><select id="mediaSubtitleSelect" onchange="mediaSetSubtitleTrack()"><option value="-1">Altyazı kapalı / seç</option></select><select id="mediaAudioTrack" onchange="mediaSetAudioTrack()"><option value="-1">Varsayılan ses</option></select><input id="mediaSubtitleUrl" type="url" placeholder="Altyazı .vtt adresi" style="flex:1;min-width:190px"><input id="mediaSubtitleFile" type="file" accept=".vtt,text/vtt" style="max-width:220px"><button type="button" onclick="mediaLoadSubtitle()">Altyazı ekle</button><label>Altyazı gecikmesi (sn) <input id="mediaSubtitleOffset" type="number" step="0.1" value="0" style="width:90px" onchange="mediaSubtitleSync()"></label><span class="muted">WebVTT altyazı dosyası veya doğrudan URL.</span></div><div id="mediaLibraryPanel" class="status"><b>★ Favorilerim • Son izlenenler • Oynatma listeleri</b><div id="mediaLibraryStatus" class="muted">Giriş yapıldığında hesabınıza kaydedilir; oturum yoksa bu cihazda saklanır.</div><div id="mediaLibraryItems"></div><hr><b>📁 Çiftlik medya kütüphanesi</b><div class="player-tools"><select id="mediaLocalCategory"><option value="films">Filmler</option><option value="series">Diziler</option><option value="music">Müzik</option><option value="live_tv">Canlı TV kayıtları</option></select><button type="button" onclick="mediaLoadLocalLibrary()">Kütüphaneyi aç</button><input id="mediaUploadFile" type="file" accept="video/*,audio/*,.m3u8" style="max-width:240px"><button type="button" onclick="mediaUploadLocal()">Dosya yükle (yönetici)</button></div><div id="mediaLocalLibraryItems" class="muted">Sunucuya yüklenen medya dosyalarını burada listeleyebilirsiniz.</div></div></section><section class="panel"><h2>🔎 Her şeyi tek yerden ara</h2><div class="searchline"><input id="searchQuery" type="search" autocomplete="off" placeholder="Kanal, film, dizi, YouTube videosu, şarkı ara..."><select id="searchCategory" onchange="mediaToggleCountryFilter()"><option value="all">Hepsi</option><option value="live_tv">Canlı TV kanalları</option><option value="films">Filmler</option><option value="series">Diziler</option><option value="music">Müzik</option></select><select id="mediaTvCountry" style="display:none"><option value="">Tüm ülkeler</option><option value="TR">Türkiye</option><option value="US">ABD</option><option value="GB">Birleşik Krallık</option><option value="DE">Almanya</option><option value="FR">Fransa</option><option value="NL">Hollanda</option><option value="IT">İtalya</option><option value="ES">İspanya</option><option value="JP">Japonya</option><option value="CA">Kanada</option><option value="AU">Avustralya</option></select><button id="searchButton" onclick="runMediaSearch()">🔍 ARA</button></div><div class="muted">Arama; YouTube, Google Video, Bing Video, Brave, DuckDuckGo, Yandex Video, Dailymotion, açık canlı TV, Internet Archive, Wikimedia Commons, Apple Music/iTunes, Deezer, TVmaze ve TMDB kaynaklarını birleştirir. Film/dizi katalogları doğrudan yayın değil, bilgi sonucu da verebilir.</div><div id="providerStatus" class="status muted">Aramak istediğiniz adı yazıp ARA'ya dokunun.</div><div id="searchResults" class="results"></div></section>
</main><script>
const v=document.getElementById('video'),a=document.getElementById('audio');let searchItems=[],hls=null;
function mediaToggleCountryFilter(){const el=document.getElementById('mediaTvCountry');if(el)el.style.display=document.getElementById('searchCategory').value==='live_tv'?'inline-block':'none'}
function esc(s){return String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]))}
async function runMediaSearch(){const q=document.getElementById('searchQuery').value.trim();if(q.length<2){document.getElementById('providerStatus').textContent='En az 2 karakter yazın.';return}const btn=document.getElementById('searchButton');btn.disabled=true;btn.textContent='Aranıyor…';document.getElementById('searchResults').innerHTML='';document.getElementById('providerStatus').textContent='Video, film, dizi, müzik ve canlı TV kaynakları birlikte aranıyor…';try{const cat=document.getElementById('searchCategory').value;const controller=new AbortController();const timeoutId=setTimeout(()=>controller.abort(),45000);let r;try{const country=cat==='live_tv'?document.getElementById('mediaTvCountry')?.value||'':'';r=await fetch('/api/media/search?q='+encodeURIComponent(q)+'&category='+encodeURIComponent(cat)+'&limit=30&country='+encodeURIComponent(country),{signal:controller.signal})}finally{clearTimeout(timeoutId)}const raw=await r.text();let d;try{d=JSON.parse(raw)}catch(parseError){throw Error('Sunucu geçerli JSON döndürmedi (HTTP '+r.status+'). '+raw.slice(0,180).replace(/\s+/g,' '))}if(!r.ok)throw Error(d.detail||'Arama başarısız (HTTP '+r.status+')');searchItems=d.items||[];document.getElementById('providerStatus').innerHTML=(d.providers||[]).map(p=>`<span class="provider">${esc(p.provider)}: ${p.status==='ok'?'✓ sonuç alındı':p.status==='empty'?'sonuç yok':p.status==='skipped'?'API anahtarı/sağlayıcı yok':p.status==='error'?'erişim hatası':p.status==='timeout'?'zaman aşımı':'kısmi sonuç'}</span>`).join('')+`<div class="muted">${esc(d.notice||'')} • ${d.count||0} sonuç</div>`;document.getElementById('searchResults').innerHTML=searchItems.map((x,i)=>`<article class="result">${x.thumbnail?`<img loading="lazy" src="${esc(x.thumbnail)}" alt="">`:''}<span class="tag">${esc(x.source)}</span><span class="tag">${esc(x.kind||'medya')}</span><h3>${esc(x.title)}</h3><p>${esc(x.description||x.artist||x.creator||x.channel||'')}</p><button class="playbtn" onclick="playSearchResult(${i})">▶ ${x.play_url||x.embed_url?'OYNAT':'İÇERİĞİ AÇ'}</button>${x.url?`<a class="source-link" href="${esc(x.url)}" target="_blank" rel="noopener noreferrer">Kaynakta aç ↗</a>`:''}</article>`).join('')||'<p class="muted">Sonuç bulunamadı. Farklı bir arama kelimesi deneyin.</p>'}catch(e){document.getElementById('providerStatus').textContent='Arama hatası: '+(e.name==='AbortError'?'Arama 45 saniyede tamamlanmadı. Daha kısa bir sorgu deneyin.':e.message)}finally{btn.disabled=false;btn.textContent='🔍 ARA'}}
function togglePlayerFullscreen(){const stage=document.getElementById('screenStage');if(document.fullscreenElement){document.exitFullscreen?.()}else if(stage.requestFullscreen){stage.requestFullscreen().catch(()=>{document.getElementById('playerStatus').textContent='Tam ekran bu tarayıcıda açılamadı.'})}else{document.getElementById('playerStatus').textContent='Tarayıcınız tam ekran özelliğini desteklemiyor.'}}
function stopPlayers(){mediaHlsRetry=0;if(mediaProgressTimer){clearInterval(mediaProgressTimer);mediaProgressTimer=null}if(mediaSubtitleObjectUrl){URL.revokeObjectURL(mediaSubtitleObjectUrl);mediaSubtitleObjectUrl=null}v.querySelectorAll('track[data-user-subtitle]').forEach(x=>x.remove());if(hls){hls.destroy();hls=null}v.pause();a.pause();v.removeAttribute('src');a.removeAttribute('src');v.load();a.load();document.getElementById('embedPlayer').src='about:blank';document.getElementById('embedWrap').style.display='none';document.getElementById('idleScreen').style.display='none';v.style.display='none';a.style.display='none'}
function playSearchResult(i){const x=searchItems[i];if(!x)return;mediaPersistProgress(true);stopPlayers();mediaLastProgressSave=0;mediaCurrentIndex=i;mediaCurrentItem=x;mediaResumePosition=0;document.getElementById('playerTitle').textContent=x.title+' • '+x.source;document.getElementById('playerPanel').scrollIntoView({behavior:'smooth',block:'start'});document.getElementById('idleScreen').style.display='none';const st=mediaLocalLoad(),key=mediaKey(x),resume=st.resume?.[key];document.getElementById('mediaFavoriteBtn').textContent=st.favorites?.[key]?'★ Favorilerden çıkar':'☆ Favoriye ekle';mediaRememberItem(x);if(x.embed_url){document.getElementById('embedPlayer').src=x.embed_url+(x.embed_url.includes('?')?'&':'?')+'autoplay=1';document.getElementById('embedWrap').style.display='block';document.getElementById('playerStatus').textContent='Video '+x.source+' oynatıcısında açılıyor. Harici oynatıcıların geçmiş konumu sağlayıcıya bağlıdır.';return}if(x.play_url){let u=x.play_url;if(x.source==='Çiftlik medya kütüphanesi'){const token=localStorage.getItem('feyzullah_token')||'';if(token)u+=(u.includes('?')?'&':'?')+'access_token='+encodeURIComponent(token)}const isAudio=x.kind==='audio'||/\.(mp3|m4a|ogg|wav|flac)(\?|$)/i.test(u);const el=isAudio?a:v;el.style.display='block';if(!isAudio&&/\.m3u8(\?|$)/i.test(u)&&window.Hls&&Hls.isSupported()){hls=new Hls({enableWorker:true,lowLatencyMode:true});hls.loadSource(u);hls.attachMedia(v);hls.on(Hls.Events.MANIFEST_PARSED,()=>{mediaPopulateQuality();mediaRefreshSubtitleTracks();mediaRefreshAudioTracks();if(resume?.position_seconds>5&&resume.position_seconds<(resume.duration_seconds||Infinity)-10)v.currentTime=resume.position_seconds;v.play().catch(()=>{});mediaProgressStart()});hls.on(Hls.Events.SUBTITLE_TRACKS_UPDATED,mediaRefreshSubtitleTracks);hls.on(Hls.Events.AUDIO_TRACKS_UPDATED,mediaRefreshAudioTracks);hls.on(Hls.Events.ERROR,(_,data)=>{if(data.fatal&&mediaHlsRetry<2){mediaHlsRetry++;document.getElementById('playerStatus').textContent='Yayın bağlantısı yenileniyor ('+mediaHlsRetry+'/2)…';if(data.type===Hls.ErrorTypes.NETWORK_ERROR)hls.startLoad();else if(data.type===Hls.ErrorTypes.MEDIA_ERROR)hls.recoverMediaError();else document.getElementById('playerStatus').textContent='Bu yayın kaynağı yeniden başlatılamadı.'}else if(data.fatal)document.getElementById('playerStatus').textContent='Yayın yeniden bağlanamadı. Kaynak sayfasını deneyin.'})}else{el.src=u;el.load();el.addEventListener('loadedmetadata',function onMeta(){el.removeEventListener('loadedmetadata',onMeta);mediaRefreshSubtitleTracks();mediaRefreshAudioTracks();if(resume?.position_seconds>5&&resume.position_seconds<(el.duration||Infinity)-10)el.currentTime=resume.position_seconds;mediaProgressStart()});el.play().catch(()=>{document.getElementById('playerStatus').textContent='Oynatmak için oynat düğmesine dokunun. Kaynak tarayıcıda desteklenmiyor olabilir.'})}document.getElementById('playerStatus').textContent=x.kind==='live_tv'?'Canlı yayın bağlanıyor…':'Medya oynatılıyor…';return}document.getElementById('playerStatus').textContent='Bu sonuç katalog bilgisidir; doğrudan oynatılabilir yayın adresi sunmuyor. Kaynak sayfası açılıyor.';if(x.url)window.open(x.url,'_blank','noopener')}
function mediaPopulateQuality(){const sel=document.getElementById('quality');if(!sel)return;const current=sel.value;sel.innerHTML='<option value="auto">Otomatik kalite</option>';if(hls?.levels?.length){hls.levels.map((x,i)=>({i,h:x.height||0,b:x.bitrate||0})).sort((a,b)=>b.h-a.h).forEach(x=>{const o=document.createElement('option');o.value=String(x.i);o.textContent=(x.h?x.h+'p':'Seviye '+(x.i+1))+(x.b?' • '+Math.round(x.b/1000)+' kbps':'');sel.appendChild(o)})}if([...sel.options].some(o=>o.value===current))sel.value=current}
function mediaSelectQuality(){if(!hls)return;const value=document.getElementById('quality').value;hls.currentLevel=value==='auto'?-1:Number(value);document.getElementById('playerStatus').textContent=value==='auto'?'Otomatik HLS kalite seçimi etkin.':'HLS kalite seviyesi: '+document.getElementById('quality').selectedOptions[0].textContent}

function setQuality(){const q=document.getElementById('quality').value;if(hls&&q!=='auto'){mediaSelectQuality();return}v.preload='metadata';a.preload='metadata';document.getElementById('playerStatus').textContent='Otomatik kalite/ön yükleme. Kalite seçimi yalnızca HLS kaynağı destekliyorsa kullanılabilir.'}
v.addEventListener('timeupdate',()=>{if(mediaCurrentItem&&v.currentTime>0)mediaPersistProgress()});v.addEventListener('pause',()=>mediaPersistProgress(true));v.addEventListener('ended',()=>mediaPersistProgress(true));a.addEventListener('timeupdate',()=>{if(mediaCurrentItem&&a.currentTime>0)mediaPersistProgress()});a.addEventListener('pause',()=>mediaPersistProgress(true));a.addEventListener('ended',()=>mediaPersistProgress(true));
v.addEventListener('error',()=>{document.getElementById('playerStatus').textContent='Bu yayın tarayıcıda oynatılamadı. Kaynakta aç bağlantısını deneyin; bazı yayınlar bölge, CORS veya DRM kısıtlamalıdır.'});
a.addEventListener('error',()=>{document.getElementById('playerStatus').textContent='Bu ses dosyası tarayıcıda oynatılamadı. Kaynakta aç bağlantısını deneyin.'});
document.getElementById('searchQuery').addEventListener('keydown',e=>{if(e.key==='Enter')runMediaSearch()});
// V33.9.11 ERCAN MEDIA CENTER: player controls, resume, history, favorites and playlists.
let mediaCurrentIndex=-1, mediaProgressTimer=null, mediaHlsRetry=0, mediaQualityLevels=[], mediaLocalFiles=[], mediaCurrentItem=null, mediaResumePosition=0, mediaSubtitleObjectUrl=null, mediaCastSession=null, mediaLastProgressSave=0;
function mediaLocalKey(){return 'ercan_media_state_'+(localStorage.getItem('feyzullah_token')||'guest')}
function mediaLocalLoad(){try{return JSON.parse(localStorage.getItem(mediaLocalKey())||'{"favorites":{},"history":[],"playlists":[],"resume":{}}')}catch(e){return {favorites:{},history:[],playlists:[],resume:{}}}}
function mediaLocalSave(st){localStorage.setItem(mediaLocalKey(),JSON.stringify(st))}
function mediaAuthHeaders(){const t=localStorage.getItem('feyzullah_token')||'';return t?{'Authorization':'Bearer '+t,'Content-Type':'application/json'}:{'Content-Type':'application/json'}}
async function mediaApi(path,body,method='POST'){const opt=body===undefined?{headers:mediaAuthHeaders()}:{method,headers:mediaAuthHeaders(),body:JSON.stringify(body)};const r=await fetch(path,opt);let d={};try{d=await r.json()}catch{}if(!r.ok)throw Error(d.detail||('HTTP '+r.status));return d}
function mediaKey(x){return String(x?.id||x?.url||x?.play_url||x?.title||'').slice(0,900)}
function mediaSetSpeed(){const rate=Number(document.getElementById('mediaSpeed')?.value||1);[v,a].forEach(el=>{try{el.playbackRate=rate}catch(e){}})}
function mediaSetVolume(value){const n=Math.max(0,Math.min(1,Number(value)||0));[v,a].forEach(el=>{el.volume=n;el.muted=n===0})}
function mediaRememberItem(item){if(!item)return;const st=mediaLocalLoad(),key=mediaKey(item);st.history=[item,...(st.history||[]).filter(x=>mediaKey(x)!==key)].slice(0,100);mediaLocalSave(st);if(localStorage.getItem('feyzullah_token'))mediaApi('/api/media/history',{item,position_seconds:0,duration_seconds:0}).catch(()=>{});mediaRenderLibrary()}
function mediaPersistProgress(force=false){if(!mediaCurrentItem)return;const now=Date.now();if(!force&&now-mediaLastProgressSave<8000)return;mediaLastProgressSave=now;const el=mediaCurrentItem.kind==='audio'||mediaCurrentItem.source==='Deezer'||/\.(mp3|m4a|ogg|wav|flac)(\?|$)/i.test(mediaCurrentItem.play_url||'')?a:v;const pos=Number(el.currentTime)||0,dur=Number(el.duration)||0,key=mediaKey(mediaCurrentItem),st=mediaLocalLoad();st.resume=st.resume||{};st.resume[key]={position_seconds:pos,duration_seconds:dur};st.history=[mediaCurrentItem,...(st.history||[]).filter(x=>mediaKey(x)!==key)].slice(0,100);mediaLocalSave(st);if(localStorage.getItem('feyzullah_token'))mediaApi('/api/media/history',{item:mediaCurrentItem,position_seconds:pos,duration_seconds:dur}).catch(()=>{})}
function mediaProgressStart(){if(mediaProgressTimer)clearInterval(mediaProgressTimer);mediaProgressTimer=setInterval(mediaPersistProgress,10000)}
function mediaToggleFavorite(){if(!mediaCurrentItem)return;const st=mediaLocalLoad(),key=mediaKey(mediaCurrentItem),active=!st.favorites?.[key];st.favorites=st.favorites||{};if(active)st.favorites[key]=mediaCurrentItem;else delete st.favorites[key];mediaLocalSave(st);document.getElementById('mediaFavoriteBtn').textContent=active?'★ Favorilerden çıkar':'☆ Favoriye ekle';if(localStorage.getItem('feyzullah_token'))mediaApi('/api/media/favorite',{item:mediaCurrentItem,active}).catch(()=>{});mediaRenderLibrary()}
function mediaAddPlaylist(){if(!mediaCurrentItem)return;const st=mediaLocalLoad(),name=(prompt('Oynatma listesi adı:')||'').trim();if(!name)return;st.playlists=st.playlists||[];let p=st.playlists.find(x=>x.name===name);if(!p){p={name,items:[]};st.playlists.push(p)}if(!p.items.some(x=>mediaKey(x)===mediaKey(mediaCurrentItem)))p.items.push(mediaCurrentItem);mediaLocalSave(st);if(localStorage.getItem('feyzullah_token'))mediaApi('/api/media/playlist',{name,item:mediaCurrentItem}).then(d=>{if(d.id){p.id=d.id;mediaLocalSave(st)}}).catch(()=>{});mediaRenderLibrary()}
async function mediaRemovePlaylistItem(nameEncoded,keyEncoded){const name=decodeURIComponent(nameEncoded||''),key=decodeURIComponent(keyEncoded||''),st=mediaLocalLoad(),p=(st.playlists||[]).find(x=>x.name===name);if(!p)return;p.items=(p.items||[]).filter(x=>mediaKey(x)!==key);mediaLocalSave(st);if(p.id&&localStorage.getItem('feyzullah_token'))try{const r=await fetch('/api/media/playlist/'+p.id+'/item',{method:'DELETE',headers:mediaAuthHeaders(),body:JSON.stringify({media_key:key})});if(!r.ok)throw Error('HTTP '+r.status)}catch(e){alert('Sunucu listesinden çıkarılamadı: '+e.message)}mediaRenderLibrary()}
async function mediaRenamePlaylist(encoded){const oldName=decodeURIComponent(encoded||''),newName=(prompt('Yeni liste adı:',oldName)||'').trim();if(!newName||newName===oldName)return;const st=mediaLocalLoad(),p=(st.playlists||[]).find(x=>x.name===oldName);if(!p)return;p.name=newName;mediaLocalSave(st);if(p.id&&localStorage.getItem('feyzullah_token'))try{await mediaApi('/api/media/playlist/'+p.id,{name:newName},'PUT')}catch(e){alert('Sunucu listesi güncellenemedi: '+e.message)}mediaRenderLibrary()}
async function mediaDeletePlaylist(encoded){const name=decodeURIComponent(encoded||'');if(!confirm('“'+name+'” oynatma listesi silinsin mi?'))return;const st=mediaLocalLoad(),p=(st.playlists||[]).find(x=>x.name===name);st.playlists=(st.playlists||[]).filter(x=>x.name!==name);mediaLocalSave(st);if(p?.id&&localStorage.getItem('feyzullah_token'))try{await fetch('/api/media/playlist/'+p.id,{method:'DELETE',headers:mediaAuthHeaders()})}catch(e){}mediaRenderLibrary()}
function mediaPlayStored(encoded){const key=decodeURIComponent(encoded||''),st=mediaLocalLoad();let item=Object.values(st.favorites||{}).find(x=>mediaKey(x)===key)||(st.history||[]).find(x=>mediaKey(x)===key);for(const p of st.playlists||[])item=item||((p.items||[]).find(x=>mediaKey(x)===key));if(!item)return;searchItems=[item];playSearchResult(0)}
function mediaPrevious(){if(mediaCurrentIndex>0)playSearchResult(mediaCurrentIndex-1);else document.getElementById('playerStatus').textContent='Önceki içerik yok.'}
function mediaNext(){if(mediaCurrentIndex>=0&&mediaCurrentIndex<searchItems.length-1)playSearchResult(mediaCurrentIndex+1);else document.getElementById('playerStatus').textContent='Sonraki içerik yok.'}
function mediaLoadSubtitle(){const file=document.getElementById('mediaSubtitleFile')?.files?.[0],url=document.getElementById('mediaSubtitleUrl').value.trim();let src='';if(file){if(!/\.vtt$/i.test(file.name)&&file.type!=='text/vtt')return alert('Yalnızca WebVTT (.vtt) altyazı dosyası desteklenir.');if(mediaSubtitleObjectUrl)URL.revokeObjectURL(mediaSubtitleObjectUrl);mediaSubtitleObjectUrl=URL.createObjectURL(file);src=mediaSubtitleObjectUrl}else if(/^https?:\/\//i.test(url))src=url;else return alert('Bir WebVTT dosyası seçin veya geçerli https:// altyazı adresi girin.');let old=v.querySelector('track[data-user-subtitle]');if(old)old.remove();let track=document.createElement('track');track.kind='subtitles';track.label='Harici altyazı';track.srclang='tr';track.src=src;track.default=true;track.dataset.userSubtitle='1';v.appendChild(track);track.addEventListener('load',mediaSubtitleSync);document.getElementById('playerStatus').textContent='Altyazı eklendi. Oynatıcı/WebVTT desteğine bağlıdır.'}
function mediaSetSubtitleTrack(){const i=Number(document.getElementById('mediaSubtitleSelect')?.value??-1);if(hls&&hls.subtitleTracks){hls.subtitleTrack=i;return}Array.from(v.textTracks||[]).forEach((t,n)=>{t.mode=i===n?'showing':'disabled'})}
function mediaRefreshSubtitleTracks(){const sel=document.getElementById('mediaSubtitleSelect');if(!sel)return;const tracks=hls?.subtitleTracks||Array.from(v.textTracks||[]).map((t,i)=>({name:t.label||t.language||('Altyazı '+(i+1)),lang:t.language||'',i}));sel.innerHTML='<option value="-1">Altyazı kapalı / seç</option>';tracks.forEach((t,i)=>{const o=document.createElement('option');o.value=String(hls?.subtitleTracks?i:(t.i??i));o.textContent=t.name||t.lang||('Altyazı '+(i+1));sel.appendChild(o)})}
function mediaSetAudioTrack(){const i=Number(document.getElementById('mediaAudioTrack')?.value??-1);if(hls&&hls.audioTracks){hls.audioTrack=i;return}if(v.audioTracks)Array.from(v.audioTracks).forEach((t,n)=>{t.enabled=i===n})}
function mediaRefreshAudioTracks(){const sel=document.getElementById('mediaAudioTrack');if(!sel)return;const tracks=hls?.audioTracks||Array.from(v.audioTracks||[]);sel.innerHTML='<option value="-1">Varsayılan ses</option>';tracks.forEach((t,i)=>{const o=document.createElement('option');o.value=String(i);o.textContent=t.name||t.lang||('Ses '+(i+1));sel.appendChild(o)})}
function mediaSubtitleSync(){const offset=Number(document.getElementById('mediaSubtitleOffset')?.value||0),track=v.querySelector('track[data-user-subtitle]');if(!track?.track?.cues||!offset||typeof VTTCue==='undefined')return;const old=track.track;if(track.dataset.adjusted==='1')return;try{const adjusted=v.addTextTrack('subtitles','Zaman ayarlı','tr');adjusted.mode='showing';for(const cue of Array.from(old.cues)){const start=Math.max(0,cue.startTime+offset),end=Math.max(start+0.05,cue.endTime+offset);adjusted.addCue(new VTTCue(start,end,cue.text))}old.mode='disabled';track.dataset.adjusted='1';document.getElementById('playerStatus').textContent='Altyazı gecikmesi '+offset+' sn uygulandı.'}catch(e){document.getElementById('playerStatus').textContent='Bu tarayıcı altyazı zaman ayarını desteklemiyor.'}}
function mediaAirPlay(){if(typeof v.webkitShowPlaybackTargetPicker==='function'){v.webkitShowPlaybackTargetPicker();return}document.getElementById('playerStatus').textContent='AirPlay bu tarayıcı/cihazda desteklenmiyor. Uyumlu Apple cihazındaki Safari ile deneyin.'}
function mediaCast(){try{if(!window.cast?.framework){document.getElementById('playerStatus').textContent='Chromecast SDK yüklenmedi veya bu tarayıcı desteklemiyor.';return}const item=mediaCurrentItem;if(!item?.play_url){document.getElementById('playerStatus').textContent='Bu içerik doğrudan yayın adresi sağlamıyor; Chromecast için oynatılabilir URL gerekir.';return}const context=cast.framework.CastContext.getInstance();context.requestSession().then(()=>{const session=context.getCurrentSession(),info=new chrome.cast.media.MediaInfo(item.play_url,/\.m3u8(\?|$)/i.test(item.play_url)?'application/x-mpegURL':(item.kind==='audio'?'audio/mpeg':'video/mp4'));info.metadata=new chrome.cast.media.GenericMediaMetadata();info.metadata.title=item.title||'ERCAN MEDIA CENTER';info.metadata.subtitle=item.source||'';session.loadMedia(new chrome.cast.media.LoadRequest(info));}).catch(e=>{document.getElementById('playerStatus').textContent='Chromecast bağlantısı kurulamadı: '+(e.message||e)})}catch(e){document.getElementById('playerStatus').textContent='Chromecast kullanılamıyor: '+e.message}}
function ercanVpnNative(){return window.ErcanVPN||window.ercanVpn||null}
async function ercanVpnLoadCountries(){const api=ercanVpnNative();if(!api?.getCountries)return;try{const rows=await Promise.resolve(api.getCountries()),sel=document.getElementById('ercanVpnCountry');if(!Array.isArray(rows)||!sel)return;const old=sel.value;sel.innerHTML='';rows.forEach(x=>{const code=String(x.code||x.countryCode||'').toUpperCase(),name=String(x.name||x.countryName||code);if(!/^[A-Z]{2}$/.test(code))return;const o=document.createElement('option');o.value=code;o.textContent=name;sel.appendChild(o)});if([...sel.options].some(o=>o.value===old))sel.value=old}catch(e){}}
async function ercanVpnRefresh(){const box=document.getElementById('ercanVpnStatus'),api=ercanVpnNative();if(!api){box.textContent='VPN Companion bağlı değil. Android companion uygulamasını açın; gerçek VPN durumu tarayıcıdan tahmin edilmez.';return}try{const state=await Promise.resolve(api.getStatus?.());box.textContent=state?.connected?'Bağlı • '+(state.countryName||state.country||'Ülke doğrulandı'):'Bağlı değil • '+(state?.message||'VPN servisi bağlantı bildirmiyor.')}catch(e){box.textContent='VPN durumu okunamadı: '+e.message}}
async function ercanVpnConnect(){const country=document.getElementById('ercanVpnCountry').value,api=ercanVpnNative();if(api?.connect){try{await Promise.resolve(api.connect(country));await ercanVpnRefresh()}catch(e){document.getElementById('ercanVpnStatus').textContent='VPN bağlantısı başlatılamadı: '+e.message}return}document.getElementById('ercanVpnStatus').textContent='VPN Companion uygulamasına geçiş deneniyor. Bağlantı durumu companion doğrulayana kadar bağlı gösterilmeyecek.';location.href='ercanvpn://connect?country='+encodeURIComponent(country)}
async function ercanVpnDisconnect(){const api=ercanVpnNative();if(api?.disconnect){try{await Promise.resolve(api.disconnect());await ercanVpnRefresh()}catch(e){document.getElementById('ercanVpnStatus').textContent='VPN bağlantısı kesilemedi: '+e.message}return}location.href='ercanvpn://disconnect';document.getElementById('ercanVpnStatus').textContent='Bağlantı kesme isteği companion uygulamasına gönderildi; durumu yenileyin.'}
window.addEventListener('ercanvpnstatus',ercanVpnRefresh);
function mediaRenderLibrary(){const st=mediaLocalLoad(),box=document.getElementById('mediaLibraryItems');if(!box)return;let fav=Object.values(st.favorites||{}),hist=(st.history||[]).slice(0,8),lists=st.playlists||[];box.innerHTML='<div class="muted">★ '+fav.length+' favori • Son izlenen '+hist.length+' içerik • '+lists.length+' oynatma listesi</div><b>Favoriler</b><div>'+fav.slice(0,12).map(x=>`<button type="button" onclick="mediaPlayStored('${encodeURIComponent(mediaKey(x))}')">★ ${esc(x.title||'İçerik')}</button>`).join(' ')+'</div><b>Son izlenenler</b><div>'+hist.map(x=>`<button type="button" onclick="mediaPlayStored('${encodeURIComponent(mediaKey(x))}')">▶ ${esc(x.title||'İçerik')} <small>(${esc(x.source||'')})</small></button>`).join(' ')+'</div>'+lists.map(p=>`<div class="muted"><b>♫ ${esc(p.name)}</b> • ${(p.items||[]).length} içerik <button type="button" onclick="mediaRenamePlaylist('${encodeURIComponent(p.name)}')">Adını değiştir</button> <button type="button" onclick="mediaDeletePlaylist('${encodeURIComponent(p.name)}')">Listeyi sil</button><div>${(p.items||[]).map(x=>`<span><button type="button" onclick="mediaPlayStored('${encodeURIComponent(mediaKey(x))}')">▶ ${esc(x.title||'İçerik')}</button><button type="button" onclick="mediaRemovePlaylistItem('${encodeURIComponent(p.name)}','${encodeURIComponent(mediaKey(x))}')">Listeden çıkar</button></span>`).join(' ')}</div></div>`).join('');}
async function mediaLoadLocalLibrary(){const box=document.getElementById('mediaLocalLibraryItems'),category=document.getElementById('mediaLocalCategory').value;if(!localStorage.getItem('feyzullah_token')){box.textContent='Çiftlik kütüphanesini görüntülemek için önce Akıllı Çiftlik hesabınıza giriş yapın.';return}box.textContent='Kütüphane yükleniyor…';try{const d=await mediaApi('/api/media/library?category='+encodeURIComponent(category));mediaLocalFiles=(d.items||[]).map(x=>{const ext=(x.name.split('.').pop()||'').toLowerCase(),audio=['mp3','m4a','ogg','wav','flac'].includes(ext);return {id:'local:'+category+':'+x.name,title:x.name,source:'Çiftlik medya kütüphanesi',kind:audio?'audio':category==='live_tv'?'live_tv':'video',url:x.url,play_url:x.url,size_mb:x.size_mb}});box.innerHTML=mediaLocalFiles.map((x,i)=>`<div><button type=\"button\" onclick=\"mediaPlayLocal(${i})\">▶ ${esc(x.title)}</button> <small>${x.size_mb} MB</small></div>`).join('')||'Bu kategoride henüz dosya yok.'}catch(e){box.textContent=e.message}}
async function mediaUploadLocal(){const file=document.getElementById('mediaUploadFile')?.files?.[0],category=document.getElementById('mediaLocalCategory').value,box=document.getElementById('mediaLocalLibraryItems');if(!file){box.textContent='Önce yüklenecek medya dosyasını seçin.';return}const form=new FormData();form.append('file',file);box.textContent='Dosya yükleniyor…';try{const token=localStorage.getItem('feyzullah_token')||'',r=await fetch('/api/media/upload?category='+encodeURIComponent(category),{method:'POST',headers:token?{'Authorization':'Bearer '+token}:{},body:form}),d=await r.json();if(!r.ok)throw Error(d.detail||('HTTP '+r.status));document.getElementById('mediaUploadFile').value='';box.textContent='Yüklendi: '+d.name;await mediaLoadLocalLibrary()}catch(e){box.textContent='Yükleme başarısız: '+e.message}}
function mediaPlayLocal(index){const item=mediaLocalFiles[Number(index)];if(!item)return;searchItems.unshift(item);playSearchResult(0)}
async function mediaLoadServerState(){if(!localStorage.getItem('feyzullah_token')){mediaRenderLibrary();return}try{const d=await mediaApi('/api/media/state'),st=mediaLocalLoad();st.favorites={};(d.favorites||[]).forEach(x=>{st.favorites[mediaKey(x)]=x});st.history=(d.history||[]).map(x=>x.item||{});st.resume=st.resume||{};(d.history||[]).forEach(x=>{if(x.item)st.resume[mediaKey(x.item)]={position_seconds:x.position_seconds||0,duration_seconds:x.duration_seconds||0}});st.playlists=(d.playlists||[]).map(x=>({id:x.id,name:x.name,items:x.items||[]}));mediaLocalSave(st);document.getElementById('mediaLibraryStatus').textContent='Favoriler, geçmiş ve listeler hesabınızla eşitlendi.'}catch(e){document.getElementById('mediaLibraryStatus').textContent='Hesap eşitlemesi yapılamadı; bu cihazdaki kayıtlar kullanılmaya devam ediyor.'}mediaRenderLibrary()}
window.addEventListener('beforeunload',()=>{try{mediaPersistProgress(true)}catch(e){}});window.addEventListener('load',()=>{mediaLoadServerState();ercanVpnLoadCountries();ercanVpnRefresh();});mediaLoadServerState();mediaRenderLibrary();
window.__onGCastApiAvailable=function(available){if(available&&window.cast?.framework){cast.framework.CastContext.getInstance().setOptions({receiverApplicationId:chrome.cast.media.DEFAULT_MEDIA_RECEIVER_APP_ID,autoJoinPolicy:chrome.cast.AutoJoinPolicy.ORIGIN_SCOPED});document.getElementById('castBtn').disabled=false}else{const b=document.getElementById('castBtn');if(b)b.disabled=true}};
</script></body></html>'''

# V33.9.11 ERCAN MEDIA CENTER — self-contained federated search module.
# Prefer an installed package; otherwise load the embedded provider engine.
_ERCAN_MEDIA_SEARCH_ZLIB_B64 = """eNrNfdty3Eay4Du/ogxvDBom2E1Ksne25baOLMlntB7ZDpnynFmq3QF2V5Mw0UATQPMiihv7J47Yl3ldvezTvEnzI/slm5e6AugmKdsRxxEyybpmZWVlZmVmJYIg+EbOZJnUciYqmZTTYzEvSrFcHWbpNBbLLLlMDjMpFnKWJqIqVuVUVmJVQfPDS/Hs5ZPH34kXz54+fyyePPtu/9nLfhAEW/OyWIjJZL6qV6WcTES6WBZlLZI8L+qkTou82lJFv1RFHouiikUpY3FcLzLuPF/l07ooskr3zcrVZJpMjyXXT4t8uipLmdd9nsU03D8uZTL7Afo+u5DTVV2UsUiqybRYLDMJq4zFeZLWPMqqzLL0sL9Mykrq/qerogZQoIqKNaClPF3Jqq62tl49FiMRvCjepFmWDD7v74re39J8VpxX4rt9sbfb330ooOCLBw/FxRcPIvF4CRP/TR5+m9aDz+//1/79L8STY5hdDvbuPRA/JvOkTHXFs3Ka5C8Q1U9gabIc3L8fbO0/f/Hs+1f7MGvvfiy+iIT4VBwWq3wGewB4yOW0HuCaRZ0uZLGqeQPL4iyFjQXAqzRLZT6VWz89f/rs+8mz/6Chgv5i+SCIRdA/l4cL+qU4OqOfi0L9fKB+3l/9OYi2Hr96+tzrfl+1SlTvIx4u4V7zLJlCr62ZnIvJkax7gFIgqKRMFtXouyLH/QaoZcl/RcMtAf+VMLjGdb/Zi3/EeqEjhRk70FXwqpLlzuMjwF4wFK8ex+Kzz3qqVgBarq6j64gn6pdJWskJIGtSAVWuqp6qkEBPuSgV5EihLdA1sNy0ubqoT5302s+T7KRXHP4SixN5qXqmc5FWaQ4Tw8Zw5Syd1qpWtYDmIs0F1MLRmTU6HEDtuNUL/7tMZTYTuompQqo4S7KVVGP26Q9YdVdvPsEEOTVj2KmhzDqAz9LKBaM51x2mYJzJi7pMpvXkErZ5dSgnaZ7WaZJNZkmd9JBNqMmA2zzjpuLvxWofmoaVkItDOcPT8d9//P47cZ7Wx+IQmshBVZdpfiSS8wQ4h6wqYlY4zCKpgfEh5fWZCfbKsPdoeJaUr6vt6NFl/ZznfwrTv64+G8G/kNlVpLeThrDrBOQA1xhxcV/ms57GXiVtq655v6ymZbqsD37+arydzkYBwOHN/1bhYgdxEQXY7rOvLEDISfvPI5eOgOs2wXOoF+n5RrDPj1MQAlz7pchkzptAdIm/HVDVuJ9W1RIw7RIVd9oeiT2NKi75auSMA/TiDCM+GYnwKrRjNEGdyWV9HANxTXhLYyGrabKELR+J3Vh8kwCW1Y8tTY/LokJqLJP8SPZontgC4MBLG0LAQI+xi0cznY9HqFGz++W03QYsC4ypw4OEs4E4ef062NR5v1yt7RsGYburAbVzZpAadZo7Q64fzB3Ig8KF4KrRibbHbnmz+XVn8x23uQKKa6DPbnuJdXnZLnSIBXlwPyuSWdWzpDVEKtgWe+Ooje2LKcwmntEP0FE2Dm4o0f2beZdmWepAn8ZiCtrVUVFeIptcpJpPfip+KOUcJHR9LEUxn6dTONTi8Q/P++L5nAqJ/1diAYcKNwBoOM2BVaagw8DiqQnraWo8pb8tkyP5UOQS+B6MvkwAg0ldy8WyrkRdiGQKGlwFCkJ6BnDhqKAiwBZPa9YnalSqaEScfwTKGcphmZ/1gr+DsH319bMJADn59tnfUcwHUR87L7XwTM6hi8EOCzGLydaeIQ+DDixlg+O6XlbDweD8/Lx/VBRHmUyWadUH3W2gsDo4uz/gZcLkV60dCkAAo+APqjxdLmWNEJ7C37AJQX25lFiFalGBFYvk4qWsVlldQTHtTNweEMCHWvg/dKiSufyRJ4dxclgkDlOUoFxgQSkzeYYiMbj2qYuRctC7IIUmSGcI+3XEfxE4z2dBFAtVb2GHRlEXFSI7u0BmhuhTg8IGV9DlYBxZnrWZpNt7hSVAcFhkm+LhwS2iadwdUltC21MqRALMAe/PBHS4EnF3CiutQZ5vNff8ZhnfFGJY0ynDrgKt7+JGKE0AN4c1OyyUZVmUQdx5qIMFnAg4NU5ngZocXHiSy3lSfXgHN5989f4f4kyWoPpA8YdfM6iffXjXx2kY+0PA/vVWY9d9BQz0DgBS4klWug+uKVZU+VLVBg2dDOrymtQErm9QTpNrIqqoS5uFtTg/ncq0zuSkXOVVaw6qcskVWzGZtYfAe1HQ/6VIc03quO/MJSzN2tki4WBkzXxVive2fTWO3hxaerVqLH0GArN7GViDShWcgR/d07VxTdjpdksyEzeQcrxaHHbgFIvzJM1cGExhFyTTY7g6y6x7bcV5LkuFoJuWpAa63arcWVsMrZ8AFkE97BGh4bHnrRvy/sJQDs6hFP/qYK/OsocwBloZVgschFB3sLM3RoJWiETdGZZ4DYMr2PbVnOrPJruErqjhAbgRqZvI49tn4hAuzyfMAvEY20OLmFDLq3LWHc+HWzcftNYhI/jx1lzlHbttNk2tno7F5pagZyTAb7mpKYUrqHMiaDF6m65Q6gB3u6yHAeg/alVmz8xs9DeN2jpr/u41tlcP4JYSOcXOHut9bYzERqUG48a1QMk6eXOO95NHZyO7GH9MugJO1g+ykxfTojhJeTRqPVg31kmazzzFQVGbs2yPHGnd154x4RbyqTgJ6I5BJEiUHqDKdumKF/p5ra7IKGTTMznRJjo+iek8lWXM5roJqjxKlHiaF8qcUUPnUsP1i/JosJB1gm0GwTaZw7yhUQsawQotpc3hbliNrC5Cfwexy3ySLCvO5WxkbUh4ZTZAjkZBspqlRcArN4YqeymDi2YKM8A8DYmK1zKGwD+EebKQozkDhL8HcdAhKaGij5CVvQhvuxUaC3oKWL7d4hlXwyiVuSmdffj0iZvbyYymv1g+GCyW92NU3XPS36sFTAY1QBk4NmwAXLWhrkrfyAp0ELgE5MDg+x2I6FdFWfdAMx2BJnI4S8R82NvFRTUX3V6gMv75e/CJvwds4Isi/msPL9t1b66F8hvFJXajqKWlWRCHnTYGVdZJeTNYLN7ZNlLedmCq7VwHu+MDXvNYt+xFn8T9yc7/VPu+Xhv2LnF4uPTZuvkO9xzNtLmsxWPuIvTZQUUa9EcJwmCmLZpiyQZZYN198a2US6ABaKaNuVWhBgU4QCoBGgQp0YhRQiwIKrixzYo3EvSAYi4q2FljLSVqBX6SZURZ58cFaGR4F2TisRuNEu4KDymrrMECOBOc1zGyIyBD2SpdrKp0SoVMIFiWIXrqM6/pNVGHxZSpgfaqJ+8EQsH3A9RExPcvhdJGSAQmWE1i6sIqIw74PAZu8eRU2c5Wh70yOHh9sbu78/pibz7GGQX87zTqlxIY5FT2wiCMRShCc189GO7t7jIv6b6FunSZzOhmN2OC6C+Pl979ky6Z86B3ZVd2HYnH3z0VPRKpw94VwwulsFpHSjo1kSNig3l2MCbk2gMQGIHd0K5QJCFpFNTkEkCk661GJeyXHbcsznF/F4DsvXsgJ5ILdCowTaOwXvI1aA8vtqt6uaLbNOKFpBVwHAIr0Oe0orWwEVWJvFkxRQqz4gAIflnklafWYyNHP/1UfCPREmrcTa9e/rWizS/yjC0d87SEAzSX5w5/eQibv0jg3pgfqWNVwQlIs0wNmqX5CZo8oH+qfVrmoM7goKYZnRBQ7MhWUoMYh3IAAkfEWTXsfIY0eBO9RvhxMERcfs62RCyI1AXcrAVOG98KyRLd9k/1YBMm50V5gh6Mz0HqVGIJ1Y41XXm4YCCsQHKH7eq1NABjQXBoxpoVLD1E0bCr5UZLg7904PQdA1z7gnlVY0fX99ZTK2kI0E5Lnp7wQPXBn+Mx4AB+9nmzez7INxvw1o5pjSDtGwDf8GDVjuqv7uObkHiH60Gnpp4mnZr6RUtR71JZG8p5Xep7nlvBan4AjPDB57vE/Y0i3pRqTeW/oVh7EpyOVUXatCPCozVKtVI5LEE5ZEqWcU8nUSq4P5TmfQY5DjOkCwhxRFOr+SNVIUWoa4ImDmrFMG9S4tso+i3a/Hl6krKIu0nlYEf8yMWcbghaNItqD1tb6zR+OJYL4G19Mzft3/kgWaZawAXJlGkoYGteHMCJWCQgFJRMCI5kjtEDiP5AG2aDo6pUvw/ndBsgiX5Fk1yLq9NrboPaGrmLguEXVEDrDIZsi0VsL2HUdAEcOs3nsO9BmqpC2LK38qLWypa2uBJKR87hJe4O57dnxZFaiRFF2KSiv/Vluu0YxelHPVKm+MQbmKjTwdX1OAIF9CGGDYyw3F7IidKaXAEqjFZeLTNg5sGjIN7DMayWbm5B2+bWBIz7NtwjUFs7DLbx7Fu48TdoAJJeMxRbZ1SLv2l6AK5XXaL1M7BqVPANbOeQ7hXuMTS8w+n9hIGAtnTAbqI7+Muo9R1QuTCAahdMcBUuBPYooyteMRjnlNwK5xbT3hlazwi61msZAfIBe+LdA5/Wq1xWN55253SzOAFSDkDFBwTwUY+GnVBR0Il4gU0G6T5O5YFVnaARdObApi3XXYyCYUUSy9heYj0wATDBRTA8jZlvw9AMVhygSAR+B3MV+RH87Z/tKVx5anQPBPsv1x7ehovD+BhAaxxuJH8GWFG/lphlMj157tG+W/Md3hxZIr54/8836UlTydatk7JOq5qasxDpPAVd+Odj4E76UyrPX6HRDufVwqvIMklc19TGDm1faFOEPDPVW10GVRdgVC6hKdx3cDDvZMRqQZ3r22jHuonG1pJ+fbZI3tx8tw6C4JtSclhIhh7L/Z92+H6KHZKsOBJzOAmHgEfR04IgplOSsNESFR+ZLCIT5rH+NJmrb9SyCvhif/8nBN4X9u3TJNzj1HGzBAnLWHCO06A6xrsZHqpTdput00jhDqdPRiOQiFFAwTjMvw7GTiAGjI/35eJcWXHgb21Bbqm2WGnclR+n1aKM9EfCktaMQMoLUJ0pzsG2NaVaTbXLWC0WCVkOzM3/SwqW+Urf+u0oqq1RdTcN8rraNgNwre/abjMbXw1ltZ23lVR3ZD6duq+xuhs4c8t9nqZv0hts7QydUtvX2dKZUMUJH5VuBd4CsNJcaO5Zy10iReocXNGCrpvDuSb+Hm1z26PhFBdlepTmSdYQ4c61YNoNttLkDXEADpoYAIW0JBOWXZwqYlWt2T5L8qMVGz1sD1OoCKfRx7k1UCCjqb32TgpaBIhi1nmfrOfp1rzmLhcL5raL2eGNvBatyE6cx/6Lp19vCPJQTAAjOzoFA3Zfp24YTzu1EkmeHNdJ+eGdKLIFcHAy5S8U1X74dQW/LZPM97Jr3rpBN2LjZmyZ+p3hvIVaRFz8WJKNc3ZISqwOThksQFNJ+Qa1TCcURIIxJOrygfpSmk+z1UxOkhk68obBHKO0UE8y9Ag6wg5oR7Gyye39JjUJbmAj94Y94Su2S65QYvFIqyKL41nQeetwkI/XdMY42aBxnE+giIe4RV+1S15nMi23eyL3Gdk4mughu5tHbbPIRYuzfvg/7/8BM50Em3THeYAHZoiG3Ouh4XaaZbNvu1MvLM5kiSqZ0gqtKkh03sWGmQk3GK5PTwTGwAHD8llHPSfGimBTn3qwHJzff3Av2L44CJZFBRr6BIPAgnHkmFncCqUr+KzMMGPLixX71Ro1xjsllcR4HR/nZKqdJGnpVLGdaeyqsjjfOob51YjZ5WYW2T7BnUrnv5nHA2hqRefVaC9iJyps4tlELbDXiOpuXIKg5Q7gt38E98TVYT8tkAEMlOe3opBvNCndrhtrprqXYtXKp7KeW0MB35vQC3bjBVG7aNZdEd//48O7E/EkyYG/gnJ9S06oFxwr7Rp10wYaqV0u5QwO5ml/CjQyL7JZzxcieiU99YsiEt2mv4L5S9WWopGt65cnniyS5UjpkcgGuRQRoABzQ5B1h34laxU80eNSz4GPGwhsU7MEdX8wU0yPnbgYZ3zyNU+PG37XhyLJ6lGgfVu6HgonZPnSCol3BDQ22JlXml76wqwPk8bPJyNV081neRcMZeC82wDRNgBBP90pckkXRTNF5Gxdx+hwhmFHqloh3yKe8EyewGPLqAmvHPxdAl4d+xjNRMG52jmNZweODlTpUxREke/15/kQhIPhvbGvVSlqadoEgESHwbYDU0yuZGUlQNajOjoWAsRXg93rc3OZXH54l4vk5MO7f/0KB+j//a//3cCnv2Xv/5mdSHEosxQu2NkCFNh//RpEzZAoKzae/wD3XWAcIuH5TkBVyuhqJ6tUGxNcRHpmgkaF4uSaI3iCRIMLR7fAtia6hRdvLDWtZV13BltpJBoOfmfe4/FwNdz1eivEDUOZtxlynb5splBseELGgwnW9vREMcZ+xaB/XOh4GhXwiKFwiJaQCkIOadRliEEuCn9ig0SRr6arcEudH7c/+iob3aFFo4QhM+Uhj6TDyywsencb3UlTCKOOF0gcl9V8TaQHpp88RFVOeVCnjOBZPyicCQ6l4S7OUQ8poDfsmNE45Pg8uIvjksbKyLdbHcuSy/W20RCMNLQQucNQaRg1QG5WK5To0EOjgKMv1gAdprNwaOY0RmYTeSAw8mAH5toW4TCE/+NjMcEiq4ck5bABRUcmjDJ0mA+UWixwGGsDDW5jRSHO0ApzFlQEBcBD1hUqgwW3iZhVM+5JFPGvoJmbvoSSMHRhR0IYCnoJ59CgjvoTIbIh+FMh1+moeI67QF3kL1C59hqlyao+1oXeimerMmliDpjUUX3cRJxuSMWWYrpq9QzmFhqi3l+FOkKUCORAFY59msMic1TCtCpIRdZdaXS3QI9FZd5QpmtjHVSkwh++x9iJZAYXkzqFvZqlpZzWGFh0XuHDEBAWZDxFeUpRZqICpaIo8R3Iq5d/xXLQUGmsjCyHbQ9K+AhIm1woek1Q3+HBivRCtISixcBP+6IwbIV3hkrEE1aotn8oB7rUeUTX8XBv9JY6vMYebwdn8I9jPKPeweOd/5HsvNnd+W+TnfHVF/F1FNJrX08LW2h4TRwpARzeMpIUDxMolmWxWvb2PAM6jmrkDAekKHGjru3uO9JYWPHTsIz/QE+WVUQLDbCjXv/wOBT6UvWFeuBCkYTqDFONspNX1WohZ1phNnbyJVtu2+9azTsOEp3A2qXMyTaNv+p4DKSnOjkiEpb9OZx5uI/gw8bk9SG/Vux/9ujLQfJVGNNM/GJRvMUfPzqy4LiU80nXC8nXh1il3mH2DoLX4TjqwZjR6z0YEqb2R2war+24HVotVqr3f310aeALvJ7toTb1XtRhG55TZ0++DQYg3PSYinqQOLBkc0c+1xpcTz8OlX4cxoYew6hLQzc6ijJxh8pOzoFwhKlofePX1bZu6GODWnYjABVArhZfivu4AqfkK/H57m4XyouqZm5AL+4J21EfS1F/cvQcjSyocWJZQ34phscPMMJ/rIDB6adsXH6Y5kfq19lqeoL/jgpVoIL6DsvkTA9zCUJPXvh/lKtuNPMzOd4m5oqfhrFliv5jbjwxHWNgcT+ZzXrmmbWraZCCsVG/QPViG5QLuoAkh1XvOKmOaTCM6tNKBf04GN7fbfoNWFi6mkaIe68VBsuGWMTjWo04V9LcOnRYWBA+2tKihZdbC4yRd2g2yQw6b22hgcUdN8UbpEe3xCAabV5/FE9pSY7RreXGtstk9tb4nfCPO5vNqF4JHiJ0JXbuEoCAb3fJvR/aW12oaQMI4WscV/BNh99JEhHRZQyqlU0Jywia0NiUmg9Nw69fPv7p2eTHZ49fPvmLdkRgPxu9i/vb6tDZsumy8JGKYq5TDps9a3KHAT+IgUmuwtNweOqck/BcHobXUNCJCUZry718BwwWJ2HD+RPSZRYbKccKtPqLLE8k1CnDAbvCG88ZT1ZZluQf3mWzD+8eqseNWQJ6YHKZJyfikN435vWHd9AYmpR9Z9eUWdV1b6OiYLVu10HSwh7gd3C2p5CokOuo7CrJhkEtWRtCjrHd5XjlvV0nYDlU5ghY9/5LQha/PkU3CpTVJZUlc6lmGoaLgvPdwEZZEaTzh4T/sfPj6tDwwZ394kQCN6T3v+HjKUaWwhAYCZNO6UowQAtueG1SftiXn8YjEyqSYpFuixUdKSug7+x3bRBMFbCOi1ik/D4vjcnpI3NQ33AteG894NiacfNKexHTFXa89QcQnU8RzF0Y0ej2uoHDxAD/UYp5YDQw1hvpswNmBHi673bCO1briLIuxmRPUWtOUJfgwCzQyjSTH35Ns34HJ/PiPTy2b9CiXQBwCDT1u7YExkk41LjRB0H5ETUtHmWGuo8zj9AnhtL5VX/oBuc3SdOlwUbpZCPZTqYJCKlKX9A30a9F+R9DwF1beleiZbXxD5CJ/04D8+m6pTSE4TfTvkP6LVq/w+kL3UWjPPOAFT3mBY+XaRT6oUg3CEybvcGJbiJxWR8uWF+ENRBZW9o1BH3dgMMXnHdC8UeJSzXambHXvv9Ht9jsr6Mlvi78AbT0dxr4PyctuYsmWnKB/Xhasvcw1hg8apIXNRGRlfXXjYlvJp61OP0o4lGjfSTxEPXMkjS7XBSoW9wmRPIHNgE9tb3Utj00L6L4jZQy/+yg+UfOyKhrLIBp5ZqDOsMlHdrcJF5F6EDi4lN0EKnYLDVRVjrYaOjejllbyTvM8hLOMbcYzZfOYrahO/fa2FilJ/e/2MXbWYz/KIVCH1rBLZy8Xsbg69iSCfEhP527r1VRVxPF13E4sckD40nezXGc2gdU1Vqetl8b6cBNtAZ8VESm8jgVTfeSz7o7UU4mTQ5mvE0wJHtEQmeoYdj9ut74OwxYjiPNJSadhyBs9G/4R4rbeEJ8b0iTZK0Xw+9gb/RDsRFjfJlv4K25bMc9YpfepE50QK31mjQ8Jz0zDJFz6OWGCC1xs+7cxKLjKCnOu70ezSjHux3/W2hkfuDiTMo3srwND/xWBYg/pR6C2JSOEV9JfnZpEmypyHnXq8W28k1B4mGSZcT4iQNGN7FAguP34H40UFOLOlWsbjNDugPbwd/abIdeKEzS2U2cR7e7gfkk2eFq4Y5FBWEzGpyfH3jtqKTV8Bbch7A31J5NDWd0FwbEb0E28xwGsOXx38BvDH0wMzEzcwxAo5+xgDoNFRG32rpchfCrvKYYnDjhYGyGrlnZGkkzHXolgqCqbbj1cn8/jtJxmu7ETNjZhj6MSVIjIaCDCCHXLyo7XUxINlCifB7UHEvCu7md1Ip8B0rDk2RMx9qlrqy0aX50y0sEsK6voXVYeR5BowByQkCX2fGz+lVObDPRnmF0+oIqKC+WRfXb1MAQwbnTRcV1OXrCVTtumhZDa31FPzWq4k9f/vjkPpsAa+dKcAevJe+C9VtC+9J1XE4zUJFHtPMHP+P/P1vAzaU+m04Qq6pIVaOf8wYHJzpJdapVpobdqJE+rkm24cJhwEpwuExx7ueo0q/rdVMnJyYmxdr8fL5jfNYgreRYGB3Sli1NWQt49JqEbZej7Vs3B3N4cWu0pEyTnSw5lFnY9cBW+0+R96N//xY+VN2Fc3ZslmdspLSD/76ePx0H5WCiA9WNOC/XTep6Dsn5eZNvkEQkOWqVg7DtEGy7DI3jwz/vJK04DGiNj1AndbuVY5sdanax4xH97lG9y7v7vcXywVtMa/4WU5dHvdeP3v4XFeah0iN3uBd1NMpd/G32XU9HoPot+OEdzJSkEhvP9e0Fw1Noj//+vRB/2X/xVzfJvzZTUE7/h2JWSLYDAJtLF44CzSIDQ0hMjoTfIhsciO4iIXS+oFMMo2PYw/WigySu7+mnsoGRHDgcEijbHXfuICoOs2J6wokY3fCWWXq2Vk6wKG5LCNIfvhxAV0zhzb90Sg3Xx8HzWySvgSe5AZrJJGkDtCk0x8hJmhB5WhMSSrjTSGf+UWAovBAgNMnmkB6c1+fXmyBBMcSA9Hjm7egjpm5O351blUMlOOepyqSLrY2LP3b/vBc58Q1VQwKxtuC03o1aM3VpDA2haxO+doFzy2gmHdEQbjcjmRrRNYNs8GhN8EVn1p+1n+XQ0UEUi1bJyWnV6qsQoOt7jYAiOvDKGrKazY7C+AArxp647laElkC5XVFkt1QpmhFHBh8fEbJldu53jtyyak+nqjP6o8Kb7BWeVRXYmLtEMd1CSWmqKl3iZ31U093fp91a1N3VR8l55SZnqPfmddU7pcxOVtI/4fSACSdJw6xlpUopqbtgLjQ4TwAfZjabYqpApF38ZgzaEY9QSUk5ak4LeP1Ue81L+VOfmEA/SHMvJ9/Pr89fVyYfnx4tFvMsOapG0O7Vd8+ffP/02Zr+zlRU509n1gUSWg9tIpF5LOXnSHPnMROGsevm7hsns9V6XE2bPLWKqqaQELE8LqGjOJbZsuKUnUv8ZNGK7Bt46Cm7uTgvyllFNke04ScmcwVf+jgrHDUZKSj5iBlxjxRHDUjVvO98paJYLPGrJ5g/UT0wo3YHw8+bD8qoobt8ZdW8unALzUtivfhrnzs3caLG9cwcug2+yVI0q8JxilxO9LnozfPYNL3NG0egxZerXCmAZs8rzHuZY8b47JISa5qXCUDlwO4Ep9Qb1JRwT6zQY2fIGqCHSxHlnFCWUZVbW/1lxhqJed6f0IO9yYRj4fHGWXkPQAL/mw+NHL9+MtEb0qsFzUxg2L4zx5CXQQhbdeVjseM6eQAot4B6OeslY/GTDvjPUWmG1lsrO347qBB7tGOLcOSOaAtsvC78wJ2mw7lO6F7jbfYgbBrUCEBzMaOEP90XLGzYZOXuyG2fLfWwpZxMyPFnUD0VBO5jEv5uEhG4exQ3fEBDhfsjmfZU8/VHikyN8xyNfP7msvjZMISvPzqnQ2WzVTlb1Ku8WB0SKnbOjxPp1dCjlZfCHY1f57nRRQ7vo8PbBx6rL+jNDPVwmu0DULUqQLpZn02p4c6pCmPKsxGt0yUo6SBiFL8bYlQCN7mFN6gudfOCd+mZmPgTyoYfuxD9iQsL3O6mFVhg5wHlre3B5JHZq+uhuEI1DAsPhnt/3h1fN9KgOW8jAcjYbItWVygYDVVhNLDwN74chi5/QZNzXuQ75/JQVNNjuZCcw3iRZGjS5VdIFT1KylL+3uBqqV55Kznqee1IrNmwMfVRMXp9qOx6VLQ23MW8OeQvl4385u0s6nS5mLkvF3iNrvTlNn1eng6Htw/1ETu4YXRhCAycqpd+AnHzZ7uM/M2U0n3LXNdOwOYErQasUtrTT3+OVIospfGCgjj6s2EpqoUjqlvfkyT/q7ESwUWzpi924ad68DNmioPNQEjDVVTqNNaYOddEUuBXu07QEaC3GyCa/FLQxzUOfOEbN5J3x63cmrGffC92U+TEfnay9vnx+WbcEUsfdwUTxl1RYe3RWyIqXmfzizsChWLfZ85MUyNeo8vRWkwK8Ltj0EVZB4/5TVj5jVhws13bfOa/bYk3UcV/oiWbZO2/+Vj4xORM4eR+94/D2NVlFL2Zk9rM+oNMhfUOj0QbOeR199aFr3UbjjzFXX8rRzmvFrI8crV9zKONX7TbnIxbZ0lHIU9ARF7uEvyiy+bsJXQR2pTCu+N6BHfj7vvRelkuvBgQpfPtfY5GE635Dd17DOfnpk/a4LJUGAp9khUVDf7aH34mVyfstp9cvbdrzcBcyemx3S+GzXOah3N16zzb8HO8Xo1t6hJaIeSO7Xzft9KdvEscgdRSrZAiXLUqCD5Oo/oobWrcqQM7yq5HzAf6L3oHDgX2ATQRd4c27G+S2tq77JMPgGo/vgFpilRuq4h2f4btMb9Xev9P/H6xqD68yz+8K+H/iVhlCSZc4ZdLs/TDr1I/W8owkVxuXzPBHfXo/f/FPFdpll4WZd9FuaKsKaoXWU8bd54WbNwtshnl///L/v4P5iMAogAUElIT/vQG/LbKkzPgx6SumBeahsX0q+NVjd9K6OFpGqnvkvKc6vPY1Qg/qxltbWk7KXqVjatplaenpIu6n6RC3y59EgO3vf1BKqsCY8vOTwS3DLT4iRr7wWhaLXv8kKhKtP9Rqg6OG+EAEcJN78cIvwABg/F3vfv+JwDoQ7+k6apU2CZDTex+o8n7SFTU+roqLoOvkjhex9d3yJdLlUibjYtHo7cXYTHhgAbKw6Gb2VyYtsiA3Si3S4gaEsD9YE+Rp9NEJXuZdKSh/jRwkygYt6rq19g5dzhtJe0dPPrTOOqt6sXk4OfRePvtXCZIW2+r9O1y+baU87cqr8fo4Oc/jT+jq2hsx4rWTmF+75e8sODRn9qfUQrMQ9uAEmR1g65ysXijKgzY/uhZ2Bu3EnSrvwdBw/PQBbO2xg1JbaJUrPBb+/O5Qev18E3Qd0SKBYj6s1HvwH8hvB67/lvhG+Be8xyYvUAcgdKZv9ajZ/ejboH1/jjG30ZQiz6tdgUeQbLSZlOzG2iaE1PWKTPvOm/RZsbU7TlitugHaNgggUlelIsbHRbN9XPXGq37JIadjw+5fvXg4PX5GIezU0XaUn+BSQ721DeF5NzeXxnE4VqX3e+yeRhTw+OOHOCGhmp3/cfz1Nb1YDoLsp0aX3B28Iv3dOdP/WVP2/VeV1eFX+yM6KQ/TV9OHaKL3A4ORPd9H7kpf7DV/MwLrtmTH7fi4/y9M0uAOgGVvy8qT1s7sTSVT9BeoafXI4ysV0BNQrzTNRML806HW9xrmm16jQWpI4Zj61sZ4da7aZkaGpPwFXfztljYbwk5K3F1EfvZOUPd6uxp9acRbu6r9qSIulpln/RV1xxXFzXxw2q16F0cKB1y7JxG73ukqHdKPrAHWgltNPbkKo0+FNwSuW7hJIQlQlUjahK94By1KiGmHZmbRe5QupE/nlGIbS+3k669BRB8HdkIAjdpAKD6mR7mw4ZJfqmGxkx5dxoXj55TTTn1WvcGx3TtJLa2FGFvDfyLc2WgfcLPd6+0DQe3svk5AvOR6ZhyhhPNY+JgYz/wDPVMwc2c4wZYBIPSX5knyxV9fUB/YPHCfEPI8XQE7STmOg83T6eeNYvPxJ+buceVCRSaHsJloacuaM3hYKPSqb0UPRSKi8TCOq9i0fRPxe6bwLj1ncJYtJyKsWh7EGP1qCYW7BeEny+efh3r9477P3FGygz2SJxJcZks4WYG1EWXtWyBuTEbCSgwscTlh3dTbFfStU4cpmUm//VrnZZp2Rdfr0T9/p8LCkRAiFNRU98T9ex+ho2+VX509643Kz78Wq4wh7jKzWnaPxQF3A9rmJ8vivSJhARXzFk5AXKcg8Gi+wz2xI8wIrgqA0ZwvfX/AdVnegA="""

def _ercan_media_search_module():
    try:
        from media_center.search import search_all
        import media_center.search as media_search
        return media_search
    except Exception:
        import sys
        package = sys.modules.get("media_center")
        if package is None:
            package = types.ModuleType("media_center")
            package.__path__ = []
            sys.modules["media_center"] = package
        module = sys.modules.get("media_center.search")
        if module is None:
            source = zlib.decompress(base64.b64decode(_ERCAN_MEDIA_SEARCH_ZLIB_B64)).decode("utf-8")
            module = types.ModuleType("media_center.search")
            module.__file__ = str(APP_DIR / "<embedded-media-search>")
            module.__package__ = "media_center"
            sys.modules["media_center.search"] = module
            package.search = module
            exec(compile(source, module.__file__, "exec"), module.__dict__)
        return module


@app.get("/api/media/search")
def v33_media_search(q: str = Query(..., min_length=2, max_length=100), category: str = Query(default="all"), limit: int = Query(default=15, ge=1, le=30), country: str = Query(default="")):
    if category not in {"all", "films", "series", "music", "live_tv"}:
        raise HTTPException(400, "Geçersiz arama kategorisi.")
    try:
        media_search = _ercan_media_search_module()
        return media_search.search_all(q.strip(), category, limit, country)
    except Exception as exc:
        # Keep the API response JSON even if a provider or unexpected backend error occurs.
        import traceback
        print("ERCAN MEDIA SEARCH ERROR:", traceback.format_exc(), flush=True)
        return JSONResponse(status_code=503, content={
            "query": q.strip(), "category": category, "count": 0, "items": [],
            "providers": [{"provider": "Medya arama sunucusu", "status": "error",
                           "message": f"{type(exc).__name__}: {str(exc)[:180]}"}],
            "detail": "Medya arama modülü başlatılamadı. Render Logs bölümündeki ERCAN MEDIA SEARCH ERROR kaydını kontrol edin."
        })

@app.get("/api/media/health")
def v33_media_health():
    """Lightweight diagnostics; never exposes secret API keys."""
    try:
        media_search = _ercan_media_search_module()
        return {"status": "ready", "module": str(getattr(media_search, "__file__", "embedded")),
                "providers": {
                    "youtube": "api_key" if os.getenv("YOUTUBE_API_KEY", "").strip() else "public_fallback",
                    "tmdb": "configured" if os.getenv("TMDB_API_KEY", "").strip() else "optional_key_missing",
                    "tvmaze": "keyless_public_api",
                    "brave": "configured" if (os.getenv("BRAVE_SEARCH_API_KEY", "").strip() or os.getenv("BRAVE_API_KEY", "").strip()) else "optional_key_missing",
                    "searchapi": "configured" if os.getenv("SEARCHAPI_API_KEY", "").strip() else "optional_key_missing"
                }, "categories": ["all", "live_tv", "films", "series", "music"],
                "search_providers": ["YouTube", "Bing Video", "DuckDuckGo Video", "Dailymotion", "Internet Archive", "Wikimedia Commons", "Apple Music/iTunes", "Deezer", "TVmaze", "TMDB", "Açık Canlı TV", "Brave Video Search", "Google Video", "Yandex Video"],
                "vpn_companion": "android_js_bridge_required", "vpn_connection": "only_reported_by_verified_companion"}
    except Exception as exc:
        import traceback
        print("ERCAN MEDIA HEALTH ERROR:", traceback.format_exc(), flush=True)
        return JSONResponse(status_code=503, content={"status": "error", "detail": f"{type(exc).__name__}: {str(exc)[:180]}"})


@app.get("/api/media/state")
def v3398_media_state(uid: int = Depends(current_user)):
    with db() as c:
        favorites=[json.loads(r[0] or "{}") for r in c.execute("SELECT item_json FROM media_favorites WHERE user_id=? ORDER BY created_at DESC",(uid,)).fetchall()]
        history=[dict(r) for r in c.execute("SELECT item_json,position_seconds,duration_seconds,last_played FROM media_history WHERE user_id=? ORDER BY last_played DESC LIMIT 100",(uid,)).fetchall()]
        for row in history:
            try: row["item"]=json.loads(row.pop("item_json") or "{}")
            except Exception: row["item"]={}
        playlists=[dict(r) for r in c.execute("SELECT id,name,created_at FROM media_playlists WHERE user_id=? ORDER BY name",(uid,)).fetchall()]
        for p in playlists:
            p["items"]=[json.loads(x[0] or "{}") for x in c.execute("SELECT item_json FROM media_playlist_items WHERE playlist_id=? ORDER BY added_at",(p["id"],)).fetchall()]
    return {"favorites":favorites,"history":history,"playlists":playlists}

@app.post("/api/media/favorite")
def v3398_media_favorite(data: dict = Body(...), uid: int = Depends(current_user)):
    item=data.get("item") if isinstance(data.get("item"),dict) else {}
    key=str(item.get("id") or item.get("url") or item.get("play_url") or item.get("title") or "").strip()[:900]
    if not key: raise HTTPException(400,"Medya kimliği eksik.")
    with db() as c:
        if data.get("active"):
            c.execute("INSERT INTO media_favorites(user_id,media_key,created_at,item_json) VALUES(?,?,?,?) ON CONFLICT(user_id,media_key) DO UPDATE SET item_json=excluded.item_json",(uid,key,datetime.now(timezone.utc).isoformat(),json.dumps(item,ensure_ascii=False)))
        else: c.execute("DELETE FROM media_favorites WHERE user_id=? AND media_key=?",(uid,key))
    return {"ok":True,"active":bool(data.get("active"))}

@app.post("/api/media/history")
def v3398_media_history(data: dict = Body(...), uid: int = Depends(current_user)):
    item=data.get("item") if isinstance(data.get("item"),dict) else {}
    key=str(item.get("id") or item.get("url") or item.get("play_url") or item.get("title") or "").strip()[:900]
    if not key: raise HTTPException(400,"Medya kimliği eksik.")
    pos=max(0.0,min(float(data.get("position_seconds") or 0),10**9)); dur=max(0.0,min(float(data.get("duration_seconds") or 0),10**9)); now=datetime.now(timezone.utc).isoformat()
    with db() as c:
        c.execute("INSERT INTO media_history(user_id,media_key,item_json,position_seconds,duration_seconds,last_played) VALUES(?,?,?,?,?,?) ON CONFLICT(user_id,media_key) DO UPDATE SET item_json=excluded.item_json,position_seconds=excluded.position_seconds,duration_seconds=excluded.duration_seconds,last_played=excluded.last_played",(uid,key,json.dumps(item,ensure_ascii=False),pos,dur,now))
        c.execute("DELETE FROM media_history WHERE user_id=? AND id NOT IN (SELECT id FROM media_history WHERE user_id=? ORDER BY last_played DESC LIMIT 100)",(uid,uid))
    return {"ok":True}

@app.post("/api/media/playlist")
def v3398_media_playlist(data: dict = Body(...), uid: int = Depends(current_user)):
    name=str(data.get("name") or "").strip()[:80]
    item=data.get("item") if isinstance(data.get("item"),dict) else None
    if not name: raise HTTPException(400,"Oynatma listesi adı gerekli.")
    now=datetime.now(timezone.utc).isoformat()
    with db() as c:
        c.execute("INSERT OR IGNORE INTO media_playlists(user_id,name,created_at) VALUES(?,?,?)",(uid,name,now))
        p=c.execute("SELECT id FROM media_playlists WHERE user_id=? AND name=?",(uid,name)).fetchone()
        if item:
            key=str(item.get("id") or item.get("url") or item.get("play_url") or item.get("title") or "").strip()[:900]
            if key:c.execute("INSERT OR IGNORE INTO media_playlist_items(playlist_id,media_key,item_json,added_at) VALUES(?,?,?,?)",(p["id"],key,json.dumps(item,ensure_ascii=False),now))
        rows=c.execute("SELECT COUNT(*) FROM media_playlist_items WHERE playlist_id=?",(p["id"],)).fetchone()
    return {"ok":True,"id":p["id"],"name":name,"count":rows[0]}

@app.put("/api/media/playlist/{playlist_id}")
def v33911_media_playlist_rename(playlist_id:int,data:dict=Body(...),uid:int=Depends(current_user)):
    name=str(data.get("name") or "").strip()[:80]
    if not name: raise HTTPException(400,"Yeni oynatma listesi adı gerekli.")
    with db() as c:
        try: cur=c.execute("UPDATE media_playlists SET name=? WHERE id=? AND user_id=?",(name,playlist_id,uid))
        except sqlite3.IntegrityError: raise HTTPException(409,"Bu isimde bir oynatma listesi zaten var.")
        if not cur.rowcount: raise HTTPException(404,"Oynatma listesi bulunamadı.")
    return {"ok":True,"id":playlist_id,"name":name}

@app.delete("/api/media/playlist/{playlist_id}")
def v33911_media_playlist_delete(playlist_id:int,uid:int=Depends(current_user)):
    with db() as c:
        cur=c.execute("DELETE FROM media_playlists WHERE id=? AND user_id=?",(playlist_id,uid))
        if not cur.rowcount: raise HTTPException(404,"Oynatma listesi bulunamadı.")
    return {"ok":True,"deleted":playlist_id}

@app.delete("/api/media/playlist/{playlist_id}/item")
def v33911_media_playlist_item_delete(playlist_id:int,data:dict=Body(...),uid:int=Depends(current_user)):
    key=str(data.get("media_key") or "").strip()[:900]
    if not key: raise HTTPException(400,"Medya kimliği eksik.")
    with db() as c:
        owner=c.execute("SELECT id FROM media_playlists WHERE id=? AND user_id=?",(playlist_id,uid)).fetchone()
        if not owner: raise HTTPException(404,"Oynatma listesi bulunamadı.")
        cur=c.execute("DELETE FROM media_playlist_items WHERE playlist_id=? AND media_key=?",(playlist_id,key))
    return {"ok":True,"removed":bool(cur.rowcount)}

@app.get("/media", response_class=HTMLResponse)
def v31_media_center():
    return HTMLResponse(V33_MEDIA_HTML)

@app.get("/api/media/library")
def v31_media_library(category: str, uid: int = Depends(current_user)):
    if category not in V31_MEDIA_CATEGORIES: raise HTTPException(400, "Geçersiz kategori.")
    folder = V31_MEDIA_ROOT / category
    items=[]
    for p in sorted(folder.iterdir()):
        if p.is_file() and p.suffix.lower() in V31_MEDIA_CATEGORIES[category]["extensions"]:
            items.append({"name":p.name,"category":category,"size_mb":round(p.stat().st_size/1048576,2),"url":"/api/media/file/"+category+"/"+p.name})
    return {"items":items}

@app.post("/api/media/upload")
async def v31_media_upload(category: str, file: UploadFile = File(...), uid: int = Depends(require_admin)):
    if category not in V31_MEDIA_CATEGORIES: raise HTTPException(400,"Geçersiz kategori.")
    name=Path(file.filename or "").name
    if not name or Path(name).suffix.lower() not in V31_MEDIA_CATEGORIES[category]["extensions"]: raise HTTPException(400,"Bu dosya türü desteklenmiyor.")
    if len(name)>150: raise HTTPException(400,"Dosya adı çok uzun.")
    dest=V31_MEDIA_ROOT/category/name
    if dest.exists(): raise HTTPException(409,"Aynı isimde dosya zaten var.")
    total=0
    try:
        with dest.open("xb") as out:
            while True:
                chunk=await file.read(1024*1024)
                if not chunk: break
                total+=len(chunk)
                if total>250*1024*1024: raise HTTPException(413,"Dosya boyutu en fazla 250 MB olabilir.")
                out.write(chunk)
    except Exception:
        if dest.exists(): dest.unlink(missing_ok=True)
        raise
    finally: await file.close()
    audit(uid,"medya dosyası yükle","media",None,category+"/"+name)
    return {"name":name,"size_bytes":total,"url":"/api/media/file/"+category+"/"+name}

@app.get("/api/media/file/{category}/{filename}")
def v31_media_file(category: str, filename: str, access_token: str = Query(default='')):
    # Video/audio elements cannot attach Authorization headers; accept the existing session token in query for this protected file route.
    uid = current_user(authorization=('Bearer '+access_token) if access_token else None)
    if category not in V31_MEDIA_CATEGORIES: raise HTTPException(404,"Dosya bulunamadı.")
    safe=Path(filename).name
    p=(V31_MEDIA_ROOT/category/safe).resolve()
    if p.parent != (V31_MEDIA_ROOT/category).resolve() or not p.is_file() or p.suffix.lower() not in V31_MEDIA_CATEGORIES[category]["extensions"]: raise HTTPException(404,"Dosya bulunamadı.")
    return FileResponse(p, filename=p.name)


# V33.9.5 standalone animal-health page and true countdown
HTML_CONTENT = HTML_CONTENT.replace('AKILLI ÇİFTLİK V33.9.4','AKILLI ÇİFTLİK V33.9.5')
HTML_CONTENT = HTML_CONTENT.replace('AKILLI ÇİFTLİK V33.9.2 YÖNETİM MERKEZİ','AKILLI ÇİFTLİK V33.9.5 YÖNETİM MERKEZİ')
HTML_CONTENT = re.sub(r'function openAnimalHealthPlanner\(\).*?(?=function showError)', "function openAnimalHealthPlanner(){switchPage('animalHealth');if(typeof ahUnifiedRender==='function')ahUnifiedRender()}", HTML_CONTENT, count=1, flags=re.S)
HTML_CONTENT = HTML_CONTENT.replace('<section id="feed"', '<section id="animalHealth" class="page"><div class="card" id="animalHealthPlanner">\n<h3>🐄🐑 TEK LİSTE — HAYVAN SAĞLIĞI VE BAKIM TAKVİMİ</h3>\n<p class="muted">169 sağlık ve bakım kalemi tek listede. Her kalemin kendi plan tarihi, canlı geri sayımı ve tamamlandı kaydı vardır. Klinik tekrar tarihleri otomatik uydurulmaz; plan tarihini kullanıcı veya veteriner hekim belirler.</p>\n<div class="grid-3"><div class="stat">🔴 Süresi geçen<b id="ahLate">0</b></div><div class="stat">🟡 Tarih bekleyen<b id="ahSoon">0</b></div><div class="stat">🟢 Toplam kalem<b id="ahTotal">169</b></div></div>\n<div class="grid-2"><div><label>Hayvan türü görünümü</label><select id="ahUnifiedSpecies" onchange="ahUnifiedRender()"><option value="all">Tümü</option><option value="Büyükbaş">Büyükbaş</option><option value="Buzağı">Buzağı</option><option value="Küçükbaş">Küçükbaş</option><option value="Kuzu">Kuzu</option><option value="Keçi">Keçi</option><option value="Oğlak">Oğlak</option></select></div><div><label>Liste içinde ara</label><input id="ahUnifiedSearch" placeholder="Aşı, vitamin, parazit..." oninput="ahUnifiedRender()"></div></div>\n<div class="item"><b>⏱️ Sayaç kullanımı</b><p class="muted">Her satırda plan tarihi seçildiğinde sayaç o tarihe doğru gün/saat/dakika/saniye olarak geriye sayar. Tarih girilmemişse “Plan tarihi girilmedi” yazar. Tarih geçmişse gecikme süresini gösterir. “Yapıldı” işlemi tamamlanma kaydını saklar; yeniden planlamak için tarihi değiştirin.</p></div>\n<div id="ahUnifiedList"></div><div class="muted">Doz, yaş sınırı, uygulama kararı ve tekrar aralığı için ürünün güncel ruhsatlı etiketi ve veteriner hekim değerlendirmesi esas alınmalıdır. Kayıtlar bu cihazın tarayıcı depolamasında saklanır.</div>\n</div></section>' + '<section id="feed"', 1)
HTML_CONTENT = HTML_CONTENT.replace('</script>', 'const AH_UNIFIED_ITEMS = [\n[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Şap aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Brusella aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'LSD / nodüler ekzantem aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Şarbon aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Kara yanık / blackleg aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Klostridiyal karma aşı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'IBR aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'BVD aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'BRSV aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'PI3 aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Pasteurella / Mannheimia aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Leptospiroz aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Salmonella aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Hemorajik septisemi aşısı\'],[\'Aşılar — büyükbaş\',\'Büyükbaş\',\'Buzağı ishali etkenlerine karşı aşılar\'],\n[\'Aşılar — buzağı\',\'Buzağı\',\'Buzağı başlangıç aşı programı\'],[\'Aşılar — buzağı\',\'Buzağı\',\'Solunum yolu hastalıkları aşıları\'],[\'Aşılar — buzağı\',\'Buzağı\',\'Clostridial karma aşı\'],[\'Aşılar — buzağı\',\'Buzağı\',\'İshal etkenlerine karşı koruma\'],\n[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'PPR / koyun-keçi vebası aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Koyun-keçi çiçeği aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Enterotoksemi / çelerme aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Şap aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Şarbon aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Mavi dil aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Pasteurella aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Agalaksi aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Ayak çürüğü aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Kazeöz lenfadenit (CLA) aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Chlamydia abortus aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Koyun-keçi brusellası REV-1\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Keçi bulaşıcı plöropnömonisi (CCPP) aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Ektima aşısı\'],[\'Aşılar — küçükbaş\',\'Küçükbaş\',\'Koyun septisemisi aşısı\'],\n[\'Parazit uygulamaları\',\'Büyükbaş\',\'İç parazit — bağırsak kurtları\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'İç parazit — şeritler\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'İç parazit — akciğer kurtları\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'Karaciğer kelebeği\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'Rumen kelebeği\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'Koksidiyoz kontrolü\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'Dış parazit — keneler\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'Dış parazit — bitler\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'Dış parazit — uyuz akarları\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'Sinek ve larva mücadelesi\'],[\'Parazit uygulamaları\',\'Büyükbaş\',\'Nokra / deri altı kurtları\'],[\'Parazit uygulamaları\',\'Küçükbaş\',\'Koyun biti\'],[\'Parazit uygulamaları\',\'Küçükbaş\',\'Koyun kenesi\'],[\'Parazit uygulamaları\',\'Küçükbaş\',\'Burun kurdu\'],[\'Parazit uygulamaları\',\'Küçükbaş\',\'Kuzu iç parazit uygulaması\'],[\'Parazit uygulamaları\',\'Küçükbaş\',\'Mera öncesi parazit kontrolü\'],[\'Parazit uygulamaları\',\'Küçükbaş\',\'Mera sonrası parazit kontrolü\'],[\'Parazit uygulamaları\',\'Keçi\',\'Keçi iç parazit kontrolü\'],[\'Parazit uygulamaları\',\'Keçi\',\'Keçi dış parazit kontrolü\'],\n[\'Vitaminler — oral\',\'Büyükbaş\',\'A vitamini desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'D3 vitamini desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'E vitamini desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'B12 vitamini desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'A-D3-E vitamin kompleksi\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Multivitamin desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'B grubu vitaminleri\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Biotin desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'C vitamini desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Vitamin-mineral premiksi\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Selenyum ve E vitamini desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Kalsiyum desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Magnezyum desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Fosfor desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Elektrolit desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Mineral yalama taşı / mineral karışımı\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Probiyotik desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Prebiyotik desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Maya kültürü desteği\'],[\'Vitaminler — oral\',\'Büyükbaş\',\'Vitamin tabletleri / bolus\'],[\'Vitaminler — oral\',\'Küçükbaş\',\'Koyun-keçi vitamin-mineral desteği\'],[\'Vitaminler — oral\',\'Kuzu\',\'Kuzu vitamin desteği\'],[\'Vitaminler — oral\',\'Oğlak\',\'Oğlak vitamin desteği\'],\n[\'Vitamin enjeksiyonları\',\'Büyükbaş\',\'A-D3-E vitamin enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Büyükbaş\',\'B12 vitamin enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Büyükbaş\',\'B kompleks enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Büyükbaş\',\'Selenyum + E vitamini enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Büyükbaş\',\'Kalsiyum enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Büyükbaş\',\'Fosfor enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Küçükbaş\',\'Küçükbaş vitamin enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Kuzu\',\'Kuzu vitamin enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Oğlak\',\'Oğlak vitamin enjeksiyonu\'],\n[\'İlaç ve uygulamalar\',\'Büyükbaş\',\'Ağızdan verilen ilaçlar / haplar\'],[\'İlaç ve uygulamalar\',\'Büyükbaş\',\'Antiparaziter tablet ve boluslar\'],[\'İlaç ve uygulamalar\',\'Büyükbaş\',\'Enjeksiyon uygulamaları\'],[\'İlaç ve uygulamalar\',\'Büyükbaş\',\'Dökme uygulamalar (pour-on)\'],[\'İlaç ve uygulamalar\',\'Büyükbaş\',\'Ağızdan sıvı ilaçlar\'],[\'İlaç ve uygulamalar\',\'Büyükbaş\',\'Veteriner reçeteli tedaviler\'],[\'İlaç ve uygulamalar\',\'Küçükbaş\',\'Ağızdan hap / bolus uygulamaları\'],[\'İlaç ve uygulamalar\',\'Küçükbaş\',\'Enjeksiyon uygulamaları\'],[\'İlaç ve uygulamalar\',\'Küçükbaş\',\'Dökme uygulamalar (pour-on)\'],\n[\'Bakım ve kontroller\',\'Büyükbaş\',\'Gebelik kontrolü\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Doğum öncesi bakım\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Doğum sonrası bakım\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Meme sağlığı kontrolü\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Mastitis kontrolü\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Tırnak ve ayak bakımı\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Canlı ağırlık ölçümü\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Kondisyon puanı kontrolü\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Üreme ve kızgınlık takibi\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Sürü sağlık taraması\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Veteriner muayenesi\'],[\'Bakım ve kontroller\',\'Büyükbaş\',\'Karantina ve biyogüvenlik kontrolü\'],[\'Bakım ve kontroller\',\'Buzağı\',\'Göbek bakımı ve kontrolü\'],[\'Bakım ve kontroller\',\'Buzağı\',\'Ağız sütü / kolostrum kontrolü\'],[\'Bakım ve kontroller\',\'Buzağı\',\'İshal kontrolü\'],[\'Bakım ve kontroller\',\'Buzağı\',\'Solunum yolu kontrolü\'],[\'Bakım ve kontroller\',\'Buzağı\',\'Büyüme ve tartım kontrolü\'],[\'Bakım ve kontroller\',\'Küçükbaş\',\'Gebelik kontrolü\'],[\'Bakım ve kontroller\',\'Küçükbaş\',\'Doğum öncesi koyun-keçi bakımı\'],[\'Bakım ve kontroller\',\'Küçükbaş\',\'Doğum sonrası koyun-keçi bakımı\'],[\'Bakım ve kontroller\',\'Küçükbaş\',\'Kuzu/oğlak doğum bakımı\'],[\'Bakım ve kontroller\',\'Küçükbaş\',\'Tırnak kesimi ve ayak kontrolü\'],[\'Bakım ve kontroller\',\'Küçükbaş\',\'Meme sağlığı kontrolü\'],[\'Bakım ve kontroller\',\'Küçükbaş\',\'Canlı ağırlık ve kondisyon kontrolü\'],[\'Bakım ve kontroller\',\'Küçükbaş\',\'Üreme ve koç/teke katım kontrolü\'],[\'Bakım ve kontroller\',\'Küçükbaş\',\'Veteriner muayenesi\'],[\'Bakım ve kontroller\',\'Keçi\',\'Keçi ayak ve tırnak bakımı\'],[\'Bakım ve kontroller\',\'Keçi\',\'Keçi meme sağlığı kontrolü\'],[\'Bakım ve kontroller\',\'Kuzu\',\'Kuzu büyüme ve sağlık kontrolü\'],[\'Bakım ve kontroller\',\'Oğlak\',\'Oğlak büyüme ve sağlık kontrolü\'],\n[\'Vitaminler — tüm türler\',\'Küçükbaş\',\'A vitamini — oral / tablet / bolus\'],[\'Vitaminler — tüm türler\',\'Küçükbaş\',\'D3 vitamini — oral / tablet / bolus\'],[\'Vitaminler — tüm türler\',\'Küçükbaş\',\'E vitamini — oral / tablet / bolus\'],[\'Vitaminler — tüm türler\',\'Küçükbaş\',\'B12 vitamini — oral destek\'],[\'Vitaminler — tüm türler\',\'Küçükbaş\',\'A-D3-E kompleksi — oral\'],[\'Vitaminler — tüm türler\',\'Küçükbaş\',\'Multivitamin — oral\'],[\'Vitaminler — tüm türler\',\'Kuzu\',\'A vitamini — oral\'],[\'Vitaminler — tüm türler\',\'Kuzu\',\'D3 vitamini — oral\'],[\'Vitaminler — tüm türler\',\'Kuzu\',\'E vitamini — oral\'],[\'Vitaminler — tüm türler\',\'Kuzu\',\'B12 vitamini — oral\'],[\'Vitaminler — tüm türler\',\'Kuzu\',\'A-D3-E kompleksi — oral\'],[\'Vitaminler — tüm türler\',\'Kuzu\',\'Multivitamin — oral\'],[\'Vitaminler — tüm türler\',\'Keçi\',\'A vitamini — oral / tablet / bolus\'],[\'Vitaminler — tüm türler\',\'Keçi\',\'D3 vitamini — oral / tablet / bolus\'],[\'Vitaminler — tüm türler\',\'Keçi\',\'E vitamini — oral / tablet / bolus\'],[\'Vitaminler — tüm türler\',\'Keçi\',\'B12 vitamini — oral destek\'],[\'Vitaminler — tüm türler\',\'Keçi\',\'A-D3-E kompleksi — oral\'],[\'Vitaminler — tüm türler\',\'Keçi\',\'Multivitamin — oral\'],[\'Vitaminler — tüm türler\',\'Oğlak\',\'A vitamini — oral\'],[\'Vitaminler — tüm türler\',\'Oğlak\',\'D3 vitamini — oral\'],[\'Vitaminler — tüm türler\',\'Oğlak\',\'E vitamini — oral\'],[\'Vitaminler — tüm türler\',\'Oğlak\',\'B12 vitamini — oral\'],[\'Vitaminler — tüm türler\',\'Oğlak\',\'A-D3-E kompleksi — oral\'],[\'Vitaminler — tüm türler\',\'Oğlak\',\'Multivitamin — oral\'],\n[\'Vitamin enjeksiyonları\',\'Küçükbaş\',\'A vitamini enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Küçükbaş\',\'D3 vitamini enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Küçükbaş\',\'E vitamini enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Küçükbaş\',\'A-D3-E enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Küçükbaş\',\'B12 enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Küçükbaş\',\'B kompleks enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Küçükbaş\',\'Multivitamin enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Kuzu\',\'A-D3-E vitamin enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Kuzu\',\'B12 / B kompleks enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Oğlak\',\'A-D3-E vitamin enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Oğlak\',\'B12 / B kompleks enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Keçi\',\'A-D3-E vitamin enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Keçi\',\'B12 / B kompleks enjeksiyonu\'],\n[\'Vitaminler — tüm türler\',\'Buzağı\',\'A vitamini — oral / tablet / bolus\'],[\'Vitaminler — tüm türler\',\'Buzağı\',\'D3 vitamini — oral / tablet / bolus\'],[\'Vitaminler — tüm türler\',\'Buzağı\',\'E vitamini — oral / tablet / bolus\'],[\'Vitaminler — tüm türler\',\'Buzağı\',\'B12 vitamini — oral\'],[\'Vitaminler — tüm türler\',\'Buzağı\',\'A-D3-E kompleksi — oral\'],[\'Vitaminler — tüm türler\',\'Buzağı\',\'Multivitamin — oral\'],[\'Vitamin enjeksiyonları\',\'Buzağı\',\'A-D3-E vitamin enjeksiyonu\'],[\'Vitamin enjeksiyonları\',\'Buzağı\',\'B12 / B kompleks enjeksiyonu\']\n];\nconst ahUnifiedKey=\'ah_unified_v3395_\'+(localStorage.getItem(\'feyzullah_token\')||\'local\');\nfunction ahUnifiedLoad(){try{return JSON.parse(localStorage.getItem(ahUnifiedKey)||\'{}\')}catch(e){return {}}}\nfunction ahUnifiedSave(x){localStorage.setItem(ahUnifiedKey,JSON.stringify(x))}\nfunction ahUnifiedFmt(ms){let sec=Math.floor(Math.abs(ms)/1000),d=Math.floor(sec/86400);sec%=86400;let h=Math.floor(sec/3600);sec%=3600;let m=Math.floor(sec/60);sec%=60;return (d?d+\'g \':\'\')+[h,m,sec].map(x=>String(x).padStart(2,\'0\')).join(\':\')}\nfunction ahUnifiedRender(){let box=document.getElementById(\'ahUnifiedList\');if(!box)return;let f=document.getElementById(\'ahUnifiedSpecies\')?.value||\'all\',q=(document.getElementById(\'ahUnifiedSearch\')?.value||\'\').toLocaleLowerCase(\'tr\'),state=ahUnifiedLoad(),groups={};AH_UNIFIED_ITEMS.forEach((it,i)=>{if(f!==\'all\'&&it[1]!==f)return;if(q&&!it.join(\' \').toLocaleLowerCase(\'tr\').includes(q))return;(groups[it[0]]??=[]).push({it,i})});let html=\'\',late=0,undated=0;Object.entries(groups).forEach(([g,rows])=>{html+=`<h4>${g} <span class="tag">${rows.length} kalem</span></h4>`;rows.forEach(({it,i})=>{let st=state[i]||{},done=!!st.done,date=st.date||\'\',target=date?new Date(date+\'T23:59:59\').getTime():0,remaining=target-Date.now();if(!date&&!done)undated++;if(date&&!done&&remaining<0)late++;html+=`<div class="item ah-unified-row"><b>${it[2]}</b><div class="muted">${it[1]} • ${g}</div><label>Plan tarihi</label><input type="date" value="${date}" onchange="ahUnifiedDate(${i},this.value)" ${done?\'disabled\':\'\'}><div class="ah-live-counter" data-index="${i}" style="font:700 18px monospace;color:${done?\'#4ade80\':!date?\'#94a3b8\':remaining<0?\'#f87171\':\'#fbbf24\'};margin:8px 0">${done?\'✓ Tamamlandı\':!date?\'Plan tarihi girilmedi\':remaining<0?\'Gecikme: \'+ahUnifiedFmt(-remaining):\'Kalan: \'+ahUnifiedFmt(remaining)}</div><div class="muted">${done?\'Tamamlandı: \'+new Date(st.doneAt||Date.now()).toLocaleString(\'tr-TR\'):date?\'Hedef: \'+new Date(target).toLocaleString(\'tr-TR\'):\'Sayaç, plan tarihi seçilince başlayacak.\'}</div><button class="action" style="margin-top:7px" onclick="ahUnifiedDone(${i})" ${done?\'disabled\':\'\'}>${done?\'Tamamlandı\':\'Yapıldı / tamamla\'}</button> <button class="action" style="margin-top:7px" onclick="ahUnifiedReset(${i})">Yeniden planla</button></div>`})});box.innerHTML=html||\'<p class="muted">Bu filtrede sonuç yok.</p>\';document.getElementById(\'ahTotal\').textContent=AH_UNIFIED_ITEMS.length;document.getElementById(\'ahSoon\').textContent=undated;document.getElementById(\'ahLate\').textContent=late;ahUnifiedTick()}\nfunction ahUnifiedDate(i,date){let s=ahUnifiedLoad(),old=s[i]||{};s[i]={...old,date,done:false};ahUnifiedSave(s);ahUnifiedRender()}\nfunction ahUnifiedDone(i){let s=ahUnifiedLoad(),old=s[i]||{};s[i]={...old,done:true,doneAt:Date.now()};ahUnifiedSave(s);ahUnifiedRender()}\nfunction ahUnifiedReset(i){let s=ahUnifiedLoad(),old=s[i]||{};s[i]={...old,done:false,doneAt:null};ahUnifiedSave(s);ahUnifiedRender()}\nfunction ahUnifiedTick(){let state=ahUnifiedLoad();document.querySelectorAll(\'.ah-live-counter[data-index]\').forEach(el=>{let st=state[Number(el.dataset.index)]||{};if(st.done){el.textContent=\'✓ Tamamlandı\';el.style.color=\'#4ade80\';return}if(!st.date){el.textContent=\'Plan tarihi girilmedi\';el.style.color=\'#94a3b8\';return}let ms=new Date(st.date+\'T23:59:59\').getTime()-Date.now();el.textContent=ms<0?\'Gecikme: \'+ahUnifiedFmt(-ms):\'Kalan: \'+ahUnifiedFmt(ms);el.style.color=ms<0?\'#f87171\':\'#fbbf24\'})}\nsetInterval(ahUnifiedTick,1000);window.addEventListener(\'load\',()=>{if(document.getElementById(\'animalHealthPlanner\'))ahUnifiedRender()});' + '</script>', 1)


@app.get("/",response_class=HTMLResponse)
def home(): return HTMLResponse(HTML_CONTENT)

if __name__=="__main__":
    import uvicorn
    uvicorn.run("main:app",host="0.0.0.0",port=int(os.environ.get("PORT",8000)),reload=False)
