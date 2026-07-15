# sgh-data-pipeline

Projekt akademicki realizowany w ramach studiów podyplomowych **Big Data** w **(SGH)**.

## Cel projektu
Projekt polega na zaprojektowaniu i zaimplementowaniu kompletnego potoku danych (Data Pipeline) klasy **ETL (Extract, Transform, Load)** do automatycznego pobierania, transformacji i analizy danych giełdowych oraz kryptowalutowych. 

Głównym celem jest stworzenie stabilnego, zautomatyzowanego systemu, który pobiera surowe dane w formacie JSON z zewnętrznych API finansowych (np. Alpha Vantage), przeprowadza ich czyszczenie, standaryzację i typowanie w bibliotece **Pandas**, a następnie zapisuje je do zoptymalizowanych plików CSV przygotowanych do dalszej analizy statystycznej i finansowej.

---

## Architektura i Struktura Projektu

sgh-data-pipeline/
│
├── data/
│   ├── raw/          # Surowe dane pobrane bezpośrednio z API (pliki JSON)
│   └── processed/    # Dane przetworzone, oczyszczone i gotowe do analizy (pliki CSV)
│
├── extract.py        # Moduł ekstrakcji danych (pobieranie z API)
├── transform.py      # Moduł transformacji i ładowania danych (Pandas)
└── README.md         # Dokumentacja projektu

### Opis Modułów:
1. **E (Extract) – `extract.py`**:
   - Nawiązuje połączenie z zewnętrznym API giełdowym.
   - Pobiera dane w formacie JSON i zapisuje je w katalogu `data/raw/` jako kopia zapasowa danych źródłowych.
2. **T & L (Transform & Load) – `transform.py`**:
   - Odczytuje surowe pliki JSON.
   - Wykorzystuje bibliotekę **Pandas** do wykonania transpozycji danych (zamiana dat z kolumn na wiersze).
   - Resetuje indeksy i standaryzuje nagłówki kolumn do formatu bazodanowego (`date`, `open`, `high`, `low`, `close`, `volume`).
   - Dokonuje rygorystycznego typowania danych (konwersja cen na `float64`, wolumenu na `int64`).
   - Wzbogaca dane o kolumnę identyfikującą instrument (`symbol`).
   - Zapisuje oczyszczony zbiór jako ustrukturyzowany plik CSV w folderze `data/processed/` bez zbędnych indeksów wierszowych.

---

## Wykorzystane Technologie
- **Język programowania:** Python 3
- **Biblioteki do analizy danych:** Pandas, Glob, OS, JSON
- **Kontrola wersji:** Git / GitHub

---

## Planowane Dalsze Kroki
1. **Pełna automatyzacja pętli (Bulk Processing):** Dodanie automatycznego skanowania folderu `data/raw/` za pomocą biblioteki `glob`, aby przetwarzać wiele instrumentów giełdowych i kryptowalutowych jednocześnie bez ręcznej ingerencji.
2. **Integracja danych giełdowych z kryptowalutami:** Połączenie zbiorów danych w celu analizy korelacji.
3. **Analiza finansowa:** Implementacja modułu analitycznego wyliczającego podstawowe wskaźniki finansowe (dzienne stopy zwrotu, zmienność historyczna, średnie kroczące).