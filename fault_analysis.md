# Krishi-Kavach | Fault Analysis Report

This analysis identifies potential failure points, technical risks, and architectural vulnerabilities in the current version (v2.0) of the Krishi-Kavach pipeline.

## 1. Data Integrity & Join Risks
### ⚠️ District Naming Inconsistency
The system relies on an exact or `upper()` join on `district`.
- **Fault**: Sources (IMD, Mandi, PMFBY, KCC) have different nomenclatures (e.g., "Allahabad" vs "Prayagraj").
- **Impact**: Significant loss of signal due to "silent" join failures where records are dropped because names don't match exactly.
- **Mitigation**: Implement a Levenshtein-based fuzzy matching layer or a master Cross-Reference Table (CRT) for Indian districts.

## 2. Algorithmic Vulnerabilities
### 📉 Static Threshold Sensitivity
The Silver layer uses hardcoded thresholds (e.g., `confidence_score >= 0.6` and `price_spike > 1.3`).
- **Fault**: Commodity prices exhibit seasonal and regional variance. A 30% spike in one crop might be normal, while in another it’s catastrophic.
- **Impact**: False negatives during high-inflation periods or false positives for volatile crops (e.g., Onions).
- **Mitigation**: Switch to Z-score (standard deviation) based anomalies instead of static percentage multipliers.

### 🧩 Weighted Signal Dependencies
Signal A (Weather) carries 50% weight.
- **Fault**: If the IMD sensor network in a remote district fails or reports zero rainfall as `null`, the trigger becomes nearly impossible to reach regardless of Market/Social signals.
- **Impact**: System-wide blind spots in regions with poor sensor coverage.

## 3. Fraud and Manipulation Gaps
### 🕵️ Sophisticated Sybil Attacks
The [fraud_guard.py](file:///d:/Projects/Krishi-Kavach/notebooks/fraud_guard.py) checks for bulk `device_id` submissions.
- **Fault**: A coordinated "Social-only" attack using many distributed devices (different IPs/IDs) could bypass the "5 per device" rule.
- **Impact**: Potential for organized gaming of the payout system during minor dry spells.
- **Mitigation**: Add spatial clustering analysis (if many device inquiries originate from the same GPS geofence).

## 4. Performance & Scalability
### 🐘 Heavy CSV Dependencies
The 7.8GB KCC dataset was processed via a `fast-seek` hack locally.
- **Fault**: In a production CI/CD environment, the pipeline currently expects pre-cleansed CSVs in DBFS.
- **Impact**: The initial conversion of NetCDF to CSV and 8GB filtering is currently a manual/scripted bottleneck.
- **Mitigation**: Transition to **Autoloader** (Spark Structured Streaming) to ingest files as they land, rather than batch-processing 8GB CSVs.

## 5. Summary Table
| Risk Area | Severity | Likelihood | Mitigation Effort |
|-----------|----------|------------|-------------------|
| District Mapping | High | High | Medium |
| Static Thresholds | Medium | High | High |
| Data Silos (nulls) | Medium | Medium | Low |
| Fraud (Sybil) | High | Low | High |
