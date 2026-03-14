import joblib
import numpy as np
from fastapi import FastAPI
from pydantic import BaseModel

# ── LOAD MODELS ───────────────────────────────────────────────────────────────
xgb      = joblib.load('ml/models/xgboost_fraud.pkl')
iso      = joblib.load('ml/models/isolation_forest.pkl')
features = joblib.load('ml/models/features.pkl')

app = FastAPI(title='Midas Fraud Detection API')

# ── REQUEST SCHEMA ────────────────────────────────────────────────────────────
class Transaction(BaseModel):
    amount: float
    hour_of_day: int
    is_odd_hour: int
    is_round_number: int
    is_large_amount: int
    amount_zscore: float
    avg_amount_7d: float
    tx_velocity: int
    is_high_velocity: int

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────
@app.get('/health')
def health():
    return {'status': 'ok'}

@app.post('/predict')
def predict(txn: Transaction):
    X = np.array([[getattr(txn, f) for f in features]])
    
    xgb_score  = float(xgb.predict_proba(X)[0][1])
    iso_flag   = int(iso.predict(X)[0] == -1)
    is_fraud   = 1 if xgb_score > 0.7 else 0

    return {
        'xgb_fraud_score' : round(xgb_score, 4),
        'iso_anomaly_flag': iso_flag,
        'is_fraud'        : is_fraud,
        'verdict'         : 'FRAUD' if is_fraud else 'LEGIT',
    }

if __name__ == '__main__':
    import uvicorn
    uvicorn.run(app, host='0.0.0.0', port=8090)