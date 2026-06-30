from typing import Dict, List


def score_transaction(txn: Dict) -> Dict:
    amount = float(txn["amount"])
    merchant_risk = int(txn.get("merchant_risk", 0))
    is_cross_border = bool(txn.get("is_cross_border", False))

    score = 0
    reasons: List[str] = []

    if amount > 2000:
        score += 45
        reasons.append("high_amount")
    elif amount > 500:
        score += 20
        reasons.append("moderate_amount")

    if merchant_risk >= 7:
        score += 35
        reasons.append("risky_merchant")
    elif merchant_risk >= 4:
        score += 15
        reasons.append("medium_risk_merchant")

    if is_cross_border:
        score += 20
        reasons.append("cross_border")

    return {"risk_score": min(score, 100), "reasons": reasons}


def authorize_from_score(risk_score: int) -> Dict:
    if risk_score >= 70:
        return {"approved": False, "decision_reason": "declined_high_risk"}
    if risk_score >= 40:
        return {"approved": True, "decision_reason": "approved_with_review"}
    return {"approved": True, "decision_reason": "approved_low_risk"}

