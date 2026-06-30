from src.transaction_processor.domain.risk_engine import authorize_from_score, score_transaction


def run() -> None:
    low_risk = {
        "amount": 45,
        "merchant_risk": 1,
        "is_cross_border": False,
    }
    high_risk = {
        "amount": 3200,
        "merchant_risk": 8,
        "is_cross_border": True,
    }

    low_result = score_transaction(low_risk)
    high_result = score_transaction(high_risk)

    assert low_result["risk_score"] < 40
    assert high_result["risk_score"] >= 70

    assert authorize_from_score(low_result["risk_score"])["approved"] is True
    assert authorize_from_score(high_result["risk_score"])["approved"] is False

    print("smoke test passed")


if __name__ == "__main__":
    run()


