from pydantic import BaseModel, Field


class TransactionCreateRequest(BaseModel):
    account_id: str = Field(..., examples=["acct-1"])
    card_id: str = Field(..., examples=["card-1"])
    merchant_id: str = Field(..., examples=["m-1"])
    amount: float = Field(..., gt=0)
    currency: str = Field(default="USD", min_length=3, max_length=3)
    merchant_risk: int = Field(default=1, ge=1, le=9)
    is_cross_border: bool = False


class TransactionCreateResponse(BaseModel):
    txn_id: str
    event_type: str
    message: str

