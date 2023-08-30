from pydantic import StrictFloat, StrictInt, confloat, conint

from pandantic import BaseModel


class DunnhumbySchema(BaseModel):
    household_key: StrictInt
    BASKET_ID: StrictInt
    DAY: StrictInt
    PRODUCT_ID: StrictInt
    QUANTITY: conint(ge=0, le=5)
    SALES_VALUE: StrictFloat
    STORE_ID: StrictInt
    RETAIL_DISC: confloat(le=0)
    TRANS_TIME: StrictInt
    COUPON_DISC: confloat(le=0)
    COUPON_MATCH_DISC: confloat(le=0)
