from pydantic import BaseModel


class UsersSchema(BaseModel):
    id: int
    name: str
    email: str
    created_at: str


class OrdersSchema(BaseModel):
    id: int
    user_id: int
    product_name: str
    quantity: int
    order_date: int
