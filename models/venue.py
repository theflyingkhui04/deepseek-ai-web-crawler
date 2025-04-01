from pydantic import BaseModel
from typing import Optional

class Product(BaseModel):
    name: str
    category: str
    price: str
    brand: Optional[str] = None  # Có thể không có đủ thông tin về brand
    image_url: str