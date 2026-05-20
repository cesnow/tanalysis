from datetime import datetime
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.db.mariadb import Base, engine, get_db
from app.models.product import Product

router = APIRouter(prefix="/products", tags=["Products"])


# ---------- Pydantic schemas ----------

class ProductCreate(BaseModel):
    name: str
    description: str | None = None
    jql: str
    enabled: bool = True


class ProductUpdate(BaseModel):
    name: str | None = None
    description: str | None = None
    jql: str | None = None
    enabled: bool | None = None


class ProductOut(BaseModel):
    id: int
    name: str
    description: str | None
    jql: str
    enabled: bool
    created_at: datetime
    updated_at: datetime

    model_config = {"from_attributes": True}


# ---------- Helpers ----------

def _ensure_table():
    Base.metadata.create_all(engine)


# ---------- Endpoints ----------

@router.post("", response_model=ProductOut, status_code=status.HTTP_201_CREATED)
def create_product(payload: ProductCreate, db: Session = Depends(get_db)):
    """建立新 Product，並定義其 Jira JQL 範圍。"""
    _ensure_table()
    if db.query(Product).filter(Product.name == payload.name).first():
        raise HTTPException(status_code=409, detail=f"Product '{payload.name}' already exists")
    product = Product(**payload.model_dump())
    db.add(product)
    db.commit()
    db.refresh(product)
    return product


@router.get("", response_model=list[ProductOut])
def list_products(db: Session = Depends(get_db)):
    """列出所有 Products。"""
    _ensure_table()
    return db.query(Product).order_by(Product.id).all()


@router.get("/{product_id}", response_model=ProductOut)
def get_product(product_id: int, db: Session = Depends(get_db)):
    """取得單一 Product。"""
    _ensure_table()
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    return product


@router.put("/{product_id}", response_model=ProductOut)
def update_product(product_id: int, payload: ProductUpdate, db: Session = Depends(get_db)):
    """更新 Product（含 JQL）。"""
    _ensure_table()
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    for field, value in payload.model_dump(exclude_none=True).items():
        setattr(product, field, value)
    product.updated_at = datetime.utcnow()
    db.commit()
    db.refresh(product)
    return product


@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_product(product_id: int, db: Session = Depends(get_db)):
    """刪除 Product。"""
    _ensure_table()
    product = db.query(Product).filter(Product.id == product_id).first()
    if not product:
        raise HTTPException(status_code=404, detail="Product not found")
    db.delete(product)
    db.commit()
