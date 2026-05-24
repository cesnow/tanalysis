"""Repository for Product (MariaDB / SQLAlchemy)."""

from datetime import datetime, timezone

from sqlalchemy.orm import Session

from app.models.product import Product
from app.schemas.product import ProductCreate, ProductUpdate


def get_by_id(db: Session, product_id: int) -> Product | None:
    return db.query(Product).filter(Product.id == product_id).first()


def get_by_name(db: Session, name: str) -> Product | None:
    return db.query(Product).filter(Product.name == name).first()


def list_all(db: Session) -> list[Product]:
    return db.query(Product).order_by(Product.id).all()


def list_enabled(db: Session) -> list[Product]:
    return db.query(Product).filter(Product.enabled == True).all()  # noqa: E712


def create(db: Session, payload: ProductCreate) -> Product:
    product = Product(**payload.model_dump())
    db.add(product)
    db.commit()
    db.refresh(product)
    return product


def update(db: Session, product: Product, payload: ProductUpdate) -> Product:
    for field, value in payload.model_dump(exclude_none=True).items():
        setattr(product, field, value)
    product.updated_at = datetime.now(timezone.utc).replace(tzinfo=None)  # type: ignore[assignment]
    db.commit()
    db.refresh(product)
    return product


def delete(db: Session, product: Product) -> None:
    db.delete(product)
    db.commit()
