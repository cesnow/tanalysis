from fastapi import APIRouter, HTTPException, status

from app.db.mariadb import SessionLocal
from app.repositories import product_repo
from app.schemas.product import ProductCreate, ProductOut, ProductUpdate

router = APIRouter(prefix="/products", tags=["Products"])

# ---------- Endpoints ----------


@router.post("", response_model=ProductOut, status_code=status.HTTP_201_CREATED)
def create_product(payload: ProductCreate):
    """Create a new Product and define its Jira JQL scope."""
    with SessionLocal() as db:
        if product_repo.get_by_name(db, payload.name):
            raise HTTPException(status_code=409, detail=f"Product '{payload.name}' already exists")
        return product_repo.create(db, payload)


@router.get("", response_model=list[ProductOut])
def list_products():
    """List all Products."""
    with SessionLocal() as db:
        return product_repo.list_all(db)


@router.get("/{product_id}", response_model=ProductOut)
def get_product(product_id: int):
    """Get a single Product."""
    with SessionLocal() as db:
        product = product_repo.get_by_id(db, product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        return product


@router.put("/{product_id}", response_model=ProductOut)
def update_product(product_id: int, payload: ProductUpdate):
    """Update a Product (including JQL)."""
    with SessionLocal() as db:
        product = product_repo.get_by_id(db, product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        return product_repo.update(db, product, payload)


@router.delete("/{product_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_product(product_id: int):
    """Delete a Product."""
    with SessionLocal() as db:
        product = product_repo.get_by_id(db, product_id)
        if not product:
            raise HTTPException(status_code=404, detail="Product not found")
        product_repo.delete(db, product)
