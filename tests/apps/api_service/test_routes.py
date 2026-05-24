from datetime import UTC, datetime
from types import SimpleNamespace

import main
from fastapi.testclient import TestClient

from routes import flows as flow_routes
from routes import jira as jira_routes
from routes import product as product_routes


def test_health_endpoint() -> None:
    with TestClient(main.app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json() == {"message": "TAnalysis API is running"}


def test_list_jira_tickets_uses_service(monkeypatch) -> None:
    monkeypatch.setattr(
        jira_routes,
        "get_tickets_from_mongo",
        lambda project_key=None, limit=50: [{"key": "TAN-1", "project_key": project_key, "limit": limit}],
    )

    with TestClient(main.app) as client:
        response = client.get("/jira/tickets", params={"project_key": "TAN", "limit": 1})

    assert response.status_code == 200
    assert response.json() == {
        "count": 1,
        "tickets": [{"key": "TAN-1", "project_key": "TAN", "limit": 1}],
    }


def test_list_products_uses_repository_and_dependency_override(monkeypatch) -> None:
    now = datetime(2026, 1, 1, tzinfo=UTC)

    def override_get_db():
        yield object()

    monkeypatch.setattr(
        product_routes.product_repo,
        "list_all",
        lambda db: [
            SimpleNamespace(
                id=1,
                name="Core",
                description="Core product",
                jql="project = TAN",
                enabled=True,
                created_at=now,
                updated_at=now,
            )
        ],
    )
    main.app.dependency_overrides[product_routes.get_db] = override_get_db

    try:
        with TestClient(main.app) as client:
            response = client.get("/products")
    finally:
        main.app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == [
        {
            "id": 1,
            "name": "Core",
            "description": "Core product",
            "jql": "project = TAN",
            "enabled": True,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        }
    ]


def test_trigger_product_sync_passes_expected_prefect_parameters(monkeypatch) -> None:
    triggered = {}

    async def fake_trigger_deployment(name: str, parameters: dict | None = None) -> None:
        triggered["name"] = name
        triggered["parameters"] = parameters

    monkeypatch.setattr(flow_routes, "_trigger_deployment", fake_trigger_deployment)

    with TestClient(main.app) as client:
        response = client.post(
            "/flows/jira-sync/7",
            params={"product_name": "Core", "jql": "project = TAN"},
        )

    assert response.status_code == 200
    assert response.json() == {"status": "triggered"}
    assert triggered == {
        "name": "jira_product_sync_flow/jira-product-sync",
        "parameters": {"product_id": 7, "product_name": "Core", "jql": "project = TAN"},
    }
