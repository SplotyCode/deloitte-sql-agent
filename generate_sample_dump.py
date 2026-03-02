import argparse
import json
import os
import random
import sqlite3
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, List, Optional

from faker import Faker


BASE_TABLE_COUNT = 89


@dataclass(frozen=True)
class ScaleProfile:
    tenants: int
    users_per_tenant: int
    products_per_tenant: int
    orders_per_tenant: int
    tickets_per_tenant: int
    projects_per_tenant: int
    incidents_per_tenant: int


SCALE_PROFILES: Dict[str, ScaleProfile] = {
    "tiny": ScaleProfile(tenants=2, users_per_tenant=6, products_per_tenant=12, orders_per_tenant=8, tickets_per_tenant=6, projects_per_tenant=2, incidents_per_tenant=1),
    "small": ScaleProfile(tenants=4, users_per_tenant=12, products_per_tenant=24, orders_per_tenant=18, tickets_per_tenant=12, projects_per_tenant=3, incidents_per_tenant=2),
    "medium": ScaleProfile(tenants=6, users_per_tenant=24, products_per_tenant=36, orders_per_tenant=36, tickets_per_tenant=20, projects_per_tenant=4, incidents_per_tenant=3),
    "large": ScaleProfile(tenants=10, users_per_tenant=48, products_per_tenant=64, orders_per_tenant=72, tickets_per_tenant=36, projects_per_tenant=6, incidents_per_tenant=5),
}

SCENARIO_MULTIPLIERS: Dict[str, Dict[str, float]] = {
    "enterprise": {
        "products_per_tenant": 1.0,
        "orders_per_tenant": 1.0,
        "tickets_per_tenant": 1.0,
        "projects_per_tenant": 1.0,
        "incidents_per_tenant": 1.0,
    },
    "commerce": {
        "products_per_tenant": 1.4,
        "orders_per_tenant": 1.8,
        "tickets_per_tenant": 0.7,
        "projects_per_tenant": 0.8,
        "incidents_per_tenant": 0.8,
    },
    "support": {
        "products_per_tenant": 0.8,
        "orders_per_tenant": 0.7,
        "tickets_per_tenant": 1.9,
        "projects_per_tenant": 1.4,
        "incidents_per_tenant": 1.8,
    },
}

HIDDEN_REFERENCE_RATIOS = {
    "off": 0.0,
    "light": 0.3,
    "mixed": 0.7,
    "aggressive": 1.0,
}


@dataclass(frozen=True)
class SampleDumpConfig:
    min_tables: int = 84
    scenario: str = "enterprise"
    scale: str = "medium"
    hidden_reference_mode: str = "mixed"
    seed: int = 7
    tenants: Optional[int] = None
    users_per_tenant: Optional[int] = None
    products_per_tenant: Optional[int] = None
    orders_per_tenant: Optional[int] = None
    tickets_per_tenant: Optional[int] = None
    projects_per_tenant: Optional[int] = None
    incidents_per_tenant: Optional[int] = None
    extra_noise_tables: int = 0
    write_sql_dump: bool = True


def _chance(rng: random.Random, ratio: float) -> bool:
    return ratio > 0 and rng.random() <= ratio


def _rand_ts(fake: Faker) -> str:
    return fake.date_time_between(start_date="-540d", end_date="now").isoformat()


def _pick_some(rng: random.Random, values: List[int], minimum: int, maximum: int) -> List[int]:
    count = min(len(values), rng.randint(minimum, maximum))
    if count <= 0:
        return []
    return rng.sample(values, count)


def _insert(cur: sqlite3.Cursor, sql: str, params: tuple) -> int:
    cur.execute(sql, params)
    return int(cur.lastrowid)


def _resolved_counts(config: SampleDumpConfig) -> Dict[str, int]:
    if config.scale not in SCALE_PROFILES:
        raise ValueError(f"Unknown scale '{config.scale}'.")
    if config.scenario not in SCENARIO_MULTIPLIERS:
        raise ValueError(f"Unknown scenario '{config.scenario}'.")
    if config.hidden_reference_mode not in HIDDEN_REFERENCE_RATIOS:
        raise ValueError(f"Unknown hidden reference mode '{config.hidden_reference_mode}'.")

    profile = SCALE_PROFILES[config.scale]
    scenario = SCENARIO_MULTIPLIERS[config.scenario]

    def resolve(name: str, override: Optional[int]) -> int:
        if override is not None:
            return override
        base_value = getattr(profile, name)
        return max(1, int(round(base_value * scenario[name])))

    counts = {
        "tenants": config.tenants or profile.tenants,
        "users_per_tenant": config.users_per_tenant or profile.users_per_tenant,
        "products_per_tenant": resolve("products_per_tenant", config.products_per_tenant),
        "orders_per_tenant": resolve("orders_per_tenant", config.orders_per_tenant),
        "tickets_per_tenant": resolve("tickets_per_tenant", config.tickets_per_tenant),
        "projects_per_tenant": resolve("projects_per_tenant", config.projects_per_tenant),
        "incidents_per_tenant": resolve("incidents_per_tenant", config.incidents_per_tenant),
    }

    counts["departments_per_tenant"] = 3 if counts["users_per_tenant"] < 20 else 4
    counts["teams_per_tenant"] = 3 if counts["users_per_tenant"] < 20 else 5
    counts["cost_centers_per_tenant"] = 2 if counts["users_per_tenant"] < 20 else 3
    counts["suppliers_per_tenant"] = max(2, counts["products_per_tenant"] // 8)
    counts["warehouses_per_tenant"] = max(1, min(4, counts["products_per_tenant"] // 12))
    counts["labels_per_tenant"] = max(3, counts["projects_per_tenant"] + 1)
    return counts


def _create_schema(cur: sqlite3.Cursor, extra_tables: int) -> None:
    statements = [
        """
        CREATE TABLE scenario_metadata (
            key TEXT PRIMARY KEY,
            value TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE countries (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE regions (
            id INTEGER PRIMARY KEY,
            country_id INTEGER NOT NULL,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            FOREIGN KEY (country_id) REFERENCES countries (id)
        );
        """,
        """
        CREATE TABLE currencies (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            symbol TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE tax_codes (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            rate REAL NOT NULL,
            description TEXT
        );
        """,
        """
        CREATE TABLE categories (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            risk_tier TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE tags (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE roles (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE
        );
        """,
        """
        CREATE TABLE payment_terms (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            days_due INTEGER NOT NULL
        );
        """,
        """
        CREATE TABLE shipping_methods (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            carrier TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE ticket_types (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            severity_bias INTEGER NOT NULL
        );
        """,
        """
        CREATE TABLE workflow_states (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            category TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE feature_flags (
            id INTEGER PRIMARY KEY,
            key TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE compliance_controls (
            id INTEGER PRIMARY KEY,
            code TEXT NOT NULL UNIQUE,
            description TEXT NOT NULL
        );
        """,
        """
        CREATE TABLE retention_policies (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL UNIQUE,
            keep_days INTEGER NOT NULL
        );
        """,
        """
        CREATE TABLE tenants (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            slug TEXT NOT NULL UNIQUE,
            region_id INTEGER NOT NULL,
            default_currency_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (region_id) REFERENCES regions (id),
            FOREIGN KEY (default_currency_id) REFERENCES currencies (id)
        );
        """,
        """
        CREATE TABLE departments (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        );
        """,
        """
        CREATE TABLE teams (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            department_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            parent_team_id INTEGER,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (department_id) REFERENCES departments (id),
            FOREIGN KEY (parent_team_id) REFERENCES teams (id)
        );
        """,
        """
        CREATE TABLE cost_centers (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            code TEXT NOT NULL,
            label TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        );
        """,
        """
        CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            department_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            cost_center_id INTEGER NOT NULL,
            manager_user_id INTEGER,
            username TEXT NOT NULL UNIQUE,
            email TEXT NOT NULL UNIQUE,
            full_name TEXT NOT NULL,
            title TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (department_id) REFERENCES departments (id),
            FOREIGN KEY (team_id) REFERENCES teams (id),
            FOREIGN KEY (cost_center_id) REFERENCES cost_centers (id),
            FOREIGN KEY (manager_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE user_roles (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role_id INTEGER NOT NULL,
            granted_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (role_id) REFERENCES roles (id)
        );
        """,
        """
        CREATE TABLE user_preferences (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            preference_json TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE user_sessions (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            device TEXT NOT NULL,
            ip_address TEXT NOT NULL,
            started_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE api_keys (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            key_name TEXT NOT NULL,
            last_used_at TEXT,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE auth_audit_logs (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            actor_ip TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE addresses (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER,
            country_id INTEGER NOT NULL,
            region_id INTEGER NOT NULL,
            line_1 TEXT NOT NULL,
            city TEXT NOT NULL,
            postal_code TEXT NOT NULL,
            address_kind TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (country_id) REFERENCES countries (id),
            FOREIGN KEY (region_id) REFERENCES regions (id)
        );
        """,
        """
        CREATE TABLE suppliers (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            address_id INTEGER NOT NULL,
            payment_term_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            tier TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (address_id) REFERENCES addresses (id),
            FOREIGN KEY (payment_term_id) REFERENCES payment_terms (id)
        );
        """,
        """
        CREATE TABLE supplier_contacts (
            id INTEGER PRIMARY KEY,
            supplier_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            role_name TEXT NOT NULL,
            FOREIGN KEY (supplier_id) REFERENCES suppliers (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE warehouses (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            address_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (address_id) REFERENCES addresses (id)
        );
        """,
        """
        CREATE TABLE inventory_bins (
            id INTEGER PRIMARY KEY,
            warehouse_id INTEGER NOT NULL,
            label TEXT NOT NULL,
            capacity INTEGER NOT NULL,
            FOREIGN KEY (warehouse_id) REFERENCES warehouses (id)
        );
        """,
        """
        CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            category_id INTEGER NOT NULL,
            supplier_id INTEGER NOT NULL,
            tax_code_id INTEGER NOT NULL,
            sku TEXT NOT NULL UNIQUE,
            name TEXT NOT NULL,
            list_price REAL NOT NULL,
            cost_price REAL NOT NULL,
            lifecycle_state TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (category_id) REFERENCES categories (id),
            FOREIGN KEY (supplier_id) REFERENCES suppliers (id),
            FOREIGN KEY (tax_code_id) REFERENCES tax_codes (id)
        );
        """,
        """
        CREATE TABLE product_tags (
            id INTEGER PRIMARY KEY,
            product_id INTEGER NOT NULL,
            tag_id INTEGER NOT NULL,
            FOREIGN KEY (product_id) REFERENCES products (id),
            FOREIGN KEY (tag_id) REFERENCES tags (id)
        );
        """,
        """
        CREATE TABLE price_books (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            currency_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (currency_id) REFERENCES currencies (id)
        );
        """,
        """
        CREATE TABLE price_book_entries (
            id INTEGER PRIMARY KEY,
            price_book_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            price REAL NOT NULL,
            FOREIGN KEY (price_book_id) REFERENCES price_books (id),
            FOREIGN KEY (product_id) REFERENCES products (id)
        );
        """,
        """
        CREATE TABLE product_bundles (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            bundle_name TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (product_id) REFERENCES products (id)
        );
        """,
        """
        CREATE TABLE bundle_items (
            id INTEGER PRIMARY KEY,
            bundle_id INTEGER NOT NULL,
            child_product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            FOREIGN KEY (bundle_id) REFERENCES product_bundles (id),
            FOREIGN KEY (child_product_id) REFERENCES products (id)
        );
        """,
        """
        CREATE TABLE projects (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            workflow_state_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            start_date TEXT NOT NULL,
            budget REAL NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (owner_user_id) REFERENCES users (id),
            FOREIGN KEY (workflow_state_id) REFERENCES workflow_states (id)
        );
        """,
        """
        CREATE TABLE project_members (
            id INTEGER PRIMARY KEY,
            project_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            team_id INTEGER NOT NULL,
            joined_at TEXT NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (team_id) REFERENCES teams (id)
        );
        """,
        """
        CREATE TABLE task_labels (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        );
        """,
        """
        CREATE TABLE tasks (
            id INTEGER PRIMARY KEY,
            project_id INTEGER NOT NULL,
            assignee_user_id INTEGER NOT NULL,
            workflow_state_id INTEGER NOT NULL,
            parent_task_id INTEGER,
            shadow_order_id INTEGER,
            title TEXT NOT NULL,
            priority TEXT NOT NULL,
            due_at TEXT NOT NULL,
            FOREIGN KEY (project_id) REFERENCES projects (id),
            FOREIGN KEY (assignee_user_id) REFERENCES users (id),
            FOREIGN KEY (workflow_state_id) REFERENCES workflow_states (id),
            FOREIGN KEY (parent_task_id) REFERENCES tasks (id)
        );
        """,
        """
        CREATE TABLE task_dependencies (
            id INTEGER PRIMARY KEY,
            task_id INTEGER NOT NULL,
            depends_on_task_id INTEGER NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks (id),
            FOREIGN KEY (depends_on_task_id) REFERENCES tasks (id)
        );
        """,
        """
        CREATE TABLE task_comments (
            id INTEGER PRIMARY KEY,
            task_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE task_label_links (
            id INTEGER PRIMARY KEY,
            task_id INTEGER NOT NULL,
            label_id INTEGER NOT NULL,
            FOREIGN KEY (task_id) REFERENCES tasks (id),
            FOREIGN KEY (label_id) REFERENCES task_labels (id)
        );
        """,
        """
        CREATE TABLE documents (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (project_id) REFERENCES projects (id),
            FOREIGN KEY (owner_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE document_versions (
            id INTEGER PRIMARY KEY,
            document_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            version_no INTEGER NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (document_id) REFERENCES documents (id),
            FOREIGN KEY (author_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE document_links (
            id INTEGER PRIMARY KEY,
            source_document_id INTEGER NOT NULL,
            target_document_id INTEGER NOT NULL,
            relation_type TEXT NOT NULL,
            FOREIGN KEY (source_document_id) REFERENCES documents (id),
            FOREIGN KEY (target_document_id) REFERENCES documents (id)
        );
        """,
        """
        CREATE TABLE incidents (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            project_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            workflow_state_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            severity TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (project_id) REFERENCES projects (id),
            FOREIGN KEY (owner_user_id) REFERENCES users (id),
            FOREIGN KEY (workflow_state_id) REFERENCES workflow_states (id)
        );
        """,
        """
        CREATE TABLE incident_updates (
            id INTEGER PRIMARY KEY,
            incident_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (incident_id) REFERENCES incidents (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE tickets (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            ticket_type_id INTEGER NOT NULL,
            requester_user_id INTEGER NOT NULL,
            assignee_user_id INTEGER NOT NULL,
            project_id INTEGER,
            workflow_state_id INTEGER NOT NULL,
            shadow_order_id INTEGER,
            subject TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (ticket_type_id) REFERENCES ticket_types (id),
            FOREIGN KEY (requester_user_id) REFERENCES users (id),
            FOREIGN KEY (assignee_user_id) REFERENCES users (id),
            FOREIGN KEY (project_id) REFERENCES projects (id),
            FOREIGN KEY (workflow_state_id) REFERENCES workflow_states (id)
        );
        """,
        """
        CREATE TABLE ticket_comments (
            id INTEGER PRIMARY KEY,
            ticket_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            body TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (ticket_id) REFERENCES tickets (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE knowledge_articles (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            body TEXT NOT NULL,
            retention_policy_id INTEGER NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (author_user_id) REFERENCES users (id),
            FOREIGN KEY (retention_policy_id) REFERENCES retention_policies (id)
        );
        """,
        """
        CREATE TABLE article_feedback (
            id INTEGER PRIMARY KEY,
            article_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            score INTEGER NOT NULL,
            FOREIGN KEY (article_id) REFERENCES knowledge_articles (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE carts (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE cart_items (
            id INTEGER PRIMARY KEY,
            cart_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            FOREIGN KEY (cart_id) REFERENCES carts (id),
            FOREIGN KEY (product_id) REFERENCES products (id)
        );
        """,
        """
        CREATE TABLE orders (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            billing_address_id INTEGER NOT NULL,
            shipping_address_id INTEGER NOT NULL,
            payment_term_id INTEGER NOT NULL,
            shipping_method_id INTEGER NOT NULL,
            currency_id INTEGER NOT NULL,
            shadow_project_id INTEGER,
            order_date TEXT NOT NULL,
            status TEXT NOT NULL,
            total_amount REAL NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (billing_address_id) REFERENCES addresses (id),
            FOREIGN KEY (shipping_address_id) REFERENCES addresses (id),
            FOREIGN KEY (payment_term_id) REFERENCES payment_terms (id),
            FOREIGN KEY (shipping_method_id) REFERENCES shipping_methods (id),
            FOREIGN KEY (currency_id) REFERENCES currencies (id)
        );
        """,
        """
        CREATE TABLE order_items (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            warehouse_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            unit_price REAL NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders (id),
            FOREIGN KEY (product_id) REFERENCES products (id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses (id)
        );
        """,
        """
        CREATE TABLE shipments (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            warehouse_id INTEGER NOT NULL,
            workflow_state_id INTEGER NOT NULL,
            tracking_code TEXT NOT NULL,
            shipped_at TEXT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders (id),
            FOREIGN KEY (warehouse_id) REFERENCES warehouses (id),
            FOREIGN KEY (workflow_state_id) REFERENCES workflow_states (id)
        );
        """,
        """
        CREATE TABLE shipment_items (
            id INTEGER PRIMARY KEY,
            shipment_id INTEGER NOT NULL,
            order_item_id INTEGER NOT NULL,
            FOREIGN KEY (shipment_id) REFERENCES shipments (id),
            FOREIGN KEY (order_item_id) REFERENCES order_items (id)
        );
        """,
        """
        CREATE TABLE returns (
            id INTEGER PRIMARY KEY,
            order_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            workflow_state_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            FOREIGN KEY (order_id) REFERENCES orders (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (workflow_state_id) REFERENCES workflow_states (id)
        );
        """,
        """
        CREATE TABLE return_items (
            id INTEGER PRIMARY KEY,
            return_id INTEGER NOT NULL,
            order_item_id INTEGER NOT NULL,
            quantity INTEGER NOT NULL,
            FOREIGN KEY (return_id) REFERENCES returns (id),
            FOREIGN KEY (order_item_id) REFERENCES order_items (id)
        );
        """,
        """
        CREATE TABLE invoices (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            order_id INTEGER NOT NULL,
            billing_address_id INTEGER NOT NULL,
            currency_id INTEGER NOT NULL,
            issued_at TEXT NOT NULL,
            total_amount REAL NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (order_id) REFERENCES orders (id),
            FOREIGN KEY (billing_address_id) REFERENCES addresses (id),
            FOREIGN KEY (currency_id) REFERENCES currencies (id)
        );
        """,
        """
        CREATE TABLE invoice_items (
            id INTEGER PRIMARY KEY,
            invoice_id INTEGER NOT NULL,
            order_item_id INTEGER NOT NULL,
            line_total REAL NOT NULL,
            FOREIGN KEY (invoice_id) REFERENCES invoices (id),
            FOREIGN KEY (order_item_id) REFERENCES order_items (id)
        );
        """,
        """
        CREATE TABLE payments (
            id INTEGER PRIMARY KEY,
            invoice_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            currency_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            paid_at TEXT NOT NULL,
            FOREIGN KEY (invoice_id) REFERENCES invoices (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (currency_id) REFERENCES currencies (id)
        );
        """,
        """
        CREATE TABLE refunds (
            id INTEGER PRIMARY KEY,
            payment_id INTEGER NOT NULL,
            return_id INTEGER NOT NULL,
            amount REAL NOT NULL,
            refunded_at TEXT NOT NULL,
            FOREIGN KEY (payment_id) REFERENCES payments (id),
            FOREIGN KEY (return_id) REFERENCES returns (id)
        );
        """,
        """
        CREATE TABLE subscriptions (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            product_id INTEGER NOT NULL,
            payment_term_id INTEGER NOT NULL,
            workflow_state_id INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id),
            FOREIGN KEY (product_id) REFERENCES products (id),
            FOREIGN KEY (payment_term_id) REFERENCES payment_terms (id),
            FOREIGN KEY (workflow_state_id) REFERENCES workflow_states (id)
        );
        """,
        """
        CREATE TABLE subscription_events (
            id INTEGER PRIMARY KEY,
            subscription_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            event_type TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (subscription_id) REFERENCES subscriptions (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE campaigns (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            channel TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (owner_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE campaign_members (
            id INTEGER PRIMARY KEY,
            campaign_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            enrolled_at TEXT NOT NULL,
            FOREIGN KEY (campaign_id) REFERENCES campaigns (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE notifications (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            channel TEXT NOT NULL,
            context_entity_type TEXT,
            context_entity_id INTEGER,
            payload TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE notification_deliveries (
            id INTEGER PRIMARY KEY,
            notification_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            attempted_at TEXT NOT NULL,
            FOREIGN KEY (notification_id) REFERENCES notifications (id)
        );
        """,
        """
        CREATE TABLE webhooks (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            endpoint TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE webhook_deliveries (
            id INTEGER PRIMARY KEY,
            webhook_id INTEGER NOT NULL,
            status_code INTEGER NOT NULL,
            request_body TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            FOREIGN KEY (webhook_id) REFERENCES webhooks (id)
        );
        """,
        """
        CREATE TABLE dashboards (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (owner_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE dashboard_widgets (
            id INTEGER PRIMARY KEY,
            dashboard_id INTEGER NOT NULL,
            title TEXT NOT NULL,
            source_table TEXT,
            source_record_id INTEGER,
            FOREIGN KEY (dashboard_id) REFERENCES dashboards (id)
        );
        """,
        """
        CREATE TABLE reports (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            filter_payload TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (owner_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE report_runs (
            id INTEGER PRIMARY KEY,
            report_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            started_at TEXT NOT NULL,
            output_path TEXT NOT NULL,
            FOREIGN KEY (report_id) REFERENCES reports (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE data_exports (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            requested_by_user_id INTEGER NOT NULL,
            export_kind TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (requested_by_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE export_runs (
            id INTEGER PRIMARY KEY,
            export_id INTEGER NOT NULL,
            status TEXT NOT NULL,
            completed_at TEXT,
            FOREIGN KEY (export_id) REFERENCES data_exports (id)
        );
        """,
        """
        CREATE TABLE experiments (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            feature_flag_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (owner_user_id) REFERENCES users (id),
            FOREIGN KEY (feature_flag_id) REFERENCES feature_flags (id)
        );
        """,
        """
        CREATE TABLE experiment_assignments (
            id INTEGER PRIMARY KEY,
            experiment_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            variant TEXT NOT NULL,
            FOREIGN KEY (experiment_id) REFERENCES experiments (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE audit_events (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            actor_user_id INTEGER NOT NULL,
            action TEXT NOT NULL,
            subject_table TEXT,
            subject_pk INTEGER,
            extra_context TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (actor_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE support_playbooks (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            author_user_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            body TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (author_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE compliance_cases (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            control_id INTEGER NOT NULL,
            owner_user_id INTEGER NOT NULL,
            workflow_state_id INTEGER NOT NULL,
            summary TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (control_id) REFERENCES compliance_controls (id),
            FOREIGN KEY (owner_user_id) REFERENCES users (id),
            FOREIGN KEY (workflow_state_id) REFERENCES workflow_states (id)
        );
        """,
        """
        CREATE TABLE case_events (
            id INTEGER PRIMARY KEY,
            case_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            note TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (case_id) REFERENCES compliance_cases (id),
            FOREIGN KEY (user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE ml_training_examples (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            label TEXT NOT NULL,
            feature_payload TEXT NOT NULL,
            hidden_refs_json TEXT,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        );
        """,
        """
        CREATE TABLE import_batches (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            created_by_user_id INTEGER NOT NULL,
            source_system TEXT NOT NULL,
            created_at TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id),
            FOREIGN KEY (created_by_user_id) REFERENCES users (id)
        );
        """,
        """
        CREATE TABLE import_rows (
            id INTEGER PRIMARY KEY,
            batch_id INTEGER NOT NULL,
            source_table TEXT NOT NULL,
            source_pk INTEGER NOT NULL,
            resolved_entity_id INTEGER,
            payload TEXT NOT NULL,
            FOREIGN KEY (batch_id) REFERENCES import_batches (id)
        );
        """,
        """
        CREATE TABLE entity_aliases (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            entity_type TEXT NOT NULL,
            entity_id INTEGER NOT NULL,
            alias_key TEXT NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        );
        """,
        """
        CREATE TABLE relationship_hints (
            id INTEGER PRIMARY KEY,
            tenant_id INTEGER NOT NULL,
            owner_table TEXT NOT NULL,
            owner_id INTEGER NOT NULL,
            target_table TEXT NOT NULL,
            target_id INTEGER NOT NULL,
            confidence REAL NOT NULL,
            FOREIGN KEY (tenant_id) REFERENCES tenants (id)
        );
        """,
    ]

    cur.executescript("\n".join(statements))

    for index in range(extra_tables):
        table_name = f"challenge_satellite_{index + 1:02d}"
        cur.execute(
            f"""
            CREATE TABLE {table_name} (
                id INTEGER PRIMARY KEY,
                tenant_id INTEGER NOT NULL,
                anchor_user_id INTEGER NOT NULL,
                hidden_order_id INTEGER,
                payload TEXT NOT NULL,
                FOREIGN KEY (tenant_id) REFERENCES tenants (id),
                FOREIGN KEY (anchor_user_id) REFERENCES users (id)
            );
            """
        )


def _insert_static_dimensions(cur: sqlite3.Cursor) -> Dict[str, List[int]]:
    dimensions: Dict[str, List[int]] = {}

    countries = [
        ("US", "United States"),
        ("DE", "Germany"),
        ("JP", "Japan"),
        ("BR", "Brazil"),
        ("AU", "Australia"),
        ("GB", "United Kingdom"),
    ]
    dimensions["countries"] = [_insert(cur, "INSERT INTO countries (code, name) VALUES (?, ?)", row) for row in countries]

    regions = [
        (dimensions["countries"][0], "US-WEST", "US West"),
        (dimensions["countries"][0], "US-EAST", "US East"),
        (dimensions["countries"][1], "DE-CENTRAL", "DE Central"),
        (dimensions["countries"][2], "JP-EAST", "JP East"),
        (dimensions["countries"][3], "BR-SOUTH", "BR South"),
        (dimensions["countries"][5], "GB-LON", "London"),
    ]
    dimensions["regions"] = [_insert(cur, "INSERT INTO regions (country_id, code, name) VALUES (?, ?, ?)", row) for row in regions]

    currencies = [
        ("USD", "US Dollar", "$"),
        ("EUR", "Euro", "EUR"),
        ("JPY", "Japanese Yen", "JPY"),
        ("GBP", "Pound Sterling", "GBP"),
    ]
    dimensions["currencies"] = [_insert(cur, "INSERT INTO currencies (code, name, symbol) VALUES (?, ?, ?)", row) for row in currencies]

    tax_codes = [
        ("STD", 0.20, "Standard rate"),
        ("RED", 0.10, "Reduced rate"),
        ("ZERO", 0.0, "Zero rated"),
        ("DIGI", 0.17, "Digital goods"),
        ("SERV", 0.12, "Services"),
    ]
    dimensions["tax_codes"] = [_insert(cur, "INSERT INTO tax_codes (code, rate, description) VALUES (?, ?, ?)", row) for row in tax_codes]

    categories = [
        ("Analytics", "medium"),
        ("Security", "high"),
        ("Infrastructure", "high"),
        ("Productivity", "low"),
        ("Finance", "high"),
        ("Support", "medium"),
        ("Sales", "medium"),
        ("Compliance", "high"),
        ("Logistics", "medium"),
        ("Education", "low"),
    ]
    dimensions["categories"] = [_insert(cur, "INSERT INTO categories (name, risk_tier) VALUES (?, ?)", row) for row in categories]

    tags = [
        "beta", "legacy", "critical", "ml", "priority", "bulk", "b2b", "vip",
        "regional", "migration", "sandbox", "finance", "security", "seasonal",
        "partner", "growth", "workflow", "iot", "enterprise", "renewal",
    ]
    dimensions["tags"] = [_insert(cur, "INSERT INTO tags (name) VALUES (?)", (name,)) for name in tags]

    roles = ["admin", "analyst", "operator", "support", "manager", "finance"]
    dimensions["roles"] = [_insert(cur, "INSERT INTO roles (name) VALUES (?)", (name,)) for name in roles]

    payment_terms = [("Net 15", 15), ("Net 30", 30), ("Net 45", 45), ("Due on receipt", 0)]
    dimensions["payment_terms"] = [_insert(cur, "INSERT INTO payment_terms (name, days_due) VALUES (?, ?)", row) for row in payment_terms]

    shipping_methods = [
        ("Ground", "DHL"),
        ("Express", "FedEx"),
        ("Freight", "Maersk"),
        ("Locker", "UPS"),
    ]
    dimensions["shipping_methods"] = [_insert(cur, "INSERT INTO shipping_methods (name, carrier) VALUES (?, ?)", row) for row in shipping_methods]

    ticket_types = [("bug", 5), ("access", 2), ("incident", 4), ("feature", 1)]
    dimensions["ticket_types"] = [_insert(cur, "INSERT INTO ticket_types (name, severity_bias) VALUES (?, ?)", row) for row in ticket_types]

    workflow_states = [
        ("draft", "planning"),
        ("active", "open"),
        ("blocked", "open"),
        ("resolved", "closed"),
        ("archived", "closed"),
    ]
    dimensions["workflow_states"] = [_insert(cur, "INSERT INTO workflow_states (name, category) VALUES (?, ?)", row) for row in workflow_states]

    feature_flags = [
        ("smart-routing", "Route work using traffic and backlog signals"),
        ("shadow-pricing", "Compute alternate prices for diagnostics"),
        ("auto-escalation", "Escalate aging support tickets"),
        ("risk-banner", "Surface compliance warning banners"),
        ("bulk-import-v2", "Second generation bulk import path"),
        ("warehouse-optimizer", "Warehouse balancing logic"),
    ]
    dimensions["feature_flags"] = [_insert(cur, "INSERT INTO feature_flags (key, description) VALUES (?, ?)", row) for row in feature_flags]

    controls = [
        ("SOC2-CC6", "Access governance"),
        ("SOC2-CC7", "Change management"),
        ("ISO27001-A8", "Asset inventory"),
        ("GDPR-ART30", "Processing records"),
        ("PCI-DSS-10", "Audit trails"),
    ]
    dimensions["controls"] = [_insert(cur, "INSERT INTO compliance_controls (code, description) VALUES (?, ?)", row) for row in controls]

    policies = [("Hot support", 30), ("Default workspace", 90), ("Analytics export", 180), ("Legal hold", 365)]
    dimensions["retention_policies"] = [_insert(cur, "INSERT INTO retention_policies (name, keep_days) VALUES (?, ?)", row) for row in policies]

    return dimensions


def _populate_tenants(
    cur: sqlite3.Cursor,
    fake: Faker,
    rng: random.Random,
    config: SampleDumpConfig,
    counts: Dict[str, int],
    dimensions: Dict[str, List[int]],
    extra_tables: int,
) -> Dict[int, Dict[str, List[int]]]:
    hidden_ratio = HIDDEN_REFERENCE_RATIOS[config.hidden_reference_mode]
    tenant_state: Dict[int, Dict[str, List[int]]] = defaultdict(
        lambda: {
            "departments": [],
            "teams": [],
            "cost_centers": [],
            "users": [],
            "addresses": [],
            "suppliers": [],
            "warehouses": [],
            "products": [],
            "projects": [],
            "labels": [],
            "tasks": [],
            "documents": [],
            "tickets": [],
            "articles": [],
            "carts": [],
            "orders": [],
            "order_items": [],
            "shipments": [],
            "returns": [],
            "invoices": [],
            "subscriptions": [],
            "campaigns": [],
            "notifications": [],
            "webhooks": [],
            "dashboards": [],
            "reports": [],
            "exports": [],
            "experiments": [],
            "cases": [],
            "incidents": [],
        }
    )

    cur.executemany(
        "INSERT INTO scenario_metadata (key, value) VALUES (?, ?)",
        [
            ("scenario", config.scenario),
            ("scale", config.scale),
            ("hidden_reference_mode", config.hidden_reference_mode),
            ("min_tables", str(config.min_tables)),
            ("seed", str(config.seed)),
            ("extra_noise_tables", str(extra_tables)),
        ],
    )

    tenant_ids: List[int] = []
    for index in range(counts["tenants"]):
        tenant_id = _insert(
            cur,
            "INSERT INTO tenants (name, slug, region_id, default_currency_id, created_at) VALUES (?, ?, ?, ?, ?)",
            (
                f"{fake.company()} Tenant {index + 1}",
                f"tenant_{index + 1}",
                rng.choice(dimensions["regions"]),
                rng.choice(dimensions["currencies"]),
                _rand_ts(fake),
            ),
        )
        tenant_ids.append(tenant_id)

    for tenant_index, tenant_id in enumerate(tenant_ids):
        state = tenant_state[tenant_id]

        for dep_index in range(counts["departments_per_tenant"]):
            state["departments"].append(
                _insert(
                    cur,
                    "INSERT INTO departments (tenant_id, name) VALUES (?, ?)",
                    (tenant_id, f"{rng.choice(['Platform', 'Ops', 'Sales', 'Finance', 'Support'])} {dep_index + 1}"),
                )
            )

        for team_index in range(counts["teams_per_tenant"]):
            parent_id = state["teams"][-1] if state["teams"] and team_index % 2 == 1 else None
            state["teams"].append(
                _insert(
                    cur,
                    "INSERT INTO teams (tenant_id, department_id, name, parent_team_id) VALUES (?, ?, ?, ?)",
                    (
                        tenant_id,
                        rng.choice(state["departments"]),
                        f"{rng.choice(['Enablement', 'Growth', 'Core', 'SRE', 'Field'])} Team {team_index + 1}",
                        parent_id,
                    ),
                )
            )

        for cc_index in range(counts["cost_centers_per_tenant"]):
            state["cost_centers"].append(
                _insert(
                    cur,
                    "INSERT INTO cost_centers (tenant_id, code, label) VALUES (?, ?, ?)",
                    (tenant_id, f"CC-{tenant_index + 1}-{cc_index + 1}", fake.bs().title()),
                )
            )

        for user_index in range(counts["users_per_tenant"]):
            username = f"user_{tenant_id}_{user_index + 1}"
            state["users"].append(
                _insert(
                    cur,
                    """
                    INSERT INTO users (
                        tenant_id, department_id, team_id, cost_center_id, manager_user_id,
                        username, email, full_name, title, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tenant_id,
                        rng.choice(state["departments"]),
                        rng.choice(state["teams"]),
                        rng.choice(state["cost_centers"]),
                        None,
                        username,
                        f"{username}@example.com",
                        fake.name(),
                        rng.choice(["Analyst", "Engineer", "Manager", "Specialist", "Coordinator"]),
                        _rand_ts(fake),
                    ),
                )
            )

        for position, user_id in enumerate(state["users"]):
            if position == 0 or not _chance(rng, 0.65):
                continue
            manager_id = rng.choice(state["users"][:position])
            cur.execute("UPDATE users SET manager_user_id = ? WHERE id = ?", (manager_id, user_id))

        for user_id in state["users"]:
            assigned_roles = _pick_some(rng, dimensions["roles"], 1, 2)
            for role_id in assigned_roles:
                _insert(
                    cur,
                    "INSERT INTO user_roles (tenant_id, user_id, role_id, granted_at) VALUES (?, ?, ?, ?)",
                    (tenant_id, user_id, role_id, _rand_ts(fake)),
                )

            _insert(
                cur,
                "INSERT INTO user_preferences (tenant_id, user_id, preference_json) VALUES (?, ?, ?)",
                (
                    tenant_id,
                    user_id,
                    json.dumps(
                        {
                            "timezone": rng.choice(["UTC", "America/New_York", "Europe/Berlin"]),
                            "theme": rng.choice(["classic", "contrast", "compact"]),
                            "digest": rng.choice(["daily", "weekly", "never"]),
                        }
                    ),
                ),
            )

            for _ in range(rng.randint(1, 2 if config.scale == "tiny" else 3)):
                _insert(
                    cur,
                    "INSERT INTO user_sessions (tenant_id, user_id, device, ip_address, started_at) VALUES (?, ?, ?, ?, ?)",
                    (tenant_id, user_id, rng.choice(["web", "mobile", "api"]), fake.ipv4_public(), _rand_ts(fake)),
                )

            if _chance(rng, 0.35):
                _insert(
                    cur,
                    "INSERT INTO api_keys (tenant_id, user_id, key_name, last_used_at) VALUES (?, ?, ?, ?)",
                    (tenant_id, user_id, f"key-{user_id}", _rand_ts(fake)),
                )

            for _ in range(rng.randint(1, 2)):
                _insert(
                    cur,
                    "INSERT INTO auth_audit_logs (tenant_id, user_id, action, actor_ip, created_at) VALUES (?, ?, ?, ?, ?)",
                    (tenant_id, user_id, rng.choice(["login", "logout", "mfa_challenge", "password_reset"]), fake.ipv4_public(), _rand_ts(fake)),
                )

        state["addresses"].append(
            _insert(
                cur,
                """
                INSERT INTO addresses (
                    tenant_id, user_id, country_id, region_id, line_1, city, postal_code, address_kind
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    None,
                    rng.choice(dimensions["countries"]),
                    rng.choice(dimensions["regions"]),
                    fake.street_address(),
                    fake.city(),
                    fake.postcode(),
                    "hq",
                ),
            )
        )

        for user_id in state["users"]:
            if not _chance(rng, 0.6):
                continue
            state["addresses"].append(
                _insert(
                    cur,
                    """
                    INSERT INTO addresses (
                        tenant_id, user_id, country_id, region_id, line_1, city, postal_code, address_kind
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        tenant_id,
                        user_id,
                        rng.choice(dimensions["countries"]),
                        rng.choice(dimensions["regions"]),
                        fake.street_address(),
                        fake.city(),
                        fake.postcode(),
                        rng.choice(["billing", "shipping", "remote"]),
                    ),
                )
            )

        for supplier_index in range(counts["suppliers_per_tenant"]):
            supplier_id = _insert(
                cur,
                "INSERT INTO suppliers (tenant_id, address_id, payment_term_id, name, tier) VALUES (?, ?, ?, ?, ?)",
                (
                    tenant_id,
                    rng.choice(state["addresses"]),
                    rng.choice(dimensions["payment_terms"]),
                    f"{fake.company()} Supplier {supplier_index + 1}",
                    rng.choice(["gold", "silver", "bronze"]),
                ),
            )
            state["suppliers"].append(supplier_id)
            for _ in range(rng.randint(1, 2)):
                _insert(
                    cur,
                    "INSERT INTO supplier_contacts (supplier_id, user_id, role_name) VALUES (?, ?, ?)",
                    (supplier_id, rng.choice(state["users"]), rng.choice(["owner", "ops", "finance"])),
                )

        for warehouse_index in range(counts["warehouses_per_tenant"]):
            warehouse_id = _insert(
                cur,
                "INSERT INTO warehouses (tenant_id, address_id, name) VALUES (?, ?, ?)",
                (tenant_id, rng.choice(state["addresses"]), f"Warehouse {tenant_index + 1}-{warehouse_index + 1}"),
            )
            state["warehouses"].append(warehouse_id)
            for bin_index in range(3):
                _insert(
                    cur,
                    "INSERT INTO inventory_bins (warehouse_id, label, capacity) VALUES (?, ?, ?)",
                    (warehouse_id, f"BIN-{warehouse_index + 1}-{bin_index + 1}", rng.randint(25, 150)),
                )

        for product_index in range(counts["products_per_tenant"]):
            product_id = _insert(
                cur,
                """
                INSERT INTO products (
                    tenant_id, category_id, supplier_id, tax_code_id, sku, name,
                    list_price, cost_price, lifecycle_state, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    rng.choice(dimensions["categories"]),
                    rng.choice(state["suppliers"]),
                    rng.choice(dimensions["tax_codes"]),
                    f"SKU-{tenant_id}-{product_index + 1}",
                    f"{fake.catch_phrase()} Package",
                    round(rng.uniform(35.0, 1500.0), 2),
                    round(rng.uniform(12.0, 900.0), 2),
                    rng.choice(["draft", "active", "retired"]),
                    _rand_ts(fake),
                ),
            )
            state["products"].append(product_id)
            for tag_id in _pick_some(rng, dimensions["tags"], 1, 3):
                _insert(cur, "INSERT INTO product_tags (product_id, tag_id) VALUES (?, ?)", (product_id, tag_id))

        standard_price_book = _insert(
            cur,
            "INSERT INTO price_books (tenant_id, currency_id, name) VALUES (?, ?, ?)",
            (tenant_id, rng.choice(dimensions["currencies"]), "Standard"),
        )
        promo_price_book = _insert(
            cur,
            "INSERT INTO price_books (tenant_id, currency_id, name) VALUES (?, ?, ?)",
            (tenant_id, rng.choice(dimensions["currencies"]), "Promo"),
        )

        for price_book_id in [standard_price_book, promo_price_book]:
            for product_id in state["products"]:
                if price_book_id == promo_price_book and not _chance(rng, 0.55):
                    continue
                price = cur.execute("SELECT list_price FROM products WHERE id = ?", (product_id,)).fetchone()[0]
                modifier = 1.0 if price_book_id == standard_price_book else rng.uniform(0.7, 0.95)
                _insert(
                    cur,
                    "INSERT INTO price_book_entries (price_book_id, product_id, price) VALUES (?, ?, ?)",
                    (price_book_id, product_id, round(price * modifier, 2)),
                )

        bundle_count = max(1, len(state["products"]) // 10)
        for bundle_index in range(bundle_count):
            anchor_product = rng.choice(state["products"])
            bundle_id = _insert(
                cur,
                "INSERT INTO product_bundles (tenant_id, product_id, bundle_name) VALUES (?, ?, ?)",
                (tenant_id, anchor_product, f"Bundle {bundle_index + 1}"),
            )
            for child_id in _pick_some(rng, state["products"], 2, min(4, len(state["products"]))):
                if child_id == anchor_product:
                    continue
                _insert(
                    cur,
                    "INSERT INTO bundle_items (bundle_id, child_product_id, quantity) VALUES (?, ?, ?)",
                    (bundle_id, child_id, rng.randint(1, 3)),
                )

        for project_index in range(counts["projects_per_tenant"]):
            project_id = _insert(
                cur,
                "INSERT INTO projects (tenant_id, owner_user_id, workflow_state_id, name, start_date, budget) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    tenant_id,
                    rng.choice(state["users"]),
                    rng.choice(dimensions["workflow_states"]),
                    f"{fake.bs().title()} Program {project_index + 1}",
                    fake.date_between(start_date="-180d", end_date="+45d").isoformat(),
                    round(rng.uniform(20_000.0, 250_000.0), 2),
                ),
            )
            state["projects"].append(project_id)
            for member_user_id in _pick_some(rng, state["users"], 3, min(6, len(state["users"]))):
                _insert(
                    cur,
                    "INSERT INTO project_members (project_id, user_id, team_id, joined_at) VALUES (?, ?, ?, ?)",
                    (project_id, member_user_id, rng.choice(state["teams"]), _rand_ts(fake)),
                )

        for label_index in range(counts["labels_per_tenant"]):
            state["labels"].append(
                _insert(cur, "INSERT INTO task_labels (tenant_id, name) VALUES (?, ?)", (tenant_id, f"label_{tenant_id}_{label_index + 1}"))
            )

        for project_id in state["projects"]:
            project_tasks: List[int] = []
            for task_index in range(rng.randint(4, 7)):
                parent_task_id = project_tasks[-1] if project_tasks and _chance(rng, 0.25) else None
                task_id = _insert(
                    cur,
                    """
                    INSERT INTO tasks (
                        project_id, assignee_user_id, workflow_state_id, parent_task_id,
                        shadow_order_id, title, priority, due_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        project_id,
                        rng.choice(state["users"]),
                        rng.choice(dimensions["workflow_states"]),
                        parent_task_id,
                        None,
                        f"Task {task_index + 1} for project {project_id}",
                        rng.choice(["low", "medium", "high", "urgent"]),
                        fake.date_between(start_date="-15d", end_date="+60d").isoformat(),
                    ),
                )
                project_tasks.append(task_id)
                state["tasks"].append(task_id)

            for task_id in project_tasks[1:]:
                if _chance(rng, 0.55):
                    _insert(
                        cur,
                        "INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (?, ?)",
                        (task_id, rng.choice(project_tasks[:-1])),
                    )

            for task_id in project_tasks:
                for _ in range(rng.randint(1, 2)):
                    _insert(
                        cur,
                        "INSERT INTO task_comments (task_id, user_id, body, created_at) VALUES (?, ?, ?, ?)",
                        (task_id, rng.choice(state["users"]), fake.sentence(nb_words=10), _rand_ts(fake)),
                    )
                for label_id in _pick_some(rng, state["labels"], 1, min(2, len(state["labels"]))):
                    _insert(cur, "INSERT INTO task_label_links (task_id, label_id) VALUES (?, ?)", (task_id, label_id))

            document_ids: List[int] = []
            for doc_index in range(rng.randint(2, 3)):
                document_id = _insert(
                    cur,
                    "INSERT INTO documents (tenant_id, project_id, owner_user_id, title) VALUES (?, ?, ?, ?)",
                    (tenant_id, project_id, rng.choice(state["users"]), f"Document {doc_index + 1} for project {project_id}"),
                )
                document_ids.append(document_id)
                state["documents"].append(document_id)
                for version_no in range(1, rng.randint(2, 3) + 1):
                    _insert(
                        cur,
                        "INSERT INTO document_versions (document_id, author_user_id, version_no, body, created_at) VALUES (?, ?, ?, ?, ?)",
                        (document_id, rng.choice(state["users"]), version_no, fake.paragraph(nb_sentences=3), _rand_ts(fake)),
                    )

            for source_document_id in document_ids[:-1]:
                _insert(
                    cur,
                    "INSERT INTO document_links (source_document_id, target_document_id, relation_type) VALUES (?, ?, ?)",
                    (source_document_id, rng.choice(document_ids), rng.choice(["duplicates", "depends_on", "supersedes"])),
                )

        for incident_index in range(counts["incidents_per_tenant"]):
            incident_id = _insert(
                cur,
                "INSERT INTO incidents (tenant_id, project_id, owner_user_id, workflow_state_id, title, severity) VALUES (?, ?, ?, ?, ?, ?)",
                (
                    tenant_id,
                    rng.choice(state["projects"]),
                    rng.choice(state["users"]),
                    rng.choice(dimensions["workflow_states"]),
                    f"Incident {tenant_id}-{incident_index + 1}",
                    rng.choice(["sev1", "sev2", "sev3"]),
                ),
            )
            state["incidents"].append(incident_id)
            for _ in range(rng.randint(1, 3)):
                _insert(
                    cur,
                    "INSERT INTO incident_updates (incident_id, user_id, body, created_at) VALUES (?, ?, ?, ?)",
                    (incident_id, rng.choice(state["users"]), fake.sentence(nb_words=12), _rand_ts(fake)),
                )

        for ticket_index in range(counts["tickets_per_tenant"]):
            ticket_id = _insert(
                cur,
                """
                INSERT INTO tickets (
                    tenant_id, ticket_type_id, requester_user_id, assignee_user_id,
                    project_id, workflow_state_id, shadow_order_id, subject
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    rng.choice(dimensions["ticket_types"]),
                    rng.choice(state["users"]),
                    rng.choice(state["users"]),
                    rng.choice(state["projects"]) if state["projects"] and _chance(rng, 0.8) else None,
                    rng.choice(dimensions["workflow_states"]),
                    None,
                    f"Ticket {tenant_id}-{ticket_index + 1}: {fake.bs()}",
                ),
            )
            state["tickets"].append(ticket_id)
            for _ in range(rng.randint(1, 2)):
                _insert(
                    cur,
                    "INSERT INTO ticket_comments (ticket_id, user_id, body, created_at) VALUES (?, ?, ?, ?)",
                    (ticket_id, rng.choice(state["users"]), fake.sentence(nb_words=12), _rand_ts(fake)),
                )

        for article_index in range(rng.randint(2, 4)):
            article_id = _insert(
                cur,
                """
                INSERT INTO knowledge_articles (
                    tenant_id, author_user_id, title, body, retention_policy_id
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    rng.choice(state["users"]),
                    f"Playbook Article {article_index + 1}",
                    fake.paragraph(nb_sentences=5),
                    rng.choice(dimensions["retention_policies"]),
                ),
            )
            state["articles"].append(article_id)
            for user_id in _pick_some(rng, state["users"], 1, min(3, len(state["users"]))):
                _insert(
                    cur,
                    "INSERT INTO article_feedback (article_id, user_id, score) VALUES (?, ?, ?)",
                    (article_id, user_id, rng.randint(1, 5)),
                )

        for user_id in state["users"]:
            if not _chance(rng, 0.45):
                continue
            cart_id = _insert(
                cur,
                "INSERT INTO carts (tenant_id, user_id, created_at) VALUES (?, ?, ?)",
                (tenant_id, user_id, _rand_ts(fake)),
            )
            state["carts"].append(cart_id)
            for product_id in _pick_some(rng, state["products"], 1, min(3, len(state["products"]))):
                _insert(cur, "INSERT INTO cart_items (cart_id, product_id, quantity) VALUES (?, ?, ?)", (cart_id, product_id, rng.randint(1, 4)))

        required_products = state["products"][:]
        rng.shuffle(required_products)
        order_count = max(counts["orders_per_tenant"], (len(state["products"]) + 2) // 3)
        for order_index in range(order_count):
            order_id = _insert(
                cur,
                """
                INSERT INTO orders (
                    tenant_id, user_id, billing_address_id, shipping_address_id,
                    payment_term_id, shipping_method_id, currency_id, shadow_project_id,
                    order_date, status, total_amount
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    rng.choice(state["users"]),
                    rng.choice(state["addresses"]),
                    rng.choice(state["addresses"]),
                    rng.choice(dimensions["payment_terms"]),
                    rng.choice(dimensions["shipping_methods"]),
                    rng.choice(dimensions["currencies"]),
                    rng.choice(state["projects"]) if state["projects"] and _chance(rng, hidden_ratio) else None,
                    _rand_ts(fake),
                    rng.choice(["pending", "processing", "shipped", "delivered", "cancelled"]),
                    0.0,
                ),
            )
            state["orders"].append(order_id)

            selected_products: List[int] = []
            if required_products:
                selected_products.append(required_products.pop())
            while len(selected_products) < rng.randint(1, 3):
                selected_products.append(rng.choice(state["products"]))

            total_amount = 0.0
            for product_id in selected_products:
                list_price = float(cur.execute("SELECT list_price FROM products WHERE id = ?", (product_id,)).fetchone()[0])
                quantity = rng.randint(1, 4)
                warehouse_id = rng.choice(state["warehouses"])
                order_item_id = _insert(
                    cur,
                    """
                    INSERT INTO order_items (
                        order_id, product_id, warehouse_id, quantity, unit_price
                    ) VALUES (?, ?, ?, ?, ?)
                    """,
                    (order_id, product_id, warehouse_id, quantity, list_price),
                )
                state["order_items"].append(order_item_id)
                total_amount += list_price * quantity

            cur.execute("UPDATE orders SET total_amount = ? WHERE id = ?", (round(total_amount, 2), order_id))

        if hidden_ratio > 0 and state["orders"]:
            for task_id in state["tasks"]:
                if _chance(rng, hidden_ratio):
                    cur.execute("UPDATE tasks SET shadow_order_id = ? WHERE id = ?", (rng.choice(state["orders"]), task_id))
            for ticket_id in state["tickets"]:
                if _chance(rng, hidden_ratio):
                    cur.execute("UPDATE tickets SET shadow_order_id = ? WHERE id = ?", (rng.choice(state["orders"]), ticket_id))

        order_to_items: Dict[int, List[int]] = defaultdict(list)
        for order_item_id, order_id in cur.execute("SELECT id, order_id FROM order_items WHERE order_id IN ({})".format(",".join("?" * len(state["orders"]))), tuple(state["orders"])).fetchall():
            order_to_items[order_id].append(order_item_id)

        payment_by_order: Dict[int, int] = {}
        for order_id in state["orders"]:
            if _chance(rng, 0.8):
                shipment_id = _insert(
                    cur,
                    "INSERT INTO shipments (order_id, warehouse_id, workflow_state_id, tracking_code, shipped_at) VALUES (?, ?, ?, ?, ?)",
                    (
                        order_id,
                        rng.choice(state["warehouses"]),
                        rng.choice(dimensions["workflow_states"]),
                        fake.bothify(text="TRK-########"),
                        _rand_ts(fake),
                    ),
                )
                state["shipments"].append(shipment_id)
                for order_item_id in order_to_items.get(order_id, []):
                    _insert(cur, "INSERT INTO shipment_items (shipment_id, order_item_id) VALUES (?, ?)", (shipment_id, order_item_id))

            if order_to_items.get(order_id) and _chance(rng, 0.2):
                return_id = _insert(
                    cur,
                    "INSERT INTO returns (order_id, user_id, workflow_state_id, reason) VALUES (?, ?, ?, ?)",
                    (order_id, rng.choice(state["users"]), rng.choice(dimensions["workflow_states"]), rng.choice(["damaged", "wrong item", "late arrival"])),
                )
                state["returns"].append(return_id)
                candidate_item = rng.choice(order_to_items[order_id])
                quantity = int(cur.execute("SELECT quantity FROM order_items WHERE id = ?", (candidate_item,)).fetchone()[0])
                _insert(cur, "INSERT INTO return_items (return_id, order_item_id, quantity) VALUES (?, ?, ?)", (return_id, candidate_item, max(1, quantity // 2)))

            total_amount = float(cur.execute("SELECT total_amount FROM orders WHERE id = ?", (order_id,)).fetchone()[0])
            billing_address_id = int(cur.execute("SELECT billing_address_id FROM orders WHERE id = ?", (order_id,)).fetchone()[0])
            currency_id = int(cur.execute("SELECT currency_id FROM orders WHERE id = ?", (order_id,)).fetchone()[0])
            invoice_id = _insert(
                cur,
                "INSERT INTO invoices (tenant_id, order_id, billing_address_id, currency_id, issued_at, total_amount) VALUES (?, ?, ?, ?, ?, ?)",
                (tenant_id, order_id, billing_address_id, currency_id, _rand_ts(fake), total_amount),
            )
            state["invoices"].append(invoice_id)
            for order_item_id in order_to_items.get(order_id, []):
                quantity, unit_price = cur.execute("SELECT quantity, unit_price FROM order_items WHERE id = ?", (order_item_id,)).fetchone()
                _insert(
                    cur,
                    "INSERT INTO invoice_items (invoice_id, order_item_id, line_total) VALUES (?, ?, ?)",
                    (invoice_id, order_item_id, round(float(quantity) * float(unit_price), 2)),
                )

            if _chance(rng, 0.92):
                payer_user_id = int(cur.execute("SELECT user_id FROM orders WHERE id = ?", (order_id,)).fetchone()[0])
                payment_id = _insert(
                    cur,
                    "INSERT INTO payments (invoice_id, user_id, currency_id, amount, paid_at) VALUES (?, ?, ?, ?, ?)",
                    (invoice_id, payer_user_id, currency_id, total_amount, _rand_ts(fake)),
                )
                payment_by_order[order_id] = payment_id

        if state["returns"]:
            for return_id, order_id in cur.execute("SELECT id, order_id FROM returns WHERE id IN ({})".format(",".join("?" * len(state["returns"]))), tuple(state["returns"])).fetchall():
                payment_id = payment_by_order.get(order_id)
                if payment_id is None:
                    continue
                _insert(
                    cur,
                    "INSERT INTO refunds (payment_id, return_id, amount, refunded_at) VALUES (?, ?, ?, ?)",
                    (payment_id, return_id, round(rng.uniform(10.0, 250.0), 2), _rand_ts(fake)),
                )

        for user_id in _pick_some(rng, state["users"], 1, max(1, len(state["users"]) // 3)):
            subscription_id = _insert(
                cur,
                """
                INSERT INTO subscriptions (
                    tenant_id, user_id, product_id, payment_term_id, workflow_state_id, started_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    user_id,
                    rng.choice(state["products"]),
                    rng.choice(dimensions["payment_terms"]),
                    rng.choice(dimensions["workflow_states"]),
                    _rand_ts(fake),
                ),
            )
            state["subscriptions"].append(subscription_id)
            for event_type in ["created", "renewed"]:
                _insert(
                    cur,
                    "INSERT INTO subscription_events (subscription_id, user_id, event_type, created_at) VALUES (?, ?, ?, ?)",
                    (subscription_id, user_id, event_type, _rand_ts(fake)),
                )

        campaign_count = 2 if counts["users_per_tenant"] < 20 else 3
        for campaign_index in range(campaign_count):
            campaign_id = _insert(
                cur,
                "INSERT INTO campaigns (tenant_id, owner_user_id, name, channel) VALUES (?, ?, ?, ?)",
                (tenant_id, rng.choice(state["users"]), f"Campaign {campaign_index + 1}", rng.choice(["email", "partner", "webinar"])),
            )
            state["campaigns"].append(campaign_id)
            for member_user_id in _pick_some(rng, state["users"], 2, min(5, len(state["users"]))):
                _insert(
                    cur,
                    "INSERT INTO campaign_members (campaign_id, user_id, enrolled_at) VALUES (?, ?, ?)",
                    (campaign_id, member_user_id, _rand_ts(fake)),
                )

        for user_id in _pick_some(rng, state["users"], 2, min(8, len(state["users"]))):
            context_entity_type = None
            context_entity_id = None
            if hidden_ratio > 0 and _chance(rng, hidden_ratio):
                context_entity_type = rng.choice(["orders", "tickets", "projects"])
                if context_entity_type == "orders" and state["orders"]:
                    context_entity_id = rng.choice(state["orders"])
                elif context_entity_type == "tickets" and state["tickets"]:
                    context_entity_id = rng.choice(state["tickets"])
                elif context_entity_type == "projects" and state["projects"]:
                    context_entity_id = rng.choice(state["projects"])

            notification_id = _insert(
                cur,
                """
                INSERT INTO notifications (
                    tenant_id, user_id, channel, context_entity_type, context_entity_id, payload, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    user_id,
                    rng.choice(["email", "sms", "in_app"]),
                    context_entity_type,
                    context_entity_id,
                    json.dumps({"headline": fake.sentence(nb_words=6), "priority": rng.choice(["low", "normal", "high"])}),
                    _rand_ts(fake),
                ),
            )
            state["notifications"].append(notification_id)
            _insert(
                cur,
                "INSERT INTO notification_deliveries (notification_id, status, attempted_at) VALUES (?, ?, ?)",
                (notification_id, rng.choice(["sent", "queued", "failed"]), _rand_ts(fake)),
            )

        for webhook_index in range(1 if config.scale == "tiny" else 2):
            webhook_id = _insert(
                cur,
                "INSERT INTO webhooks (tenant_id, user_id, name, endpoint) VALUES (?, ?, ?, ?)",
                (tenant_id, rng.choice(state["users"]), f"Webhook {webhook_index + 1}", fake.url()),
            )
            state["webhooks"].append(webhook_id)
            for _ in range(2):
                payload = {"event": rng.choice(["order.created", "ticket.updated", "export.completed"])}
                if hidden_ratio > 0 and state["orders"] and _chance(rng, hidden_ratio):
                    payload["shadow_order_id"] = rng.choice(state["orders"])
                _insert(
                    cur,
                    "INSERT INTO webhook_deliveries (webhook_id, status_code, request_body, sent_at) VALUES (?, ?, ?, ?)",
                    (webhook_id, rng.choice([200, 202, 500]), json.dumps(payload), _rand_ts(fake)),
                )

        for dashboard_index in range(1 if config.scale == "tiny" else 2):
            dashboard_id = _insert(
                cur,
                "INSERT INTO dashboards (tenant_id, owner_user_id, name) VALUES (?, ?, ?)",
                (tenant_id, rng.choice(state["users"]), f"Dashboard {dashboard_index + 1}"),
            )
            state["dashboards"].append(dashboard_id)
            for widget_index in range(rng.randint(2, 4)):
                source_table = None
                source_record_id = None
                if hidden_ratio > 0 and _chance(rng, hidden_ratio):
                    source_table = rng.choice(["orders", "tickets", "incidents", "projects"])
                    lookup = {
                        "orders": state["orders"],
                        "tickets": state["tickets"],
                        "incidents": state["incidents"],
                        "projects": state["projects"],
                    }.get(source_table, [])
                    if lookup:
                        source_record_id = rng.choice(lookup)
                _insert(
                    cur,
                    "INSERT INTO dashboard_widgets (dashboard_id, title, source_table, source_record_id) VALUES (?, ?, ?, ?)",
                    (dashboard_id, f"Widget {widget_index + 1}", source_table, source_record_id),
                )

        for report_index in range(2):
            filter_payload = {"date_window_days": rng.choice([7, 14, 30, 90])}
            if hidden_ratio > 0 and _chance(rng, hidden_ratio):
                filter_payload["shadow_refs"] = {
                    "user_id": rng.choice(state["users"]),
                    "project_id": rng.choice(state["projects"]) if state["projects"] else None,
                    "product_id": rng.choice(state["products"]),
                }
            report_id = _insert(
                cur,
                "INSERT INTO reports (tenant_id, owner_user_id, name, filter_payload) VALUES (?, ?, ?, ?)",
                (tenant_id, rng.choice(state["users"]), f"Report {report_index + 1}", json.dumps(filter_payload)),
            )
            state["reports"].append(report_id)
            for run_index in range(2):
                _insert(
                    cur,
                    "INSERT INTO report_runs (report_id, user_id, started_at, output_path) VALUES (?, ?, ?, ?)",
                    (report_id, rng.choice(state["users"]), _rand_ts(fake), f"/tmp/report_{report_id}_{run_index + 1}.csv"),
                )

        export_id = _insert(
            cur,
            "INSERT INTO data_exports (tenant_id, requested_by_user_id, export_kind) VALUES (?, ?, ?)",
            (tenant_id, rng.choice(state["users"]), rng.choice(["orders", "tickets", "audit"])),
        )
        state["exports"].append(export_id)
        for status in ["running", "completed"]:
            _insert(
                cur,
                "INSERT INTO export_runs (export_id, status, completed_at) VALUES (?, ?, ?)",
                (export_id, status, None if status == "running" else _rand_ts(fake)),
            )

        experiment_id = _insert(
            cur,
            "INSERT INTO experiments (tenant_id, owner_user_id, feature_flag_id, name) VALUES (?, ?, ?, ?)",
            (tenant_id, rng.choice(state["users"]), rng.choice(dimensions["feature_flags"]), f"Experiment {tenant_id}"),
        )
        state["experiments"].append(experiment_id)
        for user_id in _pick_some(rng, state["users"], 2, min(6, len(state["users"]))):
            _insert(
                cur,
                "INSERT INTO experiment_assignments (experiment_id, user_id, variant) VALUES (?, ?, ?)",
                (experiment_id, user_id, rng.choice(["control", "variant_a", "variant_b"])),
            )

        for _ in range(max(4, len(state["users"]) // 2)):
            subject_table = None
            subject_pk = None
            if hidden_ratio > 0 and _chance(rng, hidden_ratio):
                subject_table = rng.choice(["orders", "tickets", "products", "compliance_cases"])
                subject_lookup = {
                    "orders": state["orders"],
                    "tickets": state["tickets"],
                    "products": state["products"],
                    "compliance_cases": state["cases"],
                }.get(subject_table, [])
                if subject_lookup:
                    subject_pk = rng.choice(subject_lookup)
            _insert(
                cur,
                """
                INSERT INTO audit_events (
                    tenant_id, actor_user_id, action, subject_table, subject_pk, extra_context, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    rng.choice(state["users"]),
                    rng.choice(["create", "update", "approve", "archive", "escalate"]),
                    subject_table,
                    subject_pk,
                    json.dumps({"source": rng.choice(["ui", "api", "workflow"]), "trace": fake.uuid4()}),
                    _rand_ts(fake),
                ),
            )

        for playbook_index in range(2):
            _insert(
                cur,
                "INSERT INTO support_playbooks (tenant_id, author_user_id, name, body) VALUES (?, ?, ?, ?)",
                (tenant_id, rng.choice(state["users"]), f"Playbook {playbook_index + 1}", fake.paragraph(nb_sentences=4)),
            )

        for case_index in range(2):
            case_id = _insert(
                cur,
                """
                INSERT INTO compliance_cases (
                    tenant_id, control_id, owner_user_id, workflow_state_id, summary
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    tenant_id,
                    rng.choice(dimensions["controls"]),
                    rng.choice(state["users"]),
                    rng.choice(dimensions["workflow_states"]),
                    f"Compliance case {case_index + 1} for tenant {tenant_id}",
                ),
            )
            state["cases"].append(case_id)
            for _ in range(2):
                _insert(
                    cur,
                    "INSERT INTO case_events (case_id, user_id, note, created_at) VALUES (?, ?, ?, ?)",
                    (case_id, rng.choice(state["users"]), fake.sentence(nb_words=10), _rand_ts(fake)),
                )

        ml_rows = 2 if config.scale == "tiny" else 4
        for example_index in range(ml_rows):
            hidden_refs_json = None
            if hidden_ratio > 0 and _chance(rng, hidden_ratio):
                hidden_refs_json = json.dumps(
                    {
                        "ticket_ids": _pick_some(rng, state["tickets"], 1, min(2, len(state["tickets"]))),
                        "order_ids": _pick_some(rng, state["orders"], 1, min(2, len(state["orders"]))) if state["orders"] else [],
                        "project_ids": _pick_some(rng, state["projects"], 1, min(2, len(state["projects"]))) if state["projects"] else [],
                    }
                )
            _insert(
                cur,
                "INSERT INTO ml_training_examples (tenant_id, label, feature_payload, hidden_refs_json) VALUES (?, ?, ?, ?)",
                (
                    tenant_id,
                    rng.choice(["upsell", "fraud_review", "priority_support", "renewal_risk"]),
                    json.dumps({"feature_version": 2, "age_days": rng.randint(0, 365), "signal_count": rng.randint(1, 12)}),
                    hidden_refs_json,
                ),
            )

        batch_id = _insert(
            cur,
            "INSERT INTO import_batches (tenant_id, created_by_user_id, source_system, created_at) VALUES (?, ?, ?, ?)",
            (tenant_id, rng.choice(state["users"]), rng.choice(["erp", "crm", "legacy"]), _rand_ts(fake)),
        )
        import_source_tables = [("orders", state["orders"]), ("products", state["products"]), ("tickets", state["tickets"])]
        for source_table, source_ids in import_source_tables:
            if not source_ids:
                continue
            source_pk = rng.choice(source_ids)
            resolved_entity_id = source_pk if hidden_ratio > 0 and _chance(rng, hidden_ratio) else None
            _insert(
                cur,
                """
                INSERT INTO import_rows (
                    batch_id, source_table, source_pk, resolved_entity_id, payload
                ) VALUES (?, ?, ?, ?, ?)
                """,
                (
                    batch_id,
                    source_table,
                    source_pk,
                    resolved_entity_id,
                    json.dumps({"batch_hint": fake.uuid4(), "tenant_id": tenant_id}),
                ),
            )

        hidden_entities = [("users", state["users"]), ("orders", state["orders"]), ("products", state["products"])]
        for entity_type, entity_ids in hidden_entities:
            if not entity_ids:
                continue
            _insert(
                cur,
                "INSERT INTO entity_aliases (tenant_id, entity_type, entity_id, alias_key) VALUES (?, ?, ?, ?)",
                (tenant_id, entity_type, rng.choice(entity_ids), f"{entity_type}:{tenant_id}:{rng.randint(1000, 9999)}"),
            )

        if hidden_ratio > 0:
            hint_pairs = [
                ("tickets", state["tickets"], "orders", state["orders"]),
                ("projects", state["projects"], "users", state["users"]),
                ("products", state["products"], "tickets", state["tickets"]),
            ]
            for owner_table, owner_ids, target_table, target_ids in hint_pairs:
                if not owner_ids or not target_ids:
                    continue
                _insert(
                    cur,
                    "INSERT INTO relationship_hints (tenant_id, owner_table, owner_id, target_table, target_id, confidence) VALUES (?, ?, ?, ?, ?, ?)",
                    (tenant_id, owner_table, rng.choice(owner_ids), target_table, rng.choice(target_ids), round(rng.uniform(0.5, 0.99), 2)),
                )

        for extra_index in range(extra_tables):
            table_name = f"challenge_satellite_{extra_index + 1:02d}"
            cur.execute(
                f"INSERT INTO {table_name} (tenant_id, anchor_user_id, hidden_order_id, payload) VALUES (?, ?, ?, ?)",
                (
                    tenant_id,
                    rng.choice(state["users"]),
                    rng.choice(state["orders"]) if hidden_ratio > 0 and state["orders"] and _chance(rng, hidden_ratio) else None,
                    json.dumps({"satellite": table_name, "tenant": tenant_id, "scenario": config.scenario}),
                ),
            )

    return tenant_state


def generate_sample_dump(
    filename: str = "sample_dump.sql",
    db_filename: str = "sample_dump.db",
    config: Optional[SampleDumpConfig] = None,
) -> None:
    config = config or SampleDumpConfig()
    counts = _resolved_counts(config)
    extra_tables = max(config.extra_noise_tables, max(0, config.min_tables - BASE_TABLE_COUNT))

    if os.path.exists(db_filename):
        os.remove(db_filename)
    if config.write_sql_dump and os.path.exists(filename):
        os.remove(filename)

    Faker.seed(config.seed)
    fake = Faker()
    fake.seed_instance(config.seed)
    rng = random.Random(config.seed)

    conn = sqlite3.connect(db_filename)
    try:
        cur = conn.cursor()
        cur.execute("PRAGMA foreign_keys = ON;")

        print(f"Creating schema with {BASE_TABLE_COUNT + extra_tables} tables...")
        _create_schema(cur, extra_tables)

        print("Seeding dimensions and scenarios...")
        dimensions = _insert_static_dimensions(cur)
        _populate_tenants(cur, fake, rng, config, counts, dimensions, extra_tables)

        conn.commit()

        if config.write_sql_dump:
            print(f"Writing SQL dump to {filename}...")
            with open(filename, "w", encoding="utf-8") as handle:
                for line in conn.iterdump():
                    handle.write(f"{line}\n")

        print(f"SQLite database generated: {db_filename}")
        if config.write_sql_dump:
            print(f"SQL dump generated: {filename}")
    finally:
        conn.close()


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a complex SQLite fixture for subset-agent testing.")
    parser.add_argument("--filename", default="sample_dump.sql", help="Path to the SQL dump output.")
    parser.add_argument("--db-filename", default="sample_dump.db", help="Path to the SQLite database output.")
    parser.add_argument("--min-tables", type=int, default=84, help="Minimum number of tables to generate.")
    parser.add_argument("--scenario", choices=sorted(SCENARIO_MULTIPLIERS), default="enterprise", help="High-level workload shape.")
    parser.add_argument("--scale", choices=sorted(SCALE_PROFILES), default="medium", help="Entity volume profile.")
    parser.add_argument(
        "--hidden-reference-mode",
        choices=sorted(HIDDEN_REFERENCE_RATIOS),
        default="mixed",
        help="How aggressively to inject non-declared relationships.",
    )
    parser.add_argument("--seed", type=int, default=7, help="Deterministic seed for Faker and random.")
    parser.add_argument("--tenants", type=int, help="Override tenant count.")
    parser.add_argument("--users-per-tenant", type=int, help="Override user count per tenant.")
    parser.add_argument("--products-per-tenant", type=int, help="Override product count per tenant.")
    parser.add_argument("--orders-per-tenant", type=int, help="Override order count per tenant.")
    parser.add_argument("--tickets-per-tenant", type=int, help="Override ticket count per tenant.")
    parser.add_argument("--projects-per-tenant", type=int, help="Override project count per tenant.")
    parser.add_argument("--incidents-per-tenant", type=int, help="Override incident count per tenant.")
    parser.add_argument("--extra-noise-tables", type=int, default=0, help="Add extra challenge tables beyond the base schema.")
    parser.add_argument("--skip-sql-dump", action="store_true", help="Generate only the SQLite database.")
    return parser


def main() -> None:
    parser = _build_arg_parser()
    args = parser.parse_args()

    generate_sample_dump(
        filename=args.filename,
        db_filename=args.db_filename,
        config=SampleDumpConfig(
            min_tables=args.min_tables,
            scenario=args.scenario,
            scale=args.scale,
            hidden_reference_mode=args.hidden_reference_mode,
            seed=args.seed,
            tenants=args.tenants,
            users_per_tenant=args.users_per_tenant,
            products_per_tenant=args.products_per_tenant,
            orders_per_tenant=args.orders_per_tenant,
            tickets_per_tenant=args.tickets_per_tenant,
            projects_per_tenant=args.projects_per_tenant,
            incidents_per_tenant=args.incidents_per_tenant,
            extra_noise_tables=args.extra_noise_tables,
            write_sql_dump=not args.skip_sql_dump,
        ),
    )


if __name__ == "__main__":
    main()
