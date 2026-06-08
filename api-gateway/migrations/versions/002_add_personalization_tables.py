"""Add personalization tables: watchlist_items, user_preferences,
portfolio_positions, trade_orders, agent_conversations, agent_messages.

Revision ID: 002
Revises: 001
Create Date: 2026-06-08
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "002"
down_revision = "001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ── watchlist_items ────────────────────────────────────────────────
    op.create_table(
        "watchlist_items",
        sa.Column("id", UUID, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", UUID, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("notes", sa.Text, nullable=True),
        sa.Column("alert_on_signal", sa.Boolean, nullable=False, server_default="TRUE"),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )
    op.create_index("ix_watchlist_items_user_id", "watchlist_items", ["user_id"])
    op.create_unique_constraint(
        "uq_watchlist_user_symbol", "watchlist_items", ["user_id", "symbol"]
    )

    # ── user_preferences ──────────────────────────────────────────────
    op.create_table(
        "user_preferences",
        sa.Column("id", UUID, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "user_id", UUID, sa.ForeignKey("users.id", ondelete="CASCADE"),
            nullable=False, unique=True,
        ),
        sa.Column("risk_tolerance", sa.String(20), nullable=False, server_default="moderate"),
        sa.Column("investment_horizon", sa.String(20), nullable=False, server_default="medium_term"),
        sa.Column("preferred_sectors", JSONB, nullable=False, server_default="'[]'"),
        sa.Column("notification_channels", JSONB, nullable=False, server_default="'{}'"),
        sa.Column("digest_frequency", sa.String(20), nullable=False, server_default="daily"),
        sa.Column("auto_trading_enabled", sa.Boolean, nullable=False, server_default="FALSE"),
        sa.Column("broker_paper_trading", sa.Boolean, nullable=False, server_default="TRUE"),
        sa.Column("broker_api_key_encrypted", sa.Text, nullable=True),
        sa.Column("max_daily_loss_pct", sa.Float, nullable=False, server_default="0.02"),
        sa.Column("max_position_size_pct", sa.Float, nullable=False, server_default="0.10"),
        sa.Column("confirmation_threshold_usd", sa.Float, nullable=False, server_default="1000.0"),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )
    op.create_index("ix_user_preferences_user_id", "user_preferences", ["user_id"])

    # ── portfolio_positions ───────────────────────────────────────────
    op.create_table(
        "portfolio_positions",
        sa.Column("id", UUID, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", UUID, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("quantity", sa.Float, nullable=False),
        sa.Column("avg_cost_basis", sa.Float, nullable=False),
        sa.Column("is_open", sa.Boolean, nullable=False, server_default="TRUE"),
        sa.Column("source", sa.String(50), nullable=False, server_default="manual"),
        sa.Column(
            "opened_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("closed_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index("ix_portfolio_positions_user_id", "portfolio_positions", ["user_id"])
    op.create_index("ix_portfolio_positions_symbol", "portfolio_positions", ["symbol"])

    # ── trade_orders ──────────────────────────────────────────────────
    op.create_table(
        "trade_orders",
        sa.Column("id", UUID, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", UUID, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("symbol", sa.String(20), nullable=False),
        sa.Column("side", sa.String(10), nullable=False),
        sa.Column("qty", sa.Float, nullable=False),
        sa.Column("order_type", sa.String(20), nullable=False, server_default="market"),
        sa.Column("status", sa.String(30), nullable=False, server_default="pending"),
        sa.Column("broker_order_id", sa.String(255), nullable=True),
        sa.Column("agent_reasoning", sa.Text, nullable=True),
        sa.Column("risk_score", sa.Float, nullable=True),
        sa.Column("is_paper", sa.Boolean, nullable=False, server_default="TRUE"),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column("filled_at", sa.TIMESTAMP(timezone=True), nullable=True),
    )
    op.create_index("ix_trade_orders_user_id", "trade_orders", ["user_id"])
    op.create_index("ix_trade_orders_symbol", "trade_orders", ["symbol"])

    # ── agent_conversations ───────────────────────────────────────────
    op.create_table(
        "agent_conversations",
        sa.Column("id", UUID, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column("user_id", UUID, sa.ForeignKey("users.id", ondelete="CASCADE"), nullable=False),
        sa.Column("agent_type", sa.String(50), nullable=False, server_default="supervisor"),
        sa.Column("thread_id", sa.String(255), nullable=False, unique=True),
        sa.Column("title", sa.String(255), nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
        sa.Column(
            "updated_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )
    op.create_index("ix_agent_conversations_user_id", "agent_conversations", ["user_id"])

    # ── agent_messages ────────────────────────────────────────────────
    op.create_table(
        "agent_messages",
        sa.Column("id", UUID, primary_key=True, server_default=sa.text("gen_random_uuid()")),
        sa.Column(
            "conversation_id",
            UUID,
            sa.ForeignKey("agent_conversations.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("role", sa.String(20), nullable=False),
        sa.Column("content", sa.Text, nullable=False),
        sa.Column("tool_calls", JSONB, nullable=True),
        sa.Column(
            "created_at",
            sa.TIMESTAMP(timezone=True),
            nullable=False,
            server_default=sa.text("NOW()"),
        ),
    )
    op.create_index("ix_agent_messages_conversation_id", "agent_messages", ["conversation_id"])


def downgrade() -> None:
    op.drop_table("agent_messages")
    op.drop_table("agent_conversations")
    op.drop_table("trade_orders")
    op.drop_table("portfolio_positions")
    op.drop_table("user_preferences")
    op.drop_table("watchlist_items")
